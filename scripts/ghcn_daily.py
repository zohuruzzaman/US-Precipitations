#!/usr/bin/env python3
"""
NOAA Daily Weather Pipeline (GHCNd → Parquet)  — Multiprocessing Edition
=========================================================================
Downloads daily PRCP, TMAX, TMIN for all US stations via the NCEI endpoint.
Designed for speed (all CPU cores) AND memory safety (streams to temp files).

Architecture
────────────
  Phase 1 — Download + Parse  (process pool × thread pool per process)
    Stations are chunked across worker processes.  Each process downloads
    in mini-batches (default 150 stations).  After each mini-batch the rows
    are partitioned by year and flushed to year-specific temp Parquet files:
        tmp/{YEAR}/proc{P}_batch{B}.parquet
    Then the mini-batch is freed.  Peak RAM per process ≈ 150 stations' data.

  Phase 2 — Merge + Save  (process pool, one year per process)
    Each process reads only its year's temp files, merges station metadata,
    writes the final Parquet, then deletes the temp files for that year.
    Peak RAM per process ≈ one year of US daily data (~15-25 M rows).

  Cleanup — temp directory is removed at the end.

Usage:
    python ghcn_daily.py --mode full
    python ghcn_daily.py --mode full --start-year 2000 --end-year 2025
    python ghcn_daily.py --mode refresh
    python ghcn_daily.py --mode refresh --refresh-years 5
    python ghcn_daily.py --mode full --workers 16 --io-threads 12
    python ghcn_daily.py --mode full --batch-size 200
"""

import os
import sys
import glob
import time
import shutil
import argparse
import tempfile
import multiprocessing as mp
import requests
import pandas as pd
from io import StringIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

# ──────────────────────────── CONFIGURATION ────────────────────────────────
NCEI_BASE       = "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily"
STATION_URL     = f"{NCEI_BASE}/doc/ghcnd-stations.txt"
ACCESS_BASE     = f"{NCEI_BASE}/access"
OUTPUT_DIR      = "data/GHCNd/daily"
STATION_META    = "data/GHCNd/ghcnd_us_stations.parquet"
TARGET_VARS     = ["PRCP", "TMAX", "TMIN"]
RETRY_LIMIT     = 3
RETRY_DELAY     = 2
REQUEST_TIMEOUT = 120
# ───────────────────────────────────────────────────────────────────────────


# ─────────────────────── HELPERS ───────────────────────────────────────────

def fetch_with_retry(url, timeout=REQUEST_TIMEOUT):
    """GET with retries and exponential back-off."""
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            if attempt == RETRY_LIMIT:
                raise
            time.sleep(RETRY_DELAY * attempt)


def get_us_stations(force_refresh=False):
    """Download + parse station metadata; cache as Parquet."""
    if not force_refresh and os.path.exists(STATION_META):
        print(f"Loading cached station metadata from {STATION_META}")
        return pd.read_parquet(STATION_META)

    print(f"Fetching station metadata from {STATION_URL} ...")
    resp = fetch_with_retry(STATION_URL)
    colspecs = [(0, 11), (12, 20), (21, 30), (31, 37), (38, 40), (41, 71)]
    names    = ["STATION", "LAT", "LON", "ELEV", "STATE", "NAME"]
    df = pd.read_fwf(StringIO(resp.text), colspecs=colspecs, names=names)
    us = df[df["STATION"].str.startswith("US")].copy()
    us["NAME"] = us["NAME"].str.strip()
    print(f"  Found {len(us):,} US stations.")
    os.makedirs(os.path.dirname(STATION_META), exist_ok=True)
    us.to_parquet(STATION_META, index=False)
    return us[["STATION", "LAT", "LON", "STATE", "ELEV", "NAME"]]


# ─────────────────── PHASE 1: DOWNLOAD + PARSE ────────────────────────────

def _download_one(args):
    """Download and parse a single station CSV. Returns DataFrame or None."""
    station_id, start_year, end_year = args
    url = f"{ACCESS_BASE}/{station_id}.csv"
    try:
        resp = fetch_with_retry(url)
    except Exception:
        return None
    try:
        df = pd.read_csv(StringIO(resp.text), low_memory=False)
    except Exception:
        return None
    if df.empty or "DATE" not in df.columns:
        return None

    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df = df.dropna(subset=["DATE"])
    df = df[(df["DATE"].dt.year >= start_year) & (df["DATE"].dt.year <= end_year)]
    if df.empty:
        return None

    keep = ["STATION", "DATE"]
    for v in TARGET_VARS:
        if v in df.columns:
            keep.append(v)
    df = df[keep].copy()

    for v in TARGET_VARS:
        if v in df.columns:
            df[v] = pd.to_numeric(df[v], errors="coerce") / 10.0
    return df


def _flush_to_year_files(frames, tmp_dir, proc_id, batch_id):
    """
    Concatenate a mini-batch of station DataFrames, partition by year,
    and write one temp Parquet per year.  Returns count of rows flushed.
    """
    if not frames:
        return 0
    combined = pd.concat(frames, ignore_index=True)
    combined["_YEAR"] = combined["DATE"].dt.year
    total = 0
    for year, grp in combined.groupby("_YEAR"):
        year_dir = os.path.join(tmp_dir, str(int(year)))
        os.makedirs(year_dir, exist_ok=True)
        path = os.path.join(year_dir, f"p{proc_id}_b{batch_id}.parquet")
        grp.drop(columns=["_YEAR"]).to_parquet(path, index=False)
        total += len(grp)
    return total


def _process_chunk(chunk_args):
    """
    Runs inside a WORKER PROCESS.
    Downloads stations in mini-batches, flushing each batch to
    year-partitioned temp Parquet files.
    Returns (n_stations_ok, n_skipped, n_failed, n_rows).
    """
    station_ids, start_year, end_year, io_threads, batch_size, tmp_dir, proc_id = chunk_args

    ok = 0
    skipped = 0
    failed = 0
    total_rows = 0
    batch_id = 0

    # Process stations in mini-batches
    for batch_start in range(0, len(station_ids), batch_size):
        batch_sids = station_ids[batch_start : batch_start + batch_size]
        work = [(sid, start_year, end_year) for sid in batch_sids]
        frames = []

        with ThreadPoolExecutor(max_workers=io_threads) as tpool:
            futs = {tpool.submit(_download_one, w): w[0] for w in work}
            for fut in as_completed(futs):
                try:
                    df = fut.result()
                    if df is not None and not df.empty:
                        frames.append(df)
                        ok += 1
                    else:
                        skipped += 1
                except Exception:
                    failed += 1

        # Flush this mini-batch to disk and free memory
        rows = _flush_to_year_files(frames, tmp_dir, proc_id, batch_id)
        total_rows += rows
        del frames
        batch_id += 1

    return ok, skipped, failed, total_rows


def download_parallel(station_ids, start_year, end_year, workers, io_threads,
                      batch_size, tmp_dir):
    """
    Phase 1: split stations across worker processes; each downloads in
    mini-batches and flushes year-partitioned temp files to disk.
    """
    total = len(station_ids)
    chunk_size = max(1, total // workers)
    chunks = [station_ids[i:i + chunk_size] for i in range(0, total, chunk_size)]

    print(f"\n╔══ Phase 1: Download + Parse ══════════════════════════════════╗")
    print(f"║  {total:,} stations  │  {len(chunks)} processes  │  "
          f"{io_threads} I/O threads each")
    print(f"║  Mini-batch size: {batch_size} stations  (flush to disk per batch)")
    print(f"║  Date range: {start_year} → {end_year}")
    print(f"║  Temp dir: {tmp_dir}")
    print(f"╚══════════════════════════════════════════════════════════════╝\n")

    chunk_args = [
        (chunk, start_year, end_year, io_threads, batch_size, tmp_dir, i)
        for i, chunk in enumerate(chunks)
    ]

    grand_ok = 0
    grand_skip = 0
    grand_fail = 0
    grand_rows = 0

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_process_chunk, ca): i for i, ca in enumerate(chunk_args)}
        for fut in as_completed(futs):
            idx = futs[fut]
            ok, skip, fail, rows = fut.result()
            grand_ok   += ok
            grand_skip += skip
            grand_fail += fail
            grand_rows += rows
            print(f"  Process {idx:>2d}/{len(chunks)}  done  │  "
                  f"OK: {ok:,}  skip: {skip:,}  fail: {fail:,}  rows: {rows:,}")

    print(f"\n  Totals — stations OK: {grand_ok:,}  │  "
          f"skipped: {grand_skip:,}  │  failed: {grand_fail:,}  │  "
          f"rows flushed: {grand_rows:,}")


# ─────────────────── PHASE 2: MERGE + SAVE BY YEAR ────────────────────────

def _save_one_year(args):
    """
    Runs in a worker process.
    Reads all temp Parquet files for a single year, merges station metadata,
    writes final output Parquet.
    """
    year, tmp_dir, station_meta_path, output_dir = args

    year_dir = os.path.join(tmp_dir, str(year))
    if not os.path.isdir(year_dir):
        return year, 0

    files = glob.glob(os.path.join(year_dir, "*.parquet"))
    if not files:
        return year, 0

    # Read all temp files for this year (much smaller than full dataset)
    frames = []
    for f in files:
        try:
            frames.append(pd.read_parquet(f))
        except Exception:
            continue

    if not frames:
        return year, 0

    combined = pd.concat(frames, ignore_index=True)
    del frames  # free immediately

    # Merge station metadata
    meta = pd.read_parquet(station_meta_path)
    combined = combined.merge(meta, on="STATION", how="inner")

    os.makedirs(output_dir, exist_ok=True)
    out = os.path.join(output_dir, f"{year}.parquet")
    tmp_out = f"{out}.{os.getpid()}.tmp"
    combined.to_parquet(tmp_out, index=False)
    os.replace(tmp_out, out)

    n = len(combined)
    del combined
    return year, n


def save_parallel(tmp_dir, start_year, end_year, workers):
    """Phase 2: write one final Parquet per year, in parallel."""
    # Only process years that actually have temp data
    all_year_dirs = []
    for name in os.listdir(tmp_dir):
        full = os.path.join(tmp_dir, name)
        if os.path.isdir(full) and name.isdigit():
            yr = int(name)
            if start_year <= yr <= end_year:
                all_year_dirs.append(yr)
    all_year_dirs.sort()

    if not all_year_dirs:
        print("No year data found in temp directory.")
        return

    print(f"\n╔══ Phase 2: Merge + Save ({len(all_year_dirs)} years, "
          f"{workers} workers) ════════════╗")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    work = [(yr, tmp_dir, STATION_META, OUTPUT_DIR) for yr in all_year_dirs]

    saved = 0
    with ProcessPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_save_one_year, w): w[0] for w in work}
        for fut in as_completed(futs):
            year, count = fut.result()
            if count:
                saved += count
                print(f"  [{year}] {count:,} records → {OUTPUT_DIR}/{year}.parquet")

    print(f"\n  Grand total: {saved:,} records across {len(all_year_dirs)} years")
    print(f"╚══════════════════════════════════════════════════════════════╝")


# ────────────────────── MAIN ───────────────────────────────────────────────

def main():
    global OUTPUT_DIR, STATION_META
    cpu_count = mp.cpu_count()

    parser = argparse.ArgumentParser(
        description="NOAA GHCNd → Parquet (multiprocessing, memory-safe)"
    )
    parser.add_argument("--mode", choices=["full", "refresh"], default="full")
    parser.add_argument("--start-year", type=int, default=1950)
    parser.add_argument("--end-year", type=int, default=datetime.now().year)
    parser.add_argument("--refresh-years", type=int, default=2)
    parser.add_argument(
        "--workers", type=int, default=cpu_count,
        help=f"CPU worker processes (default: {cpu_count} = all cores)"
    )
    parser.add_argument(
        "--io-threads", type=int, default=8,
        help="Download threads PER worker process (default: 8)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=150,
        help="Stations per mini-batch before flushing to disk (default: 150)"
    )
    parser.add_argument("--force-station-refresh", action="store_true")
    parser.add_argument(
        "--output-dir", default=OUTPUT_DIR,
        help="Directory for yearly GHCNd parquet outputs",
    )
    parser.add_argument(
        "--existing-dir", default=None,
        help="Directory to inspect for active station IDs during refresh mode",
    )
    parser.add_argument(
        "--station-meta", default=STATION_META,
        help="Cached station metadata parquet path",
    )
    args = parser.parse_args()

    OUTPUT_DIR = args.output_dir
    STATION_META = args.station_meta

    total_concurrent = args.workers * args.io_threads

    print(f"{'━' * 64}")
    print(f"  GHCNd Pipeline  │  mode={args.mode}")
    print(f"  CPUs: {args.workers}  │  I/O threads/proc: {args.io_threads}  │  "
          f"total dl: ~{total_concurrent}")
    print(f"  Mini-batch: {args.batch_size} stations  (flush + free per batch)")
    print(f"{'━' * 64}\n")

    # 1. Station metadata
    us_stations = get_us_stations(force_refresh=args.force_station_refresh)
    all_ids = us_stations["STATION"].tolist()

    # 2. Determine station list + year range
    if args.mode == "refresh":
        start_year = args.end_year - args.refresh_years + 1
        end_year   = args.end_year
        existing_dir = args.existing_dir or OUTPUT_DIR
        existing = [f for f in os.listdir(existing_dir) if f.endswith(".parquet")] \
                   if os.path.isdir(existing_dir) else []
        if existing:
            latest = os.path.join(existing_dir, sorted(existing)[-1])
            station_ids = pd.read_parquet(latest, columns=["STATION"])["STATION"].unique().tolist()
            print(f"Refresh: {len(station_ids):,} active stations, years {start_year}-{end_year}")
        else:
            station_ids = all_ids
    else:
        start_year  = args.start_year
        end_year    = args.end_year
        station_ids = all_ids

    # 3. Phase 1 — download + parse → year-partitioned temp files
    tmp_dir = tempfile.mkdtemp(prefix="ghcnd_")
    try:
        download_parallel(
            station_ids, start_year, end_year,
            args.workers, args.io_threads, args.batch_size, tmp_dir
        )

        # 4. Phase 2 — merge temp files → final yearly Parquet
        save_parallel(tmp_dir, start_year, end_year, args.workers)

    finally:
        # Always clean up temp files
        print(f"\nCleaning up temp dir: {tmp_dir}")
        shutil.rmtree(tmp_dir, ignore_errors=True)

    print(f"\n{'━' * 64}")
    print("Done!")


if __name__ == "__main__":
    main()
