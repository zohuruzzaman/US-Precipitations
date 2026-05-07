#!/usr/bin/env python3
"""
NOAA Hourly Weather Pipeline  (GHCNh / ISD → Parquet)
======================================================
Downloads hourly precipitation, temperature, and dew point for
US stations via the NCEI Access Data Service API.

Data source : global-hourly (ISD/GHCNh) — no auth token required
Station meta: ISD History file → saved as separate parquet for crosswalk
Output      : data/GHCNh/hourly/   (partitioned by year / state)
              data/GHCNh/stations.parquet  (metadata / crosswalk)

Usage:
    # Full run — all US stations, 1950-present (will take days; resumable)
    python ghcnh_hourly.py

    # Specific states and years
    python ghcnh_hourly.py --states MS,AL,LA --start-year 2000 --end-year 2024

    # Resume an interrupted run (skips already-downloaded station-years)
    python ghcnh_hourly.py --resume

    # Update with latest year
    python ghcnh_hourly.py --start-year 2025 --end-year 2025
"""

import os
import re
import io
import sys
import json
import time
import argparse
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ──────────────────────────── CONFIGURATION ────────────────────────────────
MAX_WORKERS   = 6             # Parallel API calls (be gentle with NOAA)
API_BASE      = "https://www.ncei.noaa.gov/access/services/data/v1"
ISD_HISTORY   = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv"
GHCNH_STATIONS = "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/doc/ghcnh-station-list.txt"

OUTPUT_DIR    = "data/GHCNh/hourly"
META_DIR      = "data/GHCNh"
PROGRESS_FILE = "data/GHCNh/_progress.json"
RETRY_LIMIT   = 3
RETRY_DELAY   = 5            # seconds between retries
API_DELAY     = 0.3          # seconds between API calls (rate limiting)

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(META_DIR, exist_ok=True)

# Parquet schema for hourly observations
OBS_SCHEMA = pa.schema([
    ("station_id",    pa.string()),     # ISD id: USAF+WBAN e.g. "72530094846"
    ("state",         pa.string()),     # 2-letter state code
    ("date",          pa.string()),     # ISO datetime e.g. "2024-01-01T00:51:00"
    ("report_type",   pa.string()),     # FM-12, FM-15, etc.
    ("year",          pa.int16()),
    ("month",         pa.int8()),
    ("day",           pa.int8()),
    ("hour",          pa.int8()),
    ("minute",        pa.int8()),
    ("temperature_c", pa.float32()),    # °C
    ("dewpoint_c",    pa.float32()),    # °C
    ("precip_mm",     pa.float32()),    # mm (hourly liquid precipitation)
    ("precip_hours",  pa.int8()),       # accumulation period in hours
    ("temp_qc",       pa.string()),     # quality code
    ("dew_qc",        pa.string()),
    ("precip_qc",     pa.string()),
])


# ──────────────────────────── STATION METADATA ─────────────────────────────

def download_station_metadata():
    """
    Download ISD station history → filter to US → save as parquet.
    Also downloads GHCNh station list for crosswalk purposes.
    Returns DataFrame of US stations.
    """
    meta_path = os.path.join(META_DIR, "stations.parquet")

    print("Downloading ISD station history …")
    r = requests.get(ISD_HISTORY, timeout=60)
    r.raise_for_status()

    df = pd.read_csv(io.StringIO(r.text), dtype=str)
    df.columns = df.columns.str.strip().str.replace('"', '')

    # Filter to US only
    df = df[df["CTRY"] == "US"].copy()

    # Clean up columns
    df["USAF"]  = df["USAF"].str.strip()
    df["WBAN"]  = df["WBAN"].str.strip()
    df["STATE"] = df["STATE"].str.strip()
    df["BEGIN"] = df["BEGIN"].str.strip()
    df["END"]   = df["END"].str.strip()

    # Build the ISD station ID the API expects: USAF + WBAN (no separator)
    df["isd_id"] = df["USAF"] + df["WBAN"]

    # Parse lat/lon/elev
    for col in ["LAT", "LON", "ELEV(M)"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.rename(columns={
        "STATION NAME": "name",
        "LAT": "lat", "LON": "lon", "ELEV(M)": "elevation_m",
        "STATE": "state", "BEGIN": "begin", "END": "end",
    })

    # Parse begin/end dates
    df["begin_year"] = pd.to_numeric(df["begin"].str[:4], errors="coerce").fillna(9999).astype(int)
    df["end_year"]   = pd.to_numeric(df["end"].str[:4], errors="coerce").fillna(0).astype(int)

    # Keep only stations with valid coordinates and some data
    df = df.dropna(subset=["lat", "lon"])
    df = df[df["lat"].between(17, 72)]    # rough US bbox
    df = df[df["lon"].between(-180, -65)]

    # Remove unidentified stations (999999 in either USAF or WBAN)
    df = df[df["USAF"] != "999999"]
    df = df[df["WBAN"] != "99999"]

    # Must have a valid state code
    df = df[df["state"].str.len() == 2]
    df = df[df["state"] != ""]

    keep_cols = ["isd_id", "USAF", "WBAN", "name", "state", "lat", "lon",
                 "elevation_m", "begin_year", "end_year"]
    df = df[keep_cols].drop_duplicates(subset=["isd_id"]).reset_index(drop=True)

    # Save
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, meta_path, compression="snappy")
    print(f"  Saved {len(df)} US stations → {meta_path}")

    # Also download GHCNh station list for crosswalk
    _download_ghcnh_crosswalk(df)

    return df


def _download_ghcnh_crosswalk(isd_df):
    """Download GHCNh station list and save for crosswalk purposes."""
    crosswalk_path = os.path.join(META_DIR, "ghcnh_station_list.txt")
    try:
        print("Downloading GHCNh station list (for crosswalk) …")
        r = requests.get(GHCNH_STATIONS, timeout=60)
        r.raise_for_status()
        with open(crosswalk_path, "w") as f:
            f.write(r.text)
        print(f"  Saved → {crosswalk_path}")
    except Exception as e:
        print(f"  Warning: could not download GHCNh station list: {e}")


# ──────────────────────────── ISD FIELD PARSING ────────────────────────────

def _parse_tmp(raw: str):
    """Parse ISD TMP field: '+0022,1' → (temp_c=2.2, qc='1')"""
    if not raw or raw == "" or pd.isna(raw):
        return None, ""
    # Format: [+-]NNNN,Q  (tenths of °C, quality code)
    m = re.match(r"([+-]?\d{4}),(\w)", str(raw))
    if not m:
        return None, ""
    val = int(m.group(1))
    qc  = m.group(2)
    if val == 9999 or val == -9999:
        return None, qc
    return val / 10.0, qc


def _parse_dew(raw: str):
    """Parse ISD DEW field — same format as TMP."""
    return _parse_tmp(raw)


def _parse_aa1(raw: str):
    """
    Parse ISD AA1 field: '01,0038,9,5' → (period_hrs=1, depth_mm=3.8, condition, qc)
    Format: period_hours, depth_in_tenths_mm, condition_code, quality_code
    """
    if not raw or raw == "" or pd.isna(raw):
        return None, None, ""
    parts = str(raw).split(",")
    if len(parts) < 4:
        return None, None, ""
    try:
        period = int(parts[0])
        depth  = int(parts[1])
        qc     = parts[3]
        if depth == 9999:
            return period, None, qc
        return period, depth / 10.0, qc   # tenths of mm → mm
    except (ValueError, IndexError):
        return None, None, ""


# ──────────────────────────── API FETCHER ──────────────────────────────────

def fetch_station_year(isd_id: str, year: int, state: str) -> list[dict]:
    """
    Fetch one station-year from the NCEI API. Returns list of parsed records.
    """
    url = (
        f"{API_BASE}?dataset=global-hourly"
        f"&stations={isd_id}"
        f"&startDate={year}-01-01"
        f"&endDate={year}-12-31"
        f"&dataTypes=TMP,DEW,AA1"
        f"&format=csv"
        f"&includeAttributes=false"
    )

    for attempt in range(RETRY_LIMIT):
        try:
            r = requests.get(url, timeout=120)
            if r.status_code == 503:
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue
            r.raise_for_status()
            break
        except requests.exceptions.RequestException:
            if attempt < RETRY_LIMIT - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                return []

    text = r.text.strip()
    if not text or text.startswith("{"):  # empty or JSON error
        return []

    lines = text.split("\n")
    if len(lines) < 2:
        return []

    # Parse header
    header = [h.strip().strip('"') for h in lines[0].split(",")]

    # Find column indices
    col_map = {h: i for i, h in enumerate(header)}
    date_idx = col_map.get("DATE")
    tmp_idx  = col_map.get("TMP")
    dew_idx  = col_map.get("DEW")
    aa1_idx  = col_map.get("AA1")
    rpt_idx  = col_map.get("REPORT_TYPE")

    if date_idx is None:
        return []

    records = []
    for line in lines[1:]:
        if not line.strip():
            continue

        # CSV parse (handle quoted fields)
        fields = _csv_split(line)
        if len(fields) <= max(x for x in [date_idx, tmp_idx, dew_idx, aa1_idx, rpt_idx] if x is not None):
            continue

        datestr = fields[date_idx].strip('"')

        # Parse datetime
        try:
            dt = datetime.fromisoformat(datestr.replace("Z", "+00:00").replace("z", "+00:00"))
        except ValueError:
            continue

        # Parse fields
        temp_c, temp_qc  = _parse_tmp(fields[tmp_idx].strip('"')) if tmp_idx else (None, "")
        dew_c, dew_qc    = _parse_dew(fields[dew_idx].strip('"')) if dew_idx else (None, "")

        precip_mm    = None
        precip_hrs   = None
        precip_qc    = ""
        if aa1_idx and aa1_idx < len(fields):
            precip_hrs, precip_mm, precip_qc = _parse_aa1(fields[aa1_idx].strip('"'))

        # Skip rows with no useful data at all
        if temp_c is None and dew_c is None and precip_mm is None:
            continue

        records.append({
            "station_id":    isd_id,
            "state":         state,
            "date":          datestr,
            "report_type":   fields[rpt_idx].strip('"') if rpt_idx and rpt_idx < len(fields) else "",
            "year":          dt.year,
            "month":         dt.month,
            "day":           dt.day,
            "hour":          dt.hour,
            "minute":        dt.minute,
            "temperature_c": temp_c,
            "dewpoint_c":    dew_c,
            "precip_mm":     precip_mm,
            "precip_hours":  precip_hrs,
            "temp_qc":       temp_qc,
            "dew_qc":        dew_qc,
            "precip_qc":     precip_qc,
        })

    return records


def _csv_split(line: str) -> list[str]:
    """Simple CSV field splitter that handles quoted fields with commas."""
    fields = []
    current = []
    in_quote = False
    for ch in line:
        if ch == '"':
            in_quote = not in_quote
            current.append(ch)
        elif ch == ',' and not in_quote:
            fields.append("".join(current))
            current = []
        else:
            current.append(ch)
    fields.append("".join(current))
    return fields


# ──────────────────────────── PARQUET WRITER ───────────────────────────────

def flush_records(records: list[dict]):
    """Write records to partitioned parquet dataset."""
    if not records:
        return

    df = pd.DataFrame(records)
    df["year"]  = df["year"].astype("int16")
    df["month"] = df["month"].astype("int8")
    df["day"]   = df["day"].astype("int8")
    df["hour"]  = df["hour"].astype("int8")
    df["minute"] = df["minute"].astype("int8")

    # Nullable int for precip_hours (avoids float64 when all null)
    df["precip_hours"] = pd.array(df["precip_hours"], dtype=pd.Int8Dtype())

    for col in ["station_id", "state", "date", "report_type",
                "temp_qc", "dew_qc", "precip_qc"]:
        df[col] = df[col].astype("string")

    table = pa.Table.from_pandas(df, schema=OBS_SCHEMA, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=OUTPUT_DIR,
        partition_cols=["year", "state"],
        compression="snappy",
    )


# ──────────────────────────── PROGRESS TRACKING ────────────────────────────

def load_progress() -> set:
    """Load set of completed 'isd_id:year' keys."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return set(json.load(f))
    return set()


def save_progress(done: set):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(sorted(done), f)


# ──────────────────────────── WORKER ───────────────────────────────────────

def process_station_year(args):
    """Worker function: fetch + parse + flush one station-year."""
    isd_id, year, state = args
    time.sleep(API_DELAY)  # rate limit

    records = fetch_station_year(isd_id, year, state)
    if records:
        flush_records(records)
    return isd_id, year, len(records)


# ──────────────────────────── MAIN ─────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="NOAA Hourly Weather → Parquet  (GHCNh / ISD via NCEI API)"
    )
    parser.add_argument("--start-year", type=int, default=1950)
    parser.add_argument("--end-year",   type=int, default=datetime.now().year)
    parser.add_argument("--states",     type=str, default=None,
                        help="Comma-separated state codes, e.g. MS,AL,LA. Default: all US.")
    parser.add_argument("--workers",    type=int, default=MAX_WORKERS)
    parser.add_argument("--resume",     action="store_true",
                        help="Skip station-years already downloaded.")
    args = parser.parse_args()

    current_year = datetime.now().year

    print("━" * 65)
    print("  NOAA Hourly Weather Pipeline  (GHCNh / ISD)")
    print(f"  API     : {API_BASE}")
    print(f"  Output  : {OUTPUT_DIR}/")
    print(f"  Meta    : {META_DIR}/stations.parquet")
    print(f"  Years   : {args.start_year}–{args.end_year}")
    print(f"  Workers : {args.workers}")
    if args.states:
        print(f"  States  : {args.states}")
    print("━" * 65)

    # 1. Station metadata
    meta_path = os.path.join(META_DIR, "stations.parquet")
    if os.path.exists(meta_path) and args.resume:
        print(f"Loading cached station metadata from {meta_path}")
        stations = pd.read_parquet(meta_path)
    else:
        stations = download_station_metadata()

    # 2. Filter
    if args.states:
        state_list = [s.strip().upper() for s in args.states.split(",")]
        stations = stations[stations["state"].isin(state_list)]
        print(f"  Filtered to {len(stations)} stations in {state_list}")

    # Only include stations whose period of record overlaps our year range
    stations = stations[
        (stations["begin_year"] <= args.end_year) &
        (stations["end_year"] >= args.start_year)
    ]
    print(f"  {len(stations)} stations with data in {args.start_year}–{args.end_year}")

    # 3. Build task list: (isd_id, year, state)
    done = load_progress() if args.resume else set()
    if done:
        print(f"  Resuming — {len(done)} station-years already complete.")

    tasks = []
    for _, row in stations.iterrows():
        yr_start = max(args.start_year, int(row["begin_year"]))
        yr_end   = min(args.end_year, int(row["end_year"]), current_year)
        for yr in range(yr_start, yr_end + 1):
            key = f"{row['isd_id']}:{yr}"
            if key not in done:
                tasks.append((row["isd_id"], yr, row["state"]))

    print(f"  {len(tasks)} station-years to download.\n")

    if not tasks:
        print("Nothing to do!")
        return

    # 4. Process
    completed = 0
    errors    = 0
    total_rec = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_station_year, t): t for t in tasks}

        for future in as_completed(futures):
            completed += 1
            task = futures[future]
            try:
                isd_id, year, n_records = future.result()
                total_rec += n_records
                done.add(f"{isd_id}:{year}")

                if completed % 50 == 0 or completed == len(tasks):
                    save_progress(done)
                    print(f"  [{completed}/{len(tasks)}]  {total_rec:,} records  "
                          f"({errors} errors)  last: {isd_id} {year} ({n_records})")

            except Exception as e:
                errors += 1
                isd_id, year, state = task
                if completed % 50 == 0:
                    print(f"  [{completed}/{len(tasks)}]  ERROR {isd_id} {year}: {e}")

    # Final save
    save_progress(done)

    print(f"\n{'━' * 65}")
    print(f"  Done!  {completed} station-years, {total_rec:,} records, {errors} errors")
    print(f"  Data   → {OUTPUT_DIR}/")
    print(f"  Meta   → {META_DIR}/stations.parquet")
    print(f"{'━' * 65}")


if __name__ == "__main__":
    main()