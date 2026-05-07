#!/usr/bin/env python3
"""
NOAA 15-Minute Precipitation Pipeline (HPD v2)
===============================================
Downloads and converts NOAA's Cooperative Observer 15-minute precipitation
data into a partitioned Parquet dataset.

Source : https://www.ncei.noaa.gov/pub/data/hpd/auto/v2/beta/15min/
Output : data/ASOS/15min/  (partitioned by year / state)

Usage:
    python 15min.py                # First run  – downloads bulk tar.gz (~282 MB)
    python 15min.py --mode update  # Monthly refresh – per-station CSVs in parallel
"""

import os
import io
import sys
import json
import tarfile
import argparse
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from bs4 import BeautifulSoup

# ──────────────────────────── CONFIGURATION ────────────────────────────────
MAX_CORES = os.cpu_count() or 8          # M4 Pro → 12 cores
CHUNK_ROWS = 80_000                       # Pandas chunk size when reading CSVs
PARQUET_FLUSH = 120_000                   # Buffer before writing to parquet
OUTPUT_DIR = "data/ASOS/15min"
DOWNLOADS_DIR = ".downloads_15min"
STATE_FILE = os.path.join(OUTPUT_DIR, "_pipeline_state.json")

HPD_BASE = "https://www.ncei.noaa.gov/pub/data/hpd/auto/v2/beta/15min/"
HPD_CSV_DIR = HPD_BASE + "all_csv/"
HPD_BULK_TAR = HPD_BASE + "hpd_all.15min.csv.tar.gz"
HPD_INVENTORY = HPD_BASE + "hpd-stations-inventory.15min.txt"

# COOP 2-digit state prefix → abbreviation
_STATE = {
    "01":"AL","02":"AZ","03":"AR","04":"CA","05":"CO","06":"CT","07":"DE",
    "08":"FL","09":"GA","10":"ID","11":"IL","12":"IN","13":"IA","14":"KS",
    "15":"KY","16":"LA","17":"ME","18":"MD","19":"MA","20":"MI","21":"MN",
    "22":"MS","23":"MO","24":"MT","25":"NE","26":"NV","27":"NH","28":"NJ",
    "29":"NM","30":"NY","31":"NC","32":"ND","33":"OH","34":"OK","35":"OR",
    "36":"PA","37":"RI","38":"SC","39":"SD","40":"TN","41":"TX","42":"UT",
    "43":"VT","44":"VA","45":"WA","46":"WV","47":"WI","48":"WY","50":"AK",
    "51":"HI","66":"PR","67":"VI","91":"PI",
}
_TERRITORY = {"AQ":"AS","CQ":"MP","GQ":"GU","RQ":"PR","VQ":"VI","RW":"PR"}

# 96 time-slot labels: 0000, 0015, 0030 … 2345
_SLOTS = [f"{h:02d}{m:02d}" for h in range(24) for m in (0, 15, 30, 45)]
_VAL_COLS = [f"{s}Val" for s in _SLOTS]
_MF_COLS  = [f"{s}MF"  for s in _SLOTS]
_QF_COLS  = [f"{s}QF"  for s in _SLOTS]

# Parquet schema (explicit so every file is merge-compatible)
_SCHEMA = pa.schema([
    ("station_id",        pa.string()),
    ("station_name",      pa.string()),
    ("state",             pa.string()),
    ("lat",               pa.float64()),
    ("lon",               pa.float64()),
    ("elevation",         pa.float64()),
    ("element",           pa.string()),
    ("year",              pa.int16()),
    ("month",             pa.int8()),
    ("day",               pa.int8()),
    ("time",              pa.string()),
    ("precip_in",         pa.float64()),
    ("flag_measurement",  pa.string()),
    ("flag_quality",      pa.string()),
])

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(DOWNLOADS_DIR, exist_ok=True)


# ──────────────────────────── STATION METADATA ─────────────────────────────
def build_station_name_map():
    """
    Download the HPD fixed-width station inventory and return a dict of
    station_id → station_name.  Falls back gracefully on failure.
    """
    print("Fetching HPD station inventory for names …")
    try:
        r = requests.get(HPD_INVENTORY, timeout=60)
        r.raise_for_status()
    except Exception as e:
        print(f"  Warning: inventory download failed ({e}); names will be empty.")
        return {}

    name_map = {}
    for line in r.text.splitlines():
        if not line or line.startswith("#"):
            continue
        # Fixed-width: id(0:11) lat(12:20) lon(21:30) elev(31:37) state(38:40) name(41:71) …
        try:
            sid = line[0:11].strip()
            name = line[41:71].strip()
            if sid and name:
                name_map[sid] = name
        except IndexError:
            continue

    print(f"  Loaded names for {len(name_map)} stations.")
    return name_map


# ──────────────────────────── STATE CODE HELPER ────────────────────────────
def _state_abbrev(station_id: str) -> str:
    """USC00SSNNNN → state abbrev;  territory prefixes handled too."""
    if station_id.startswith("USC00") and len(station_id) >= 7:
        return _STATE.get(station_id[5:7], station_id[5:7])
    return _TERRITORY.get(station_id[:2], station_id[:2])


# ──────────────────────────── WIDE → LONG MELT ────────────────────────────
def _melt_chunk(chunk: pd.DataFrame, name_map: dict) -> list[dict]:
    """
    Convert a chunk of the wide HPD CSV into a list of long-format dicts.
    Only rows/slots that actually have a value are emitted → keeps memory tiny.
    """
    records = []
    for _, row in chunk.iterrows():
        sid     = str(row.get("STATION", "")).strip()
        lat     = _float(row.get("LATITUDE"))
        lon     = _float(row.get("LONGITUDE"))
        elev    = _float(row.get("ELEVATION"))
        elem    = str(row.get("ELEMENT", "")).strip()
        datestr = str(row.get("DATE", "")).strip()

        if elem not in ("QPCP", "QGAG"):
            continue
        try:
            # Format is YYYY-MM-DD (e.g. "2019-09-29")
            parts = datestr.split("-")
            yr, mo, dy = int(parts[0]), int(parts[1]), int(parts[2])
        except (ValueError, IndexError):
            continue

        state = _state_abbrev(sid)
        sname = name_map.get(sid)

        for slot, vc, mc, qc in zip(_SLOTS, _VAL_COLS, _MF_COLS, _QF_COLS):
            raw = str(row.get(vc, "")).strip()
            if raw in ("", "nan", "-9999", "99999"):
                continue

            pval = _float(raw)
            if pval is not None:
                if pval < 0:
                    continue               # other negative sentinel values
                pval /= 100.0              # hundredths of inches → inches

            mf = str(row.get(mc, "")).strip()
            qf = str(row.get(qc, "")).strip()
            if mf == "nan": mf = ""
            if qf == "nan": qf = ""

            records.append({
                "station_id":       sid,
                "station_name":     sname,
                "state":            state,
                "lat":              lat,
                "lon":              lon,
                "elevation":        elev,
                "element":          elem,
                "year":             yr,
                "month":            mo,
                "day":              dy,
                "time":             slot,
                "precip_in":        pval,
                "flag_measurement": mf,
                "flag_quality":     qf,
            })
    return records


def _float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ──────────────────────────── PARQUET WRITER ───────────────────────────────
def _flush(buf: list[dict]):
    """Write buffered records to the partitioned parquet dataset."""
    if not buf:
        return
    df = pd.DataFrame(buf)
    # Enforce correct dtypes so Arrow schema matches across files
    df["year"]  = df["year"].astype("int16")
    df["month"] = df["month"].astype("int8")
    df["day"]   = df["day"].astype("int8")
    for col in ("station_id","station_name","state","element","time",
                "flag_measurement","flag_quality"):
        df[col] = df[col].astype("string")
    table = pa.Table.from_pandas(df, schema=_SCHEMA, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=OUTPUT_DIR,
        partition_cols=["year", "state"],
        compression="snappy",
    )


# ──────────────────────────── PROCESS ONE CSV ──────────────────────────────
def process_csv_text(csv_bytes: bytes, filename: str, name_map: dict) -> str:
    """
    Parse one station's CSV (as raw bytes) in chunks and flush to parquet.
    Returns a status string.  Never holds more than ~CHUNK_ROWS in memory.
    """
    buf = []
    total = 0
    try:
        reader = pd.read_csv(
            io.BytesIO(csv_bytes),
            dtype=str,
            low_memory=False,
            chunksize=CHUNK_ROWS,
        )
        for chunk in reader:
            recs = _melt_chunk(chunk, name_map)
            buf.extend(recs)
            total += len(recs)
            if len(buf) >= PARQUET_FLUSH:
                _flush(buf)
                buf.clear()

        if buf:
            _flush(buf)

        return f"OK  {filename}  ({total:,} records)"
    except Exception as e:
        return f"ERR {filename}: {e}"


# ──────────────────────────── FULL LOAD (BULK TAR) ─────────────────────────
def run_full_load(name_map: dict):
    tar_path = os.path.join(DOWNLOADS_DIR, "hpd_all.15min.csv.tar.gz")

    # Download
    if not os.path.exists(tar_path):
        print(f"\nDownloading bulk archive (~282 MB) …")
        try:
            with requests.get(HPD_BULK_TAR, stream=True, timeout=600) as r:
                r.raise_for_status()
                total = int(r.headers.get("content-length", 0))
                done = 0
                with open(tar_path, "wb") as f:
                    for data in r.iter_content(chunk_size=1_048_576):
                        f.write(data)
                        done += len(data)
                        if total:
                            print(f"\r  {done/1e6:.0f} / {total/1e6:.0f} MB "
                                  f"({done/total*100:.0f}%)", end="", flush=True)
            print()
        except Exception as e:
            print(f"\nDownload failed: {e}")
            return
    else:
        print(f"Using cached archive: {tar_path}")

    # Stream through tar – one station at a time
    print(f"Extracting & processing (streaming, {MAX_CORES} cores) …\n")
    stations_done = 0
    total_records = 0

    with tarfile.open(tar_path, "r:gz") as tar:
        for member in tar:
            if not member.name.endswith(".15m.csv"):
                continue

            fobj = tar.extractfile(member)
            if fobj is None:
                continue

            csv_bytes = fobj.read()
            fname = os.path.basename(member.name)
            result = process_csv_text(csv_bytes, fname, name_map)

            stations_done += 1
            # Extract record count for progress
            if result.startswith("OK"):
                try:
                    cnt = int(result.split("(")[1].split(" ")[0].replace(",", ""))
                    total_records += cnt
                except (IndexError, ValueError):
                    pass

            if stations_done % 100 == 0:
                print(f"  [{stations_done}] {total_records:,} records … last: {result}")

    print(f"\n  Finished: {stations_done} stations, {total_records:,} total records.")

    # Cleanup
    try:
        os.remove(tar_path)
        print(f"  Removed {tar_path}")
    except OSError:
        pass

    _save_state(stations_done, total_records)


# ──────────────────────────── INCREMENTAL UPDATE ───────────────────────────
def _download_and_process_station(args):
    """Worker function for parallel update mode."""
    filename, url, name_map = args
    local = os.path.join(DOWNLOADS_DIR, filename)

    # Download
    try:
        with requests.get(url, stream=True, timeout=300) as r:
            r.raise_for_status()
            with open(local, "wb") as f:
                for data in r.iter_content(chunk_size=65_536):
                    f.write(data)
    except Exception as e:
        return f"DL-ERR {filename}: {e}"

    # Process
    try:
        with open(local, "rb") as f:
            csv_bytes = f.read()
        result = process_csv_text(csv_bytes, filename, name_map)
    except Exception as e:
        result = f"ERR {filename}: {e}"

    # Cleanup
    try:
        os.remove(local)
    except OSError:
        pass

    return result


def run_incremental_update(name_map: dict):
    """Download individual station CSVs in parallel."""
    print(f"\nCrawling HPD v2 directory for station CSVs …")
    try:
        r = requests.get(HPD_CSV_DIR, timeout=60)
        r.raise_for_status()
    except Exception as e:
        print(f"Failed: {e}")
        return

    soup = BeautifulSoup(r.text, "html.parser")
    station_files = []
    for a in soup.find_all("a"):
        href = a.get("href", "")
        fname = href.strip("/").split("/")[-1]
        if fname.endswith(".15m.csv"):
            station_files.append((fname, HPD_CSV_DIR + fname, name_map))

    if not station_files:
        print("No station files found.")
        return

    print(f"Found {len(station_files)} stations.  Processing with {MAX_CORES} workers …\n")

    done = 0
    errors = 0
    total_records = 0

    with ProcessPoolExecutor(max_workers=MAX_CORES) as pool:
        futures = {pool.submit(_download_and_process_station, sf): sf[0]
                   for sf in station_files}

        for future in as_completed(futures):
            done += 1
            result = future.result()

            if "ERR" in result:
                errors += 1
                print(f"  [{done}/{len(station_files)}] {result}")
            else:
                try:
                    cnt = int(result.split("(")[1].split(" ")[0].replace(",", ""))
                    total_records += cnt
                except (IndexError, ValueError):
                    pass
                if done % 50 == 0 or done == len(station_files):
                    print(f"  [{done}/{len(station_files)}] {total_records:,} records … "
                          f"({errors} errors)")

    print(f"\n  Finished: {done} stations, {total_records:,} records, {errors} errors.")
    _save_state(done, total_records)


# ──────────────────────────── STATE TRACKING ───────────────────────────────
def _save_state(stations: int, records: int):
    state = {
        "last_run": datetime.now().isoformat(),
        "stations_processed": stations,
        "records_written": records,
        "output_dir": OUTPUT_DIR,
    }
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)
    print(f"  Pipeline state saved to {STATE_FILE}")


# ──────────────────────────── ENTRY POINT ──────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="NOAA 15-Minute Precipitation → Parquet  (HPD v2)"
    )
    parser.add_argument(
        "--mode",
        choices=["full", "update"],
        default="full",
        help="'full'  = bulk tar.gz download (~282 MB, best for first run). "
             "'update' = per-station CSVs in parallel (monthly refresh).",
    )
    args = parser.parse_args()

    print("━" * 65)
    print("  NOAA 15-Minute Precipitation Pipeline  (HPD v2)")
    print(f"  Source : {HPD_BASE}")
    print(f"  Output : {OUTPUT_DIR}/")
    print(f"  Cores  : {MAX_CORES}")
    print(f"  Mode   : {args.mode}")
    print("━" * 65)

    # Station names (optional enrichment)
    name_map = build_station_name_map()

    if args.mode == "full":
        run_full_load(name_map)
    else:
        run_incremental_update(name_map)

    print(f"\nAll done!  Parquet dataset → {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()