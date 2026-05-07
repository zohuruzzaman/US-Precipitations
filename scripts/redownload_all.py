#!/usr/bin/env python3
"""
redownload_all.py — Full from-scratch restoration of the WeatherData archive.

Run from the project root:
    python redownload_all.py

Parallelism model:
  ┌─────────────────────────────────────────────────────┐
  │  All 5 sources run SIMULTANEOUSLY                   │
  │  ┌──────────┐ ┌─────┐ ┌──────┐ ┌───────┐ ┌──────┐ │
  │  │ GHCN     │ │ HPD │ │ NCEI │ │ISD    │ │1-min │ │
  │  │ Pool(N)  │ │ N   │ │ N    │ │ N     │ │ N    │ │
  │  │ processes│ │thd  │ │thd   │ │ thd   │ │ thd  │ │
  │  └──────────┘ └─────┘ └──────┘ └───────┘ └──────┘ │
  │  After ALL complete → recon pipeline runs           │
  └─────────────────────────────────────────────────────┘

Sources use their local recovered files if found; otherwise download from NOAA.
ASOS ISD and 1-min always download from NOAA (no local data recovered).

Resumable: every source skips files that already exist as parquet.
Log:       redownload.log   (tail -f redownload.log to monitor)
"""

import datetime
import logging
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Pool, cpu_count
from pathlib import Path

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq

# ─── Paths ────────────────────────────────────────────────────────────────────
HERE   = Path(__file__).resolve().parent
DATA   = HERE / "data"
RECON  = DATA / "recon"
DB_PATH = RECON / "file_inventory.duckdb"
LOG_PATH = HERE / "redownload.log"

# Recovery source paths (old layout — where pasted files live)
GHCN_DLY_DIR  = HERE / "GHCN" / "ghcnd_all"
HPD_CSV_DIR   = HERE / "HPD_Daily" / "raw_station_files"
NCEI_SRC_DIR  = HERE / "NCEI-15min" / "raw_parquet" / "hpd15_station_parquet"

# Target data paths (data/ layout — where pipeline expects them)
GHCN_OUT_DIR  = DATA / "GHCN_daily" / "ghcnd_all_parquet"
HPD_OUT_DIR   = DATA / "HPD_1hr" / "raw" / "HPD_1hr"
NCEI_OUT_DIR  = DATA / "NCEI_15min_raw" / "raw_parquet" / "hpd15_station_parquet"
ISD_OUT_DIR   = DATA / "ASOS" / "processed_parquet"
ASOS1_OUT_DIR = DATA / "ASOS" / "asos_1min_parquet_master"

# ─── Tuning ───────────────────────────────────────────────────────────────────
N_CPU          = cpu_count()
N_CONVERT      = min(N_CPU, 8)      # parallel DuckDB/pyarrow conversion workers
N_DL_ISD       = 16                 # parallel download threads — ASOS ISD
N_DL_1MIN      = 12                 # parallel download threads — ASOS 1-min
NOAA_RATE      = 8                  # max requests/sec per source (NOAA tolerance)

# ─── GHCN constants ───────────────────────────────────────────────────────────
US_GHCND_PREFIXES = frozenset({
    "US", "RQ", "VQ", "CQ", "GQ", "AQ", "MQ", "FQ", "SQ", "RW",
})
KEEP_ELEMENTS = frozenset({"PRCP", "TMAX", "TMIN", "TAVG"})
GHCN_SCHEMA = pa.schema([
    ("station_id", pa.string()), ("date", pa.date32()),
    ("element", pa.string()),    ("value", pa.int32()),
    ("mflag", pa.string()),      ("qflag", pa.string()),
    ("sflag", pa.string()),
])

# ─── Thread-safe logging ──────────────────────────────────────────────────────
_log_lock = threading.Lock()

def _make_logger(name: str) -> logging.Logger:
    """Return a logger that prefixes every message with [name]."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    logger.propagate = True
    return logger


def _setup_root_log():
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-7s] %(name)-10s %(message)s",
        datefmt="%H:%M:%S",
    )
    fh = logging.FileHandler(LOG_PATH)
    fh.setFormatter(fmt)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    root.addHandler(fh)
    root.addHandler(ch)


# ─── HTTP helpers ─────────────────────────────────────────────────────────────
class _RateLimiter:
    def __init__(self, rate: float):
        self._lock = threading.Lock()
        self._interval = 1.0 / max(rate, 0.1)
        self._last = 0.0

    def wait(self):
        with self._lock:
            gap = self._interval - (time.monotonic() - self._last)
            if gap > 0:
                time.sleep(gap)
            self._last = time.monotonic()


def _get(client: httpx.Client, url: str, retries: int = 3) -> httpx.Response:
    for attempt in range(retries + 1):
        try:
            r = client.get(url)
            r.raise_for_status()
            return r
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404 or attempt == retries:
                raise
            time.sleep(2 ** attempt)
        except Exception:
            if attempt == retries:
                raise
            time.sleep(2 ** attempt)
    raise RuntimeError("unreachable")


def _download(client: httpx.Client, url: str, dest: Path,
              rate: _RateLimiter | None = None) -> int:
    """Stream url → dest atomically (via .tmp). Returns bytes written."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + f".{os.getpid()}.{threading.get_ident()}.tmp")
    nbytes = 0
    if rate:
        rate.wait()
    try:
        with client.stream("GET", url, follow_redirects=True) as r:
            r.raise_for_status()
            with open(tmp, "wb") as fh:
                for chunk in r.iter_bytes(65536):
                    fh.write(chunk)
                    nbytes += len(chunk)
        tmp.rename(dest)
        return nbytes
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


# ─── Phase 0: Directory setup + DuckDB init ───────────────────────────────────
def phase0_setup():
    log = _make_logger("setup")
    log.info("Creating directory structure …")
    for d in [GHCN_OUT_DIR, HPD_OUT_DIR, NCEI_OUT_DIR,
              ISD_OUT_DIR, ASOS1_OUT_DIR, RECON]:
        d.mkdir(parents=True, exist_ok=True)

    if not DB_PATH.exists():
        log.info("Initialising fresh file_inventory.duckdb …")
        con = duckdb.connect(str(DB_PATH))
        con.execute("""
            CREATE TABLE IF NOT EXISTS file_inventory (
                filepath        VARCHAR PRIMARY KEY, filename VARCHAR,
                extension       VARCHAR, file_size_bytes BIGINT,
                source          VARCHAR, subfolder VARCHAR, schema_group VARCHAR,
                station_id_raw  VARCHAR, id_format VARCHAR, date_part VARCHAR,
                is_data_file    BOOLEAN, anomaly_flag VARCHAR
            )""")
        con.execute("""
            CREATE TABLE IF NOT EXISTS data_profile (
                filepath VARCHAR PRIMARY KEY, source VARCHAR,
                station_id_raw VARCHAR, id_format VARCHAR,
                total_rows BIGINT, min_datetime VARCHAR,
                max_datetime VARCHAR, has_data BOOLEAN, error VARCHAR
            )""")
        con.execute("""
            CREATE TABLE IF NOT EXISTS station_coverage (
                station_id_raw VARCHAR, source VARCHAR, id_format VARCHAR,
                temporal_resolution VARCHAR, min_datetime VARCHAR,
                max_datetime VARCHAR, total_rows BIGINT, file_count BIGINT
            )""")
        con.execute("""
            CREATE TABLE IF NOT EXISTS station_coverage_geo (
                station_id_raw VARCHAR, source VARCHAR,
                temporal_resolution VARCHAR, lat DOUBLE, lon DOUBLE,
                min_datetime VARCHAR, max_datetime VARCHAR
            )""")
        con.close()
        log.info("DuckDB schema created.")
    else:
        log.info("file_inventory.duckdb already exists — skipping init.")


# ─── GHCN: per-file converter (module-level so Pool can pickle it) ─────────────
def _convert_dly_file(args: tuple) -> tuple:
    """(dly_path_str, out_path_str) → ('ok'|'skip'|'error', path, rows_or_msg)"""
    import datetime as _dt
    dly_path = Path(args[0])
    out_path = Path(args[1])
    if out_path.exists():
        return ("skip", args[1], 0)
    sid_list, dates, elements, values, mfs, qfs, sfs = [], [], [], [], [], [], []
    try:
        for line in dly_path.read_bytes().decode("latin-1", errors="replace").splitlines():
            if len(line) < 29:
                continue
            element = line[17:21]
            if element not in KEEP_ELEMENTS:
                continue
            try:
                year, month = int(line[11:15]), int(line[15:17])
            except ValueError:
                continue
            for day in range(1, 32):
                off = 21 + (day - 1) * 8
                if off + 8 > len(line):
                    break
                try:
                    val = int(line[off:off + 5])
                except ValueError:
                    continue
                mf = line[off+5:off+6].strip()
                qf = line[off+6:off+7].strip()
                sf = line[off+7:off+8].strip()
                if val == -9999 and not mf and not qf and not sf:
                    continue
                try:
                    d = _dt.date(year, month, day)
                except ValueError:
                    continue
                sid_list.append(line[0:11])
                dates.append(d)
                elements.append(element)
                values.append(None if val == -9999 else val)
                mfs.append(mf or None); qfs.append(qf or None); sfs.append(sf or None)

        if not dates:
            return ("skip", args[1], 0)

        table = pa.table({
            "station_id": pa.array(sid_list, type=pa.string()),
            "date":       pa.array(dates,    type=pa.date32()),
            "element":    pa.array(elements, type=pa.string()),
            "value":      pa.array(values,   type=pa.int32()),
            "mflag":      pa.array(mfs,      type=pa.string()),
            "qflag":      pa.array(qfs,      type=pa.string()),
            "sflag":      pa.array(sfs,      type=pa.string()),
        }, schema=GHCN_SCHEMA)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, str(out_path), compression="snappy")
        return ("ok", args[1], len(dates))
    except Exception as e:
        return ("error", args[1], str(e)[:120])


# ─── Phase 1: GHCN ────────────────────────────────────────────────────────────
def phase1_ghcn():
    log = _make_logger("GHCN")
    log.info(f"Starting  (workers={N_CPU} processes)")

    if GHCN_DLY_DIR.exists():
        dly_files = [
            f for f in GHCN_DLY_DIR.iterdir()
            if f.suffix == ".dly" and f.stem[:2] in US_GHCND_PREFIXES
        ]
        log.info(f"Found {len(dly_files):,} US .dly files locally")
    else:
        dly_files = []

    if not dly_files:
        log.info("No local .dly files — downloading tarball via refresh_data.py …")
        subprocess.run([
            sys.executable, "scripts/refresh_data.py",
            "--source", "GHCN_daily", "--method", "tarball",
            "--force", "--no-post-refresh",
        ], cwd=str(HERE), check=False)
        log.info("GHCN tarball download complete.")
        return

    tasks = [
        (str(f), str(GHCN_OUT_DIR / (f.stem + ".parquet")))
        for f in dly_files
        if not (GHCN_OUT_DIR / (f.stem + ".parquet")).exists()
    ]
    already = len(dly_files) - len(tasks)
    log.info(f"Already converted: {already:,}  |  To convert: {len(tasks):,}")

    if not tasks:
        log.info("All GHCN parquets exist — done.")
        return

    t0 = time.time()
    ok = skip = err = 0
    # Use all CPU cores — GHCN conversion is CPU-bound (parsing fixed-width text)
    with Pool(N_CPU) as pool:
        for i, (status, _, val) in enumerate(
            pool.imap_unordered(_convert_dly_file, tasks, chunksize=200), 1
        ):
            if status == "ok":   ok   += 1
            elif status == "skip": skip += 1
            else:                err  += 1
            if i % 5000 == 0 or i == len(tasks):
                elapsed = time.time() - t0
                rate = i / elapsed if elapsed else 1
                eta  = int((len(tasks) - i) / rate)
                log.info(f"[{i:>6,}/{len(tasks):,}] ok={ok:,} err={err} "
                         f"rate={rate:.0f}/s eta={eta}s")

    log.info(f"Done: {ok:,} converted, {err} errors  ({time.time()-t0:.0f}s)")


# ─── Phase 2: HPD ─────────────────────────────────────────────────────────────
def _convert_hpd_one(args: tuple) -> tuple:
    """(csv_path_str, dest_str) → (ok|skip|error, sid, msg)"""
    csv_path = Path(args[0])
    dest     = Path(args[1])
    sid      = csv_path.stem
    if dest.exists():
        return ("skip", sid, "")
    tmp = dest.with_suffix(".parquet.tmp")
    try:
        con = duckdb.connect()
        n = con.execute(
            f"SELECT COUNT(*) FROM read_csv_auto('{csv_path}', "
            f"ignore_errors=true, sample_size=-1)"
        ).fetchone()[0]
        if n == 0:
            con.close()
            return ("skip", sid, "empty")
        con.execute(
            f"COPY (SELECT * FROM read_csv_auto('{csv_path}', "
            f"ignore_errors=true, sample_size=-1)) "
            f"TO '{tmp}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
        )
        con.close()
        tmp.rename(dest)
        return ("ok", sid, "")
    except Exception as e:
        tmp.unlink(missing_ok=True)
        return ("error", sid, str(e)[:80])


def phase2_hpd():
    log = _make_logger("HPD")
    log.info(f"Starting  (workers={N_CONVERT} threads)")

    csv_files = list(HPD_CSV_DIR.rglob("*.csv")) if HPD_CSV_DIR.exists() else []
    log.info(f"Found {len(csv_files):,} HPD CSVs locally")

    if not csv_files:
        log.info("No local CSVs — downloading via refresh_data.py …")
        subprocess.run([
            sys.executable, "scripts/refresh_data.py",
            "--source", "HPD_1hr", "--force", "--no-post-refresh",
        ], cwd=str(HERE), check=False)
        log.info("HPD download complete.")
        return

    HPD_OUT_DIR.mkdir(parents=True, exist_ok=True)
    tasks = [
        (str(f), str(HPD_OUT_DIR / (f.stem + ".parquet")))
        for f in csv_files
    ]

    t0 = time.time()
    ok = skip = err = 0
    with ThreadPoolExecutor(max_workers=N_CONVERT) as pool:
        futs = {pool.submit(_convert_hpd_one, t): t for t in tasks}
        for i, fut in enumerate(as_completed(futs), 1):
            status, sid, msg = fut.result()
            if status == "ok":    ok   += 1
            elif status == "skip": skip += 1
            else:
                err += 1
                log.warning(f"  {sid}: {msg}")
            if i % 200 == 0 or i == len(tasks):
                log.info(f"[{i:>5,}/{len(tasks):,}] ok={ok:,} skip={skip} err={err}")

    log.info(f"Done: {ok:,} converted, {skip} skipped, {err} errors  "
             f"({time.time()-t0:.0f}s)")


# ─── Phase 3: NCEI ────────────────────────────────────────────────────────────
def phase3_ncei():
    log = _make_logger("NCEI")
    log.info("Starting")

    src_files = list(NCEI_SRC_DIR.glob("*.15m.parquet")) if NCEI_SRC_DIR.exists() else []
    log.info(f"Found {len(src_files):,} NCEI parquets locally")

    if not src_files:
        log.info("No local parquets — downloading via refresh_data.py …")
        subprocess.run([
            sys.executable, "scripts/refresh_data.py",
            "--source", "NCEI_15min", "--force", "--no-post-refresh",
        ], cwd=str(HERE), check=False)
        log.info("NCEI download complete.")
        return

    NCEI_OUT_DIR.mkdir(parents=True, exist_ok=True)
    copied = skipped = 0
    for src in src_files:
        dest = NCEI_OUT_DIR / src.name
        if dest.exists():
            skipped += 1
            continue
        shutil.copy2(src, dest)
        copied += 1

    log.info(f"Done: {copied:,} copied, {skipped:,} already present")


# ─── Phase 4: ASOS ISD — full historical ─────────────────────────────────────
def phase4_asos_isd():
    log = _make_logger("ASOS_ISD")
    log.info(f"Starting full historical download  (workers={N_DL_ISD} threads)")
    log.info("Estimated time: 24–48 hours. Fully resumable.")

    # Fetch US station list from NOAA isd-history
    isd_history_url = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv"
    log.info("Fetching US station list from isd-history.csv …")
    try:
        client0 = httpx.Client(timeout=60, follow_redirects=True)
        resp = client0.get(isd_history_url)
        resp.raise_for_status()
        client0.close()
        # Columns: USAF,WBAN,STATION NAME,CTRY,ST,CALL,LAT,LON,ELEV(M),BEGIN,END
        us_stems: set[str] = set()
        for line in resp.text.splitlines()[1:]:
            parts = line.split(",")
            if len(parts) < 5:
                continue
            ctry = parts[3].strip().strip('"')
            if ctry != "US":
                continue
            usaf = parts[0].strip().strip('"')
            wban = parts[1].strip().strip('"')
            if usaf and usaf != "999999":
                us_stems.add(f"{usaf}{wban.zfill(5)}")
        log.info(f"US stations in isd-history: {len(us_stems):,}")
    except Exception as e:
        log.error(f"Cannot fetch isd-history.csv: {e} — skipping ASOS ISD phase")
        return

    base_url = "https://www.ncei.noaa.gov/pub/data/noaa/"
    today    = datetime.date.today()
    years    = list(range(1901, today.year + 1))

    # ── Collect ALL download tasks across all years ────────────────────────────
    log.info(f"Scanning NOAA year directories {years[0]}–{years[-1]} …")
    import re as _re
    all_tasks: list[tuple] = []  # (url, pq_dest)
    rate0 = _RateLimiter(NOAA_RATE)

    with httpx.Client(timeout=60, follow_redirects=True) as cl:
        for year in years:
            year_url = f"{base_url}{year}/"
            rate0.wait()
            try:
                html = cl.get(year_url).raise_for_status().text
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    continue
                log.warning(f"  {year}: listing failed ({e.response.status_code})")
                continue
            except Exception as e:
                log.warning(f"  {year}: {e}")
                continue

            year_dir = ISD_OUT_DIR / str(year)
            for fname in _re.findall(r'href="(\d{11}\.csv)"', html):
                if fname[:11] not in us_stems:
                    continue
                pq_dest = year_dir / fname.replace(".csv", ".parquet")
                if not pq_dest.exists():
                    all_tasks.append((year_url + fname, pq_dest))

    log.info(f"Files to download: {len(all_tasks):,}  "
             f"(already present: skipped during scan)")

    if not all_tasks:
        log.info("Nothing to download — all ASOS ISD parquets already exist.")
        return

    # ── Parallel download + convert ────────────────────────────────────────────
    rate = _RateLimiter(NOAA_RATE * N_DL_ISD)   # shared bucket across threads

    def _dl_isd(args: tuple) -> str:
        url, pq_dest = args
        if pq_dest.exists():
            return "skip"
        usaf   = pq_dest.stem[:6]
        csv_tmp = pq_dest.with_suffix(".csv.tmp")
        pq_tmp  = pq_dest.with_suffix(".parquet.tmp")
        try:
            cl = _tls_client()
            _download(cl, url, csv_tmp, rate)
            con = duckdb.connect()
            con.execute(
                f"COPY (SELECT * FROM read_csv_auto('{csv_tmp}', "
                f"ignore_errors=true, sample_size=-1)) "
                f"TO '{pq_tmp}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
            )
            con.close()
            csv_tmp.unlink(missing_ok=True)
            pq_tmp.rename(pq_dest)
            return "ok"
        except Exception as e:
            csv_tmp.unlink(missing_ok=True)
            pq_tmp.unlink(missing_ok=True)
            return f"error:{e}"

    t0 = time.time()
    ok = skip = err = 0
    with ThreadPoolExecutor(max_workers=N_DL_ISD) as pool:
        futs = {pool.submit(_dl_isd, t): t for t in all_tasks}
        for i, fut in enumerate(as_completed(futs), 1):
            result = fut.result()
            if result == "ok":      ok   += 1
            elif result == "skip":  skip += 1
            else:                   err  += 1
            if i % 500 == 0 or i == len(all_tasks):
                elapsed = time.time() - t0
                rate_s  = i / elapsed if elapsed else 1
                eta     = int((len(all_tasks) - i) / rate_s)
                log.info(f"[{i:>7,}/{len(all_tasks):,}] ok={ok:,} err={err} "
                         f"rate={rate_s:.1f}/s eta={eta//3600}h{(eta%3600)//60}m")

    log.info(f"Done: {ok:,} downloaded, {skip} skipped, {err} errors  "
             f"({(time.time()-t0)/3600:.1f}h)")


# ─── Phase 5: ASOS 1-min — full historical ────────────────────────────────────
def phase5_asos_1min():
    log = _make_logger("ASOS_1min")
    log.info(f"Starting full historical download  (workers={N_DL_1MIN} threads)")
    log.info("Estimated time: 48–72 hours for ~66 GB. Fully resumable.")

    base_url = "https://www.ncei.noaa.gov/pub/data/asos-onemin/"
    today    = datetime.date.today()
    years    = list(range(2000, today.year + 1))

    log.info(f"Scanning NOAA year directories {years[0]}–{years[-1]} …")
    import re as _re
    all_tasks: list[tuple] = []   # (url, dest, is_dat)
    rate0 = _RateLimiter(NOAA_RATE)

    with httpx.Client(timeout=60, follow_redirects=True) as cl:
        for year in years:
            year_url = f"{base_url}{year}/"
            rate0.wait()
            try:
                html = cl.get(year_url).raise_for_status().text
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    continue
                log.warning(f"  {year}: listing failed ({e.response.status_code})")
                continue
            except Exception as e:
                log.warning(f"  {year}: {e}")
                continue

            year_dir = ASOS1_OUT_DIR / str(year)
            for fname in _re.findall(r'href="([^"]+\.(?:parquet|dat))"', html):
                dest_name = fname.replace(".dat", ".parquet")
                dest      = year_dir / dest_name
                if not dest.exists():
                    all_tasks.append((year_url + fname, dest, fname.endswith(".dat")))

    log.info(f"Files to download: {len(all_tasks):,}")

    if not all_tasks:
        log.info("Nothing to download — all ASOS 1-min parquets already exist.")
        return

    rate = _RateLimiter(NOAA_RATE * N_DL_1MIN)

    def _dl_1min(args: tuple) -> str:
        url, dest, is_dat = args
        if dest.exists():
            return "skip"
        pq_tmp  = dest.with_suffix(".parquet.tmp")
        dat_tmp = dest.with_suffix(".dat.tmp")
        try:
            cl = _tls_client()
            if not is_dat:
                _download(cl, url, dest, rate)
            else:
                _download(cl, url, dat_tmp, rate)
                con = duckdb.connect()
                con.execute(
                    f"COPY (SELECT * FROM read_csv_auto('{dat_tmp}', ignore_errors=true)) "
                    f"TO '{pq_tmp}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
                )
                con.close()
                dat_tmp.unlink(missing_ok=True)
                pq_tmp.rename(dest)
            return "ok"
        except Exception as e:
            for p in [dat_tmp, pq_tmp]:
                p.unlink(missing_ok=True)
            return f"error:{e}"

    t0 = time.time()
    ok = skip = err = 0
    with ThreadPoolExecutor(max_workers=N_DL_1MIN) as pool:
        futs = {pool.submit(_dl_1min, t): t for t in all_tasks}
        for i, fut in enumerate(as_completed(futs), 1):
            result = fut.result()
            if result == "ok":     ok   += 1
            elif result == "skip": skip += 1
            else:                  err  += 1
            if i % 500 == 0 or i == len(all_tasks):
                elapsed = time.time() - t0
                rate_s  = i / elapsed if elapsed else 1
                eta     = int((len(all_tasks) - i) / rate_s)
                log.info(f"[{i:>7,}/{len(all_tasks):,}] ok={ok:,} err={err} "
                         f"rate={rate_s:.1f}/s eta={eta//3600}h{(eta%3600)//60}m")

    log.info(f"Done: {ok:,} downloaded, {skip} skipped, {err} errors  "
             f"({(time.time()-t0)/3600:.1f}h)")


# ─── Thread-local httpx client (one per download thread) ─────────────────────
_tls = threading.local()

def _tls_client() -> httpx.Client:
    if not hasattr(_tls, "c"):
        _tls.c = httpx.Client(timeout=120, follow_redirects=True)
    return _tls.c


# ─── Phase 6: Recon pipeline ──────────────────────────────────────────────────
def phase6_recon():
    log = _make_logger("recon")
    steps = [
        (["python", "scripts/build_file_inventory.py", "--all"], "build_file_inventory"),
        (["python", "scripts/build_data_profile.py"],             "build_data_profile"),
        (["python", "scripts/build_master_station_list.py"],      "build_master_station_list"),
        (["python", "scripts/plot_coverage.py"],                  "plot_coverage"),
    ]
    for cmd, label in steps:
        log.info(f"Running {label} …")
        t0 = time.time()
        ret = subprocess.run(cmd, cwd=str(HERE))
        if ret.returncode == 0:
            log.info(f"{label} OK  ({time.time()-t0:.0f}s)")
        else:
            log.error(f"{label} FAILED (exit {ret.returncode})")
            log.error(f"Fix the error, then run:  python {' '.join(cmd[1:])}")
            sys.exit(1)


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    _setup_root_log()
    log = _make_logger("main")

    log.info("=" * 60)
    log.info("  WeatherData — Full Re-Download & Restore")
    log.info(f"  Started : {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")
    log.info(f"  CPUs    : {N_CPU}  |  Convert workers : {N_CONVERT}")
    log.info(f"  ISD DL threads : {N_DL_ISD}  |  1-min DL threads : {N_DL_1MIN}")
    log.info("=" * 60)
    log.info("Monitor:  tail -f redownload.log")
    log.info("")

    phase0_setup()

    # ── Run all 5 data sources simultaneously ─────────────────────────────────
    log.info("Launching all 5 sources in parallel …")
    t_all = time.time()

    phases = {
        "GHCN":     phase1_ghcn,
        "HPD":      phase2_hpd,
        "NCEI":     phase3_ncei,
        "ASOS_ISD": phase4_asos_isd,
        "ASOS_1min":phase5_asos_1min,
    }

    failures = []
    with ThreadPoolExecutor(max_workers=len(phases)) as pool:
        futs = {pool.submit(fn): name for name, fn in phases.items()}
        for fut in as_completed(futs):
            name = futs[fut]
            try:
                fut.result()
                elapsed = time.time() - t_all
                log.info(f"✓ {name} finished  ({elapsed/3600:.1f}h elapsed total)")
            except Exception as e:
                log.error(f"✗ {name} FAILED: {e}")
                failures.append(name)

    if failures:
        log.error(f"These sources failed: {failures}")
        log.error("Fix errors above and re-run — completed sources will be skipped.")
        sys.exit(1)

    # ── Recon: runs after ALL data is present ─────────────────────────────────
    log.info("")
    log.info("All sources complete — running recon pipeline …")
    phase6_recon()

    elapsed = time.time() - t_all
    h, rem = divmod(int(elapsed), 3600)
    m, s   = divmod(rem, 60)
    log.info("")
    log.info("=" * 60)
    log.info(f"  ALL DONE in {h}h {m}m {s}s")
    log.info("  Next step:  python launch.py")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
