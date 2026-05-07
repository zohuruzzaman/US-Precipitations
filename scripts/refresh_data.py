#!/usr/bin/env python3
"""
refresh_data.py — Incremental NOAA data refresh for WeatherData archive.

Downloads only data that is NEW since the last recorded refresh date.
Converts immediately to parquet. Appends to file_inventory.duckdb.
Safe to interrupt and resume.

Usage:
  python scripts/refresh_data.py --all
  python scripts/refresh_data.py --source GHCN_daily
  python scripts/refresh_data.py --source GHCN_daily --dry-run
  python scripts/refresh_data.py --source GHCN_daily --force
  python scripts/refresh_data.py --source GHCN_daily --method tarball

Cron (every 3 months at 2am on the 1st):
  0 2 1 */3 * cd ~/Documents/WeatherData && python scripts/refresh_data.py --all >> recon/refresh_cron.log 2>&1
"""

import argparse
import datetime
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from multiprocessing import Pool, cpu_count
from pathlib import Path

import duckdb
import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ─── Paths ────────────────────────────────────────────────────────────────────
BASE        = Path(__file__).resolve().parent.parent   # scripts/ → project root
DATA        = BASE / "data"
RECON       = DATA / "recon"
DB_PATH     = RECON / "file_inventory.duckdb"
MASTER_PQ   = RECON / "master_station_list.parquet"
UPDATE_LOG  = RECON / "update_log.json"
REFRESH_LOG = RECON / "refresh.log"
PENDING_FILE        = RECON / ".refresh_pending.json"
PID_FILE            = RECON / ".refresh_pid"
DONE_SENTINEL       = RECON / ".refresh_done"
INDEX_PROGRESS_FILE = RECON / ".index_progress.json"
REFRESH_COMMAND_FILE = RECON / ".refresh_command.json"
REFRESH_STATE_FILE   = RECON / ".refresh_state.json"
STAGING_ROOT         = DATA / ".refresh_staging"
TMP_ROOT             = DATA / ".refresh_tmp"

# ─── NOAA source configs ───────────────────────────────────────────────────────
# All URLs are here so they're easy to update when NOAA reorganises.
CONFIGS = {
    "GHCN_daily": {
        "base_url":          "https://www.ncei.noaa.gov/pub/data/ghcn/daily/",
        "version_file":      "ghcnd-version.txt",
        "station_list":      "ghcnd-stations.txt",
        "all_dir":           "all/",
        "tarball":           "ghcnd_all.tar.gz",
        "local_parquet_dir": DATA / "GHCNd" / "daily",
        "source_label":      "GHCNd",
        "id_format":         "GHCND_DLY",
        "subfolder":         "daily",
    },
    "ASOS_ISD": {
        "base_url":     "https://www.ncei.noaa.gov/data/global-hourly/access/",
        "local_dir":    DATA / "GHCNh" / "hourly",
        "source_label": "GHCNh",
        "id_format":    "USAF_WBAN",
        "subfolder":    "hourly",
        "inv_source":   "GHCNh",
    },
    "HPD_1hr": {
        "base_url":     "https://www.ncei.noaa.gov/data/coop-hourly-precipitation/v2/access/",
        "local_dir":    DATA / "HPD_1hr" / "raw" / "HPD_1hr",
        "source_label": "HPD_1hr",
        "id_format":    "GHCND_HPD",
        "subfolder":    "raw",
        "inv_source":   "HPD_1hr",
    },
    "NCEI_15min": {
        "base_url":     "https://www.ncei.noaa.gov/pub/data/hpd/auto/v2/beta/15min/all_csv/",
        "local_dir":    DATA / "ASOS" / "15min",
        "source_label": "ASOS",
        "id_format":    "GHCND_HPD",
        "subfolder":    "15min",
        "inv_source":   "ASOS",
    },
    "ASOS_1min": {
        "base_url":     "https://www.ncei.noaa.gov/data/automated-surface-observing-system-one-minute-pg2/access/",
        "local_dir":    DATA / "ASOS" / "1min" / "parquet_final",
        "source_label": "ASOS",
        "id_format":    "ICAO_ASOS",
        "subfolder":    "1min",
        "inv_source":   "ASOS",
    },
}

ALL_SOURCES = list(CONFIGS.keys())
APP_REFRESH_SOURCES = ["GHCN_daily", "ASOS_ISD", "NCEI_15min", "ASOS_1min"]

US_GHCND_PREFIXES = frozenset({
    "US", "RQ", "VQ", "CQ", "GQ", "AQ", "MQ", "FQ", "SQ", "RW",
})
KEEP_ELEMENTS = frozenset({"PRCP", "TMAX", "TMIN", "TAVG"})
STOP_REQUESTED = False


def _stop_requested() -> bool:
    return STOP_REQUESTED

GHCN_SCHEMA = pa.schema([
    ("STATION", pa.string()),
    ("DATE",    pa.date32()),
    ("PRCP",    pa.float64()),
    ("TMAX",    pa.float64()),
    ("TMIN",    pa.float64()),
    ("TAVG",    pa.float64()),
])

GHCNH_SCHEMA = pa.schema([
    ("station_id",    pa.string()),
    ("date",          pa.string()),
    ("report_type",   pa.string()),
    ("month",         pa.int8()),
    ("day",           pa.int8()),
    ("hour",          pa.int8()),
    ("minute",        pa.int8()),
    ("temperature_c", pa.float32()),
    ("dewpoint_c",    pa.float32()),
    ("precip_mm",     pa.float32()),
    ("precip_hours",  pa.int8()),
    ("temp_qc",       pa.string()),
    ("dew_qc",        pa.string()),
    ("precip_qc",     pa.string()),
])

MIN15_SCHEMA = pa.schema([
    ("station_id",        pa.string()),
    ("station_name",      pa.string()),
    ("lat",               pa.float64()),
    ("lon",               pa.float64()),
    ("elevation",         pa.float64()),
    ("element",           pa.string()),
    ("month",             pa.int8()),
    ("day",               pa.int8()),
    ("time",              pa.string()),
    ("precip_in",         pa.float64()),
    ("flag_measurement",  pa.string()),
    ("flag_quality",      pa.string()),
])

# ─── Logging ──────────────────────────────────────────────────────────────────
log = logging.getLogger("refresh")

def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    log.setLevel(level)
    log.propagate = False   # prevent double-logging via root logger
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    if not log.handlers:
        fh = logging.FileHandler(REFRESH_LOG)
        fh.setLevel(level)
        fh.setFormatter(fmt)
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(fmt)
        log.addHandler(fh)
        log.addHandler(ch)


# ─── Index progress (written during --index-only so app can show progress) ────
def _write_index_progress(phase: str, done: int, total: int, msg: str):
    try:
        INDEX_PROGRESS_FILE.write_text(json.dumps({
            "phase": phase, "done": done, "total": total, "msg": msg,
            "ts": time.time(),
        }))
    except Exception:
        pass


def _atomic_write_json(path: Path, data: dict):
    """Write JSON through a same-directory temp file so restarts never see half a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
    os.replace(tmp, path)


def _load_refresh_state() -> dict:
    try:
        if REFRESH_STATE_FILE.exists():
            return json.loads(REFRESH_STATE_FILE.read_text())
    except Exception as e:
        log.warning("Could not read refresh state; starting a fresh state: %s", e)
    return {}


def _save_refresh_state(state: dict):
    _atomic_write_json(REFRESH_STATE_FILE, state)


def _refresh_signature(args, sources: list[str]) -> dict:
    return {
        "sources": sources,
        "force": bool(args.force),
        "method": args.method,
        "workers": int(args.workers),
    }


def _init_refresh_state(args, sources: list[str]) -> dict:
    sig = _refresh_signature(args, sources)
    state = _load_refresh_state()
    if state.get("signature") != sig or state.get("phase") in {"complete", "failed"}:
        run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        state = {
            "run_id": run_id,
            "signature": sig,
            "completed": {},
            "sources_complete": {},
            "created_at": datetime.datetime.now().isoformat(timespec="seconds"),
        }
    state.update({
        "phase": "downloading",
        "pid": os.getpid(),
        "updated_at": datetime.datetime.now().isoformat(timespec="seconds"),
    })
    _save_refresh_state(state)
    return state


def _stage_data_root(run_id: str) -> Path:
    return STAGING_ROOT / run_id / "data"


def _stage_updates_root(run_id: str, source: str) -> Path:
    return STAGING_ROOT / run_id / "updates" / source


def _is_completed(state: dict, key: str) -> bool:
    return key in state.get("completed", {})


def _mark_completed(state: dict, key: str):
    state.setdefault("completed", {})[key] = datetime.datetime.now().isoformat(timespec="seconds")
    state["updated_at"] = datetime.datetime.now().isoformat(timespec="seconds")
    _save_refresh_state(state)


def _source_completed(state: dict, source: str) -> bool:
    return source in state.get("sources_complete", {})


def _mark_source_completed(state: dict, source: str):
    state.setdefault("sources_complete", {})[source] = datetime.datetime.now().isoformat(timespec="seconds")
    state["updated_at"] = datetime.datetime.now().isoformat(timespec="seconds")
    _save_refresh_state(state)


def _safe_station_file(station_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(station_id))


def _write_stage_parquet(path: Path, df: pd.DataFrame, physical_cols: list[str], schema: pa.Schema):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(f".{os.getpid()}.tmp")
    table = pa.Table.from_pandas(df[physical_cols], schema=schema, preserve_index=False)
    pq.write_table(table, tmp, compression="snappy")
    os.replace(tmp, path)


def _stage_partition_update(
    run_id: str,
    source: str,
    partition_values: dict[str, str | int],
    station_id: str,
    rows: pd.DataFrame,
    physical_cols: list[str],
    schema: pa.Schema,
):
    stage_dir = _stage_updates_root(run_id, source)
    for key, value in partition_values.items():
        stage_dir = stage_dir / f"{key}={value}"
    out = stage_dir / f"{_safe_station_file(station_id)}.parquet"
    _write_stage_parquet(out, rows, physical_cols, schema)


def _parse_partition_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for part in path.parts:
        if "=" in part:
            key, value = part.split("=", 1)
            values[key] = value
    return values


# ─── Update log ───────────────────────────────────────────────────────────────
DEFAULT_LOG_DATA = {
    "last_refresh": {
        "GHCN_daily":  {"date": "2025-03-14", "stations_updated": 76198, "new_stations": 0},
        "ASOS_1min":   {"date": "2025-03-14", "stations_updated": 958,   "new_stations": 0},
        "ASOS_ISD":    {"date": "2025-03-14", "stations_updated": 7509,  "new_stations": 0},
        "HPD_1hr":     {"date": "2025-03-14", "stations_updated": 2080,  "new_stations": 0},
        "NCEI_15min":  {"date": "2025-03-14", "stations_updated": 2080,  "new_stations": 0},
    },
    "created":         "2025-03-14",
    "archive_version": "1.0",
}

def load_update_log() -> dict:
    if UPDATE_LOG.exists():
        with open(UPDATE_LOG) as f:
            return json.load(f)
    log.info("No update_log.json found — using defaults")
    return dict(DEFAULT_LOG_DATA)

def save_update_log(data: dict):
    with open(UPDATE_LOG, "w") as f:
        json.dump(data, f, indent=4)

def get_last_refresh_date(log_data: dict, source: str) -> datetime.date:
    s = log_data.get("last_refresh", {}).get(source, {}).get("date", "2020-01-01")
    return datetime.date.fromisoformat(s)

def record_refresh(log_data: dict, source: str, stations_updated: int, new_stations: int):
    log_data.setdefault("last_refresh", {})[source] = {
        "date":             datetime.date.today().isoformat(),
        "stations_updated": stations_updated,
        "new_stations":     new_stations,
    }


class _RateLimiter:
    """Token-bucket rate limiter safe for use across threads."""
    def __init__(self, rate: float):
        self._lock     = threading.Lock()
        self._interval = 1.0 / max(rate, 0.01)
        self._last     = 0.0

    def wait(self):
        with self._lock:
            now  = time.monotonic()
            gap  = self._interval - (now - self._last)
            if gap > 0:
                time.sleep(gap)
            self._last = time.monotonic()


# ─── NOAA HTTP client ─────────────────────────────────────────────────────────
class NOAAClient:
    """Rate-limited HTTP client with exponential-backoff retries."""

    RATE_LIMIT   = 5              # max requests per second
    RETRY_WAITS  = [2, 8, 30]    # seconds before retries 1/2/3

    def __init__(self):
        self._client      = httpx.Client(timeout=120, follow_redirects=True)
        self._interval    = 1.0 / self.RATE_LIMIT
        self._last_req    = 0.0

    def _throttle(self):
        elapsed = time.time() - self._last_req
        if elapsed < self._interval:
            time.sleep(self._interval - elapsed)
        self._last_req = time.time()

    def get(self, url: str) -> httpx.Response:
        for attempt, wait in enumerate([0] + self.RETRY_WAITS):
            if wait:
                log.debug(f"Retry {attempt} for {url} (wait {wait}s)")
                time.sleep(wait)
            self._throttle()
            try:
                resp = self._client.get(url)
                resp.raise_for_status()
                return resp
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    raise
                if attempt < len(self.RETRY_WAITS):
                    log.warning(f"HTTP {e.response.status_code}: {url}")
                else:
                    raise
            except (httpx.TimeoutException, httpx.ConnectError) as e:
                if attempt < len(self.RETRY_WAITS):
                    log.warning(f"{e.__class__.__name__}: {url}")
                else:
                    raise
        raise RuntimeError(f"Exhausted retries: {url}")  # unreachable

    def head(self, url: str) -> httpx.Response | None:
        try:
            self._throttle()
            return self._client.head(url, follow_redirects=True)
        except Exception as e:
            log.debug(f"HEAD failed: {url}: {e}")
            return None

    def download(self, url: str, dest: Path, show_progress: bool = False) -> int:
        """Stream url → dest (via .tmp, renamed on success). Returns bytes written."""
        dest.parent.mkdir(parents=True, exist_ok=True)
        # Include PID in the tmp name so parallel workers (or duplicate processes)
        # targeting the same dest never collide on the same temp file.
        tmp  = dest.with_suffix(f".{os.getpid()}.tmp")
        t0   = time.time()
        nbytes = 0
        self._throttle()
        try:
            with self._client.stream("GET", url, follow_redirects=True) as resp:
                resp.raise_for_status()
                with open(tmp, "wb") as fh:
                    for chunk in resp.iter_bytes(65536):
                        fh.write(chunk)
                        nbytes += len(chunk)
                        if show_progress and nbytes % (50 << 20) < 65536:
                            elapsed = time.time() - t0
                            rate = nbytes / 1e6 / elapsed if elapsed > 0 else 0
                            log.info(f"  ... {nbytes/1e6:.0f} MB  ({rate:.1f} MB/s)")
            tmp.rename(dest)
            return nbytes
        except Exception:
            tmp.unlink(missing_ok=True)
            raise

    def close(self):
        self._client.close()


# ─── Directory listing parser ─────────────────────────────────────────────────
# NOAA uses multiple listing formats depending on the server.
#
# Format A — NOAA table style (ghcn/daily/all/, coop-hourly-precip, etc.):
#   <a href="FILE">FILE</a></td>
#   <td ...>2026-03-14 05:04</td>
#
# Format B — Apache/nginx inline style:
#   <a href="FILE">FILE</a>   2024-01-15 10:30   12K
#
# Both date formats: "2024-01-15 10:30" and "15-Jan-2024 10:30"

_TABLE_RE = re.compile(
    r'href="([^"?/][^"]*)"[^>]*>[^<]*</a>\s*</td>\s*<td[^>]*>\s*'
    r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}|\d{2}-\w{3}-\d{4} \d{2}:\d{2})',
    re.IGNORECASE | re.DOTALL,
)
_INLINE_RE = re.compile(
    r'href="([^"?/][^"]*)"[^>]*>[^<]*</a>\s+'
    r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}|\d{2}-\w{3}-\d{4} \d{2}:\d{2})',
    re.IGNORECASE,
)
_NGINX_MON = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
              "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}

def _parse_date(ds: str) -> datetime.datetime:
    if ds[4] == "-":
        return datetime.datetime.strptime(ds, "%Y-%m-%d %H:%M")
    day, mon, year, hh, mm = re.match(
        r"(\d{2})-(\w{3})-(\d{4}) (\d{2}):(\d{2})", ds
    ).groups()
    return datetime.datetime(int(year), _NGINX_MON[mon.lower()], int(day), int(hh), int(mm))

def parse_dir_listing(html: str) -> dict[str, datetime.datetime]:
    """Return {filename: last_modified} parsed from an HTML directory listing."""
    results: dict[str, datetime.datetime] = {}
    # Try table format first (more specific), then inline
    for pattern in (_TABLE_RE, _INLINE_RE):
        for m in pattern.finditer(html):
            fname = m.group(1)
            if fname.endswith("/") or fname in results:
                continue
            try:
                results[fname] = _parse_date(m.group(2))
            except (ValueError, AttributeError):
                pass
    return results


def _sql_escape_path(path: Path) -> str:
    return str(path).replace("'", "''")


def _rewrite_hive_partition(
    root: Path,
    partition_values: dict[str, str | int],
    station_id: str,
    station_col: str,
    new_rows: pd.DataFrame,
    physical_cols: list[str],
    schema: pa.Schema,
) -> None:
    """Replace one station inside a hive partition without duplicating rows."""
    part_dir = root
    for key, value in partition_values.items():
        part_dir = part_dir / f"{key}={value}"
    part_dir.mkdir(parents=True, exist_ok=True)

    existing_parts = []
    for fp in part_dir.glob("*.parquet"):
        try:
            existing_parts.append(pd.read_parquet(fp, columns=physical_cols))
        except Exception:
            pass

    if existing_parts:
        existing = pd.concat(existing_parts, ignore_index=True)
        existing = existing[existing[station_col].astype(str) != str(station_id)]
    else:
        existing = pd.DataFrame(columns=physical_cols)

    new_phys = new_rows[physical_cols].copy() if len(new_rows) else pd.DataFrame(columns=physical_cols)
    frames = [frame for frame in (existing, new_phys) if len(frame)]
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=physical_cols)

    tmp_dir = part_dir.parent / f".{part_dir.name}.{os.getpid()}.tmp"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(combined[physical_cols], schema=schema, preserve_index=False)
    pq.write_table(table, tmp_dir / "part-0.parquet", compression="snappy")

    backup_dir = part_dir.parent / f".{part_dir.name}.{os.getpid()}.bak"
    if backup_dir.exists():
        shutil.rmtree(backup_dir)
    if part_dir.exists():
        part_dir.rename(backup_dir)
    tmp_dir.rename(part_dir)
    shutil.rmtree(backup_dir, ignore_errors=True)


def _commit_file_tree(stage_root: Path, live_root: Path) -> int:
    """Promote staged parquet files into the live tree with atomic file replaces."""
    if not stage_root.exists():
        return 0
    n = 0
    for src in stage_root.rglob("*.parquet"):
        rel = src.relative_to(stage_root)
        dest = live_root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.replace(src, dest)
        n += 1
    return n


def _commit_partition_updates(
    run_id: str,
    source: str,
    live_root: Path,
    station_col: str,
    physical_cols: list[str],
    schema: pa.Schema,
) -> int:
    updates_root = _stage_updates_root(run_id, source)
    if not updates_root.exists():
        return 0
    n = 0
    for stage_file in sorted(updates_root.rglob("*.parquet")):
        partitions = _parse_partition_values(stage_file.relative_to(updates_root).parent)
        if "year" in partitions:
            partitions["year"] = int(partitions["year"])
        station_id = stage_file.stem
        df = pd.read_parquet(stage_file)
        _rewrite_hive_partition(
            live_root,
            partitions,
            station_id,
            station_col,
            df,
            physical_cols,
            schema,
        )
        n += 1
    return n


def _commit_staged_run(run_id: str, sources: list[str]):
    """Move staged refresh outputs into the app's live data tree."""
    stage_data = _stage_data_root(run_id)
    total = 0
    if "GHCN_daily" in sources:
        _write_index_progress("commit", total, 0, "Publishing staged GHCN daily files…")
        total += _commit_file_tree(stage_data / "GHCNd" / "daily", CONFIGS["GHCN_daily"]["local_parquet_dir"])
    if "ASOS_1min" in sources:
        _write_index_progress("commit", total, 0, "Publishing staged ASOS 1-min files…")
        total += _commit_file_tree(
            stage_data / "ASOS" / "1min" / "parquet_final",
            CONFIGS["ASOS_1min"]["local_dir"],
        )
    if "ASOS_ISD" in sources:
        _write_index_progress("commit", total, 0, "Publishing staged hourly partitions…")
        total += _commit_partition_updates(
            run_id,
            "ASOS_ISD",
            CONFIGS["ASOS_ISD"]["local_dir"],
            "station_id",
            GHCNH_SCHEMA.names,
            GHCNH_SCHEMA,
        )
    if "NCEI_15min" in sources:
        _write_index_progress("commit", total, 0, "Publishing staged 15-min partitions…")
        total += _commit_partition_updates(
            run_id,
            "NCEI_15min",
            CONFIGS["NCEI_15min"]["local_dir"],
            "station_id",
            MIN15_SCHEMA.names,
            MIN15_SCHEMA,
        )
    log.info("Committed %s staged file/partition update(s) for run %s.", f"{total:,}", run_id)


# ─── GHCN-D .dly converter (module-level for Pool pickling) ───────────────────
def _convert_dly_bytes(args: tuple) -> tuple:
    """
    Parse .dly content (bytes) → write parquet at out_path.
    Returns ('ok', out_path, rows) | ('skip', out_path, 0) | ('error', out_path, msg).
    """
    station_id, dly_bytes, out_path_str = args
    import datetime as _dt
    out_path = Path(out_path_str)
    rows: dict[datetime.date, dict] = {}
    try:
        for line in dly_bytes.decode("latin-1", errors="replace").splitlines():
            if len(line) < 29:
                continue
            element = line[17:21]
            if element not in KEEP_ELEMENTS:
                continue
            try:
                year  = int(line[11:15])
                month = int(line[15:17])
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
                mf = line[off + 5:off + 6].strip()
                qf = line[off + 6:off + 7].strip()
                sf = line[off + 7:off + 8].strip()
                if val == -9999 and not mf and not qf and not sf:
                    continue
                try:
                    d = _dt.date(year, month, day)
                except ValueError:
                    continue
                row = rows.setdefault(
                    d,
                    {"STATION": line[0:11], "DATE": d, "PRCP": None, "TMAX": None, "TMIN": None, "TAVG": None},
                )
                row[element] = None if val == -9999 else val / 10.0

        if not rows:
            return ("skip", out_path_str, 0)

        df = pd.DataFrame(rows.values()).sort_values("DATE")
        table = pa.Table.from_pandas(df[list(GHCN_SCHEMA.names)], schema=GHCN_SCHEMA, preserve_index=False)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, str(out_path), compression="snappy")
        return ("ok", out_path_str, len(df))
    except Exception as e:
        return ("error", out_path_str, str(e)[:120])


# Thread-local httpx client (one per worker thread, avoids sharing)
_tls = threading.local()
_DL_RATE: _RateLimiter | None = None   # set in main() before workers start

def _tls_client() -> httpx.Client:
    if not hasattr(_tls, "client"):
        _tls.client = httpx.Client(timeout=120, follow_redirects=True)
    return _tls.client

def _dl_dly_worker(args: tuple) -> tuple:
    """Thread worker: download + convert one GHCN .dly file.
    Returns (status, sid, nbytes, rows, is_new)
    status in {'ok', 'skip', 'error'}
    """
    sid, url, out_path_str, is_new = args
    out_path = Path(out_path_str)
    tmp_pq   = out_path.with_suffix(".parquet.tmp")
    try:
        if _DL_RATE is not None:
            _DL_RATE.wait()
        resp = _tls_client().get(url, follow_redirects=True)
        resp.raise_for_status()
        nbytes = len(resp.content)
        status, _, rows = _convert_dly_bytes((sid, resp.content, str(tmp_pq)))
        if status == "ok":
            tmp_pq.rename(out_path)
            return ("ok", sid, nbytes, rows, is_new)
        tmp_pq.unlink(missing_ok=True)
        return (status, sid, nbytes, 0, is_new)
    except httpx.HTTPStatusError as e:
        if e.response.status_code != 404:
            log.debug(f"HTTP {e.response.status_code} for {sid}")
        tmp_pq.unlink(missing_ok=True)
        return ("error", sid, 0, 0, is_new)
    except Exception as e:
        tmp_pq.unlink(missing_ok=True)
        return ("error", sid, 0, 0, str(e)[:60])


# ─── Refresh result accumulator ───────────────────────────────────────────────
class RefreshResult:
    def __init__(self, source: str):
        self.source            = source
        self.files_downloaded  = 0
        self.stations_updated  = 0
        self.new_stations      = 0
        self.rows_added        = 0
        self.bytes_downloaded  = 0
        self.errors: list[str] = []
        self._t0               = time.time()

    @property
    def elapsed(self) -> float:
        return time.time() - self._t0

    def summary_lines(self) -> list[str]:
        m, s = divmod(int(self.elapsed), 60)
        return [
            f"Source:              {self.source}",
            f"Files downloaded:    {self.files_downloaded:,}",
            f"New stations:        {self.new_stations:,}",
            f"Total rows added:    {self.rows_added:,}",
            f"Download size:       {self.bytes_downloaded / 1e6:.1f} MB",
            f"Time:                {m}m {s}s",
            *(f"Error [{i}]:          {e}" for i, e in enumerate(self.errors[:3])),
        ]


# ─── Inventory helpers ────────────────────────────────────────────────────────
def get_known_ids(source_label: str) -> set[str]:
    """Return station_id_raw values from file_inventory for a given source."""
    try:
        con  = duckdb.connect(str(DB_PATH), read_only=True)
        rows = con.execute(
            "SELECT DISTINCT station_id_raw FROM file_inventory "
            "WHERE source=? AND station_id_raw IS NOT NULL",
            [source_label],
        ).fetchall()
        con.close()
        return {r[0] for r in rows}
    except Exception as e:
        log.warning(f"Could not query inventory for {source_label}: {e}")
        return set()

def get_known_filepaths(source_label: str) -> set[str]:
    try:
        con  = duckdb.connect(str(DB_PATH), read_only=True)
        rows = con.execute(
            "SELECT filepath FROM file_inventory WHERE source=?", [source_label]
        ).fetchall()
        con.close()
        return {r[0] for r in rows}
    except Exception as e:
        log.warning(f"Could not query inventory paths for {source_label}: {e}")
        return set()


def get_station_inventory_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        con = duckdb.connect()
        rows = con.execute(
            "SELECT station_id FROM read_parquet(?) WHERE station_id IS NOT NULL",
            [str(path)],
        ).fetchall()
        con.close()
        return {r[0] for r in rows}
    except Exception as e:
        log.warning(f"Could not query station inventory {path.name}: {e}")
        return set()


def insert_inventory_rows(rows: list[tuple]) -> int:
    """
    Append rows to file_inventory (INSERT OR IGNORE).
    Each tuple: (filepath, filename, extension, file_size_bytes,
                 source, subfolder, schema_group,
                 station_id_raw, id_format, date_part, is_data_file)

    Retries up to 30× with 5s back-off if another process (e.g. Streamlit)
    holds the DB lock (total wait up to ~2.5 min).
    """
    if not rows:
        return 0
    SQL = """
        INSERT OR IGNORE INTO file_inventory
          (filepath, filename, extension, file_size_bytes,
           source, subfolder, schema_group,
           station_id_raw, id_format, date_part,
           is_data_file, anomaly_flag)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,NULL)
    """
    for attempt in range(30):
        try:
            con = duckdb.connect(str(DB_PATH))
            con.executemany(SQL, rows)
            con.close()
            return len(rows)
        except duckdb.IOException as e:
            if "lock" in str(e).lower() and attempt < 29:
                log.warning(
                    f"DB locked by another process (attempt {attempt+1}/30) — "
                    f"retrying in 5s.  Stop the Streamlit app to release the lock faster."
                )
                time.sleep(5)
            else:
                raise
    return 0  # unreachable


def _date_part_from_filename(path: Path) -> str | None:
    """Extract YYYYMM/Year date parts the same way the inventory builder does."""
    name = path.name
    m = re.match(r"^6406[A-Z0-9]{5}(\d{6})\.parquet$", name)
    if m:
        raw = m.group(1)
        return f"{raw[:4]}-{raw[4:6]}"
    m = re.match(r"^asos-1min-pg2-[A-Z0-9]{4}-(\d{6})\.parquet$", name, re.IGNORECASE)
    if m:
        raw = m.group(1)
        return f"{raw[:4]}-{raw[4:6]}"
    m = re.search(r"^(\d{4})\.parquet$", name)
    if m:
        return m.group(1)
    return None


def _inv_row(path: Path, source: str, subfolder: str, station_id: str,
             id_format: str, extension: str, date_part: str | None = None,
             size_path: Path | None = None) -> tuple:
    sg = f"{source}|{subfolder}|{extension}"
    if date_part is None:
        date_part = _date_part_from_filename(path)
    stat_path = size_path or path
    return (
        str(path), path.name, extension, stat_path.stat().st_size,
        source, subfolder, sg, station_id, id_format, date_part, True,
    )


def _save_pending_state(all_inv_rows: list, sources: list[str],
                        new_stations: int, log_data: dict, run_id: str | None = None):
    """Persist download results so --index-only can finish the job."""
    PENDING_FILE.write_text(json.dumps({
        "inv_rows":     [list(r) for r in all_inv_rows],
        "sources":      sources,
        "new_stations": new_stations,
        "log_data":     log_data,
        "run_id":       run_id,
    }))
    DONE_SENTINEL.touch()
    log.info(f"Pending state saved ({len(all_inv_rows):,} rows) → {PENDING_FILE}")


def _run_index_phase():
    """Read pending state, flush to DB, run post_refresh pipeline."""
    if not PENDING_FILE.exists():
        log.error("No pending state file found — nothing to index.")
        return
    data      = json.loads(PENDING_FILE.read_text())
    inv_rows  = [tuple(r) for r in data["inv_rows"]]
    sources   = data["sources"]
    new_st    = data["new_stations"]
    log_data  = data["log_data"]
    run_id    = data.get("run_id")

    if run_id:
        _write_index_progress("commit", 0, 0, "Publishing staged refresh files…")
        try:
            _commit_staged_run(run_id, sources)
        except Exception as e:
            log.error("Could not commit staged refresh files: %s", e, exc_info=True)
            DONE_SENTINEL.touch()
            return

    _write_index_progress("inventory", 0, len(inv_rows),
                          f"Inserting {len(inv_rows):,} new file records…")
    log.info(f"Index phase: inserting {len(inv_rows):,} inventory rows …")
    try:
        inserted = insert_inventory_rows(inv_rows)
    except Exception as e:
        if "lock" in str(e).lower():
            log.warning(
                "Index phase: DB still locked after all retries. "
                "PENDING_FILE left intact — the Streamlit app will "
                "complete indexing automatically on its next page render once the lock "
                "is released. No data was lost."
            )
            # Do NOT touch DONE_SENTINEL here — re-creating it would trigger
            # the app to launch another index process on the next render,
            # causing an infinite re-launch loop.
        else:
            log.error(f"Index phase failed unexpectedly: {e}")
        return
    log.info(f"  Inserted {inserted:,} new rows.")
    _write_index_progress("profiling", 0, 0, "Profiling new files (reading metadata)…")

    if sources:
        log.info(f"Running post_refresh for: {sources}")
        post_refresh(sources, new_stations=new_st)

    save_update_log(log_data)
    PENDING_FILE.unlink(missing_ok=True)
    DONE_SENTINEL.unlink(missing_ok=True)
    REFRESH_COMMAND_FILE.unlink(missing_ok=True)
    REFRESH_STATE_FILE.unlink(missing_ok=True)
    if run_id:
        shutil.rmtree(STAGING_ROOT / run_id, ignore_errors=True)
    _write_index_progress("done", 1, 1, "Indexing complete — new data is ready.")
    log.info("Index phase complete.")


# ─── Source: GHCN Daily ───────────────────────────────────────────────────────
def refresh_ghcn_daily(
    client: NOAAClient,
    log_data: dict,
    dry_run: bool,
    force: bool,
    method: str,
    result: RefreshResult,
    all_inv_rows: list, workers: int = 1, state: dict | None = None,
):
    cfg       = CONFIGS["GHCN_daily"]
    base_url  = cfg["base_url"]
    out_dir   = cfg["local_parquet_dir"]
    last_date = get_last_refresh_date(log_data, "GHCN_daily")
    log.info(f"GHCN_daily: last_refresh={last_date}  method={method}")

    if state and _source_completed(state, "GHCN_daily"):
        log.info("GHCN_daily already staged in this refresh run — skipping download.")
        result.files_downloaded = 1
        result.stations_updated = int(log_data.get("last_refresh", {}).get("GHCN_daily", {}).get("stations_updated", 0) or 0)
        return

    # ── Step 1: Version check ─────────────────────────────────────────────────
    try:
        new_version = client.get(base_url + cfg["version_file"]).text.strip()
    except Exception as e:
        log.error(f"Cannot fetch GHCN version file: {e}")
        return

    saved_version = log_data.get("ghcn_version", "")
    log.info(f"GHCN version: server={new_version!r}  saved={saved_version!r}")

    if new_version == saved_version and not force:
        log.info("GHCN version unchanged — skipping.  Use --force to override.")
        return

    # ── Step 2: Station list ──────────────────────────────────────────────────
    try:
        station_lines = client.get(base_url + cfg["station_list"]).text.splitlines()
    except Exception as e:
        log.error(f"Cannot fetch GHCN station list: {e}")
        return

    all_us_stations = {
        line[:11].strip()
        for line in station_lines
        if len(line) >= 11 and line[:2] in US_GHCND_PREFIXES
    }
    known_ids      = get_station_inventory_ids(RECON / "station_inventory_daily.parquet")
    new_station_ids = all_us_stations - known_ids
    log.info(
        f"GHCN stations: server={len(all_us_stations):,}  "
        f"in_archive={len(known_ids):,}  new={len(new_station_ids):,}"
    )

    if dry_run:
        log.info(f"[DRY RUN] Would refresh {len(all_us_stations):,} GHCN stations "
                 f"(version {saved_version!r} → {new_version!r})")
        if new_station_ids:
            sample = sorted(new_station_ids)[:5]
            log.info(f"[DRY RUN] New station sample: {sample}")
        return

    # The app reads year-level GHCNd parquets. Delegate to the maintained
    # year-rebuild pipeline so refreshes replace those year files instead of
    # adding duplicate station-level files beside them.
    refresh_years = max(2, datetime.date.today().year - last_date.year + 1)
    stage_out_dir = (
        _stage_data_root(state["run_id"]) / "GHCNd" / "daily"
        if state else out_dir
    )
    cmd = [
        sys.executable,
        "scripts/ghcn_daily.py",
        "--mode", "refresh",
        "--refresh-years", str(refresh_years),
        "--workers", str(max(1, min(workers, 4))),
        "--io-threads", "4",
        "--output-dir", str(stage_out_dir),
        "--existing-dir", str(out_dir),
    ]
    log.info("Running GHCNd year-level refresh: %s", " ".join(cmd))
    proc = subprocess.Popen(cmd, cwd=str(BASE))
    while proc.poll() is None:
        if _stop_requested():
            log.warning("Stopping GHCNd child refresh process …")
            proc.terminate()
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            result.errors.append("GHCNd refresh stopped")
            return
        time.sleep(2)
    if proc.returncode != 0:
        log.error("GHCNd refresh failed with exit code %s.", proc.returncode)
        result.errors.append("GHCNd refresh failed")
        return

    result.files_downloaded = refresh_years
    result.stations_updated = len(all_us_stations)
    log_data["ghcn_version"] = new_version
    if state:
        _mark_source_completed(state, "GHCN_daily")


def _ghcn_incremental(
    client, base_url, cfg, out_dir,
    all_us_stations, new_station_ids, known_ids,
    last_date, force, result: RefreshResult,
    all_inv_rows: list, workers: int = 1,
):
    """Fetch NOAA directory listing, find modified files, download and convert."""
    all_dir_url = base_url + cfg["all_dir"]
    file_dates: dict[str, datetime.datetime] = {}

    log.info(f"Fetching GHCN directory listing …")
    try:
        html       = client.get(all_dir_url).text
        file_dates = parse_dir_listing(html)
        log.info(f"  Listing: {len(file_dates):,} entries parsed")
    except Exception as e:
        log.warning(f"Directory listing failed ({e}) — will download new stations only")

    cutoff = (
        # Use end-of-day *before* last_date so any file updated on the same
        # calendar day as the previous refresh is always re-checked.
        datetime.datetime.combine(last_date - datetime.timedelta(days=1), datetime.time.max)
        if not force else datetime.datetime(1970, 1, 1)
    )
    to_download: list[str] = []

    for sid in all_us_stations:
        fname    = f"{sid}.dly"
        out_path = out_dir / f"{sid}.parquet"
        if file_dates:
            fdt = file_dates.get(fname)
            if fdt is None:
                continue
            if fdt <= cutoff and not force:
                continue
        else:
            if out_path.exists() and not force:
                continue
        to_download.append(sid)

    log.info(f"GHCN incremental: {len(to_download):,} stations to download")
    out_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    tasks = [
        (sid,
         base_url + cfg["all_dir"] + f"{sid}.dly",
         str(out_dir / f"{sid}.parquet"),
         sid in new_station_ids)
        for sid in to_download
    ]

    if workers > 1:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        log.info(f"  Parallel download: {workers} workers")
        done_ref  = [0]          # mutable container so the value is shared
        done_lock = threading.Lock()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {pool.submit(_dl_dly_worker, t): t[0] for t in tasks}
            for fut in as_completed(futs):
                if _stop_requested():
                    log.warning("Stop requested — ending GHCN incremental loop.")
                    break
                status, sid, nbytes, rows, is_new = fut.result()
                with done_lock:
                    done_ref[0] += 1
                    done_count = done_ref[0]
                if status == "ok":
                    result.files_downloaded += 1
                    result.rows_added       += rows
                    result.bytes_downloaded += nbytes
                    if is_new:
                        result.new_stations += 1
                    all_inv_rows.append(
                        _inv_row(out_dir / f"{sid}.parquet",
                                 "GHCN_daily", "ghcnd_all_parquet",
                                 sid, "GHCND_DLY", ".parquet")
                    )
                elif status == "error":
                    result.errors.append(str(rows or sid)[:60])

                if done_count % 500 == 0 or done_count == len(tasks):
                    elapsed = time.time() - t0
                    rate    = done_count / elapsed if elapsed > 0 else 1
                    eta     = int((len(tasks) - done_count) / rate)
                    log.info(
                        f"  [{done_count:>6,}/{len(tasks):,}]  "
                        f"done={result.files_downloaded:,}  "
                        f"errors={len(result.errors)}  eta={eta}s"
                    )
    else:
        for i, (sid, url, out_path_str, is_new) in enumerate(tasks, 1):
            if _stop_requested():
                log.warning("Stop requested — ending GHCN incremental loop.")
                break
            out_path = Path(out_path_str)
            tmp_pq   = out_path.with_suffix(".parquet.tmp")
            try:
                resp = client.get(url)
                result.bytes_downloaded += len(resp.content)
                status, _, rows = _convert_dly_bytes((sid, resp.content, str(tmp_pq)))
                if status == "ok":
                    tmp_pq.rename(out_path)
                    result.files_downloaded += 1
                    result.rows_added       += rows
                    if is_new:
                        result.new_stations += 1
                    all_inv_rows.append(
                        _inv_row(out_path, "GHCN_daily", "ghcnd_all_parquet",
                                 sid, "GHCND_DLY", ".parquet")
                    )
                elif status == "error":
                    result.errors.append(f"{sid}: {rows}")
            except httpx.HTTPStatusError as e:
                if e.response.status_code != 404:
                    result.errors.append(f"{sid}: HTTP {e.response.status_code}")
            except Exception as e:
                result.errors.append(f"{sid}: {e}")
                tmp_pq.unlink(missing_ok=True)

            if i % 500 == 0 or i == len(tasks):
                elapsed = time.time() - t0
                rate    = i / elapsed if elapsed > 0 else 1
                eta     = int((len(tasks) - i) / rate)
                log.info(
                    f"  [{i:>6,}/{len(tasks):,}]  "
                    f"done={result.files_downloaded:,}  "
                    f"errors={len(result.errors)}  eta={eta}s"
                )

    result.stations_updated = result.files_downloaded
    log.info(f"GHCN incremental done: {result.files_downloaded:,} updated")


def _ghcn_tarball(
    client, base_url, cfg, out_dir,
    all_us_stations, result: RefreshResult,
    all_inv_rows: list,
):
    """Download full tarball, stream-extract US .dly, parallel-convert to parquet."""
    tarball_url = base_url + cfg["tarball"]
    log.info(f"Downloading GHCN tarball (~3 GB): {tarball_url}")

    with tempfile.TemporaryDirectory(prefix="ghcn_tar_") as tmpdir:
        tar_path = Path(tmpdir) / "ghcnd_all.tar.gz"
        try:
            result.bytes_downloaded += client.download(tarball_url, tar_path, show_progress=True)
        except Exception as e:
            log.error(f"Tarball download failed: {e}")
            return

        log.info("Streaming .dly files from tarball …")
        out_dir.mkdir(parents=True, exist_ok=True)
        tasks:    list[tuple] = []
        n_nonUS = 0

        try:
            with tarfile.open(tar_path, "r:gz") as tf:
                for member in tf:
                    if _stop_requested():
                        log.warning("Stop requested — ending GHCN tarball extraction.")
                        break
                    if not member.name.endswith(".dly"):
                        continue
                    stem = os.path.splitext(os.path.basename(member.name))[0].upper()
                    if stem[:2] not in US_GHCND_PREFIXES:
                        n_nonUS += 1
                        continue
                    fobj = tf.extractfile(member)
                    if fobj is None:
                        continue
                    out_path = out_dir / f"{stem}.parquet"
                    tasks.append((stem, fobj.read(), str(out_path)))
        except Exception as e:
            log.error(f"Tarball extraction error: {e}")
            return

        log.info(
            f"  {len(tasks):,} US .dly files to convert  "
            f"({n_nonUS:,} non-US skipped)"
        )
        n_workers = min(cpu_count(), 8)
        inv_rows: list[tuple] = []

        with Pool(n_workers) as pool:
            for status, path_str, rows_or_msg in pool.imap_unordered(
                _convert_dly_bytes, tasks, chunksize=50
            ):
                if status == "ok":
                    result.files_downloaded += 1
                    result.rows_added       += rows_or_msg
                    p = Path(path_str)
                    inv_rows.append(
                        _inv_row(p, "GHCN_daily", "ghcnd_all_parquet",
                                 p.stem, "GHCND_DLY", ".parquet")
                    )
                elif status == "error":
                    result.errors.append(str(rows_or_msg)[:80])

    all_inv_rows.extend(inv_rows)
    result.stations_updated = result.files_downloaded
    log.info(f"GHCN tarball done: {result.files_downloaded:,} converted")


# ─── Source: ASOS ISD (Global Hourly) ────────────────────────────────────────
def _parse_isd_tmp(raw: str):
    if raw is None or pd.isna(raw):
        return None, ""
    m = re.match(r"([+-]?\d{4}),(\w)", str(raw))
    if not m:
        return None, ""
    val = int(m.group(1))
    qc = m.group(2)
    if val in (9999, -9999):
        return None, qc
    return val / 10.0, qc


def _parse_isd_aa1(raw: str):
    if raw is None or pd.isna(raw) or not str(raw):
        return None, None, ""
    parts = str(raw).split(",")
    if len(parts) < 4:
        return None, None, ""
    try:
        period = int(parts[0])
        depth = int(parts[1])
        qc = parts[3]
        return period, None if depth == 9999 else depth / 10.0, qc
    except (ValueError, IndexError):
        return None, None, ""


def _parse_isd_csv(csv_path: Path, station_id: str, state: str) -> pd.DataFrame:
    src = pd.read_csv(csv_path, dtype=str, low_memory=False)
    if src.empty or "DATE" not in src.columns:
        return pd.DataFrame(columns=["year", "state", *GHCNH_SCHEMA.names])

    records = []
    for _, row in src.iterrows():
        datestr = str(row.get("DATE", "")).strip()
        try:
            dt = datetime.datetime.fromisoformat(datestr.replace("Z", "+00:00").replace("z", "+00:00"))
        except ValueError:
            continue

        temp_c, temp_qc = _parse_isd_tmp(row.get("TMP"))
        dew_c, dew_qc = _parse_isd_tmp(row.get("DEW"))
        precip_hours, precip_mm, precip_qc = _parse_isd_aa1(row.get("AA1"))
        if temp_c is None and dew_c is None and precip_mm is None:
            continue

        records.append({
            "station_id": station_id,
            "state": state,
            "date": datestr,
            "report_type": str(row.get("REPORT_TYPE", "") or ""),
            "year": dt.year,
            "month": dt.month,
            "day": dt.day,
            "hour": dt.hour,
            "minute": dt.minute,
            "temperature_c": temp_c,
            "dewpoint_c": dew_c,
            "precip_mm": precip_mm,
            "precip_hours": precip_hours,
            "temp_qc": temp_qc,
            "dew_qc": dew_qc,
            "precip_qc": precip_qc,
        })

    return pd.DataFrame(records)


def refresh_asos_isd(
    client: NOAAClient,
    log_data: dict,
    dry_run: bool,
    force: bool,
    result: RefreshResult,
    all_inv_rows: list,
    state: dict | None = None,
):
    cfg       = CONFIGS["ASOS_ISD"]
    base_url  = cfg["base_url"]
    local_dir = cfg["local_dir"]
    last_date = get_last_refresh_date(log_data, "ASOS_ISD")
    log.info(f"ASOS_ISD: last_refresh={last_date}")
    if state and _source_completed(state, "ASOS_ISD"):
        log.info("ASOS_ISD already staged in this refresh run — skipping download.")
        result.files_downloaded = 1
        result.stations_updated = int(log_data.get("last_refresh", {}).get("ASOS_ISD", {}).get("stations_updated", 0) or 0)
        return

    # Load ISD IDs for stations in the live hourly archive.
    try:
        con = duckdb.connect()
        inv_hourly = RECON / "station_inventory_hourly.parquet"
        if inv_hourly.exists():
            rows = con.execute(
                "SELECT station_id, state FROM read_parquet(?) WHERE station_id IS NOT NULL",
                [str(inv_hourly)],
            ).fetchall()
        else:
            rows = []
        con.close()
    except Exception as e:
        log.error(f"Cannot load ISD IDs from hourly station inventory: {e}")
        return

    state_by_isd: dict[str, str] = {}
    state_by_usaf: dict[str, str] = {}
    for isd_id, row_state in rows:
        if not isd_id or not row_state:
            continue
        isd_id = str(isd_id)
        state_by_isd[isd_id] = str(row_state)
        state_by_usaf.setdefault(isd_id[:6], str(row_state))

    our_usaf = set(state_by_usaf)
    log.info(f"ASOS_ISD: {len(our_usaf):,} USAF stations to check")

    today  = datetime.date.today()
    years  = [today.year - 1, today.year]
    cutoff = (
        # Use end-of-day *before* last_date so any file updated on the same
        # calendar day as the previous refresh is always re-checked.
        datetime.datetime.combine(last_date - datetime.timedelta(days=1), datetime.time.max)
        if not force else datetime.datetime(1970, 1, 1)
    )

    # (isd_id, state, year, url, local_csv_dest)
    to_download: list[tuple[str, str, int, str, Path]] = []

    for year in years:
        year_url = base_url + f"{year}/"
        try:
            file_dates = parse_dir_listing(client.get(year_url).text)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                log.debug(f"ISD {year}/ not published yet — skipping")
            else:
                log.warning(f"Cannot list ISD year dir {year}: {e}")
            continue
        except Exception as e:
            log.warning(f"Cannot list ISD year dir {year}: {e}")
            continue

        for fname, fdt in file_dates.items():
            if not fname.endswith(".csv"):
                continue
            isd_id = os.path.splitext(fname)[0]
            usaf = isd_id[:6]
            if usaf not in our_usaf:
                continue
            state_code = state_by_isd.get(isd_id) or state_by_usaf.get(usaf)
            if not state_code:
                continue
            if fdt <= cutoff and not force:
                continue
            key = f"ASOS_ISD:{year}:{isd_id}"
            if state and _is_completed(state, key):
                continue
            tmp_csv = TMP_ROOT / "ASOS_ISD" / str(year) / fname
            to_download.append((isd_id, state_code, year, year_url + fname, tmp_csv))

    log.info(f"ASOS_ISD: {len(to_download):,} station-year files to download")

    if dry_run:
        log.info(f"[DRY RUN] Would download {len(to_download):,} ISD files")
        return

    inv_rows: list[tuple] = []

    for i, (isd_id, state_code, src_year, url, csv_path) in enumerate(to_download, 1):
        if _stop_requested():
            log.warning("Stop requested — ending ASOS_ISD loop.")
            break
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            nbytes = client.download(url, csv_path)
            df = _parse_isd_csv(csv_path, isd_id, state_code)
            csv_path.unlink(missing_ok=True)
            if df.empty:
                if state:
                    _mark_completed(state, f"ASOS_ISD:{src_year}:{isd_id}")
                continue
            for (year, part_state), grp in df.groupby(["year", "state"], dropna=False):
                if state:
                    _stage_partition_update(
                        state["run_id"],
                        "ASOS_ISD",
                        {"year": int(year), "state": str(part_state)},
                        isd_id,
                        grp,
                        GHCNH_SCHEMA.names,
                        GHCNH_SCHEMA,
                    )
                else:
                    _rewrite_hive_partition(
                        local_dir,
                        {"year": int(year), "state": str(part_state)},
                        isd_id,
                        "station_id",
                        grp,
                        GHCNH_SCHEMA.names,
                        GHCNH_SCHEMA,
                    )
            if not state:
                done_marker = local_dir / str(int(df["year"].max())) / f".{isd_id}.done"
                done_marker.parent.mkdir(parents=True, exist_ok=True)
                done_marker.touch()
            else:
                _mark_completed(state, f"ASOS_ISD:{src_year}:{isd_id}")
            result.files_downloaded += 1
            result.bytes_downloaded += nbytes
        except Exception as e:
            log.warning(f"ISD {isd_id}: {e}")
            result.errors.append(f"{isd_id}: {e}")
            csv_path.unlink(missing_ok=True)

        if i % 200 == 0 or i == len(to_download):
            log.info(f"  [{i:>5,}/{len(to_download):,}]  done={result.files_downloaded:,}")

    all_inv_rows.extend(inv_rows)
    result.stations_updated = result.files_downloaded
    if state:
        _mark_source_completed(state, "ASOS_ISD")
    log.info(f"ASOS_ISD done: {result.files_downloaded:,} files")


# ─── Source: HPD Hourly Precipitation ────────────────────────────────────────
def refresh_hpd_1hr(
    client: NOAAClient,
    log_data: dict,
    dry_run: bool,
    force: bool,
    result: RefreshResult,
    all_inv_rows: list,
):
    cfg       = CONFIGS["HPD_1hr"]
    base_url  = cfg["base_url"]
    local_dir = cfg["local_dir"]
    last_date = get_last_refresh_date(log_data, "HPD_1hr")
    log.info(f"HPD_1hr: last_refresh={last_date}")

    try:
        file_dates = parse_dir_listing(client.get(base_url).text)
    except Exception as e:
        log.error(f"Cannot fetch HPD directory listing: {e}")
        return

    cutoff    = (
        datetime.datetime.combine(last_date, datetime.time.min)
        if not force else datetime.datetime(1970, 1, 1)
    )
    known_ids = get_known_ids("HPD_1hr")

    to_download: list[tuple[str, str, Path, bool]] = []
    for fname, fdt in file_dates.items():
        if not fname.endswith(".csv"):
            continue
        sid  = os.path.splitext(fname)[0]
        dest = local_dir / (sid + ".parquet")
        is_new = sid not in known_ids
        if fdt <= cutoff and dest.exists() and not force:
            continue
        to_download.append((sid, base_url + fname, dest, is_new))

    log.info(
        f"HPD_1hr: {len(to_download):,} files to update  "
        f"({sum(1 for *_, n in to_download if n)} new stations)"
    )

    if dry_run:
        log.info(f"[DRY RUN] Would download {len(to_download):,} HPD files")
        return

    local_dir.mkdir(parents=True, exist_ok=True)
    inv_rows: list[tuple] = []

    for i, (sid, url, dest, is_new) in enumerate(to_download, 1):
        if _stop_requested():
            log.warning("Stop requested — ending HPD loop.")
            break
        # Use .csv (not .csv.tmp) — download() appends .tmp internally
        csv_path = dest.with_suffix(".csv")
        tmp_pq   = dest.with_suffix(".parquet.tmp")
        try:
            nbytes   = client.download(url, csv_path)
            con2     = duckdb.connect()
            csv_rows = con2.execute(
                f"SELECT COUNT(*) FROM read_csv_auto('{csv_path}', ignore_errors=true, sample_size=-1)"
            ).fetchone()[0]
            con2.execute(
                f"COPY (SELECT * FROM read_csv_auto('{csv_path}', ignore_errors=true, sample_size=-1)) "
                f"TO '{tmp_pq}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
            )
            pq_rows = con2.execute(
                f"SELECT COUNT(*) FROM read_parquet('{tmp_pq}')"
            ).fetchone()[0]
            con2.close()

            # HPD CSVs have metadata header lines that DuckDB counts but skips
            # during COPY, so pq_rows < csv_rows is normal. Only reject empty output.
            if pq_rows == 0:
                log.warning(f"HPD {sid}: empty parquet (csv={csv_rows} rows) — skipping")
                for p in [csv_path, tmp_pq]:
                    p.unlink(missing_ok=True)
                result.errors.append(f"{sid}: empty parquet")
                continue
            if pq_rows > csv_rows:
                log.warning(f"HPD {sid}: row mismatch csv={csv_rows} pq={pq_rows} — skipping")

            csv_path.unlink(missing_ok=True)
            tmp_pq.rename(dest)
            result.files_downloaded += 1
            result.bytes_downloaded += nbytes
            if is_new:
                result.new_stations += 1
            inv_rows.append(
                _inv_row(dest, "HPD_1hr", "raw", sid, "GHCND_HPD", ".parquet")
            )
        except Exception as e:
            log.warning(f"HPD {sid}: {e}")
            result.errors.append(f"{sid}: {e}")
            for p in [csv_path, tmp_pq]:
                p.unlink(missing_ok=True)

        if i % 200 == 0 or i == len(to_download):
            log.info(f"  [{i:>5,}/{len(to_download):,}]  done={result.files_downloaded:,}")

    all_inv_rows.extend(inv_rows)
    result.stations_updated = result.files_downloaded
    log.info(f"HPD_1hr done: {result.files_downloaded:,} files")


# ─── Source: NCEI 15-min ──────────────────────────────────────────────────────
_STATE_15MIN = {
    "01":"AL","02":"AZ","03":"AR","04":"CA","05":"CO","06":"CT","07":"DE",
    "08":"FL","09":"GA","10":"ID","11":"IL","12":"IN","13":"IA","14":"KS",
    "15":"KY","16":"LA","17":"ME","18":"MD","19":"MA","20":"MI","21":"MN",
    "22":"MS","23":"MO","24":"MT","25":"NE","26":"NV","27":"NH","28":"NJ",
    "29":"NM","30":"NY","31":"NC","32":"ND","33":"OH","34":"OK","35":"OR",
    "36":"PA","37":"RI","38":"SC","39":"SD","40":"TN","41":"TX","42":"UT",
    "43":"VT","44":"VA","45":"WA","46":"WV","47":"WI","48":"WY","50":"AK",
    "51":"HI","66":"PR","67":"VI","91":"PI",
}
_TERRITORY_15MIN = {"AQ":"AS","CQ":"MP","GQ":"GU","RQ":"PR","VQ":"VI","RW":"PR"}
_SLOTS_15MIN = [f"{h:02d}{m:02d}" for h in range(24) for m in (0, 15, 30, 45)]


def _state_15min(station_id: str) -> str:
    if station_id.startswith("USC00") and len(station_id) >= 7:
        return _STATE_15MIN.get(station_id[5:7], station_id[5:7])
    return _TERRITORY_15MIN.get(station_id[:2], station_id[:2])


def _parse_15min_csv(csv_path: Path, station_id: str) -> pd.DataFrame:
    records = []
    for chunk in pd.read_csv(csv_path, dtype=str, chunksize=50_000, low_memory=False):
        for _, row in chunk.iterrows():
            sid = str(row.get("STATION", station_id) or station_id).strip()
            elem = str(row.get("ELEMENT", "") or "").strip()
            if elem not in ("QPCP", "QGAG"):
                continue
            datestr = str(row.get("DATE", "") or "").strip()
            try:
                yr_s, mo_s, dy_s = datestr.split("-")[:3]
                yr, mo, dy = int(yr_s), int(mo_s), int(dy_s)
            except (ValueError, IndexError):
                continue

            state = _state_15min(sid)
            lat = pd.to_numeric(row.get("LATITUDE"), errors="coerce")
            lon = pd.to_numeric(row.get("LONGITUDE"), errors="coerce")
            elev = pd.to_numeric(row.get("ELEVATION"), errors="coerce")
            station_name = row.get("NAME") or row.get("STATION_NAME") or None

            for slot in _SLOTS_15MIN:
                raw = str(row.get(f"{slot}Val", "") or "").strip()
                if raw in ("", "nan", "-9999", "99999"):
                    continue
                value = pd.to_numeric(raw, errors="coerce")
                if pd.isna(value) or value < 0:
                    continue
                mf = str(row.get(f"{slot}MF", "") or "").strip()
                qf = str(row.get(f"{slot}QF", "") or "").strip()
                records.append({
                    "station_id": sid,
                    "station_name": station_name,
                    "state": state,
                    "lat": None if pd.isna(lat) else float(lat),
                    "lon": None if pd.isna(lon) else float(lon),
                    "elevation": None if pd.isna(elev) else float(elev),
                    "element": elem,
                    "year": yr,
                    "month": mo,
                    "day": dy,
                    "time": slot,
                    "precip_in": float(value) / 100.0,
                    "flag_measurement": "" if mf == "nan" else mf,
                    "flag_quality": "" if qf == "nan" else qf,
                })
    return pd.DataFrame(records)


def refresh_ncei_15min(
    client: NOAAClient,
    log_data: dict,
    dry_run: bool,
    force: bool,
    result: RefreshResult,
    all_inv_rows: list,
    state: dict | None = None,
):
    cfg       = CONFIGS["NCEI_15min"]
    base_url  = cfg["base_url"]
    local_dir = cfg["local_dir"]
    last_date = get_last_refresh_date(log_data, "NCEI_15min")
    log.info(f"NCEI_15min: last_refresh={last_date}")
    if state and _source_completed(state, "NCEI_15min"):
        log.info("NCEI_15min already staged in this refresh run — skipping download.")
        result.files_downloaded = 1
        result.stations_updated = int(log_data.get("last_refresh", {}).get("NCEI_15min", {}).get("stations_updated", 0) or 0)
        return

    try:
        file_dates = parse_dir_listing(client.get(base_url).text)
    except Exception as e:
        log.error(f"Cannot fetch NCEI 15-min directory listing: {e}")
        return

    cutoff    = (
        datetime.datetime.combine(last_date - datetime.timedelta(days=1), datetime.time.max)
        if not force else datetime.datetime(1970, 1, 1)
    )
    known_ids = get_station_inventory_ids(RECON / "station_inventory_15min.parquet")
    if not known_ids:
        known_ids = get_known_ids("ASOS")

    to_download: list[tuple[str, str, Path, bool]] = []
    for fname, fdt in file_dates.items():
        # New endpoint uses "{STATIONID}.15m.csv" naming
        if not fname.endswith(".15m.csv"):
            continue
        sid  = fname[:-len(".15m.csv")]
        dest = local_dir / (sid + ".15m.parquet")
        is_new = sid not in known_ids
        if fdt <= cutoff and not force:
            continue
        if state and _is_completed(state, f"NCEI_15min:{sid}"):
            continue
        to_download.append((sid, base_url + fname, dest, is_new))

    log.info(f"NCEI_15min: {len(to_download):,} files to update")

    if dry_run:
        log.info(f"[DRY RUN] Would download {len(to_download):,} NCEI 15-min files")
        return

    inv_rows: list[tuple] = []

    for i, (sid, url, dest, is_new) in enumerate(to_download, 1):
        if _stop_requested():
            log.warning("Stop requested — ending NCEI_15min loop.")
            break
        # Use .raw.csv (not .raw.tmp) — download() appends .tmp internally
        raw_csv = TMP_ROOT / "15min" / (sid + ".raw.csv")
        try:
            raw_csv.parent.mkdir(parents=True, exist_ok=True)
            nbytes = client.download(url, raw_csv)
            df = _parse_15min_csv(raw_csv, sid)
            raw_csv.unlink(missing_ok=True)
            if df.empty:
                if state:
                    _mark_completed(state, f"NCEI_15min:{sid}")
                continue
            for (year, part_state), grp in df.groupby(["year", "state"], dropna=False):
                if state:
                    _stage_partition_update(
                        state["run_id"],
                        "NCEI_15min",
                        {"year": int(year), "state": str(part_state)},
                        sid,
                        grp,
                        MIN15_SCHEMA.names,
                        MIN15_SCHEMA,
                    )
                else:
                    _rewrite_hive_partition(
                        local_dir,
                        {"year": int(year), "state": str(part_state)},
                        sid,
                        "station_id",
                        grp,
                        MIN15_SCHEMA.names,
                        MIN15_SCHEMA,
                    )
            result.files_downloaded += 1
            result.bytes_downloaded += nbytes
            if is_new:
                result.new_stations += 1
            if state:
                _mark_completed(state, f"NCEI_15min:{sid}")
        except Exception as e:
            log.warning(f"NCEI 15-min {sid}: {e}")
            result.errors.append(f"{sid}: {e}")
            raw_csv.unlink(missing_ok=True)

        if i % 200 == 0 or i == len(to_download):
            log.info(f"  [{i:>5,}/{len(to_download):,}]  done={result.files_downloaded:,}")

    all_inv_rows.extend(inv_rows)
    result.stations_updated = result.files_downloaded
    if state:
        _mark_source_completed(state, "NCEI_15min")
    log.info(f"NCEI_15min done: {result.files_downloaded:,} files")


# ─── Source: ASOS 1-min ───────────────────────────────────────────────────────
_ASOS_1MIN_NEW_RE = re.compile(r"^asos-1min-pg2-([A-Z0-9]{4})-(\d{6})\.dat$", re.IGNORECASE)
_ASOS_1MIN_OLD_RE = re.compile(r"^6406([A-Z0-9]{5})(\d{6})\.dat$", re.IGNORECASE)


def _asos_1min_parts(fname: str) -> tuple[str | None, str | None]:
    """Return (ICAO, YYYY-MM) for old 6406 and current pg2 filenames."""
    m = _ASOS_1MIN_NEW_RE.match(fname)
    if m:
        raw = m.group(2)
        return m.group(1).upper(), f"{raw[:4]}-{raw[4:6]}"
    m = _ASOS_1MIN_OLD_RE.match(fname)
    if m:
        raw = m.group(2)
        return m.group(1)[1:].upper(), f"{raw[:4]}-{raw[4:6]}"
    return None, None


def _parse_float(raw: str):
    raw = str(raw).strip()
    if not raw or raw.upper() in {"M", "MM", "9999"}:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _parse_asos_1min_pg2_dat(raw_path: Path, out_path: Path) -> int:
    records = []
    with open(raw_path, "r", errors="ignore") as fh:
        for line in fh:
            parts = line.split()
            if len(parts) < 10 or len(parts[0]) < 9 or len(parts[1]) < 19:
                continue

            obs = parts[1]
            try:
                local_dt = datetime.datetime.strptime(obs[3:15], "%Y%m%d%H%M")
                utc_hhmm = obs[15:19]
                utc_dt = local_dt.replace(hour=int(utc_hhmm[:2]), minute=int(utc_hhmm[2:]))
                delta_min = (utc_dt.hour * 60 + utc_dt.minute) - (local_dt.hour * 60 + local_dt.minute)
                if delta_min < -720:
                    utc_dt += datetime.timedelta(days=1)
                elif delta_min > 720:
                    utc_dt -= datetime.timedelta(days=1)
            except ValueError:
                continue

            precip_idx = 4 if len(parts) >= 11 and _parse_float(parts[4]) is not None else 3
            records.append({
                "wban": parts[0][:5],
                "icao": parts[0][5:9],
                "timestamp_utc": utc_dt,
                "precip_id": parts[2],
                "precip_in": _parse_float(parts[precip_idx]),
                "temp_f": _parse_float(parts[-2]),
                "dew_f": _parse_float(parts[-1]),
            })

    if not records:
        return 0
    df = pd.DataFrame.from_records(records)
    df.to_parquet(out_path, index=False)
    return len(df)


def refresh_asos_1min(
    client: NOAAClient,
    log_data: dict,
    dry_run: bool,
    force: bool,
    result: RefreshResult,
    all_inv_rows: list,
    state: dict | None = None,
):
    cfg       = CONFIGS["ASOS_1min"]
    base_url  = cfg["base_url"]
    local_dir = cfg["local_dir"]
    last_date = get_last_refresh_date(log_data, "ASOS_1min")
    log.info(f"ASOS_1min: last_refresh={last_date}")
    if state and _source_completed(state, "ASOS_1min"):
        log.info("ASOS_1min already staged in this refresh run — skipping download.")
        result.files_downloaded = 1
        result.stations_updated = int(log_data.get("last_refresh", {}).get("ASOS_1min", {}).get("stations_updated", 0) or 0)
        return

    today  = datetime.date.today()
    # Re-check two years before the previous refresh because this feed can
    # backfill monthly files after the original observation month.
    years  = list(range(max(2000, last_date.year - 2), today.year + 1))
    cutoff = (
        # Use end-of-day *before* last_date so any file updated on the same
        # calendar day as the previous refresh is always re-checked.
        datetime.datetime.combine(last_date - datetime.timedelta(days=1), datetime.time.max)
        if not force else datetime.datetime(1970, 1, 1)
    )

    # (year, fname, url, dest, station_id_raw, date_part)
    to_download: list[tuple[int, str, str, Path, str | None, str | None]] = []

    for year in years:
        year_url = base_url + f"{year}/"
        try:
            year_dates = parse_dir_listing(client.get(year_url).text)
        except Exception as e:
            log.warning(f"Cannot list ASOS 1-min year dir {year}: {e}")
            continue

        month_dirs = sorted({
            name.rstrip("/")
            for name in year_dates
            if re.fullmatch(r"\d{2}/?", name)
        })
        listings: list[tuple[str, dict[str, datetime.datetime]]] = []
        if month_dirs:
            for month in month_dirs:
                month_url = year_url + f"{month}/"
                try:
                    listings.append((month_url, parse_dir_listing(client.get(month_url).text)))
                except Exception as e:
                    log.warning(f"Cannot list ASOS 1-min month dir {year}/{month}: {e}")
        else:
            listings.append((year_url, year_dates))

        year_dir = local_dir / str(year)
        for listing_url, file_dates in listings:
            for fname, fdt in file_dates.items():
                # Server files may be .dat (raw) or .parquet (pre-converted)
                if not (fname.endswith(".dat") or fname.endswith(".parquet")):
                    continue
                parts_name = fname if fname.endswith(".dat") else fname.replace(".parquet", ".dat")
                icao, date_part = _asos_1min_parts(parts_name)
                if not icao:
                    continue
                dest_name = fname.replace(".dat", ".parquet") if fname.endswith(".dat") else fname
                dest      = year_dir / dest_name
                if fdt <= cutoff and dest.exists() and not force:
                    continue
                if state and _is_completed(state, f"ASOS_1min:{year}:{dest_name}"):
                    continue
                station_id = f"0{icao}" if icao else None
                to_download.append((year, fname, listing_url + fname, dest, station_id, date_part))

    log.info(f"ASOS_1min: {len(to_download):,} files across years {years}")

    if dry_run:
        log.info(f"[DRY RUN] Would download {len(to_download):,} ASOS 1-min files")
        return

    inv_rows: list[tuple] = []

    for i, (year, fname, url, live_dest, station_id, date_part) in enumerate(to_download, 1):
        if _stop_requested():
            log.warning("Stop requested — ending ASOS_1min loop.")
            break
        dest = (
            _stage_data_root(state["run_id"]) / "ASOS" / "1min" / "parquet_final" / str(year) / live_dest.name
            if state else live_dest
        )
        dest.parent.mkdir(parents=True, exist_ok=True)
        raw_dest = dest
        try:
            raw_dest = dest.with_suffix(".dat") if fname.endswith(".dat") else dest
            nbytes = client.download(url, raw_dest)

            if fname.endswith(".parquet"):
                # download() already renamed tmp → dest; nothing more to do
                rows = 0
            else:
                tmp_pq = dest.with_suffix(".parquet.tmp")
                try:
                    rows = _parse_asos_1min_pg2_dat(raw_dest, tmp_pq)
                    if rows <= 0:
                        tmp_pq.unlink(missing_ok=True)
                        raw_dest.unlink(missing_ok=True)
                        continue
                    raw_dest.unlink(missing_ok=True)
                    tmp_pq.rename(dest)
                except Exception as e:
                    log.warning(
                        f"ASOS 1-min .dat parse failed for {fname}: {e}\n"
                        "  → run ASOS/asos1min.py manually for this station-year"
                    )
                    raw_dest.unlink(missing_ok=True)
                    tmp_pq.unlink(missing_ok=True)
                    result.errors.append(f"{fname}: dat parse failed")
                    continue

            if not station_id:
                stem = os.path.splitext(fname)[0]
                station_id = ("0" + stem[5:9]) if len(stem) >= 9 else stem

            result.files_downloaded += 1
            result.bytes_downloaded += nbytes
            result.rows_added       += rows
            if state:
                _mark_completed(state, f"ASOS_1min:{year}:{live_dest.name}")
            inv_rows.append(
                _inv_row(
                    live_dest if state else dest,
                    "ASOS", "1min", station_id, "ICAO_ASOS", ".parquet",
                    date_part=date_part, size_path=dest if state else None,
                )
            )
        except Exception as e:
            log.warning(f"ASOS 1-min {fname}: {e}")
            result.errors.append(f"{fname}: {e}")
            raw_dest.unlink(missing_ok=True)
            dest.unlink(missing_ok=True)

        if i % 200 == 0 or i == len(to_download):
            log.info(f"  [{i:>5,}/{len(to_download):,}]  done={result.files_downloaded:,}")

    all_inv_rows.extend(inv_rows)
    result.stations_updated = result.files_downloaded
    if state:
        _mark_source_completed(state, "ASOS_1min")
    log.info(f"ASOS_1min done: {result.files_downloaded:,} files")


# ─── Post-refresh pipeline ────────────────────────────────────────────────────
def post_refresh(sources: list[str], new_stations: int = 0):
    """
    After downloading new files:
      1. Profile new parquet files (footer-only, Pool(8))  ← always fast
      2. Rebuild affected station inventory parquet(s), coverage tables, and
         master_station_list.parquet. Existing stations still need updated
         file counts/date ranges, so this cannot depend only on new station IDs.
    Note: build_file_inventory.py is NOT called here; insert_inventory_rows()
    already inserted new rows with INSERT OR IGNORE during the download phase.
    Note: plot_coverage.py is NOT called here; run manually for recon purposes.
    """
    root = str(BASE)

    refresh_to_inventory_source = {
        "GHCN_daily": "GHCNd",
        "ASOS_ISD": "GHCNh",
        "NCEI_15min": "ASOS",
        "ASOS_1min": "ASOS",
    }
    file_inventory_sources = sorted(
        {refresh_to_inventory_source[s] for s in sources if s in refresh_to_inventory_source}
    )

    for inv_source in file_inventory_sources:
        _write_index_progress("inventory", 0, len(file_inventory_sources),
                              f"Rebuilding file inventory for {inv_source}…")
        log.info("Rebuilding file_inventory rows for %s …", inv_source)
        ret = subprocess.run(
            [sys.executable, "scripts/build_file_inventory.py", "--source", inv_source],
            cwd=root, capture_output=True, text=True,
        )
        if ret.returncode != 0:
            log.warning("  file inventory rebuild failed for %s:\n%s", inv_source, ret.stderr[:800])

    source_to_inventory = {
        "GHCN_daily": "daily",
        "ASOS_ISD": "hourly",
        "NCEI_15min": "15min",
        "ASOS_1min": "1min",
    }
    inventory_sources = sorted(
        {source_to_inventory[s] for s in sources if s in source_to_inventory}
    )

    if inventory_sources:
        _write_index_progress("coverage", 0, 3, "Rebuilding affected station inventories…")
        cmd = [sys.executable, "scripts/build_data_profile.py", "--station-inventory-only"]
        for src in inventory_sources:
            cmd += ["--source", src]
        log.info(f"Rebuilding station inventories: {', '.join(inventory_sources)}")
        ret = subprocess.run(cmd, cwd=root, capture_output=True, text=True)
        if ret.returncode != 0:
            log.warning(f"  station inventory rebuild failed:\n{ret.stderr[:800]}")
        else:
            log.info("  Station inventories rebuilt.")

        _write_index_progress("coverage", 1, 3, "Rebuilding station coverage tables…")
        ret = subprocess.run(
            [sys.executable, "scripts/build_data_profile.py", "--coverage-tables-only"],
            cwd=root, capture_output=True, text=True,
        )
        if ret.returncode != 0:
            log.warning(f"  coverage table rebuild failed:\n{ret.stderr[:800]}")
        else:
            log.info("  Coverage tables rebuilt.")
    else:
        log.info("No station-inventory rebuild target for downloaded source(s): %s", sources)

    _write_index_progress("master_list", 2, 3, "Rebuilding master station list…")
    log.info("Rebuilding master_station_list.parquet …")
    ret = subprocess.run(
        [sys.executable, "scripts/build_master_station_list.py"],
        cwd=root, capture_output=True, text=True,
    )
    if ret.returncode != 0:
        log.warning(f"  build_master_station_list failed:\n{ret.stderr[:800]}")
    else:
        log.info("  master_station_list.parquet updated.")


def _profile_new_files():
    """Profile parquet files in file_inventory that have no data_profile row yet."""
    create_profile_sql = """
        CREATE TABLE IF NOT EXISTS data_profile (
            filepath       VARCHAR PRIMARY KEY,
            source         VARCHAR,
            station_id_raw VARCHAR,
            id_format      VARCHAR,
            total_rows     BIGINT,
            min_datetime   VARCHAR,
            max_datetime   VARCHAR,
            has_data       BOOLEAN,
            error          VARCHAR
        )
    """
    try:
        con  = duckdb.connect(str(DB_PATH))
        con.execute(create_profile_sql)
        rows = con.execute("""
            SELECT fi.filepath, fi.source, fi.station_id_raw, fi.id_format,
                   fi.subfolder, fi.extension
            FROM   file_inventory fi
            LEFT JOIN data_profile dp ON fi.filepath = dp.filepath
            WHERE  fi.is_data_file
              AND  fi.extension IN ('.parquet', '.15m.parquet')
              AND  dp.filepath IS NULL
            LIMIT  100000
        """).fetchall()
        con.close()
    except Exception as e:
        log.warning(f"Cannot find unprofiled files: {e}")
        return

    if not rows:
        log.info("No new files to profile.")
        return

    log.info(f"Profiling {len(rows):,} new files …")

    sys.path.insert(0, str(BASE / "scripts"))
    try:
        from build_data_profile import _profile_one, DATETIME_COL_MAP
    except ImportError as e:
        log.warning(f"Cannot import build_data_profile: {e}")
        return

    args = [
        (fp, src, sid, idf, DATETIME_COL_MAP.get((src, sub, ext)))
        for fp, src, sid, idf, sub, ext in rows
    ]

    n_workers = min(cpu_count(), 8)
    total_files = len(args)
    results   = []
    with Pool(n_workers) as pool:
        for i, res in enumerate(pool.imap_unordered(_profile_one, args, chunksize=50), 1):
            results.append(res)
            if i % 500 == 0 or i == total_files:
                _write_index_progress(
                    "profiling", i, total_files,
                    f"Profiling new files: {i:,} / {total_files:,}",
                )

    _write_index_progress("writing", 0, 1, f"Writing {len(results):,} profile records to database…")
    con = duckdb.connect(str(DB_PATH))
    con.executemany("""
        INSERT OR REPLACE INTO data_profile
          (filepath, source, station_id_raw, id_format,
           total_rows, min_datetime, max_datetime, has_data, error)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, results)
    con.close()
    log.info(f"  Profiled {len(results):,} files.")


# ─── Summary ──────────────────────────────────────────────────────────────────
def print_final_summary(results: list[RefreshResult], log_data: dict):
    print("\n" + "=" * 55)
    print("=== REFRESH COMPLETE ===")
    print("=" * 55)

    for r in results:
        print()
        for line in r.summary_lines():
            print(f"  {line}")

    # DB row counts
    print()
    try:
        con = duckdb.connect(str(DB_PATH), read_only=True)
        inv  = con.execute("SELECT COUNT(*) FROM file_inventory").fetchone()[0]
        prof = con.execute("SELECT COUNT(*) FROM data_profile").fetchone()[0]
        con.close()
        print(f"  file_inventory.duckdb:   {inv:,} rows")
        print(f"  data_profile:            {prof:,} rows profiled")
    except Exception:
        pass

    try:
        con2   = duckdb.connect()
        master = con2.execute(
            f"SELECT COUNT(*) FROM read_parquet('{MASTER_PQ}')"
        ).fetchone()[0]
        con2.close()
        print(f"  master_station_list:     {master:,} stations")
    except Exception:
        pass

    next_refresh = datetime.date.today() + datetime.timedelta(days=180)
    print(f"\n  Next scheduled refresh:  {next_refresh.isoformat()}  (+6 months)")
    print()
    print("  Cron (every 3 months at 2am on the 1st):")
    print("  0 2 1 */3 * cd ~/Documents/WeatherData && \\")
    print("    python scripts/refresh_data.py --all >> recon/refresh_cron.log 2>&1")
    print("=" * 55)


# ─── CLI ──────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(
        description="Incremental NOAA data refresh for WeatherData archive.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python scripts/refresh_data.py --all --dry-run
  python scripts/refresh_data.py --source GHCN_daily --dry-run
  python scripts/refresh_data.py --source GHCN_daily --method tarball
  python scripts/refresh_data.py --source HPD_1hr --force
  python scripts/refresh_data.py --all
""",
    )
    g = p.add_mutually_exclusive_group(required=False)
    g.add_argument("--all",    action="store_true",      help="Refresh all sources")
    g.add_argument("--source", choices=ALL_SOURCES,      help="Refresh one source")

    p.add_argument("--dry-run",  action="store_true",
                   help="Check what's available without downloading")
    p.add_argument("--force",    action="store_true",
                   help="Re-download even if version appears current")
    p.add_argument("--method",   choices=["incremental", "tarball"], default=None,
                   help="GHCN_daily: download strategy (default: auto)")
    p.add_argument("--no-post-refresh", action="store_true",
                   help="Skip inventory rebuild and coverage regeneration")
    p.add_argument("--workers", type=int, default=1, metavar="N",
                   help="Parallel download workers for GHCN (default: 1)")
    p.add_argument("--index-only", action="store_true",
                   help="Skip downloads; just flush pending inventory to DB and run post_refresh")
    p.add_argument("--pid-file", default=str(PID_FILE),
                   help=argparse.SUPPRESS)
    p.add_argument("--verbose", "-v", action="store_true")
    return p.parse_args()


def _ghcn_method(log_data: dict, override: str | None) -> str:
    """Auto-select tarball if last GHCN refresh was ≥60 days ago."""
    if override:
        return override
    last  = get_last_refresh_date(log_data, "GHCN_daily")
    days  = (datetime.date.today() - last).days
    chosen = "tarball" if days >= 60 else "incremental"
    log.info(f"GHCN method auto-selected: {chosen} ({days} days since last refresh)")
    return chosen


def main():
    args = parse_args()
    setup_logging(args.verbose)
    pid_file = Path(args.pid_file)

    # ── --index-only: flush pending state to DB, run post_refresh ─────────────
    if getattr(args, "index_only", False):
        pid_file.write_text(str(os.getpid()))
        try:
            _run_index_phase()
        finally:
            pid_file.unlink(missing_ok=True)
            # Keep progress file a moment so app can detect "done" on next poll,
            # then clean it up after 30s (the app will have caught it by then).
            # We simply leave it; app removes it via _read_index_progress check.
        return

    if not args.all and not args.source:
        log.error("Specify --all, --source SOURCE, or --index-only")
        sys.exit(1)

    # ── Write PID file so app can track/stop us ────────────────────────────────
    pid_file.write_text(str(os.getpid()))

    # ── Graceful SIGTERM handler ───────────────────────────────────────────────
    _stop = [False]
    def _handle_sigterm(sig, frame):
        global STOP_REQUESTED
        log.warning("SIGTERM received — stopping after current file …")
        STOP_REQUESTED = True
        _stop[0] = True
    signal.signal(signal.SIGTERM, _handle_sigterm)

    log.info("=" * 55)
    log.info(f"WeatherData refresh  {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")
    log.info(f"dry_run={args.dry_run}  force={args.force}  "
             f"method={args.method}  workers={args.workers}")
    log.info("=" * 55)

    log_data = load_update_log()
    sources  = APP_REFRESH_SOURCES if args.all else [args.source]
    workers  = max(1, min(args.workers, 8))
    refresh_state = None if args.dry_run else _init_refresh_state(args, sources)

    # Set up global rate limiter (shared across download threads)
    global _DL_RATE
    _DL_RATE = _RateLimiter(rate=5.0 * workers)   # scale rate with workers

    client = NOAAClient()
    results: list[RefreshResult] = []
    all_inv_rows: list = []

    try:
        for source in sources:
            if _stop[0]:
                break
            log.info(f"\n{'─'*40}")
            log.info(f"Refreshing: {source}")
            log.info(f"{'─'*40}")
            r = RefreshResult(source)
            results.append(r)
            try:
                if source == "GHCN_daily":
                    refresh_ghcn_daily(client, log_data, args.dry_run, args.force,
                                       _ghcn_method(log_data, args.method), r,
                                       all_inv_rows, workers, refresh_state)
                elif source == "ASOS_ISD":
                    refresh_asos_isd(client, log_data, args.dry_run, args.force,
                                     r, all_inv_rows, refresh_state)
                elif source == "HPD_1hr":
                    refresh_hpd_1hr(client, log_data, args.dry_run, args.force,
                                    r, all_inv_rows)
                elif source == "NCEI_15min":
                    refresh_ncei_15min(client, log_data, args.dry_run, args.force,
                                       r, all_inv_rows, refresh_state)
                elif source == "ASOS_1min":
                    refresh_asos_1min(client, log_data, args.dry_run, args.force,
                                      r, all_inv_rows, refresh_state)
                # Record refresh date (JSON only, no DB)
                record_refresh(log_data, source, r.stations_updated, r.new_stations)

            except KeyboardInterrupt:
                log.warning(f"Interrupted during {source} — saving progress")
                break
            except Exception as e:
                log.error(f"{source} refresh failed: {e}", exc_info=True)
                r.errors.append(str(e))

        # ── Post-download: save pending state + optionally run index phase ─────
        if not args.dry_run:
            sources_dl   = [r.source for r in results if r.files_downloaded > 0]
            total_new    = sum(r.new_stations for r in results)
            if not refresh_state:
                save_update_log(log_data)

            if sources_dl:
                log.info(f"\nDownloads complete: {sum(r.files_downloaded for r in results):,} files")
                if refresh_state:
                    refresh_state["phase"] = "downloads_complete"
                    refresh_state["updated_at"] = datetime.datetime.now().isoformat(timespec="seconds")
                    _save_refresh_state(refresh_state)
                _save_pending_state(
                    all_inv_rows,
                    sources_dl,
                    total_new,
                    log_data,
                    refresh_state.get("run_id") if refresh_state else None,
                )

                if not args.no_post_refresh:
                    log.info("Running index phase …")
                    _run_index_phase()
            else:
                log.info("No files downloaded — nothing to index")
                REFRESH_COMMAND_FILE.unlink(missing_ok=True)
                REFRESH_STATE_FILE.unlink(missing_ok=True)

    finally:
        client.close()
        pid_file.unlink(missing_ok=True)

    print_final_summary(results, log_data)


if __name__ == "__main__":
    main()
