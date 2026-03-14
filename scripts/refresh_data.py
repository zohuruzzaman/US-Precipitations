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
import subprocess
import sys
import tarfile
import tempfile
import time
from multiprocessing import Pool, cpu_count
from pathlib import Path

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq

# ─── Paths ────────────────────────────────────────────────────────────────────
BASE        = Path.home() / "Documents" / "WeatherData"
RECON       = BASE / "recon"
DB_PATH     = RECON / "file_inventory.duckdb"
MASTER_PQ   = RECON / "master_station_list.parquet"
UPDATE_LOG  = RECON / "update_log.json"
REFRESH_LOG = RECON / "refresh.log"

# ─── NOAA source configs ───────────────────────────────────────────────────────
# All URLs are here so they're easy to update when NOAA reorganises.
CONFIGS = {
    "GHCN_daily": {
        "base_url":          "https://www.ncei.noaa.gov/pub/data/ghcn/daily/",
        "version_file":      "ghcnd-version.txt",
        "station_list":      "ghcnd-stations.txt",
        "all_dir":           "all/",
        "tarball":           "ghcnd_all.tar.gz",
        "local_parquet_dir": BASE / "GHCN_daily" / "ghcnd_all_parquet",
        "source_label":      "GHCN_daily",
        "id_format":         "GHCND_DLY",
        "subfolder":         "ghcnd_all_parquet",
    },
    "ASOS_ISD": {
        "base_url":     "https://www.ncei.noaa.gov/data/global-hourly/access/",
        "local_dir":    BASE / "ASOS" / "processed_parquet",
        "source_label": "ASOS",
        "id_format":    "USAF_WBAN",
        "subfolder":    "processed_parquet",
        "inv_source":   "ASOS",
    },
    "HPD_1hr": {
        "base_url":     "https://www.ncei.noaa.gov/data/coop-hourly-precipitation/v2/access/",
        "local_dir":    BASE / "HPD_1hr" / "raw" / "HPD_1hr",
        "source_label": "HPD_1hr",
        "id_format":    "GHCND_HPD",
        "subfolder":    "raw",
        "inv_source":   "HPD_1hr",
    },
    "NCEI_15min": {
        "base_url":     "https://www.ncei.noaa.gov/data/15-min-precipitation/access/",
        "local_dir":    BASE / "NCEI_15min_raw" / "raw_parquet" / "hpd15_station_parquet",
        "source_label": "NCEI_15min_raw",
        "id_format":    "GHCND_HPD",
        "subfolder":    "raw_parquet",
        "inv_source":   "NCEI_15min_raw",
    },
    "ASOS_1min": {
        "base_url":     "https://www.ncei.noaa.gov/data/automated-surface-observing-system-one-minute-pg1/access/",
        "local_dir":    BASE / "ASOS" / "asos_1min_parquet_master",
        "source_label": "ASOS",
        "id_format":    "ICAO_ASOS",
        "subfolder":    "asos_1min_parquet_master",
        "inv_source":   "ASOS",
    },
}

ALL_SOURCES = list(CONFIGS.keys())

US_GHCND_PREFIXES = frozenset({
    "US", "RQ", "VQ", "CQ", "GQ", "AQ", "MQ", "FQ", "SQ", "RW",
})
KEEP_ELEMENTS = frozenset({"PRCP", "TMAX", "TMIN", "TAVG"})

GHCN_SCHEMA = pa.schema([
    ("station_id", pa.string()),
    ("date",       pa.date32()),
    ("element",    pa.string()),
    ("value",      pa.int32()),
    ("mflag",      pa.string()),
    ("qflag",      pa.string()),
    ("sflag",      pa.string()),
])

# ─── Logging ──────────────────────────────────────────────────────────────────
log = logging.getLogger("refresh")

def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    log.setLevel(level)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh = logging.FileHandler(REFRESH_LOG)
    fh.setLevel(level)
    fh.setFormatter(fmt)
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)
    log.addHandler(fh)
    log.addHandler(ch)


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
        tmp  = dest.with_suffix(dest.suffix + ".tmp")
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
# NOAA uses Apache/nginx HTML listings.  Both formats captured by one regex.
_LISTING_RE = re.compile(
    r'href="([^"?/][^"]*)"[^>]*>\s*(?:\1|[^<]+)</a>\s+'
    r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}'       # Apache:  2024-01-15 10:30
    r'|\d{2}-\w{3}-\d{4} \d{2}:\d{2})',      # nginx:   15-Jan-2024 10:30
    re.IGNORECASE,
)
_NGINX_MON = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
              "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}

def parse_dir_listing(html: str) -> dict[str, datetime.datetime]:
    """Return {filename: last_modified} parsed from an HTML directory listing."""
    results: dict[str, datetime.datetime] = {}
    for m in _LISTING_RE.finditer(html):
        fname = m.group(1)
        if fname.endswith("/"):
            continue
        ds = m.group(2)
        try:
            if ds[4] == "-":
                dt = datetime.datetime.strptime(ds, "%Y-%m-%d %H:%M")
            else:
                day, mon, year, hh, mm = re.match(
                    r"(\d{2})-(\w{3})-(\d{4}) (\d{2}):(\d{2})", ds
                ).groups()
                dt = datetime.datetime(
                    int(year), _NGINX_MON[mon.lower()], int(day), int(hh), int(mm)
                )
            results[fname] = dt
        except (ValueError, AttributeError):
            pass
    return results


# ─── GHCN-D .dly converter (module-level for Pool pickling) ───────────────────
def _convert_dly_bytes(args: tuple) -> tuple:
    """
    Parse .dly content (bytes) → write parquet at out_path.
    Returns ('ok', out_path, rows) | ('skip', out_path, 0) | ('error', out_path, msg).
    """
    station_id, dly_bytes, out_path_str = args
    import datetime as _dt
    out_path = Path(out_path_str)
    sid_list, dates, elements, values, mf_list, qf_list, sf_list = [], [], [], [], [], [], []
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
                sid_list.append(line[0:11])
                dates.append(d)
                elements.append(element)
                values.append(None if val == -9999 else val)
                mf_list.append(mf or None)
                qf_list.append(qf or None)
                sf_list.append(sf or None)

        if not dates:
            return ("skip", out_path_str, 0)

        table = pa.table({
            "station_id": pa.array(sid_list,   type=pa.string()),
            "date":       pa.array(dates,       type=pa.date32()),
            "element":    pa.array(elements,    type=pa.string()),
            "value":      pa.array(values,      type=pa.int32()),
            "mflag":      pa.array(mf_list,     type=pa.string()),
            "qflag":      pa.array(qf_list,     type=pa.string()),
            "sflag":      pa.array(sf_list,     type=pa.string()),
        }, schema=GHCN_SCHEMA)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, str(out_path), compression="snappy")
        return ("ok", out_path_str, len(dates))
    except Exception as e:
        return ("error", out_path_str, str(e)[:120])


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

def insert_inventory_rows(rows: list[tuple]) -> int:
    """
    Append rows to file_inventory (INSERT OR IGNORE).
    Each tuple: (filepath, filename, extension, file_size_bytes,
                 source, subfolder, schema_group,
                 station_id_raw, id_format, date_part, is_data_file)
    """
    if not rows:
        return 0
    con = duckdb.connect(str(DB_PATH))
    SQL = """
        INSERT OR IGNORE INTO file_inventory
          (filepath, filename, extension, file_size_bytes,
           source, subfolder, schema_group,
           station_id_raw, id_format, date_part,
           is_data_file, anomaly_flag)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,NULL)
    """
    con.executemany(SQL, rows)
    con.close()
    return len(rows)

def _inv_row(path: Path, source: str, subfolder: str, station_id: str,
             id_format: str, extension: str, date_part: str | None = None) -> tuple:
    sg = f"{source}|{subfolder}|{extension}"
    return (
        str(path), path.name, extension, path.stat().st_size,
        source, subfolder, sg, station_id, id_format, date_part, True,
    )


# ─── Source: GHCN Daily ───────────────────────────────────────────────────────
def refresh_ghcn_daily(
    client: NOAAClient,
    log_data: dict,
    dry_run: bool,
    force: bool,
    method: str,
    result: RefreshResult,
):
    cfg       = CONFIGS["GHCN_daily"]
    base_url  = cfg["base_url"]
    out_dir   = cfg["local_parquet_dir"]
    last_date = get_last_refresh_date(log_data, "GHCN_daily")
    log.info(f"GHCN_daily: last_refresh={last_date}  method={method}")

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
    known_ids      = get_known_ids("GHCN_daily")
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

    # ── Step 3: Download + convert ────────────────────────────────────────────
    if method == "tarball":
        _ghcn_tarball(client, base_url, cfg, out_dir, all_us_stations, result)
    else:
        _ghcn_incremental(client, base_url, cfg, out_dir, all_us_stations,
                          new_station_ids, known_ids, last_date, force, result)

    log_data["ghcn_version"] = new_version
    record_refresh(log_data, "GHCN_daily", result.stations_updated, result.new_stations)
    save_update_log(log_data)


def _ghcn_incremental(
    client, base_url, cfg, out_dir,
    all_us_stations, new_station_ids, known_ids,
    last_date, force, result: RefreshResult,
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
        datetime.datetime.combine(last_date, datetime.time.min)
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
            if fdt <= cutoff and out_path.exists():
                continue
        else:
            if out_path.exists() and not force:
                continue
        to_download.append(sid)

    log.info(f"GHCN incremental: {len(to_download):,} stations to download")
    out_dir.mkdir(parents=True, exist_ok=True)
    inv_rows = []
    t0       = time.time()

    for i, sid in enumerate(to_download, 1):
        url      = base_url + cfg["all_dir"] + f"{sid}.dly"
        out_path = out_dir / f"{sid}.parquet"
        tmp_pq   = out_path.with_suffix(".parquet.tmp")

        try:
            resp = client.get(url)
            result.bytes_downloaded += len(resp.content)
            status, _, rows = _convert_dly_bytes((sid, resp.content, str(tmp_pq)))

            if status == "ok":
                tmp_pq.rename(out_path)
                result.files_downloaded += 1
                result.rows_added       += rows
                if sid in new_station_ids:
                    result.new_stations += 1
                inv_rows.append(
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

        if i % 500 == 0 or i == len(to_download):
            elapsed = time.time() - t0
            rate    = i / elapsed if elapsed > 0 else 0
            eta     = (len(to_download) - i) / rate if rate > 0 else 0
            log.info(
                f"  [{i:>6,}/{len(to_download):,}]  "
                f"done={result.files_downloaded:,}  errors={len(result.errors)}  "
                f"eta={eta:.0f}s"
            )

    inserted = insert_inventory_rows(inv_rows)
    result.stations_updated = result.files_downloaded
    log.info(f"GHCN incremental done: {result.files_downloaded:,} updated, "
             f"{inserted:,} inventory rows")


def _ghcn_tarball(
    client, base_url, cfg, out_dir,
    all_us_stations, result: RefreshResult,
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

    inserted = insert_inventory_rows(inv_rows)
    result.stations_updated = result.files_downloaded
    log.info(f"GHCN tarball done: {result.files_downloaded:,} converted, "
             f"{inserted:,} inventory rows")


# ─── Source: ASOS ISD (Global Hourly) ────────────────────────────────────────
def refresh_asos_isd(
    client: NOAAClient,
    log_data: dict,
    dry_run: bool,
    force: bool,
    result: RefreshResult,
):
    cfg       = CONFIGS["ASOS_ISD"]
    base_url  = cfg["base_url"]
    local_dir = cfg["local_dir"]
    last_date = get_last_refresh_date(log_data, "ASOS_ISD")
    log.info(f"ASOS_ISD: last_refresh={last_date}")

    # Load USAF codes for stations in our archive
    try:
        con = duckdb.connect()
        our_usaf = {
            r[0] for r in con.execute(
                f"SELECT DISTINCT usaf_id FROM read_parquet('{MASTER_PQ}') "
                "WHERE usaf_id IS NOT NULL AND has_asos_isd"
            ).fetchall() if r[0]
        }
        con.close()
    except Exception as e:
        log.error(f"Cannot load USAF IDs from master list: {e}")
        return

    log.info(f"ASOS_ISD: {len(our_usaf):,} USAF stations to check")

    today  = datetime.date.today()
    years  = [today.year - 1, today.year]
    cutoff = (
        datetime.datetime.combine(last_date, datetime.time.min)
        if not force else datetime.datetime(1970, 1, 1)
    )

    # (usaf_id, url, local_csv_dest, local_parquet_dest)
    to_download: list[tuple[str, str, Path, Path]] = []

    for year in years:
        year_url = base_url + f"{year}/"
        try:
            file_dates = parse_dir_listing(client.get(year_url).text)
        except Exception as e:
            log.warning(f"Cannot list ISD year dir {year}: {e}")
            continue

        year_dir = local_dir / str(year)
        for fname, fdt in file_dates.items():
            if not fname.endswith(".csv"):
                continue
            usaf = fname[:6]
            if usaf not in our_usaf:
                continue
            pq_dest = year_dir / fname.replace(".csv", ".parquet")
            if fdt <= cutoff and pq_dest.exists() and not force:
                continue
            to_download.append((usaf, year_url + fname, year_dir / fname, pq_dest))

    log.info(f"ASOS_ISD: {len(to_download):,} station-year files to download")

    if dry_run:
        log.info(f"[DRY RUN] Would download {len(to_download):,} ISD files")
        return

    inv_rows: list[tuple] = []

    for i, (usaf, url, csv_path, pq_dest) in enumerate(to_download, 1):
        pq_dest.parent.mkdir(parents=True, exist_ok=True)
        tmp_csv = csv_path.with_suffix(".csv.tmp")
        tmp_pq  = pq_dest.with_suffix(".parquet.tmp")
        try:
            nbytes = client.download(url, tmp_csv)
            con2   = duckdb.connect()
            con2.execute(
                f"COPY (SELECT * FROM read_csv_auto('{tmp_csv}', ignore_errors=true)) "
                f"TO '{tmp_pq}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
            )
            con2.close()
            tmp_csv.unlink(missing_ok=True)
            tmp_pq.rename(pq_dest)
            result.files_downloaded += 1
            result.bytes_downloaded += nbytes
            year_str = pq_dest.parent.name
            inv_rows.append(
                _inv_row(pq_dest, "ASOS", "processed_parquet",
                         usaf, "USAF_WBAN", ".parquet", year_str)
            )
        except Exception as e:
            log.warning(f"ISD {pq_dest.name}: {e}")
            result.errors.append(f"{pq_dest.name}: {e}")
            for p in [tmp_csv, tmp_pq]:
                p.unlink(missing_ok=True)

        if i % 200 == 0 or i == len(to_download):
            log.info(f"  [{i:>5,}/{len(to_download):,}]  done={result.files_downloaded:,}")

    inserted = insert_inventory_rows(inv_rows)
    result.stations_updated = result.files_downloaded
    record_refresh(log_data, "ASOS_ISD", result.stations_updated, result.new_stations)
    save_update_log(log_data)
    log.info(f"ASOS_ISD done: {result.files_downloaded:,} files, {inserted:,} inventory rows")


# ─── Source: HPD Hourly Precipitation ────────────────────────────────────────
def refresh_hpd_1hr(
    client: NOAAClient,
    log_data: dict,
    dry_run: bool,
    force: bool,
    result: RefreshResult,
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
        tmp_csv = dest.with_suffix(".csv.tmp")
        tmp_pq  = dest.with_suffix(".parquet.tmp")
        try:
            nbytes   = client.download(url, tmp_csv)
            con2     = duckdb.connect()
            csv_rows = con2.execute(
                f"SELECT COUNT(*) FROM read_csv_auto('{tmp_csv}', ignore_errors=true)"
            ).fetchone()[0]
            con2.execute(
                f"COPY (SELECT * FROM read_csv_auto('{tmp_csv}', ignore_errors=true)) "
                f"TO '{tmp_pq}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
            )
            pq_rows = con2.execute(
                f"SELECT COUNT(*) FROM read_parquet('{tmp_pq}')"
            ).fetchone()[0]
            con2.close()

            if pq_rows != csv_rows:
                log.warning(f"HPD {sid}: row mismatch csv={csv_rows} pq={pq_rows}")
                for p in [tmp_csv, tmp_pq]:
                    p.unlink(missing_ok=True)
                result.errors.append(f"{sid}: row mismatch")
                continue

            tmp_csv.unlink(missing_ok=True)
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
            for p in [tmp_csv, tmp_pq]:
                p.unlink(missing_ok=True)

        if i % 200 == 0 or i == len(to_download):
            log.info(f"  [{i:>5,}/{len(to_download):,}]  done={result.files_downloaded:,}")

    inserted = insert_inventory_rows(inv_rows)
    result.stations_updated = result.files_downloaded
    record_refresh(log_data, "HPD_1hr", result.stations_updated, result.new_stations)
    save_update_log(log_data)
    log.info(f"HPD_1hr done: {result.files_downloaded:,} files, {inserted:,} inventory rows")


# ─── Source: NCEI 15-min ──────────────────────────────────────────────────────
def refresh_ncei_15min(
    client: NOAAClient,
    log_data: dict,
    dry_run: bool,
    force: bool,
    result: RefreshResult,
):
    cfg       = CONFIGS["NCEI_15min"]
    base_url  = cfg["base_url"]
    local_dir = cfg["local_dir"]
    last_date = get_last_refresh_date(log_data, "NCEI_15min")
    log.info(f"NCEI_15min: last_refresh={last_date}")

    try:
        file_dates = parse_dir_listing(client.get(base_url).text)
    except Exception as e:
        log.error(f"Cannot fetch NCEI 15-min directory listing: {e}")
        return

    cutoff    = (
        datetime.datetime.combine(last_date, datetime.time.min)
        if not force else datetime.datetime(1970, 1, 1)
    )
    known_ids = get_known_ids("NCEI_15min_raw")

    to_download: list[tuple[str, str, Path, bool]] = []
    for fname, fdt in file_dates.items():
        if not fname.endswith(".csv"):
            continue
        sid  = fname.split(".")[0]
        dest = local_dir / (sid + ".15m.parquet")
        is_new = sid not in known_ids
        if fdt <= cutoff and dest.exists() and not force:
            continue
        to_download.append((sid, base_url + fname, dest, is_new))

    log.info(f"NCEI_15min: {len(to_download):,} files to update")

    if dry_run:
        log.info(f"[DRY RUN] Would download {len(to_download):,} NCEI 15-min files")
        return

    local_dir.mkdir(parents=True, exist_ok=True)
    inv_rows: list[tuple] = []

    for i, (sid, url, dest, is_new) in enumerate(to_download, 1):
        tmp_raw = dest.with_suffix(".raw.tmp")
        tmp_pq  = dest.with_suffix(".pq.tmp")
        try:
            nbytes = client.download(url, tmp_raw)
            con2   = duckdb.connect()
            con2.execute(
                f"COPY (SELECT * FROM read_csv_auto('{tmp_raw}', ignore_errors=true)) "
                f"TO '{tmp_pq}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
            )
            con2.close()
            tmp_raw.unlink(missing_ok=True)
            tmp_pq.rename(dest)
            result.files_downloaded += 1
            result.bytes_downloaded += nbytes
            if is_new:
                result.new_stations += 1
            inv_rows.append(
                _inv_row(dest, "NCEI_15min_raw", "raw_parquet",
                         sid, "GHCND_HPD", ".15m.parquet")
            )
        except Exception as e:
            log.warning(f"NCEI 15-min {sid}: {e}")
            result.errors.append(f"{sid}: {e}")
            for p in [tmp_raw, tmp_pq]:
                p.unlink(missing_ok=True)

        if i % 200 == 0 or i == len(to_download):
            log.info(f"  [{i:>5,}/{len(to_download):,}]  done={result.files_downloaded:,}")

    inserted = insert_inventory_rows(inv_rows)
    result.stations_updated = result.files_downloaded
    record_refresh(log_data, "NCEI_15min", result.stations_updated, result.new_stations)
    save_update_log(log_data)
    log.info(f"NCEI_15min done: {result.files_downloaded:,} files, {inserted:,} inventory rows")


# ─── Source: ASOS 1-min ───────────────────────────────────────────────────────
def refresh_asos_1min(
    client: NOAAClient,
    log_data: dict,
    dry_run: bool,
    force: bool,
    result: RefreshResult,
):
    cfg       = CONFIGS["ASOS_1min"]
    base_url  = cfg["base_url"]
    local_dir = cfg["local_dir"]
    last_date = get_last_refresh_date(log_data, "ASOS_1min")
    log.info(f"ASOS_1min: last_refresh={last_date}")

    today  = datetime.date.today()
    # 1-min data typically has a 1-2 year publishing lag
    years  = [today.year - 2, today.year - 1, today.year]
    cutoff = (
        datetime.datetime.combine(last_date, datetime.time.min)
        if not force else datetime.datetime(1970, 1, 1)
    )

    # (year, fname, url, dest)
    to_download: list[tuple[int, str, str, Path]] = []

    for year in years:
        year_url = base_url + f"{year}/"
        try:
            file_dates = parse_dir_listing(client.get(year_url).text)
        except Exception as e:
            log.warning(f"Cannot list ASOS 1-min year dir {year}: {e}")
            continue

        year_dir = local_dir / str(year)
        for fname, fdt in file_dates.items():
            # Server files may be .dat (raw) or .parquet (pre-converted)
            if not (fname.endswith(".dat") or fname.endswith(".parquet")):
                continue
            dest_name = fname.replace(".dat", ".parquet") if fname.endswith(".dat") else fname
            dest      = year_dir / dest_name
            if fdt <= cutoff and dest.exists() and not force:
                continue
            to_download.append((year, fname, year_url + fname, dest))

    log.info(f"ASOS_1min: {len(to_download):,} files across years {years}")

    if dry_run:
        log.info(f"[DRY RUN] Would download {len(to_download):,} ASOS 1-min files")
        return

    inv_rows: list[tuple] = []

    for i, (year, fname, url, dest) in enumerate(to_download, 1):
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".tmp")
        try:
            nbytes = client.download(url, tmp)

            if fname.endswith(".parquet"):
                # Already parquet — direct move
                tmp.rename(dest)
            else:
                # .dat → try DuckDB CSV auto-detect; warn if it fails
                tmp_pq = dest.with_suffix(".parquet.tmp")
                try:
                    con2 = duckdb.connect()
                    con2.execute(
                        f"COPY (SELECT * FROM read_csv_auto('{tmp}', ignore_errors=true)) "
                        f"TO '{tmp_pq}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
                    )
                    con2.close()
                    tmp.unlink(missing_ok=True)
                    tmp_pq.rename(dest)
                except Exception as e:
                    log.warning(
                        f"ASOS 1-min .dat parse failed for {fname}: {e}\n"
                        "  → run ASOS/asos1min.py manually for this station-year"
                    )
                    tmp.unlink(missing_ok=True)
                    tmp_pq.unlink(missing_ok=True)
                    result.errors.append(f"{fname}: dat parse failed")
                    continue

            # station_id_raw: extract ICAO from filename
            # e.g. "64060KABE200002" → icao_part="KABE" → stored as "0KABE"
            stem       = os.path.splitext(fname)[0]
            station_id = ("0" + stem[5:9]) if len(stem) >= 9 else stem

            result.files_downloaded += 1
            result.bytes_downloaded += nbytes
            inv_rows.append(
                _inv_row(dest, "ASOS", "asos_1min_parquet_master",
                         station_id, "ICAO_ASOS", ".parquet")
            )
        except Exception as e:
            log.warning(f"ASOS 1-min {fname}: {e}")
            result.errors.append(f"{fname}: {e}")
            tmp.unlink(missing_ok=True)

        if i % 200 == 0 or i == len(to_download):
            log.info(f"  [{i:>5,}/{len(to_download):,}]  done={result.files_downloaded:,}")

    inserted = insert_inventory_rows(inv_rows)
    result.stations_updated = result.files_downloaded
    record_refresh(log_data, "ASOS_1min", result.stations_updated, result.new_stations)
    save_update_log(log_data)
    log.info(f"ASOS_1min done: {result.files_downloaded:,} files, {inserted:,} inventory rows")


# ─── Post-refresh pipeline ────────────────────────────────────────────────────
def post_refresh(sources: list[str]):
    """
    After downloading new files:
      1. Re-index file_inventory for affected sources
      2. Profile new parquet files (footer-only, Pool(8))
      3. Rebuild station_coverage + station_coverage_geo
      4. Regenerate coverage maps
    """
    root = str(BASE)

    # 1. Re-index
    inv_sources_done: set[str] = set()
    for src in sources:
        inv_src = CONFIGS[src].get("inv_source", src)
        if inv_src in inv_sources_done:
            continue
        inv_sources_done.add(inv_src)
        log.info(f"Re-indexing file_inventory for source={inv_src} …")
        ret = subprocess.run(
            [sys.executable, "recon/build_file_inventory.py", "--source", inv_src],
            cwd=root, capture_output=True, text=True,
        )
        if ret.returncode != 0:
            log.warning(f"  build_file_inventory failed:\n{ret.stderr[:400]}")
        else:
            log.info("  Done.")

    # 2. Profile new files
    _profile_new_files()

    # 3+4. Rebuild coverage tables and maps via build_data_profile.py
    log.info("Rebuilding station_coverage + station_coverage_geo …")
    ret = subprocess.run(
        [sys.executable, "recon/build_data_profile.py"],
        cwd=root, capture_output=True, text=True,
    )
    if ret.returncode != 0:
        log.warning(f"  build_data_profile.py failed:\n{ret.stderr[:400]}")
    else:
        log.info("  Done.")

    log.info("Regenerating coverage maps …")
    ret = subprocess.run(
        [sys.executable, "recon/plot_coverage.py"],
        cwd=root, capture_output=True, text=True,
    )
    if ret.returncode != 0:
        log.warning(f"  plot_coverage.py failed:\n{ret.stderr[:300]}")
    else:
        log.info("  Coverage maps saved.")


def _profile_new_files():
    """Profile parquet files in file_inventory that have no data_profile row yet."""
    try:
        con  = duckdb.connect(str(DB_PATH))
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

    sys.path.insert(0, str(RECON))
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
    results   = []
    with Pool(n_workers) as pool:
        for res in pool.imap_unordered(_profile_one, args, chunksize=50):
            results.append(res)

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
    g = p.add_mutually_exclusive_group(required=True)
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

    log.info("=" * 55)
    log.info(f"WeatherData refresh  {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")
    log.info(f"dry_run={args.dry_run}  force={args.force}  method={args.method}")
    log.info("=" * 55)

    log_data = load_update_log()
    sources  = ALL_SOURCES if args.all else [args.source]
    client   = NOAAClient()
    results: list[RefreshResult] = []

    try:
        for source in sources:
            log.info(f"\n{'─'*40}")
            log.info(f"Refreshing: {source}")
            log.info(f"{'─'*40}")
            r = RefreshResult(source)
            results.append(r)
            try:
                if source == "GHCN_daily":
                    refresh_ghcn_daily(client, log_data, args.dry_run, args.force,
                                       _ghcn_method(log_data, args.method), r)
                elif source == "ASOS_ISD":
                    refresh_asos_isd(client, log_data, args.dry_run, args.force, r)
                elif source == "HPD_1hr":
                    refresh_hpd_1hr(client, log_data, args.dry_run, args.force, r)
                elif source == "NCEI_15min":
                    refresh_ncei_15min(client, log_data, args.dry_run, args.force, r)
                elif source == "ASOS_1min":
                    refresh_asos_1min(client, log_data, args.dry_run, args.force, r)
            except KeyboardInterrupt:
                log.warning(f"Interrupted during {source} — saving progress")
                save_update_log(log_data)
                break
            except Exception as e:
                log.error(f"{source} refresh failed: {e}", exc_info=True)
                r.errors.append(str(e))

        # Post-refresh: re-index, profile, rebuild coverage, regenerate maps
        if not args.dry_run and not args.no_post_refresh:
            refreshed = [r.source for r in results if r.files_downloaded > 0]
            if refreshed:
                log.info(f"\nPost-refresh pipeline for: {refreshed}")
                post_refresh(refreshed)
            else:
                log.info("No files downloaded — skipping post-refresh pipeline")

    finally:
        client.close()
        save_update_log(log_data)

    print_final_summary(results, log_data)


if __name__ == "__main__":
    main()
