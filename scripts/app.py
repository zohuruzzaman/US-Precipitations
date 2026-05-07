"""
app.py — Weather Data Explorer
Streamlit single-file app for exploring and extracting from the WeatherData archive.

Run: cd ~/Documents/WeatherData && streamlit run app.py
"""

import base64
import gc
import io
import json
import os
import re
import signal
import subprocess
import sys
import time
import warnings
from datetime import datetime
from math import asin, cos, radians, sin, sqrt

import duckdb
import folium
import numpy as np
import pandas as pd
import streamlit as st
from folium.plugins import Draw, MarkerCluster
from streamlit_folium import st_folium

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
from pathlib import Path as _Path
_PROJECT_DIR        = _Path(__file__).resolve().parent
_DATA_DIR           = _PROJECT_DIR.parent / "data"
DB_PATH             = str(_DATA_DIR / "recon" / "file_inventory.duckdb")
MASTER_PQ           = str(_DATA_DIR / "recon" / "master_station_list.parquet")
LOG_PATH            = str(_DATA_DIR / "recon" / "update_log.json")
INDEX_PROGRESS_FILE = str(_DATA_DIR / "recon" / ".index_progress.json")

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Weather Data Explorer",
    page_icon="🌦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# DB connection — open fresh per call, never cached.
# ---------------------------------------------------------------------------
def get_db():
    """Open a fresh read-only DuckDB connection.

    NOT cached.  DuckDB local-file connections open in <1ms so there is no
    meaningful cost.  Keeping the connection uncached means the app never
    holds a persistent OS-level file lock, so the download/index process can
    always acquire the write lock without contention.

    Returns None if the DB is locked (write window active) — callers must
    guard with ``if con is None``.
    """
    try:
        return duckdb.connect(DB_PATH, read_only=True)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Load master station list (cached, ~96K rows)
# Read directly from parquet via pandas — no DuckDB connection needed,
# so this works even while the index process holds the write lock.
# ---------------------------------------------------------------------------
@st.cache_data
def load_stations() -> pd.DataFrame:
    cols = [
        "ncei_id", "station_name", "state", "county",
        "lat_dec", "lon_dec", "elev_m", "is_active",
        "icao_ids", "ghcnd_ids", "wban_ids", "hpd_ids", "usaf_id",
        "has_asos_1min", "asos_1min_start", "asos_1min_end",
        "asos_1min_files", "asos_isd_files",
        "hpd_files", "ncei_files", "ghcn_daily_files",
        "has_asos_isd", "has_hpd_1hr", "has_ncei_15min", "has_ghcn_daily",
    ]
    df = pd.read_parquet(MASTER_PQ, columns=cols)
    df = df.rename(columns={"lat_dec": "lat", "lon_dec": "lon"})
    df = df[
        df["lat"].notna() & df["lon"].notna() &
        (
            df["has_asos_1min"].fillna(False).astype(bool) |
            df["has_asos_isd"].fillna(False).astype(bool)  |
            df["has_hpd_1hr"].fillna(False).astype(bool)   |
            df["has_ncei_15min"].fillna(False).astype(bool) |
            df["has_ghcn_daily"].fillna(False).astype(bool)
        )
    ].copy()

    # Coerce nullable booleans → plain bool (parquet NAs → False)
    bool_cols = ["has_asos_1min", "has_asos_isd", "has_hpd_1hr", "has_ncei_15min", "has_ghcn_daily"]
    for c in bool_cols:
        if c in df.columns:
            df[c] = df[c].fillna(False).astype(bool)

    # Coerce nullable integer file-count columns
    int_cols = ["asos_1min_files", "asos_isd_files", "hpd_files", "ncei_files", "ghcn_daily_files"]
    for c in int_cols:
        if c in df.columns:
            df[c] = df[c].fillna(0).astype(int)

    # ── Actual per-station date ranges from inventory parquets ─────────────
    # Join master ghcnd_ids / wban_ids against the on-disk inventory files
    # to obtain real earliest / latest observation years per station.
    _INV_DAILY  = str(_DATA_DIR / "recon" / "station_inventory_daily.parquet")
    _INV_HOURLY = str(_DATA_DIR / "recon" / "station_inventory_hourly.parquet")
    _INV_15MIN  = str(_DATA_DIR / "recon" / "station_inventory_15min.parquet")
    _INV_1MIN   = str(_DATA_DIR / "recon" / "station_inventory_1min.parquet")

    try:
        _con = duckdb.connect()
        _con.register("_master", df)

        # GHCNd — join via pipe-split ghcnd_ids
        _daily = _con.execute(f"""
            WITH ids AS (
                SELECT ncei_id,
                       TRIM(unnest(string_split(COALESCE(ghcnd_ids,''), '|'))) AS gid
                FROM _master WHERE has_ghcn_daily AND ghcnd_ids IS NOT NULL
            )
            SELECT ids.ncei_id,
                   YEAR(MIN(inv.earliest_dt))::INTEGER AS ghcnd_start,
                   YEAR(MAX(inv.latest_dt))::INTEGER   AS ghcnd_end
            FROM ids
            JOIN read_parquet('{_INV_DAILY}') inv ON inv.station_id = ids.gid
            WHERE ids.gid <> ''
            GROUP BY ids.ncei_id
        """).fetchdf()

        # GHCNh — hourly station_id is USAF(6)+WBAN(5); match last 5 chars to wban_ids
        _hourly = _con.execute(f"""
            WITH ids AS (
                SELECT ncei_id,
                       TRIM(unnest(string_split(COALESCE(wban_ids,''), '|'))) AS wban
                FROM _master WHERE has_asos_isd AND wban_ids IS NOT NULL
            )
            SELECT ids.ncei_id,
                   YEAR(MIN(inv.earliest_dt))::INTEGER AS ghcnh_start,
                   YEAR(MAX(inv.latest_dt))::INTEGER   AS ghcnh_end
            FROM ids
            JOIN read_parquet('{_INV_HOURLY}') inv
                ON RIGHT(inv.station_id, 5) = ids.wban
            WHERE ids.wban <> ''
            GROUP BY ids.ncei_id
        """).fetchdf()

        # NCEI 15-min — same GHCNd-format IDs
        _15min = _con.execute(f"""
            WITH ids AS (
                SELECT ncei_id,
                       TRIM(unnest(string_split(COALESCE(ghcnd_ids,''), '|'))) AS gid
                FROM _master WHERE has_ncei_15min AND ghcnd_ids IS NOT NULL
            )
            SELECT ids.ncei_id,
                   YEAR(MIN(inv.earliest_dt))::INTEGER AS ncei15_start,
                   YEAR(MAX(inv.latest_dt))::INTEGER   AS ncei15_end
            FROM ids
            JOIN read_parquet('{_INV_15MIN}') inv ON inv.station_id = ids.gid
            WHERE ids.gid <> ''
            GROUP BY ids.ncei_id
        """).fetchdf()

        # ASOS 1-min — join via pipe-split ICAO IDs
        _1min = _con.execute(f"""
            WITH ids AS (
                SELECT ncei_id,
                       TRIM(unnest(string_split(COALESCE(icao_ids,''), '|'))) AS icao
                FROM _master WHERE has_asos_1min AND icao_ids IS NOT NULL
            )
            SELECT ids.ncei_id,
                   YEAR(MIN(inv.earliest_dt))::INTEGER AS asos1min_start,
                   YEAR(MAX(inv.latest_dt))::INTEGER   AS asos1min_end
            FROM ids
            JOIN read_parquet('{_INV_1MIN}') inv ON inv.station_id = ids.icao
            WHERE ids.icao <> '' AND inv.earliest_dt IS NOT NULL AND inv.latest_dt IS NOT NULL
            GROUP BY ids.ncei_id
        """).fetchdf()

        _con.close()

        df = df.merge(_daily,  on="ncei_id", how="left")
        df = df.merge(_hourly, on="ncei_id", how="left")
        df = df.merge(_15min,  on="ncei_id", how="left")
        df = df.merge(_1min,   on="ncei_id", how="left")

    except Exception:
        df["ghcnd_start"] = df["ghcnd_end"] = None
        df["ghcnh_start"] = df["ghcnh_end"] = None
        df["ncei15_start"] = df["ncei15_end"] = None
        df["asos1min_start"] = df["asos1min_end"] = None

    def _year_from_text(value, default: int) -> int:
        """Extract the leading year from strings like YYYY-MM or full dates."""
        try:
            year = int(str(value)[:4])
            return year
        except Exception:
            return default

    # Per-resolution year ranges used by the sidebar filter.  Keep these
    # separate from data_start/data_end so a selected source must overlap the
    # requested years itself.
    df["daily_start_year"] = df.apply(
        lambda r: int(r["ghcnd_start"]) if r["has_ghcn_daily"] and pd.notna(r.get("ghcnd_start")) else 1950,
        axis=1,
    )
    df["daily_end_year"] = df.apply(
        lambda r: int(r["ghcnd_end"]) if r["has_ghcn_daily"] and pd.notna(r.get("ghcnd_end")) else 2026,
        axis=1,
    )
    df["hourly_start_year"] = df.apply(
        lambda r: int(r["ghcnh_start"]) if r["has_asos_isd"] and pd.notna(r.get("ghcnh_start")) else 1930,
        axis=1,
    )
    df["hourly_end_year"] = df.apply(
        lambda r: int(r["ghcnh_end"]) if r["has_asos_isd"] and pd.notna(r.get("ghcnh_end")) else 2026,
        axis=1,
    )
    df["ncei15_start_year"] = df.apply(
        lambda r: int(r["ncei15_start"]) if r["has_ncei_15min"] and pd.notna(r.get("ncei15_start")) else 1970,
        axis=1,
    )
    df["ncei15_end_year"] = df.apply(
        lambda r: int(r["ncei15_end"]) if r["has_ncei_15min"] and pd.notna(r.get("ncei15_end")) else 2026,
        axis=1,
    )
    df["asos1min_start_year"] = df.apply(
        lambda r: (
            int(r["asos1min_start"])
            if r["has_asos_1min"] and pd.notna(r.get("asos1min_start"))
            else _year_from_text(r.get("asos_1min_start"), 2000)
        ) if r["has_asos_1min"] else 2000,
        axis=1,
    )
    df["asos1min_end_year"] = df.apply(
        lambda r: (
            int(r["asos1min_end"])
            if r["has_asos_1min"] and pd.notna(r.get("asos1min_end"))
            else _year_from_text(r.get("asos_1min_end"), 2026)
        ) if r["has_asos_1min"] else 2026,
        axis=1,
    )
    # Compute per-station data_start / data_end year using actual inventory dates
    def _year_range(row):
        starts, ends = [], []
        if row["has_ghcn_daily"]:
            starts.append(row["daily_start_year"])
            ends.append(row["daily_end_year"])
        if row["has_asos_isd"]:
            starts.append(row["hourly_start_year"])
            ends.append(row["hourly_end_year"])
        if row["has_ncei_15min"]:
            starts.append(row["ncei15_start_year"])
            ends.append(row["ncei15_end_year"])
        if row["has_asos_1min"]:
            starts.append(row["asos1min_start_year"])
            ends.append(row["asos1min_end_year"])
        return (min(starts) if starts else 1900, max(ends) if ends else 2026)

    yr = df.apply(_year_range, axis=1)
    df["data_start"] = yr.apply(lambda x: x[0])
    df["data_end"]   = yr.apply(lambda x: x[1])

    # Dominant resolution label (for marker coloring)
    def _res(row):
        if row["has_asos_1min"]:  return "1min"
        if row["has_ncei_15min"]: return "15min"
        if row["has_asos_isd"]:   return "hourly"
        if row["has_ghcn_daily"]: return "daily"
        return "none"
    df["dominant_res"] = df.apply(_res, axis=1)

    return df


# ---------------------------------------------------------------------------
# Last refreshed date (from update_log.json)
# ---------------------------------------------------------------------------
def load_last_refreshed() -> str:
    try:
        with open(LOG_PATH) as f:
            data = json.load(f)
        import datetime as _dt
        dates = [
            _dt.date.fromisoformat(v["date"])
            for v in data.get("last_refresh", {}).values()
            if isinstance(v, dict) and "date" in v
        ]
        if dates:
            return max(dates).strftime("%B %-d, %Y")
    except Exception:
        pass
    return "Unknown"


# ---------------------------------------------------------------------------
# Colour helpers
# ---------------------------------------------------------------------------
RES_COLOR = {
    "1min":          "#1f77b4",
    "15min":         "#2ca02c",
    "hourly":        "#d62728",
    "hourly_precip": "#ff7f0e",
    "daily":         "#aec7e8",
    "none":          "#999999",
}

RES_LABELS = {
    "1min":          "1-min ASOS",
    "15min":         "15-min NCEI",
    "hourly":        "GHCNh Hourly",
    "daily":         "Daily (GHCN)",
}


# ---------------------------------------------------------------------------
# Haversine distance (vectorised)
# ---------------------------------------------------------------------------
def filter_by_radius(df: pd.DataFrame, clat: float, clon: float, radius_km: float) -> pd.DataFrame:
    dlat = np.radians(df["lat"].values - clat)
    dlon = np.radians(df["lon"].values - clon)
    a = (np.sin(dlat / 2) ** 2
         + np.cos(np.radians(clat)) * np.cos(np.radians(df["lat"].values))
         * np.sin(dlon / 2) ** 2)
    dist = 2 * 6371.0 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
    return df[dist <= radius_km].copy()


# ---------------------------------------------------------------------------
# File-path lookup from inventory
# ---------------------------------------------------------------------------
def get_file_paths(
    station_df: pd.DataFrame,
    resolutions: list[str],
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Return (files_by_res, station_ids_by_res) for the selected stations.

    For hive-partitioned / year-based sources the returned file list is a
    directory path or a list of year files; extract_raw() uses station_ids_by_res
    to add a WHERE filter so only the requested stations are returned.
    For 1-min the station is encoded in the filename so station_ids is empty.
    """
    files:       dict[str, list[str]] = {}
    station_ids: dict[str, list[str]] = {}

    # ── Flatten pipe-separated ID strings from master station list ────────────
    def _split(col: str) -> list[str]:
        out: list[str] = []
        for v in station_df[col].dropna():
            out.extend(str(v).split("|"))
        return list(dict.fromkeys(out))  # unique, order-preserving

    ghcnd_ids = _split("ghcnd_ids")
    icao_ids  = _split("icao_ids")
    wban_ids  = _split("wban_ids")

    def _sql_in(vals: list) -> str:
        return "'" + "','".join(str(v).replace("'", "''") for v in vals) + "'"

    # ── GHCNd daily (year-based parquets) ─────────────────────────────────────
    if "Daily" in resolutions and ghcnd_ids:
        daily_dir = _DATA_DIR / "GHCNd" / "daily"
        year_files = sorted(daily_dir.glob("*.parquet")) if daily_dir.is_dir() else []
        files["Daily"]       = [str(f) for f in year_files]
        station_ids["Daily"] = ghcnd_ids  # filter on STATION column

    # ── GHCNh hourly (Hive-partitioned) ───────────────────────────────────────
    if "GHCNh Hourly" in resolutions and wban_ids:
        hourly_dir = _DATA_DIR / "GHCNh" / "hourly"
        if hourly_dir.is_dir():
            files["GHCNh Hourly"]       = [str(hourly_dir)]  # directory sentinel
            station_ids["GHCNh Hourly"] = wban_ids  # filter via SUBSTRING(station_id,7,5)
        else:
            files["GHCNh Hourly"] = []

    # ── ASOS 15-min (Hive-partitioned) ────────────────────────────────────────
    if "15-min" in resolutions and ghcnd_ids:
        min15_dir = _DATA_DIR / "ASOS" / "15min"
        if min15_dir.is_dir():
            files["15-min"]       = [str(min15_dir)]  # directory sentinel
            station_ids["15-min"] = ghcnd_ids  # filter on station_id column
        else:
            files["15-min"] = []

    # ── ASOS 1-min (per-station files in inventory) ───────────────────────────
    if "1-min" in resolutions and icao_ids:
        con = get_db()
        if con is not None:
            rows = con.execute(f"""
                SELECT filepath FROM file_inventory
                WHERE source='ASOS' AND subfolder='1min'
                  AND SUBSTRING(station_id_raw, 2, 4) IN ({_sql_in(icao_ids)})
            """).fetchall()
            con.close()
            files["1-min"] = [r[0] for r in rows]
        station_ids["1-min"] = []  # implicit in filename

    # "Hourly Precip" (HPD) removed — no data available

    return files, station_ids


# ---------------------------------------------------------------------------
# Raw parquet extraction
# ---------------------------------------------------------------------------
MAX_EXTRACT_FILES = 1000
MAX_EXTRACT_ROWS  = 5_000_000
MAX_STATION_FILTER_IDS = 2000

DATE_COL_MAP = {
    "Daily":        ("DATE", False),                  # GHCNd uses uppercase DATE
    "15-min":       ("year", False),                  # Hive partition column (INT), not a date string
    "GHCNh Hourly": ("date", False),
    "1-min":        (("timestamp_utc", "datetime_utc"), True),
}

# SQL expression identifying a station in each resolution's parquet
STATION_COL_MAP = {
    "Daily":      "STATION",                       # GHCNd GHCND station ID
    "GHCNh Hourly": "SUBSTRING(station_id, 7, 5)",   # extracts WBAN from ISD station_id
    "15-min":     "station_id",                    # GHCND station ID
    "1-min":      None,                            # implicit in filename
}


def extract_raw(
    file_paths:  list[str],
    date_col:    str | tuple[str, ...] | list[str],
    year_start:  int,
    year_end:    int,
    datetime_col: bool = False,
    station_ids: list[str] | None = None,
    station_col: str | None = None,
    row_limit: int = MAX_EXTRACT_ROWS,
) -> pd.DataFrame | None:
    """Read parquet data filtered by year range (and optionally station IDs).

    Supports two path styles:
    - List of specific files (GHCNd year files, 1-min per-station files)
    - Single directory path (Hive-partitioned GHCNh / 15-min)
    """
    import os as _os
    if not file_paths:
        return None

    # Detect Hive-partitioned directory sentinel (single directory path)
    is_hive_dir = len(file_paths) == 1 and _os.path.isdir(file_paths[0])

    if is_hive_dir:
        hive_path = file_paths[0].rstrip("/")
        paths_sql = f"'{hive_path}/**/*.parquet'"
        hive_kw   = ", hive_partitioning=true"
    else:
        capped    = file_paths[:MAX_EXTRACT_FILES]
        paths_sql = "[" + ", ".join(
            f"'{p.replace(chr(39), chr(39)+chr(39))}'" for p in capped
        ) + "]"
        hive_kw = ""

    con2 = duckdb.connect()
    try:
        if isinstance(date_col, (list, tuple)):
            cols = {
                d[0]
                for d in con2.execute(
                    f"DESCRIBE SELECT * FROM read_parquet({paths_sql}, union_by_name=true{hive_kw})"
                ).fetchall()
            }
            resolved = next((c for c in date_col if c in cols), None)
            if resolved is None:
                raise ValueError(
                    f"None of the expected date columns were found: {', '.join(date_col)}"
                )
            date_col = resolved

        if date_col == "year":
            # Hive partition column is already an integer year
            yr_filter = f"year BETWEEN {year_start} AND {year_end}"
        elif datetime_col:
            yr_filter = f"YEAR({date_col}) BETWEEN {year_start} AND {year_end}"
        else:
            yr_filter = f"YEAR(TRY_CAST({date_col} AS DATE)) BETWEEN {year_start} AND {year_end}"

        station_filter = ""
        if station_col is not None and station_ids is not None and len(station_ids) == 0:
            return None
        if station_ids and station_col:
            if len(station_ids) > MAX_STATION_FILTER_IDS:
                st.error(
                    f"Extraction for this source needs {len(station_ids):,} station IDs. "
                    f"Refine the map/sidebar selection below {MAX_STATION_FILTER_IDS:,} stations "
                    "to avoid an incomplete export."
                )
                return None
            ids_sql = "'" + "','".join(
                s.replace("'", "''") for s in station_ids
            ) + "'"
            station_filter = f"AND {station_col} IN ({ids_sql})"

        df = con2.execute(f"""
            SELECT * FROM read_parquet({paths_sql}, union_by_name=true{hive_kw})
            WHERE {yr_filter} {station_filter}
            LIMIT {int(row_limit)}
        """).fetchdf()
    except Exception as e:
        st.error(f"Extraction error ({date_col}): {e}")
        df = None
    finally:
        con2.close()
    return df


# ---------------------------------------------------------------------------
# Normalise raw extraction → common tidy schema
# ---------------------------------------------------------------------------
OUTPUT_COLS = [
    "station_id", "station_name", "state", "lat", "lon",
    "datetime", "resolution", "variable", "value", "unit",
    "source", "quality_flag",
]

GHCN_UNITS = {
    "PRCP": "mm", "TMAX": "degC",
    "TMIN": "degC", "TAVG": "degC",
}
# GHCN stores PRCP in tenths of mm and temperatures in tenths of °C
GHCN_SCALE = {
    # ghcn_daily.py already divides raw tenths→SI at download time;
    # parquets store mm / °C directly — no further scaling needed here.
    "PRCP": 1.0, "TMAX": 1.0, "TMIN": 1.0, "TAVG": 1.0,
}


def build_meta_df(station_df: pd.DataFrame, res_key: str) -> pd.DataFrame:
    """station_id → (station_name, state, lat, lon) for the given resolution."""
    meta_cols = ["station_name", "state", "lat", "lon"]

    _empty = pd.DataFrame(columns=["station_id"] + meta_cols)

    if res_key == "GHCNh Hourly":
        # GHCNh station_id is USAF(6)+WBAN(5).  Match via WBAN, which may be
        # pipe-separated in the master list — explode to one row per WBAN.
        rows = []
        for _, row in station_df.dropna(subset=["wban_ids"]).iterrows():
            for wban in str(row["wban_ids"]).split("|"):
                wban = wban.strip()
                if wban:
                    rows.append({"station_id": wban, **{c: row[c] for c in meta_cols}})
        if not rows:
            return _empty
        return (
            pd.DataFrame(rows)
            .drop_duplicates("station_id")
            .reset_index(drop=True)
        )

    if res_key in ("Daily", "15-min"):
        # ghcnd_ids may be pipe-separated — explode to one row per GHCND ID
        rows = []
        for _, row in station_df.dropna(subset=["ghcnd_ids"]).iterrows():
            for gid in str(row["ghcnd_ids"]).split("|"):
                gid = gid.strip()
                if gid:
                    rows.append({"station_id": gid, **{c: row[c] for c in meta_cols}})
        if not rows:
            return _empty
        return (
            pd.DataFrame(rows)
            .drop_duplicates("station_id")
            .reset_index(drop=True)
        )

    if res_key == "1-min":
        # icao_ids can be pipe-separated too.  Explode it the same way as
        # GHCND/WBAN IDs so raw ICAO rows can merge back to station metadata.
        rows = []
        for _, row in station_df.dropna(subset=["icao_ids"]).iterrows():
            for icao in str(row["icao_ids"]).split("|"):
                icao = icao.strip()
                if icao:
                    rows.append({"station_id": icao, **{c: row[c] for c in meta_cols}})
        if not rows:
            return _empty
        return (
            pd.DataFrame(rows)
            .drop_duplicates("station_id")
            .reset_index(drop=True)
        )

    return _empty


def normalize_df(df_raw: pd.DataFrame, res_key: str,
                 meta_df: pd.DataFrame) -> pd.DataFrame | None:
    """Normalise raw parquet data to OUTPUT_COLS tidy schema."""
    if df_raw is None or len(df_raw) == 0:
        return None
    df = df_raw.copy()

    try:
        if res_key == "Daily":
            # GHCNd — rename STATION/DATE first, then detect wide vs long format
            df = df.rename(columns={
                "STATION": "station_id", "DATE": "date",
                "ELEMENT": "element",   "VALUE": "value",
                "MFLAG": "mflag",       "QFLAG": "qflag", "SFLAG": "sflag",
            })
            # Detect wide format (PRCP/TMAX/TMIN columns, no ELEMENT column)
            import re as _re
            _GHCN_ELEM = ["PRCP", "TMAX", "TMIN", "SNWD", "SNOW", "AWND", "TAVG"]
            _attr_pat = _re.compile(r".+_ATTRIBUTES$", _re.IGNORECASE)
            _wide_cols = [c for c in df.columns if c in _GHCN_ELEM]
            if _wide_cols and "element" not in df.columns:
                df = df.melt(
                    id_vars=["station_id", "date"],
                    value_vars=_wide_cols,
                    var_name="element", value_name="value",
                )
                df["qflag"] = ""
            df = df.merge(meta_df, on="station_id", how="left")
            df["datetime"]     = df["date"].astype(str)
            df["resolution"]   = "daily"
            df["source"]       = "GHCN"
            df["variable"]     = df.get("element", pd.Series("unknown", index=df.index))
            df["unit"]         = df["variable"].map(GHCN_UNITS).fillna("unknown")
            df["quality_flag"] = (df["qflag"].fillna("") if "qflag" in df.columns else "")
            raw_val = pd.to_numeric(df.get("value", pd.Series(dtype=float)), errors="coerce")
            # Convert tenths → actual SI: PRCP tenths_mm→mm, TMAX/TMIN/TAVG tenths_°C→°C
            scale = df["variable"].map(GHCN_SCALE).fillna(1.0)
            df["value"] = raw_val * scale

        elif res_key == "1-min":
            # ASOS 1-min schema has appeared in both old and current forms:
            # datetime_utc / precip_1min_in / temp_F and
            # timestamp_utc / precip_in / temp_f.
            df["station_id"] = df["icao"].astype(str) if "icao" in df.columns else ""
            df = df.merge(meta_df, on="station_id", how="left", suffixes=("", "_meta"))
            if "lat_meta" in df.columns:
                df["lat"] = df["lat"].fillna(df["lat_meta"]) if "lat" in df.columns else df["lat_meta"]
                df["lon"] = df["lon"].fillna(df["lon_meta"]) if "lon" in df.columns else df["lon_meta"]
                df.drop(columns=["lat_meta", "lon_meta"], inplace=True, errors="ignore")
            if "state_meta" in df.columns:
                df["state"] = df["state"].fillna(df["state_meta"]) if "state" in df.columns else df["state_meta"]
                df.drop(columns=["state_meta"], inplace=True, errors="ignore")
            if "station_name_meta" in df.columns:
                df["station_name"] = (
                    df["station_name"].fillna(df["station_name_meta"])
                    if "station_name" in df.columns else df["station_name_meta"]
                )
                df.drop(columns=["station_name_meta"], inplace=True, errors="ignore")
            ts_col = next((c for c in ["timestamp_utc", "datetime_utc"] if c in df.columns), None)
            df["datetime"]     = df[ts_col].astype(str) if ts_col else ""
            df["resolution"]   = "1min"
            df["source"]       = "ASOS"
            df["quality_flag"] = ""

            precip_vars = {"precip_1min_in", "precip_in"}
            temp_vars = {"temp_F", "temp_f"}
            value_vars = [c for c in ["precip_1min_in", "precip_in", "temp_F", "temp_f"] if c in df.columns]
            if value_vars:
                id_vars = [c for c in ["station_id", "station_name", "state", "lat", "lon",
                                        "datetime", "resolution", "source", "quality_flag"]
                           if c in df.columns]
                df = df[id_vars + value_vars].melt(
                    id_vars=id_vars, value_vars=value_vars,
                    var_name="variable", value_name="value"
                )
                df["value"] = pd.to_numeric(df["value"], errors="coerce")
                # Convert precip inches → mm; temp °F → °C
                df.loc[df["variable"].isin(precip_vars), "value"] *= 25.4
                temp_mask = df["variable"].isin(temp_vars)
                df.loc[temp_mask, "value"] = (df.loc[temp_mask, "value"] - 32) * 5 / 9
                df["variable"] = df["variable"].replace({
                    "precip_in": "precip_1min_in",
                    "temp_F": "air_temp",
                    "temp_f": "air_temp",
                })
                df["unit"] = df["variable"].map({"precip_1min_in": "mm", "air_temp": "degC"}).fillna("unknown")
            else:
                df["variable"] = "unknown"; df["value"] = float("nan"); df["unit"] = "unknown"

        elif res_key == "Hourly Precip":
            # HPD hourly CSV: STATION, NAME, DATE, HlyPrcp, ...
            sid_col = next((c for c in ["STATION", "station_id", "Station"] if c in df.columns), None)
            if sid_col:
                df["station_id"] = df[sid_col].astype(str)
            df = df.merge(meta_df, on="station_id", how="left")
            date_col = next((c for c in ["DATE", "date"] if c in df.columns), None)
            df["datetime"]     = df[date_col].astype(str) if date_col else ""
            df["resolution"]   = "hourly_precip"
            df["source"]       = "HPD"
            df["quality_flag"] = ""
            prcp_col = next((c for c in df.columns
                             if c.lower() in {"hlyprcp", "hlyprcp_in", "hly_prcp"}
                             or ("prcp" in c.lower() and "qc" not in c.lower())), None)
            if prcp_col:
                df["variable"] = "HlyPrcp"
                df["value"]    = pd.to_numeric(df[prcp_col], errors="coerce") * 25.4
                df["unit"]     = "mm"
            else:
                df["variable"] = "unknown"; df["value"] = float("nan"); df["unit"] = "unknown"

        elif res_key == "15-min":
            # ASOS 15-min (HPD v2 long format):
            #   station_id, station_name, lat, lon, elevation, element,
            #   month, day, time, precip_in, flag_measurement, flag_quality,
            #   state, year  (year/state are Hive partition columns)
            df = df.merge(meta_df, on="station_id", how="left",
                          suffixes=("", "_meta"))
            # Prefer embedded lat/lon; fall back to meta
            if "lat_meta" in df.columns:
                df["lat"] = df["lat"].fillna(df["lat_meta"])
                df["lon"] = df["lon"].fillna(df["lon_meta"])
                df.drop(columns=["lat_meta", "lon_meta"], inplace=True, errors="ignore")
            if "station_name_meta" in df.columns:
                df["station_name"] = df["station_name"].fillna(df["station_name_meta"])
                df.drop(columns=["station_name_meta"], inplace=True, errors="ignore")
            # Construct datetime from year/month/day/time columns
            try:
                df["datetime"] = (
                    df["year"].astype(str).str.zfill(4) + "-"
                    + df["month"].astype(str).str.zfill(2) + "-"
                    + df["day"].astype(str).str.zfill(2) + " "
                    + df["time"].astype(str).str.zfill(4).str[:2] + ":"
                    + df["time"].astype(str).str.zfill(4).str[2:]
                )
            except Exception:
                df["datetime"] = ""
            df["resolution"]   = "15min"
            df["source"]       = "NCEI"
            df["quality_flag"] = df.get("flag_quality", pd.Series("", index=df.index)).fillna("")
            df["variable"]     = df["element"].astype(str) if "element" in df.columns else "precip"
            # precip_in → mm
            df["value"] = pd.to_numeric(df.get("precip_in", pd.Series(dtype=float)), errors="coerce") * 25.4
            df["unit"]  = "mm"

        elif res_key == "GHCNh Hourly":
            # GHCNh hourly: station_id is ISD format USAF(6)+WBAN(5) = 11 chars
            # Extract WBAN (chars 7-11, Python index [6:11]) for meta lookup
            if "station_id" not in df.columns:
                sid_col = next((c for c in ["STATION", "station"] if c in df.columns), None)
                df["station_id"] = df[sid_col].astype(str) if sid_col else ""
            df["station_id"] = df["station_id"].astype(str).str[6:11]
            df = df.merge(meta_df, on="station_id", how="left")
            date_col = next((c for c in ["date", "DATE", "datetime"] if c in df.columns), None)
            df["datetime"]     = df[date_col].astype(str) if date_col else ""
            df["resolution"]   = "hourly"
            df["source"]       = "ASOS_ISD"
            df["quality_flag"] = ""

            if "TMP" in df.columns:
                # ISD encoded: "dddd,c"  dddd = tenths °C (signed, +9999 = missing)
                raw = df["TMP"].astype(str).str.split(",").str[0]
                tmp = pd.to_numeric(raw, errors="coerce")
                df["value"]    = tmp.where(tmp.abs() < 9999) / 10.0
                df["variable"] = "air_temp"
                df["unit"]     = "degC"
            elif "AA1" in df.columns:
                # AA1 = precip accumulation: "period,depth,condition,quality"
                raw = df["AA1"].astype(str).str.split(",").str[1]
                df["value"]    = pd.to_numeric(raw, errors="coerce") / 10.0
                df["variable"] = "precip_accum"
                df["unit"]     = "mm"
            else:
                df["variable"] = "unknown"; df["value"] = float("nan"); df["unit"] = "unknown"

        else:
            return None

        # Ensure every OUTPUT_COL exists
        for c in OUTPUT_COLS:
            if c not in df.columns:
                df[c] = None

        result = df[OUTPUT_COLS].copy()
        result["value"] = pd.to_numeric(result["value"], errors="coerce")
        return result.dropna(subset=["value"])

    except Exception as e:
        st.warning(f"Could not normalise {res_key}: {e}")
        return None


# ---------------------------------------------------------------------------
# Build folium map
# ---------------------------------------------------------------------------
def build_map(display_df: pd.DataFrame, center: list, zoom: int,
              jump_coords=None) -> folium.Map:
    m = folium.Map(location=center, zoom_start=zoom, tiles=None)

    # Base layers (mutually exclusive)
    folium.TileLayer("CartoDB positron", name="Street Map", show=True).add_to(m)
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri WorldImagery",
        name="Satellite",
        show=False,
    ).add_to(m)

    # Traffic overlay (Google Maps unofficial tile — overlaid on top of base layer)
    folium.TileLayer(
        tiles="https://mt0.google.com/vt/lyrs=h@159000000,traffic&hl=en&x={x}&y={y}&z={z}",
        attr="Google",
        name="Traffic",
        overlay=True,
        show=False,
    ).add_to(m)

    Draw(draw_options={
        "polyline": False, "rectangle": True, "polygon": True,
        "circle": False, "marker": True, "circlemarker": False,
    }).add_to(m)

    n_total = len(display_df)
    if n_total > 8000:
        sample_df = display_df.sample(8000, random_state=42)
        st.caption(f"⚠ Showing 8,000 of {n_total:,} stations on map. Refine filters to see all.")
    else:
        sample_df = display_df

    cluster = MarkerCluster(
        name="Stations",
        overlay=True, control=True, show=True,
        options={"maxClusterRadius": 40, "spiderfyOnMaxZoom": True}
    ).add_to(m)

    def _yrs(row, start_col: str, end_col: str, fallback: str) -> str:
        start = row.get(start_col)
        end = row.get(end_col)
        if pd.notna(start) and pd.notna(end):
            return f"{int(start)}-{int(end)}"
        return fallback

    for _, row in sample_df.iterrows():
        color = RES_COLOR.get(row["dominant_res"], "#999999")

        res_lines = []
        if row["has_asos_1min"]:
            res_lines.append(
                f"<b style='color:{RES_COLOR['1min']}'>1-min ASOS</b> "
                f"{row.get('asos_1min_start','?')} – {row.get('asos_1min_end','?')}"
            )
        if row["has_ncei_15min"]:
            res_lines.append(
                f"<b style='color:{RES_COLOR['15min']}'>15-min NCEI</b> "
                f"{_yrs(row, 'ncei15_start_year', 'ncei15_end_year', '1970-2026')}"
            )
        if row["has_asos_isd"]:
            res_lines.append(
                f"<b style='color:{RES_COLOR['hourly']}'>GHCNh Hourly</b> "
                f"{_yrs(row, 'hourly_start_year', 'hourly_end_year', '1930-2026')}"
            )
        if row["has_ghcn_daily"]:
            res_lines.append(
                f"<b style='color:{RES_COLOR['daily']}'>Daily GHCN</b> "
                f"{_yrs(row, 'daily_start_year', 'daily_end_year', '1950-2026')}"
            )

        elev = f"{row['elev_m']:.0f}" if pd.notna(row.get("elev_m")) else "?"
        popup_html = f"""
        <div style='font-size:12px;min-width:200px'>
          <b>{row.get('station_name','Unknown')}</b><br>
          {row.get('state','?')} — {row.get('county','?')}<br>
          Lat {row['lat']:.4f} &nbsp; Lon {row['lon']:.4f} &nbsp; Elev {elev}m<br>
          <hr style='margin:4px 0'>
          {'<br>'.join(res_lines) or 'No data'}
        </div>
        """
        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=5,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.75,
            weight=1,
            popup=folium.Popup(popup_html, max_width=280),
            tooltip=row.get("station_name", ""),
        ).add_to(cluster)

    # Legend — explicit dark-text white-background to survive any Streamlit theme
    legend_html = """
    <div style="
        position:fixed; bottom:30px; left:30px; z-index:9999;
        background:#ffffff !important;
        color:#222222 !important;
        padding:9px 14px;
        border-radius:6px;
        border:1px solid #aaaaaa;
        box-shadow:0 2px 8px rgba(0,0,0,0.25);
        font-size:11px;
        line-height:1.9;
        font-family:Arial,sans-serif;
    ">
      <span style="font-weight:700;color:#111111 !important;display:block;margin-bottom:2px">
        Stations
      </span>
    """
    for res, label in RES_LABELS.items():
        c = RES_COLOR[res]
        legend_html += (
            f'<span style="display:inline-block;width:11px;height:11px;'
            f'border-radius:50%;background:{c};margin-right:5px;vertical-align:middle"></span>'
            f'<span style="color:#222222 !important">{label}</span><br>'
        )
    legend_html += "</div>"
    # Inject legend via a <script> in the map <head> — completely outside
    # Folium's layer system so it never appears in LayerControl.
    import json as _json
    _legend_js = _json.dumps(legend_html)   # properly escaped JS string literal
    _legend_script = f"""<script>
(function(){{
    var _h = {_legend_js};
    function _run(){{
        if (document.getElementById('_wd_lgd')) return;
        var _w = document.createElement('div');
        _w.innerHTML = _h;
        var _el = _w.firstElementChild;
        if (_el) {{ _el.id = '_wd_lgd'; document.body.appendChild(_el); }}
    }}
    if (document.readyState === 'loading')
        document.addEventListener('DOMContentLoaded', _run);
    else _run();
}})();
</script>"""
    m.get_root().header.add_child(folium.Element(_legend_script))

    # Jump-to-location pin marker
    if jump_coords:
        folium.Marker(
            location=list(jump_coords),
            tooltip="Jump location",
            icon=folium.Icon(color="red", icon="map-marker", prefix="fa"),
        ).add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    return m


REFRESH_SCRIPT = str(_PROJECT_DIR / "refresh_data.py")
REFRESH_LOG    = str(_DATA_DIR / "recon" / "refresh.log")
PENDING_FILE   = str(_DATA_DIR / "recon" / ".refresh_pending.json")
PID_FILE       = str(_DATA_DIR / "recon" / ".refresh_pid")
DONE_SENTINEL  = str(_DATA_DIR / "recon" / ".refresh_done")
REFRESH_STATE_FILE   = str(_DATA_DIR / "recon" / ".refresh_state.json")
REFRESH_COMMAND_FILE = str(_DATA_DIR / "recon" / ".refresh_command.json")
REFRESH_CHECK_LOG    = str(_DATA_DIR / "recon" / "refresh_check.log")
REFRESH_CHECK_PID    = str(_DATA_DIR / "recon" / ".refresh_check_pid")


def _atomic_json_write(path: str, data: dict):
    _path = _Path(path)
    _path.parent.mkdir(parents=True, exist_ok=True)
    _tmp = _path.with_name(f".{_path.name}.{os.getpid()}.tmp")
    _tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
    os.replace(_tmp, _path)


def _read_json(path: str) -> dict | None:
    try:
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
    except Exception:
        return None
    return None


def _pid_from_file(path: str) -> int | None:
    """Return PID from a pidfile if the process is still alive."""
    try:
        pid = int(open(path).read().strip())
        os.kill(pid, 0)   # raises if process is gone
        return pid
    except Exception:
        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError:
            pass
        return None


def _refresh_pid() -> int | None:
    return _pid_from_file(PID_FILE)


def _launch_detached(cmd: list[str], log_path: str, pid_file: str) -> int:
    log_fh = open(log_path, "a")
    try:
        proc = subprocess.Popen(
            cmd, stdout=log_fh, stderr=log_fh,
            cwd=str(_PROJECT_DIR), start_new_session=True,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
    finally:
        log_fh.close()
    open(pid_file, "w").write(str(proc.pid))
    return proc.pid


def _resume_refresh_if_needed():
    """Resume an interrupted background refresh from the persisted command/state."""
    if _refresh_pid() or os.path.exists(DONE_SENTINEL) or os.path.exists(PENDING_FILE):
        return
    command = _read_json(REFRESH_COMMAND_FILE)
    state = _read_json(REFRESH_STATE_FILE)
    if not command or not command.get("cmd") or not state:
        return
    if state.get("phase") in {"complete", "failed"}:
        return

    now = time.time()
    last_attempt = float(command.get("last_resume_attempt", 0) or 0)
    if now - last_attempt < 30:
        return
    command["last_resume_attempt"] = now
    command["resume_attempts"] = int(command.get("resume_attempts", 0) or 0) + 1
    _atomic_json_write(REFRESH_COMMAND_FILE, command)
    try:
        pid = _launch_detached(command["cmd"], REFRESH_LOG, PID_FILE)
        st.sidebar.info(f"Resumed background refresh (PID {pid}).")
    except Exception as exc:
        st.sidebar.error(f"Could not resume refresh: {exc}")


def _running_refresh_banner():
    pid = _refresh_pid()
    if pid is None:
        return
    # If there's a progress file this is a background index-only process;
    # _show_index_banner() handles that UI — don't show the refresh banner too.
    if os.path.exists(INDEX_PROGRESS_FILE):
        return
    state = _read_json(REFRESH_STATE_FILE) or {}
    phase = state.get("phase", "running")
    st.warning(f"🔄 **Refresh running** (PID {pid}, {phase}) — app remains usable with the current indexed data.")
    c1, c2, c3 = st.columns(3)
    if c1.button("⏹ Stop refresh", key="stop_refresh"):
        try:
            os.kill(pid, signal.SIGTERM)
            st.toast("Stop signal sent — process will finish current file and exit.")
        except Exception as e:
            st.error(str(e))
    if c2.button("⏸ Pause", key="pause_refresh"):
        try:
            os.kill(pid, signal.SIGSTOP)
            st.toast("Paused. Click Resume to continue.")
        except Exception as e:
            st.error(str(e))
    if c3.button("▶ Resume", key="resume_refresh"):
        try:
            os.kill(pid, signal.SIGCONT)
            st.toast("Resumed.")
        except Exception as e:
            st.error(str(e))


def _read_index_progress() -> dict | None:
    """Read the progress JSON written by the background index process."""
    try:
        if os.path.exists(INDEX_PROGRESS_FILE):
            with open(INDEX_PROGRESS_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return None


def _show_index_banner():
    """Floating sidebar notification while background indexing runs.

    When indexing finishes (PID gone), clears caches and triggers a rerun so
    the app picks up new data automatically.
    """
    pid = _refresh_pid()

    # Detect transition: was indexing last render, now it's done
    was_indexing = st.session_state.get("_bg_indexing", False)
    if was_indexing and not pid:
        # Indexing just finished — reload app with fresh data
        st.session_state.pop("_bg_indexing", None)
        st.cache_data.clear()
        st.cache_resource.clear()
        gc.collect()
        st.rerun()
        return

    if not pid:
        return

    # Still running
    st.session_state["_bg_indexing"] = True
    prog = _read_index_progress()

    phase_labels = {
        "inventory":  "Inserting file records",
        "commit":     "Publishing staged files",
        "profiling":  "Reading file metadata",
        "writing":    "Writing profile to database",
        "coverage":   "Rebuilding station coverage",
        "master_list": "Updating master station list",
        "done":       "Complete",
    }

    if prog:
        phase = prog.get("phase", "")
        done  = prog.get("done", 0)
        total = prog.get("total", 0)
        msg   = prog.get("msg", "Indexing new data…")
        pct   = (done / total) if total > 0 else None
        label = phase_labels.get(phase, phase)

        # Check if marked done but PID somehow still shows (race) — ignore
        if phase == "done":
            return

        with st.sidebar.container(border=True):
            st.markdown("**📦 Indexing new data in background**")
            if pct is not None:
                st.progress(pct, text=f"{label}: {done:,} / {total:,}")
            else:
                st.progress(0.0, text=label)
            st.caption(msg)
    else:
        # Progress file not yet written (process just started)
        with st.sidebar.container(border=True):
            st.markdown("**📦 Indexing new data in background**")
            st.progress(0.0, text="Starting…")
            st.caption("App is fully usable. New data appears automatically when done.")


# =============================================================================
# APP LAYOUT
# =============================================================================

# ---------------------------------------------------------------------------
# Sidebar — branding + filters
# ---------------------------------------------------------------------------
# ── Auto-detect completed download → launch index phase in background ─────────
# Guard: only launch once per session (session_state) AND only if DONE_SENTINEL
# still exists AND no index process is already running.
_resume_refresh_if_needed()
if (os.path.exists(DONE_SENTINEL)
        and not _refresh_pid()
        and not st.session_state.get("_bg_indexing")):
    # Remove sentinel immediately — prevents duplicate Popen on next re-render
    # before the child process has had time to write its own PID file.
    try:
        os.remove(DONE_SENTINEL)
    except OSError:
        pass
    # Release the persistent DB connection so the index process can get the write lock
    st.cache_data.clear()
    st.cache_resource.clear()
    gc.collect()
    # Mark indexing active in session state BEFORE launching so get_db() routes
    # around the persistent cache from the very first call this render.
    st.session_state["_bg_indexing"] = True
    # Launch indexing as a background subprocess — app continues loading immediately
    subprocess.Popen(
        [sys.executable, REFRESH_SCRIPT, "--index-only"],
        stdout=open(REFRESH_LOG, "a"),
        stderr=subprocess.STDOUT,
        cwd=str(_PROJECT_DIR),
    )
    # Fall through — app loads with existing data while indexing runs in background

_logo_path = str(_PROJECT_DIR / "logo.svg")
try:
    with open(_logo_path, "rb") as _lf:
        _logo_b64 = base64.b64encode(_lf.read()).decode()
    st.sidebar.markdown(
        f'<div style="padding:6px 0 10px 0">'
        f'<img src="data:image/svg+xml;base64,{_logo_b64}" '
        f'style="width:100%;max-width:260px;display:block">'
        f'</div>',
        unsafe_allow_html=True,
    )
except FileNotFoundError:
    st.sidebar.title("🌦 Weather Explorer")
    st.sidebar.markdown(
        "<div style='margin:-6px 0 10px 0;line-height:1.4'>"
        "<span style='font-size:12px;font-weight:700;letter-spacing:0.06em;color:#444'>CREATE LAB</span><br>"
        "<span style='font-size:11px;color:#666'>Jackson State University</span>"
        "</div>",
        unsafe_allow_html=True,
    )

all_stations = load_stations()

# Session state used by sidebar filters, jump search, map drawings, and queue.
if "selected_stations"  not in st.session_state: st.session_state["selected_stations"]  = None
if "last_drawing_count" not in st.session_state: st.session_state["last_drawing_count"] = 0
if "queue_ids"          not in st.session_state: st.session_state["queue_ids"]          = set()
if "map_center"         not in st.session_state: st.session_state["map_center"]         = None
if "map_zoom"           not in st.session_state: st.session_state["map_zoom"]           = None
if "map_version"        not in st.session_state: st.session_state["map_version"]        = 0
if "jump_coords"        not in st.session_state: st.session_state["jump_coords"]        = None
if "jump_active"        not in st.session_state: st.session_state["jump_active"]        = False
if "_last_queue_jump"   not in st.session_state: st.session_state["_last_queue_jump"]   = None

# ── Background indexing progress banner (runs on every render) ───────────────
_show_index_banner()

# Last refreshed
last_refreshed = load_last_refreshed()
st.sidebar.caption(f"Last Refreshed: {last_refreshed}")
st.sidebar.markdown("---")

states    = sorted(all_stations["state"].dropna().unique())
sel_state = st.sidebar.selectbox("State", ["All US"] + states)

sel_res = st.sidebar.multiselect(
    "Resolution",
    ["1-min", "15-min", "GHCNh Hourly", "Daily"],
    default=["1-min", "15-min", "GHCNh Hourly", "Daily"],
)

year_range = st.sidebar.slider("Year Range", 1940, 2026, (1950, 2026))

search_radius = st.sidebar.number_input(
    "Radius for point search (km)", min_value=1, max_value=500, value=1, step=1
)

# Jump to location
st.sidebar.markdown("**Jump to location:**")
jump_input = st.sidebar.text_input(
    "Lat, Lon", placeholder="e.g. 32.299, -90.185  or  32.2969° N, 90.2064° W",
    key="jump_input", label_visibility="collapsed"
)

def _parse_latlon(text: str) -> tuple[float, float] | None:
    """Parse decimal or degree-symbol+cardinal formats into (lat, lon)."""
    # Strip degree symbols, extra whitespace
    clean = re.sub(r"[°º]", " ", text).strip()
    # Match: number [N/S] separator number [E/W]
    m = re.match(
        r"([+-]?\d+\.?\d*)\s*([NSns])?\s*[,\s]+\s*([+-]?\d+\.?\d*)\s*([EWew])?",
        clean,
    )
    if not m:
        return None
    lat = float(m.group(1))
    lon = float(m.group(3))
    if m.group(2) and m.group(2).upper() == "S":
        lat = -lat
    if m.group(4) and m.group(4).upper() == "W":
        lon = -lon
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None
    return lat, lon

if jump_input:
    _parsed = _parse_latlon(jump_input)
    if _parsed:
        st.session_state["map_center"]  = list(_parsed)
        st.session_state["map_zoom"]    = 13
        st.session_state["jump_coords"] = _parsed
        st.session_state["jump_active"] = True
    else:
        st.sidebar.warning("Enter as: 32.299, -90.185  or  32.2969° N, 90.2064° W")
else:
    st.session_state["jump_active"] = False
    st.session_state["jump_coords"] = None
    st.session_state["_last_queue_jump"] = None
    st.session_state["map_center"] = None
    st.session_state["map_zoom"] = None

if st.session_state.get("jump_active"):
    if st.sidebar.button("✖ Clear jump", key="clear_jump", use_container_width=True):
        st.session_state["jump_active"] = False
        st.session_state["jump_coords"] = None
        st.session_state["_last_queue_jump"] = None
        st.session_state["map_center"] = None
        st.session_state["map_zoom"] = None
        st.session_state["map_version"] += 1
        st.rerun()

st.sidebar.markdown("---")
st.sidebar.caption("**Data sources:**")
st.sidebar.caption("ASOS 1-min · GHCNh Hourly · GHCNd Daily · NCEI 15-min")

# ---------------------------------------------------------------------------
# Refresh Database — sidebar expander
# ---------------------------------------------------------------------------
# Regex patterns to parse progress from log lines
_RE_PROGRESS = re.compile(r"\[\s*([\d,]+)\s*/\s*([\d,]+)\s*\].*eta=(\d+)s", re.IGNORECASE)
_RE_PHASE    = re.compile(
    r"(Refreshing:|Fetching|Downloading|Converting|Inserting|Post-refresh|"
    r"Building|Profiling|station inventory|directory listing)",
    re.IGNORECASE,
)
# Coarse phase → fraction of total bar
_PHASE_FRACS = [
    ("connecting",        0.02),
    ("inventory",         0.05),
    ("directory listing", 0.10),
    ("to download",       0.12),
    ("downloading",       0.15),   # actual per-station progress computed dynamically
    ("converting",        0.80),
    ("inserting",         0.85),
    ("post-refresh",      0.90),
    ("profiling",         0.93),
    ("building",          0.96),
    ("complete",          1.00),
]


def _line_to_frac(line: str, current_frac: float, current: int, total: int) -> tuple[float, int, int, str]:
    """Parse a log line → (fraction 0-1, current, total, eta_str)."""
    lower = line.lower()

    # Numeric [current/total] progress from GHCN / HPD / NCEI download loops
    m = _RE_PROGRESS.search(line)
    if m:
        cur = int(m.group(1).replace(",", ""))
        tot = int(m.group(2).replace(",", ""))
        eta_s = int(m.group(3))
        if tot > 0:
            # Download phase spans 0.15 → 0.78 of the bar
            frac = 0.15 + (cur / tot) * 0.63
            eta_str = f"{eta_s // 60}m {eta_s % 60}s"
            return frac, cur, tot, eta_str
        return current_frac, current, total, ""

    # Phase keyword → coarse jump
    for keyword, target in _PHASE_FRACS:
        if keyword in lower and target > current_frac:
            return target, current, total, ""

    return current_frac, current, total, ""


# ---------------------------------------------------------------------------
# Coverage SVG generation
# ---------------------------------------------------------------------------

def _generate_coverage_svgs() -> tuple[bytes, bytes]:
    """Return (spatial_svg_bytes, temporal_svg_bytes) using matplotlib + cartopy."""
    import matplotlib
    matplotlib.rcParams["font.family"] = "Times New Roman"
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import cartopy
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

    # temporal_resolution values stored in station_coverage / station_coverage_geo
    SOURCE_ORDER  = ["1min", "15min", "hourly", "daily"]
    SOURCE_LABELS = ["ASOS 1-min", "NCEI 15-min", "GHCNh Hourly", "GHCN Daily"]
    tab10 = plt.cm.tab10.colors
    SOURCE_COLOR  = {s: tab10[i] for i, s in enumerate(SOURCE_ORDER)}

    # --- Load data ---
    # Guard against calling during indexing — the DB write lock may be held.
    indexing = (
        st.session_state.get("_bg_indexing", False)
        or os.path.exists(str(_DATA_DIR / "recon" / ".refresh_pid"))
    )
    if indexing:
        raise RuntimeError(
            "Database is temporarily busy (indexing in progress). "
            "Please try again once indexing has finished."
        )
    try:
        con2 = duckdb.connect(DB_PATH, read_only=True)
        geo = con2.execute(
            "SELECT temporal_resolution, lat, lon FROM station_coverage_geo "
            "WHERE lat IS NOT NULL AND lon IS NOT NULL"
        ).fetchdf()
        cov_cols = {
            r[0] for r in con2.execute("DESCRIBE station_coverage").fetchall()
        }
        if {"n_stations", "total_rows"}.issubset(cov_cols):
            cov = con2.execute(
                "SELECT temporal_resolution, MIN(earliest_date) AS first_date, "
                "MAX(latest_date) AS last_date, "
                "MAX(n_stations) AS n_stations, SUM(total_rows) AS total_rows "
                "FROM station_coverage GROUP BY temporal_resolution"
            ).fetchdf()
        else:
            cov = con2.execute(
                "SELECT temporal_resolution, MIN(earliest_date) AS first_date, "
                "MAX(latest_date) AS last_date, "
                "COUNT(*) AS n_stations, SUM(total_rows) AS total_rows "
                "FROM station_coverage GROUP BY temporal_resolution"
            ).fetchdf()
        con2.close()
    except Exception:
        raise

    # --- Figure 1: Spatial map ---
    fig1, ax1 = plt.subplots(
        1, 1, figsize=(14, 8),
        subplot_kw={"projection": ccrs.LambertConformal(central_longitude=-96)},
    )
    ax1.set_extent([-125, -66, 24, 50], crs=ccrs.PlateCarree())
    ax1.coastlines(linewidth=0.4)
    ax1.add_feature(cfeature.STATES, linewidth=0.3)
    ax1.add_feature(cfeature.BORDERS, linewidth=0.4)
    legend_handles = []
    for src, label in zip(SOURCE_ORDER, SOURCE_LABELS):
        sub = geo[geo["temporal_resolution"] == src]
        if sub.empty:
            continue
        ax1.scatter(
            sub["lon"], sub["lat"], s=4, color=SOURCE_COLOR[src],
            transform=ccrs.PlateCarree(), alpha=0.6, zorder=5,
        )
        legend_handles.append(
            mpatches.Patch(color=SOURCE_COLOR[src], label=f"{label} (n={len(sub):,})")
        )
    ax1.legend(handles=legend_handles, loc="lower right", fontsize=10,
               title="Source", title_fontsize=11)
    ax1.set_title("WeatherData — Station Coverage by Source", fontsize=14)
    buf1 = io.BytesIO()
    fig1.savefig(buf1, format="svg", bbox_inches="tight")
    plt.close(fig1)

    # --- Figure 2: Temporal coverage chart ---
    import matplotlib.ticker as ticker

    fig2, ax2 = plt.subplots(figsize=(14, 5))
    y_positions = range(len(SOURCE_ORDER))
    for i, (src, label) in enumerate(zip(SOURCE_ORDER, SOURCE_LABELS)):
        row = cov[cov["temporal_resolution"] == src]
        if row.empty:
            continue
        first = pd.to_datetime(row["first_date"].iloc[0], errors="coerce")
        last  = pd.to_datetime(row["last_date"].iloc[0],  errors="coerce")
        if pd.isna(first) or pd.isna(last):
            continue
        yr0, yr1 = first.year, last.year
        n_st   = int(row["n_stations"].iloc[0])
        n_rows = int(row["total_rows"].iloc[0]) if row["total_rows"].iloc[0] else 0
        bar_width = max(1, yr1 - yr0)
        ax2.barh(i, bar_width, left=yr0, color=SOURCE_COLOR[src], alpha=0.8, height=0.6)
        # Place annotation inside the bar, right-aligned, so it never overflows
        ann = f"{n_st:,} stn  {n_rows/1e6:.1f}M rows"
        ax2.text(
            yr1 - 1, i, ann,
            ha="right", va="center", fontsize=8.5,
            color="white", fontweight="bold",
        )
        # Year range outside left edge of bar (short, never overflows)
        ax2.text(
            yr0 - 1, i, str(yr0),
            ha="right", va="center", fontsize=8, color="#444",
        )
    ax2.set_yticks(list(y_positions))
    ax2.set_yticklabels(SOURCE_LABELS, fontsize=11)
    ax2.set_xlabel("Year", fontsize=11)
    ax2.set_title("WeatherData — Temporal Coverage by Source", fontsize=14)
    ax2.xaxis.set_major_locator(ticker.MultipleLocator(20))
    ax2.xaxis.set_minor_locator(ticker.MultipleLocator(5))
    ax2.grid(axis="x", linewidth=0.4, alpha=0.5)
    ax2.margins(x=0.01)
    fig2.tight_layout()
    buf2 = io.BytesIO()
    fig2.savefig(buf2, format="svg", bbox_inches="tight")
    plt.close(fig2)

    return buf1.getvalue(), buf2.getvalue()


with st.sidebar.expander("🔄 Refresh Database", expanded=False):
    _running_pid = _refresh_pid()

    if _running_pid:
        # ── Refresh already running — show status only, no launch controls ─────
        st.info(f"🔄 **Download running** (PID {_running_pid})")
        st.caption("Downloading in background. This panel will unlock when it finishes.")
        _log_tail = ""
        try:
            with open(REFRESH_LOG) as _lf:
                _log_tail = "\n".join(_lf.read().splitlines()[-6:])
        except Exception:
            pass
        if _log_tail:
            st.code(_log_tail, language="text")
        c1, c2 = st.columns(2)
        if c1.button("⏹ Stop", key="stop_refresh2", use_container_width=True):
            try:
                os.kill(_running_pid, signal.SIGTERM)
                st.toast("Stop signal sent.")
            except Exception as e:
                st.error(str(e))
        if c2.button("🔄 Refresh status", key="ref_status", use_container_width=True):
            st.rerun()
    else:
        # ── No refresh running — show launch controls ──────────────────────────
        SOURCES = ["All Sources", "GHCN_daily", "ASOS_ISD", "NCEI_15min", "ASOS_1min"]
        ref_source  = st.selectbox("Source", SOURCES, key="ref_source")
        _cpu_max = max(8, (os.cpu_count() or 4))
        ref_workers = st.slider("Parallel workers", 1, _cpu_max,
                                min(4, _cpu_max), key="ref_workers",
                                help="More workers = faster GHCN downloads. Max = logical CPU count.")
        st.caption("ℹ Refresh downloads updates for all US stations regardless of the state filter.")

        # ── Step 1: Check ──────────────────────────────────────────────────────
        _check_pid = _pid_from_file(REFRESH_CHECK_PID)
        if _check_pid:
            st.info(f"Checking NOAA listings in background (PID {_check_pid}).")
            try:
                with open(REFRESH_CHECK_LOG) as _cf:
                    st.code("\n".join(_cf.read().splitlines()[-10:]), language="text")
            except Exception:
                pass
            if st.button("Refresh check status", use_container_width=True, key="ref_check_status"):
                st.rerun()
        else:
            if st.session_state.get("ref_check_running") and os.path.exists(REFRESH_CHECK_LOG):
                import re as _re
                with open(REFRESH_CHECK_LOG) as _cf:
                    _text = _cf.read()
                _counts = _re.findall(r"Would (?:download|refresh)\s+([\d,]+)", _text)
                _total  = sum(int(c.replace(",", "")) for c in _counts)
                st.code("\n".join(_text.splitlines()[-15:]), language="text")
                st.session_state["ref_check_total"] = _total
                st.session_state["ref_check_done"] = True
                st.session_state["ref_check_running"] = False

            if st.button("🔍 Check what's new", use_container_width=True, key="ref_check"):
                _cmd = [sys.executable, REFRESH_SCRIPT]
                if ref_source == "All Sources":
                    _cmd += ["--all"]
                else:
                    _cmd += ["--source", ref_source]
                _cmd += ["--dry-run", "--pid-file", REFRESH_CHECK_PID]
                try:
                    open(REFRESH_CHECK_LOG, "w").close()
                    _launch_detached(_cmd, REFRESH_CHECK_LOG, REFRESH_CHECK_PID)
                    st.session_state["ref_check_running"] = True
                    st.session_state["ref_check_done"] = False
                    st.toast("Refresh check started in background.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Failed to launch check: {exc}")

        # ── Step 2: Confirm & Launch ───────────────────────────────────────────
        if st.session_state.get("ref_check_done"):
            total = st.session_state.get("ref_check_total", 0)
            if total <= 0:
                st.success("No new files found.")
                if st.button("Clear refresh check", use_container_width=True, key="ref_check_clear"):
                    st.session_state["ref_check_done"] = False
                    st.session_state["ref_check_total"] = 0
                    st.rerun()
            else:
                _eta_min = max(1, int(total * 0.4 / 60 / ref_workers))
                st.warning(
                    f"**{total:,} files to download**  "
                    f"(~{_eta_min}–{_eta_min*2} min with {ref_workers} worker(s))\n\n"
                    "This runs in the background — you can keep exploring while it downloads."
                )

                if st.button("🚀 Start Refresh", use_container_width=True, key="ref_start",
                             type="primary"):
                    _cmd = [sys.executable, REFRESH_SCRIPT]
                    if ref_source == "All Sources":
                        _cmd += ["--all"]
                    else:
                        _cmd += ["--source", ref_source]
                    _cmd += ["--workers", str(ref_workers)]
                    _atomic_json_write(REFRESH_COMMAND_FILE, {
                        "cmd": _cmd,
                        "created_at": datetime.now().isoformat(timespec="seconds"),
                        "source": ref_source,
                        "workers": int(ref_workers),
                    })
                    try:
                        _pid = _launch_detached(_cmd, REFRESH_LOG, PID_FILE)
                        st.session_state["ref_check_done"]  = False
                        st.session_state["ref_check_total"] = 0
                        st.toast(f"🚀 Refresh started in background (PID {_pid})!")
                        st.rerun()
                    except Exception as exc:
                        try:
                            os.remove(REFRESH_COMMAND_FILE)
                        except OSError:
                            pass
                        st.error(f"Failed to launch: {exc}")

# ---------------------------------------------------------------------------
# Apply sidebar filters
# ---------------------------------------------------------------------------
filtered = all_stations.copy()

_jump_active = st.session_state.get("jump_active", False)
_jump_coords = st.session_state.get("jump_coords")

if _jump_active and _jump_coords:
    # Jump-to-location supersedes state dropdown — filter by radius around jumped point
    filtered = filter_by_radius(all_stations, _jump_coords[0], _jump_coords[1], search_radius)
    # Add nearby stations to queue (once per distinct jump location)
    if st.session_state.get("_last_queue_jump") != _jump_coords:
        _new_ids = set(filtered["ncei_id"].tolist()) - st.session_state["queue_ids"]
        st.session_state["queue_ids"].update(_new_ids)
        st.session_state["_last_queue_jump"] = _jump_coords
        if _new_ids:
            st.toast(f"Added {len(_new_ids):,} nearby station(s) to queue")
elif sel_state != "All US":
    filtered = filtered[filtered["state"] == sel_state]

_RES_FILTERS = {
    "1-min":         ("has_asos_1min",  "asos1min_start_year", "asos1min_end_year"),
    "15-min":        ("has_ncei_15min", "ncei15_start_year",   "ncei15_end_year"),
    "GHCNh Hourly":  ("has_asos_isd",   "hourly_start_year",   "hourly_end_year"),
    "Daily":         ("has_ghcn_daily", "daily_start_year",    "daily_end_year"),
}

res_year_mask = pd.Series(False, index=filtered.index)
for _res_label in sel_res:
    _meta = _RES_FILTERS.get(_res_label)
    if not _meta:
        continue
    _flag_col, _start_col, _end_col = _meta
    res_year_mask |= (
        filtered[_flag_col].fillna(False)
        & (filtered[_start_col] <= year_range[1])
        & (filtered[_end_col] >= year_range[0])
    )
filtered = filtered[res_year_mask]

_jump_sig = tuple(round(float(v), 6) for v in _jump_coords) if _jump_coords else None
_filter_signature = (
    sel_state,
    tuple(sel_res),
    tuple(year_range),
    int(search_radius),
    bool(_jump_active),
    _jump_sig,
)
if "_filter_signature" not in st.session_state:
    st.session_state["_filter_signature"] = _filter_signature
elif st.session_state["_filter_signature"] != _filter_signature:
    st.session_state["_filter_signature"] = _filter_signature
    st.session_state["selected_stations"] = None
    st.session_state["last_drawing_count"] = 0
    st.session_state["map_version"] += 1

st.sidebar.metric("Matching Stations", f"{len(filtered):,}")
if len(filtered) > 0:
    gb = (
        filtered["asos_1min_files"]  * 0.000463 * int("1-min"         in sel_res)
      + filtered["asos_isd_files"]   * 0.0001   * int("GHCNh Hourly"    in sel_res)
      + filtered["ncei_files"]       * 0.0006   * int("15-min"        in sel_res)
      + filtered["ghcn_daily_files"] * 0.0001   * int("Daily"         in sel_res)
    ).sum()
    st.sidebar.metric("Est. Data Size", f"~{gb:.1f} GB")

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------
if "selected_stations"  not in st.session_state: st.session_state["selected_stations"]  = None
if "last_drawing_count" not in st.session_state: st.session_state["last_drawing_count"] = 0
if "queue_ids"          not in st.session_state: st.session_state["queue_ids"]          = set()
if "map_center"         not in st.session_state: st.session_state["map_center"]         = None
if "map_zoom"           not in st.session_state: st.session_state["map_zoom"]           = None
if "map_version"        not in st.session_state: st.session_state["map_version"]        = 0
if "jump_coords"        not in st.session_state: st.session_state["jump_coords"]        = None
if "jump_active"        not in st.session_state: st.session_state["jump_active"]        = False
if "_last_queue_jump"   not in st.session_state: st.session_state["_last_queue_jump"]   = None

# ---------------------------------------------------------------------------
# Download Queue — sidebar (shown only when non-empty)
# ---------------------------------------------------------------------------
q_ids = st.session_state["queue_ids"]
if q_ids:
    queued_df = all_stations[all_stations["ncei_id"].isin(q_ids)].drop_duplicates("ncei_id").copy()
    with st.sidebar.expander(f"📥 Download Queue  ({len(q_ids):,})", expanded=True):
        for _qi, (_, qrow) in enumerate(queued_df.iterrows()):
            qc1, qc2 = st.columns([5, 1])
            qc1.caption(f"{qrow['station_name']}  ({qrow['state']})")
            if qc2.button("✕", key=f"rm_{_qi}_{qrow['ncei_id']}"):
                st.session_state["queue_ids"].discard(qrow["ncei_id"])
                st.rerun()
        if st.button("🗑 Clear Queue", use_container_width=True, key="clear_queue"):
            st.session_state["queue_ids"].clear()
            st.rerun()

# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------
if st.session_state["map_center"]:
    center = st.session_state["map_center"]
    zoom   = st.session_state.get("map_zoom") or 11
elif len(filtered) > 0:
    center = [float(filtered["lat"].mean()), float(filtered["lon"].mean())]
    zoom   = 6 if sel_state != "All US" else 4
else:
    center = [39.8, -98.5]
    zoom   = 4

_running_refresh_banner()

# Live refresh progress (polls every 5s when a refresh is active)
if _refresh_pid() or os.path.exists(DONE_SENTINEL):
    @st.fragment(run_every=5)
    def _refresh_progress():
        pid  = _refresh_pid()
        done = os.path.exists(DONE_SENTINEL)
        if not pid and not done:
            return
        try:
            with open(REFRESH_LOG) as _lf:
                _lines = _lf.readlines()
            _tail = "".join(_lines[-20:])
        except Exception:
            _tail = "(log not readable)"
        if pid:
            st.caption(f"🔄 Refresh in progress (PID {pid})")
        elif done:
            st.caption("📦 Download complete — indexing will start automatically on next render.")
        st.code(_tail, language="text")
    _refresh_progress()

m          = build_map(filtered, center, zoom, jump_coords=st.session_state.get("jump_coords"))
map_result = st_folium(
    m, width="100%", height=520,
    returned_objects=["all_drawings", "last_object_clicked"],
    key=f"station_map_{st.session_state['map_version']}",
)

# ---------------------------------------------------------------------------
# Handle map drawings → select stations
# ---------------------------------------------------------------------------
drawings   = (map_result or {}).get("all_drawings") or []
n_drawings = len(drawings)

if n_drawings != st.session_state["last_drawing_count"]:
    st.session_state["last_drawing_count"] = n_drawings
    st.session_state["selected_stations"]  = None

if drawings:
    sel_from_draw = None
    for drawing in drawings:
        geom      = drawing.get("geometry", {})
        geom_type = geom.get("type", "")
        coords    = geom.get("coordinates", [])

        if geom_type == "Point" and coords:
            click_lon, click_lat = float(coords[0]), float(coords[1])
            sel_from_draw = filter_by_radius(filtered, click_lat, click_lon, search_radius)
            st.info(f"📍 {len(sel_from_draw):,} stations within {search_radius} km of "
                    f"({click_lat:.3f}, {click_lon:.3f})")

        elif geom_type in ("Polygon", "Rectangle") and coords:
            try:
                from shapely.geometry import Point as SPoint, shape as sshape
                polygon      = sshape(geom)
                in_poly_mask = filtered.apply(
                    lambda r: polygon.contains(SPoint(r["lon"], r["lat"])), axis=1
                )
                sel_from_draw = filtered[in_poly_mask].copy()
                st.info(f"🔷 {len(sel_from_draw):,} stations inside polygon")
            except ImportError:
                st.warning("Install shapely for polygon selection: `pip install shapely`")

    if sel_from_draw is not None:
        st.session_state["selected_stations"] = sel_from_draw

# ---------------------------------------------------------------------------
# Handle marker click → add station to download queue
# ---------------------------------------------------------------------------
clicked_obj = (map_result or {}).get("last_object_clicked")
if clicked_obj and isinstance(clicked_obj, dict):
    _clat = clicked_obj.get("lat")
    _clng = clicked_obj.get("lng")
    if _clat is not None and _clng is not None and len(filtered) > 0:
        _dists = np.sqrt(
            (filtered["lat"].values - _clat) ** 2 +
            (filtered["lon"].values - _clng) ** 2
        )
        _min_i = int(np.argmin(_dists))
        if _dists[_min_i] < 0.01:   # ~1 km tolerance
            _hit = filtered.iloc[_min_i]
            _nid = _hit["ncei_id"]
            if _nid not in st.session_state["queue_ids"]:
                st.session_state["queue_ids"].add(_nid)
                st.toast(f"➕ {_hit['station_name']} added to queue")

# Keep any active map selection consistent with the current sidebar filters.
if st.session_state["selected_stations"] is not None:
    _allowed_ids = set(filtered["ncei_id"].tolist())
    _selected_now = st.session_state["selected_stations"]
    _selected_now = _selected_now[_selected_now["ncei_id"].isin(_allowed_ids)].copy()
    st.session_state["selected_stations"] = _selected_now if len(_selected_now) else None

if st.button("✖ Clear map selection", use_container_width=False, key="clr_sel_main"):
    st.session_state["selected_stations"] = None
    st.session_state["last_drawing_count"] = 0
    st.session_state["map_version"] += 1
    st.rerun()

# Active display set (drawing selection or full filtered list)
_sel = st.session_state["selected_stations"]
display_df = _sel if _sel is not None else filtered

# Queue as a DataFrame (for extraction)
_queue_ids = st.session_state["queue_ids"]
queue_df   = all_stations[all_stations["ncei_id"].isin(_queue_ids)].copy() if _queue_ids else pd.DataFrame()

# ---------------------------------------------------------------------------
# Station table
# ---------------------------------------------------------------------------
_tbl_label = (
    f"Stations — {len(display_df):,}"
    + (f"  |  📥 Queue: {len(_queue_ids):,}" if _queue_ids else "")
)
st.subheader(_tbl_label)

table_cols = [
    "station_name", "state", "county", "lat", "lon", "elev_m",
    "has_asos_1min", "has_asos_isd", "has_ncei_15min", "has_ghcn_daily",
    "data_start", "data_end",
]
show_cols = [c for c in table_cols if c in display_df.columns]
st.dataframe(
    display_df[show_cols]
    .rename(columns={"has_asos_isd": "has_ghcnh"})
    .head(500).reset_index(drop=True),
    use_container_width=True, height=260,
)

# ---------------------------------------------------------------------------
# Download row: Station List (metadata) | Extract Data (observations)
# ---------------------------------------------------------------------------
col_exp, col_ext = st.columns([1, 2])

# Station list — metadata only, timestamped filename
with col_exp:
    ts_now    = datetime.now().strftime("%Y%m%d_%H%M%S")
    _export_df = (
        display_df
        .drop(columns=["has_hpd_1hr"], errors="ignore")
        .rename(columns={"has_asos_isd": "has_ghcnh"})
    )
    csv_bytes = _export_df.to_csv(index=False).encode()
    st.download_button(
        "⬇ Station List (CSV)",
        csv_bytes,
        file_name=f"stations_{ts_now}.csv",
        mime="text/csv",
        use_container_width=True,
    )

# Extract observations — normalised, merged, single file
with col_ext:
    with st.expander("⚡ Extract Data", expanded=False):
        # Source toggle: map selection vs queue
        _ext_options = ["Map selection"]
        if not queue_df.empty:
            _ext_options.append(f"Download queue ({len(queue_df):,} stations)")
        ext_source = st.radio("Extract from", _ext_options, horizontal=True, key="ext_source_radio")
        ext_df = queue_df if (not queue_df.empty and "queue" in ext_source) else display_df

        ext_res = st.multiselect(
            "Resolutions to extract",
            ["1-min", "15-min", "GHCNh Hourly", "Daily"],
            default=[r for r in sel_res if r in ["Daily", "GHCNh Hourly", "15-min", "1-min"]],
        )
        row_cap = st.number_input(
            "Max rows per resolution", 100_000, 5_000_000, 1_000_000, step=100_000
        )

        if st.button("▶ Run Extraction", use_container_width=True):
            if not ext_res:
                st.warning("Select at least one resolution.")
            elif len(ext_df) == 0:
                st.warning("No stations in selection.")
            else:
                ts_extract                   = datetime.now().strftime("%Y%m%d_%H%M%S")
                files_by_res, sid_by_res     = get_file_paths(ext_df, ext_res)
                total_files                  = sum(len(v) for v in files_by_res.values())

                st.write("**Files found:** " + " · ".join(
                    f"{k}: {len(v):,}" for k, v in files_by_res.items()
                ))

                if total_files == 0:
                    st.warning("No matching files found for the selected stations and resolutions.")
                else:
                    all_chunks: list[pd.DataFrame] = []

                    for res_key, fp_list in files_by_res.items():
                        if not fp_list:
                            continue

                        date_col, is_dt = DATE_COL_MAP.get(res_key, ("date", False))
                        n_files_shown   = min(len(fp_list), MAX_EXTRACT_FILES)
                        st.write(f"**{res_key}** — reading {n_files_shown:,}/{len(fp_list):,} files …")

                        with st.spinner(f"Extracting {res_key} …"):
                            df_raw = extract_raw(
                                fp_list, date_col,
                                year_range[0], year_range[1],
                                datetime_col=is_dt,
                                station_ids=sid_by_res.get(res_key),
                                station_col=STATION_COL_MAP.get(res_key),
                                row_limit=int(row_cap),
                            )

                        if df_raw is None or len(df_raw) == 0:
                            st.warning(f"  No rows in {year_range[0]}–{year_range[1]} for {res_key}.")
                            continue

                        with st.spinner(f"Normalising {res_key} …"):
                            meta_df  = build_meta_df(ext_df, res_key)
                            df_norm  = normalize_df(df_raw, res_key, meta_df)

                        if df_norm is None or len(df_norm) == 0:
                            st.warning(f"  Normalisation returned 0 rows for {res_key}.")
                            continue

                        st.success(f"  ✓ {len(df_norm):,} rows from {res_key}")
                        all_chunks.append(df_norm)

                    if not all_chunks:
                        st.warning("No data extracted across any resolution.")
                    else:
                        merged = pd.concat(all_chunks, ignore_index=True)
                        state_tag = sel_state.replace(" ", "_")
                        filename  = f"extracted_data_{ts_extract}.csv"

                        st.success(
                            f"**{len(merged):,} total rows** across "
                            f"{len(all_chunks)} resolution(s)  |  "
                            f"{merged['station_id'].nunique():,} unique stations"
                        )

                        buf = io.StringIO()
                        merged.to_csv(buf, index=False)
                        st.download_button(
                            f"⬇ Download Merged CSV ({len(merged):,} rows)",
                            buf.getvalue().encode(),
                            file_name=filename,
                            mime="text/csv",
                            use_container_width=True,
                        )

                        # Schema preview
                        with st.expander("Column schema"):
                            st.write(merged.dtypes.reset_index().rename(
                                columns={"index": "column", 0: "dtype"}
                            ))
                            st.dataframe(merged.head(10), use_container_width=True)

# Export Coverage Maps
with st.expander("📊 Export Coverage Maps", expanded=False):
    st.caption("Generates spatial + temporal SVG figures for the 4 active data sources.")
    if st.button("Generate & Download SVGs", use_container_width=True):
        with st.spinner("Generating coverage maps…"):
            try:
                svg1, svg2 = _generate_coverage_svgs()
                c1, c2 = st.columns(2)
                c1.download_button(
                    "⬇ Spatial Map (SVG)", svg1,
                    file_name="coverage_spatial.svg", mime="image/svg+xml",
                    use_container_width=True,
                )
                c2.download_button(
                    "⬇ Temporal Chart (SVG)", svg2,
                    file_name="coverage_temporal.svg", mime="image/svg+xml",
                    use_container_width=True,
                )
            except Exception as e:
                st.error(f"Map generation failed: {e}  (Is cartopy installed? pip install cartopy)")
