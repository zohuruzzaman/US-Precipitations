"""
app.py — Weather Data Explorer
Streamlit single-file app for exploring and extracting from the WeatherData archive.

Run: cd ~/Documents/WeatherData && streamlit run app.py
"""

import io
import os
import json
import tempfile
import warnings
from math import radians, cos, sin, asin, sqrt

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
BASE       = os.path.expanduser("~/Documents/WeatherData")
DB_PATH    = os.path.join(BASE, "recon", "file_inventory.duckdb")
MASTER_PQ  = os.path.join(BASE, "recon", "master_station_list.parquet")

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
# DB connection (read-only, cached for the session)
# ---------------------------------------------------------------------------
@st.cache_resource
def get_db():
    return duckdb.connect(DB_PATH, read_only=True)


# ---------------------------------------------------------------------------
# Load master station list (cached, ~96K rows)
# ---------------------------------------------------------------------------
@st.cache_data
def load_stations() -> pd.DataFrame:
    con = get_db()
    df = con.execute(f"""
        SELECT
            ncei_id, station_name, state, county,
            lat_dec   AS lat,
            lon_dec   AS lon,
            elev_m,
            is_active,
            icao_ids, ghcnd_ids, wban_ids, hpd_ids, usaf_id,
            has_asos_1min,   asos_1min_start, asos_1min_end,
            asos_1min_files, asos_isd_files,
            hpd_files,       ncei_files,      ghcn_daily_files,
            has_asos_isd,
            has_hpd_1hr,
            has_ncei_15min,
            has_ghcn_daily
        FROM read_parquet('{MASTER_PQ}')
        WHERE lat_dec IS NOT NULL
          AND lon_dec IS NOT NULL
          AND (has_asos_1min OR has_asos_isd OR has_hpd_1hr OR has_ncei_15min OR has_ghcn_daily)
    """).fetchdf()

    # Coerce nullable booleans → plain bool (parquet NAs → False)
    bool_cols = ["has_asos_1min", "has_asos_isd", "has_hpd_1hr", "has_ncei_15min", "has_ghcn_daily"]
    for c in bool_cols:
        if c in df.columns:
            df[c] = df[c].fillna(False).astype(bool)

    # Compute coarse per-station data_start / data_end year
    def _year_range(row):
        starts, ends = [], []
        if row["has_ghcn_daily"]:  starts.append(1836); ends.append(2025)
        if row["has_asos_isd"]:    starts.append(1930); ends.append(2025)
        if row["has_hpd_1hr"]:     starts.append(1940); ends.append(2026)
        if row["has_ncei_15min"]:  starts.append(1970); ends.append(2026)
        if row["has_asos_1min"]:
            try:
                starts.append(int(str(row["asos_1min_start"])[:4]))
            except Exception:
                starts.append(2000)
            try:
                ends.append(int(str(row["asos_1min_end"])[:4]))
            except Exception:
                ends.append(2026)
        s = min(starts) if starts else 1900
        e = max(ends)   if ends   else 2026
        return s, e

    yr = df.apply(_year_range, axis=1)
    df["data_start"] = yr.apply(lambda x: x[0])
    df["data_end"]   = yr.apply(lambda x: x[1])

    # Dominant resolution label (for coloring)
    def _res(row):
        if row["has_asos_1min"]:  return "1min"
        if row["has_ncei_15min"]: return "15min"
        if row["has_asos_isd"]:   return "hourly"
        if row["has_hpd_1hr"]:    return "hourly_precip"
        if row["has_ghcn_daily"]: return "daily"
        return "none"
    df["dominant_res"] = df.apply(_res, axis=1)

    return df


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
    "hourly":        "Hourly ISD (ASOS)",
    "hourly_precip": "Hourly Precip (HPD)",
    "daily":         "Daily (GHCN)",
}


# ---------------------------------------------------------------------------
# Haversine distance (vectorised)
# ---------------------------------------------------------------------------
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * R * asin(sqrt(a))


def filter_by_radius(df: pd.DataFrame, clat: float, clon: float, radius_km: float) -> pd.DataFrame:
    dlat = np.radians(df["lat"].values - clat)
    dlon = np.radians(df["lon"].values - clon)
    a    = (np.sin(dlat / 2) ** 2
            + np.cos(np.radians(clat)) * np.cos(np.radians(df["lat"].values))
            * np.sin(dlon / 2) ** 2)
    dist = 2 * 6371.0 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
    return df[dist <= radius_km].copy()


# ---------------------------------------------------------------------------
# File-path lookup from inventory
# ---------------------------------------------------------------------------
def get_file_paths(station_df: pd.DataFrame, resolutions: list[str]) -> dict[str, list[str]]:
    """Return {resolution_key: [absolute_filepath, ...]} for the given stations."""
    con   = get_db()
    files: dict[str, list[str]] = {}

    ghcnd_ids = station_df["ghcnd_ids"].dropna().unique().tolist()
    icao_ids  = station_df["icao_ids"].dropna().unique().tolist()
    usaf_ids  = station_df["usaf_id"].dropna().unique().tolist()

    def _sql_in(vals: list, cap: int = 2000) -> str:
        return "'" + "','".join(str(v).replace("'", "''") for v in vals[:cap]) + "'"

    if "Daily" in resolutions and ghcnd_ids:
        rows = con.execute(f"""
            SELECT filepath FROM file_inventory
            WHERE source='GHCN_daily' AND id_format='GHCND_DLY'
              AND station_id_raw IN ({_sql_in(ghcnd_ids)})
        """).fetchall()
        files["Daily"] = [r[0] for r in rows]

    if "Hourly Precip" in resolutions and ghcnd_ids:
        rows = con.execute(f"""
            SELECT filepath FROM file_inventory
            WHERE source='HPD_1hr' AND id_format='GHCND_HPD'
              AND station_id_raw IN ({_sql_in(ghcnd_ids)})
        """).fetchall()
        files["Hourly Precip"] = [r[0] for r in rows]

    if "15-min" in resolutions and ghcnd_ids:
        rows = con.execute(f"""
            SELECT filepath FROM file_inventory
            WHERE source='NCEI_15min_raw' AND id_format='GHCND_HPD'
              AND station_id_raw IN ({_sql_in(ghcnd_ids)})
        """).fetchall()
        files["15-min"] = [r[0] for r in rows]

    if "Hourly ISD" in resolutions and usaf_ids:
        rows = con.execute(f"""
            SELECT filepath FROM file_inventory
            WHERE source='ASOS' AND subfolder='processed_parquet'
              AND station_id_raw IN ({_sql_in(usaf_ids)})
        """).fetchall()
        files["Hourly ISD"] = [r[0] for r in rows]

    if "1-min" in resolutions and icao_ids:
        # station_id_raw='0KDWH'; ICAO = last 4 chars
        rows = con.execute(f"""
            SELECT filepath FROM file_inventory
            WHERE source='ASOS' AND subfolder='asos_1min_parquet_master'
              AND SUBSTRING(station_id_raw, 2, 4) IN ({_sql_in(icao_ids)})
        """).fetchall()
        files["1-min"] = [r[0] for r in rows]

    return files


# ---------------------------------------------------------------------------
# Parquet extraction
# ---------------------------------------------------------------------------
MAX_EXTRACT_FILES = 1000   # hard cap per resolution
MAX_EXTRACT_ROWS  = 5_000_000

def extract_data(file_paths: list[str], date_col: str,
                 year_start: int, year_end: int,
                 datetime_col: bool = False) -> pd.DataFrame | None:
    if not file_paths:
        return None

    capped = file_paths[:MAX_EXTRACT_FILES]
    paths_sql = "[" + ", ".join(f"'{p.replace(chr(39), chr(39)+chr(39))}'" for p in capped) + "]"

    if datetime_col:
        yr_filter = f"YEAR({date_col}) BETWEEN {year_start} AND {year_end}"
    else:
        yr_filter = f"YEAR(TRY_CAST({date_col} AS DATE)) BETWEEN {year_start} AND {year_end}"

    con2 = duckdb.connect()      # fresh in-memory conn for reads
    try:
        df = con2.execute(f"""
            SELECT * FROM read_parquet({paths_sql}, union_by_name=true)
            WHERE {yr_filter}
            LIMIT {MAX_EXTRACT_ROWS}
        """).fetchdf()
    except Exception as e:
        st.error(f"Extraction error: {e}")
        df = None
    finally:
        con2.close()
    return df


# ---------------------------------------------------------------------------
# Build folium map
# ---------------------------------------------------------------------------
def build_map(display_df: pd.DataFrame, center: list, zoom: int) -> folium.Map:
    m = folium.Map(location=center, zoom_start=zoom, tiles="CartoDB positron")

    Draw(draw_options={
        "polyline": False, "rectangle": True, "polygon": True,
        "circle": True, "marker": True, "circlemarker": False,
    }).add_to(m)

    # Limit markers for browser performance
    n_total = len(display_df)
    if n_total > 8000:
        sample_df = display_df.sample(8000, random_state=42)
        st.caption(f"⚠ Showing 8,000 of {n_total:,} stations on map. Refine filters to see all.")
    else:
        sample_df = display_df

    cluster = MarkerCluster(
        options={"maxClusterRadius": 40, "spiderfyOnMaxZoom": True}
    ).add_to(m)

    for _, row in sample_df.iterrows():
        color = RES_COLOR.get(row["dominant_res"], "#999999")

        # Build resolution lines for popup
        res_lines = []
        if row["has_asos_1min"]:
            res_lines.append(
                f"<b style='color:{RES_COLOR['1min']}'>1-min ASOS</b> "
                f"{row.get('asos_1min_start','?')} – {row.get('asos_1min_end','?')}"
            )
        if row["has_ncei_15min"]:
            res_lines.append(f"<b style='color:{RES_COLOR['15min']}'>15-min NCEI</b> 1970–2026")
        if row["has_asos_isd"]:
            res_lines.append(f"<b style='color:{RES_COLOR['hourly']}'>Hourly ISD</b> 1930–2025")
        if row["has_hpd_1hr"]:
            res_lines.append(f"<b style='color:{RES_COLOR['hourly_precip']}'>Hourly Precip</b> 1940–2026")
        if row["has_ghcn_daily"]:
            res_lines.append(f"<b style='color:{RES_COLOR['daily']}'>Daily GHCN</b> 1836–2025")

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

    # Legend
    legend_html = """
    <div style="position:fixed;bottom:30px;left:30px;z-index:9999;
                background:white;padding:8px 12px;border-radius:6px;
                border:1px solid #ccc;font-size:11px;line-height:1.6">
      <b>Resolution</b><br>
    """
    for res, label in RES_LABELS.items():
        c = RES_COLOR[res]
        legend_html += (
            f'<span style="display:inline-block;width:10px;height:10px;'
            f'border-radius:50%;background:{c};margin-right:4px"></span>{label}<br>'
        )
    legend_html += "</div>"
    m.get_root().html.add_child(folium.Element(legend_html))

    return m


# =============================================================================
# APP LAYOUT
# =============================================================================

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
st.sidebar.title("🌦 Weather Explorer")
st.sidebar.caption("Explore 96K stations across 5 sources")

all_stations = load_stations()

states = sorted(all_stations["state"].dropna().unique())
sel_state = st.sidebar.selectbox("State", ["All US"] + states)

sel_res = st.sidebar.multiselect(
    "Resolution",
    ["1-min", "15-min", "Hourly ISD", "Hourly Precip", "Daily"],
    default=["1-min", "15-min", "Hourly ISD", "Hourly Precip", "Daily"],
)

year_range = st.sidebar.slider("Year Range", 1836, 2026, (1950, 2026))

search_radius = st.sidebar.number_input(
    "Radius for point search (km)", min_value=5, max_value=500, value=50, step=5
)

st.sidebar.markdown("---")
st.sidebar.caption("**Data sources:**")
st.sidebar.caption("ASOS 1-min · ASOS hourly ISD · GHCN-Daily · HPD 1-hr · NCEI 15-min")

# ---------------------------------------------------------------------------
# Apply sidebar filters to station list
# ---------------------------------------------------------------------------
filtered = all_stations.copy()

# State filter
if sel_state != "All US":
    filtered = filtered[filtered["state"] == sel_state]

# Resolution filter
res_mask = pd.Series(False, index=filtered.index)
if "1-min"        in sel_res: res_mask |= filtered["has_asos_1min"]
if "15-min"       in sel_res: res_mask |= filtered["has_ncei_15min"]
if "Hourly ISD"   in sel_res: res_mask |= filtered["has_asos_isd"]
if "Hourly Precip"in sel_res: res_mask |= filtered["has_hpd_1hr"]
if "Daily"        in sel_res: res_mask |= filtered["has_ghcn_daily"]
filtered = filtered[res_mask]

# Year range filter (coarse)
filtered = filtered[
    (filtered["data_start"] <= year_range[1]) &
    (filtered["data_end"]   >= year_range[0])
]

st.sidebar.metric("Matching Stations", f"{len(filtered):,}")
if len(filtered) > 0:
    gb = (
        filtered["asos_1min_files"].fillna(0)    * 0.000463 * int("1-min"         in sel_res)
      + filtered["asos_isd_files"].fillna(0)     * 0.0001   * int("Hourly ISD"    in sel_res)
      + filtered["hpd_files"].fillna(0)          * 0.0004   * int("Hourly Precip" in sel_res)
      + filtered["ncei_files"].fillna(0)         * 0.0006   * int("15-min"        in sel_res)
      + filtered["ghcn_daily_files"].fillna(0)   * 0.0001   * int("Daily"         in sel_res)
    ).sum()
    st.sidebar.metric("Est. Data Size", f"~{gb:.1f} GB")

# ---------------------------------------------------------------------------
# Session state: selected stations (from drawing)
# ---------------------------------------------------------------------------
if "selected_stations" not in st.session_state:
    st.session_state["selected_stations"] = None
if "last_drawing_count" not in st.session_state:
    st.session_state["last_drawing_count"] = 0

# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------
if len(filtered) > 0:
    center = [float(filtered["lat"].mean()), float(filtered["lon"].mean())]
    zoom   = 6 if sel_state != "All US" else 4
else:
    center = [39.8, -98.5]
    zoom   = 4

m          = build_map(filtered, center, zoom)
map_result = st_folium(m, width="100%", height=520,
                       returned_objects=["all_drawings"])

# ---------------------------------------------------------------------------
# Handle drawings → select stations
# ---------------------------------------------------------------------------
drawings = (map_result or {}).get("all_drawings") or []
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

if st.button("✖ Clear selection / drawing"):
    st.session_state["selected_stations"] = None
    st.session_state["last_drawing_count"] = 0

# Active display set
display_df = (
    st.session_state["selected_stations"]
    if st.session_state["selected_stations"] is not None
    else filtered
)

# ---------------------------------------------------------------------------
# Results table
# ---------------------------------------------------------------------------
st.subheader(f"Stations — {len(display_df):,}")

table_cols = [
    "station_name", "state", "county",
    "lat", "lon", "elev_m",
    "has_asos_1min", "has_asos_isd", "has_hpd_1hr", "has_ncei_15min", "has_ghcn_daily",
    "data_start", "data_end",
]
show_cols = [c for c in table_cols if c in display_df.columns]
st.dataframe(display_df[show_cols].head(500).reset_index(drop=True),
             use_container_width=True, height=260)

# ---------------------------------------------------------------------------
# Export + Extract
# ---------------------------------------------------------------------------
col_exp, col_ext = st.columns([1, 2])

# Export station list
with col_exp:
    csv_bytes = display_df.to_csv(index=False).encode()
    st.download_button(
        "⬇ Station List (CSV)",
        csv_bytes,
        file_name="stations.csv",
        mime="text/csv",
        use_container_width=True,
    )

# Extract data
with col_ext:
    with st.expander("⚡ Extract Data", expanded=False):
        ext_res = st.multiselect(
            "Resolutions to extract",
            ["1-min", "15-min", "Hourly ISD", "Hourly Precip", "Daily"],
            default=[r for r in sel_res if r in ["Daily", "Hourly Precip"]],
        )
        ext_fmt = st.radio("Output format", ["CSV", "Parquet"], horizontal=True)
        row_cap = st.number_input(
            "Max rows per resolution", 100_000, 5_000_000, 1_000_000, step=100_000
        )

        if st.button("▶ Run Extraction", use_container_width=True):
            if not ext_res:
                st.warning("Select at least one resolution.")
            elif len(display_df) == 0:
                st.warning("No stations in selection.")
            else:
                files_by_res = get_file_paths(display_df, ext_res)
                total_files  = sum(len(v) for v in files_by_res.values())
                st.write(f"**Files found:** " + " · ".join(
                    f"{k}: {len(v):,}" for k, v in files_by_res.items()
                ))

                if total_files == 0:
                    st.warning("No matching files found for the selected stations and resolutions.")
                else:
                    DATE_COL_MAP = {
                        "Daily":         ("date",         False),
                        "Hourly Precip": ("DATE",         False),
                        "15-min":        ("DATE",         False),
                        "Hourly ISD":    ("date",         False),
                        "1-min":         ("datetime_utc", True),
                    }

                    for res_key, fp_list in files_by_res.items():
                        if not fp_list:
                            continue
                        date_col, is_dt = DATE_COL_MAP.get(res_key, ("date", False))
                        st.write(f"**{res_key}** — reading {min(len(fp_list), MAX_EXTRACT_FILES):,}"
                                 f"/{len(fp_list):,} files …")

                        with st.spinner(f"Extracting {res_key}…"):
                            df_out = extract_data(
                                fp_list, date_col,
                                year_range[0], year_range[1],
                                datetime_col=is_dt,
                            )

                        if df_out is None or len(df_out) == 0:
                            st.warning(f"  No rows returned for {res_key} in {year_range[0]}–{year_range[1]}.")
                            continue

                        st.success(f"  ✓ {len(df_out):,} rows from {res_key}")

                        safe_name = res_key.replace("-", "").replace(" ", "_").lower()
                        state_tag  = sel_state.replace(" ", "_")

                        if ext_fmt == "CSV":
                            buf = io.StringIO()
                            df_out.to_csv(buf, index=False)
                            st.download_button(
                                f"⬇ {res_key} CSV ({len(df_out):,} rows)",
                                buf.getvalue().encode(),
                                file_name=f"weather_{safe_name}_{state_tag}_{year_range[0]}-{year_range[1]}.csv",
                                mime="text/csv",
                                key=f"dl_{safe_name}_csv",
                            )
                        else:
                            buf = io.BytesIO()
                            df_out.to_parquet(buf, index=False, compression="snappy")
                            buf.seek(0)
                            st.download_button(
                                f"⬇ {res_key} Parquet ({len(df_out):,} rows)",
                                buf.getvalue(),
                                file_name=f"weather_{safe_name}_{state_tag}_{year_range[0]}-{year_range[1]}.parquet",
                                mime="application/octet-stream",
                                key=f"dl_{safe_name}_pq",
                            )
