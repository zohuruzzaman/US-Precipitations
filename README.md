# Weather Data Explorer

An interactive Streamlit app for browsing, filtering, and downloading US weather station data across multiple resolutions — ASOS 1-min, NCEI 15-min, GHCNh Hourly, and GHCNd Daily.

Built by **CREATE Lab — Jackson State University**

---

## Quick Start (No-Code, Double-Click)

> **Requirement:** Python 3.10+ or [Miniconda](https://docs.conda.io/en/latest/miniconda.html) must be installed.

| Platform | File to double-click |
|----------|----------------------|
| **macOS** | `launch.command` |
| **Windows** | `launch.bat` |

The launcher will:
1. Detect your Python installation automatically
2. Install all required packages from `scripts/requirements.txt`
3. Open the app at **http://localhost:8501** in your browser

> **macOS note:** On first run, right-click → Open (instead of double-click) to bypass the Gatekeeper warning. After that, double-click works normally.

---

## What's Included in This Package

```
WeatherData_v2/
├── README.md                   ← this file
├── launch.command              ← macOS launcher (double-click)
├── launch.bat                  ← Windows launcher (double-click)
├── scripts/
│   ├── app.py                  ← Streamlit app (main UI)
│   ├── requirements.txt        ← Python dependencies
│   ├── logo.svg                ← App logo
│   │
│   │── Download scripts (run once to populate data/)
│   ├── ghcn_daily.py           ← GHCNd daily data (NOAA)
│   ├── ghcnh_hourly.py         ← GHCNh hourly data (NOAA)
│   ├── asos1min.py             ← ASOS 1-minute data
│   ├── 15min.py                ← NCEI 15-minute data
│   ├── download_emshr.py       ← HOMR station metadata (NOAA)
│   ├── process_emshr.py        ← Process raw HOMR download
│   │
│   │── Index / pipeline scripts (run after downloading data)
│   ├── build_data_profile.py   ← Builds file inventory + coverage tables
│   ├── build_master_station_list.py  ← Builds master_station_list.parquet
│   ├── refresh_data.py         ← In-app refresh trigger
│   ├── redownload_all.py       ← Full disaster-recovery rebuild
│   │
│   │── Optional / standalone
│   ├── plot_coverage.py        ← Coverage map visualizations (needs cartopy)
│   ├── explorer.ipynb          ← Exploratory Jupyter notebook
│   ├── build_file_inventory.py ← Earlier inventory builder (superseded)
│   └── noa_parser.py           ← Earlier GHCNh downloader (superseded)
│
└── data/
    └── recon/                  ← Pre-built station index (ships with package)
        ├── master_station_list.parquet   ← ~93k US weather stations
        ├── station_inventory_daily.parquet
        ├── station_inventory_hourly.parquet
        ├── station_inventory_15min.parquet
        ├── station_inventory_1min.parquet
        └── file_inventory.duckdb         ← Coverage tables for map export
```

> **Note:** Raw observation data (ASOS, GHCNh, GHCNd, NCEI, HOMR) is **not** included — it totals ~76 GB. The station index (`data/recon/`) is included so the map and filters work immediately.

---

## App Features

- **Interactive map** — 92,700+ US weather stations, filterable by state, data type, and year range
- **Station selection** — click a marker or draw a polygon/circle on the map to select stations
- **Download queue** — add stations, pick date range and variables, export to CSV
- **Four data resolutions:**
  - `1-min` — ASOS 1-minute
  - `15-min` — NCEI 15-minute precipitation
  - `GHCNh Hourly` — Global Historical Climatology Network hourly (ISD-based)
  - `Daily` — GHCNd daily (PRCP, TMAX, TMIN, SNOW, SNWD, AWND, TAVG)
- **Coverage map export** — cartopy SVG map (optional, see below)

---

## Running the Full Pipeline from Scratch

If you want to download all raw data yourself (requires ~80 GB disk space and several days of download time):

```bash
# 1. Download station metadata
python scripts/download_emshr.py
python scripts/process_emshr.py

# 2. Download observation data (can run in parallel)
python scripts/ghcn_daily.py
python scripts/ghcnh_hourly.py
python scripts/asos1min.py
python scripts/15min.py

# 3. Build file inventory
python scripts/build_data_profile.py --station-inventory-only

# 4. Build master station list
python scripts/build_master_station_list.py

# 5. Build coverage tables
python scripts/build_data_profile.py --coverage-tables-only

# 6. Launch the app
streamlit run scripts/app.py
```

---

## Dependencies

All listed in `scripts/requirements.txt`. Install with:

```bash
pip install -r scripts/requirements.txt
```

| Package | Purpose |
|---------|---------|
| `streamlit` | Web UI framework |
| `streamlit-folium` | Folium map in Streamlit |
| `folium` | Interactive map rendering |
| `duckdb` | Fast in-process analytics on parquet files |
| `pandas` | Data manipulation |
| `pyarrow` | Parquet read/write |
| `numpy` | Numerical operations |
| `httpx` | Async HTTP downloads |
| `requests` | HTTP downloads (metadata scripts) |
| `matplotlib` | Plotting (coverage maps) |
| `shapely` | Polygon station selection on map |
| `cartopy` | *(Optional)* Coverage map SVG export |

### Installing cartopy (optional)
```bash
# macOS / Linux
pip install cartopy

# Windows (recommended via conda)
conda install -c conda-forge cartopy
```

---

## Project Info

| | |
|-|-|
| **Institution** | Jackson State University — CREATE Lab |
| **Data sources** | NOAA NCEI, NCDC Global Historical Climatology Network |
| **Python** | 3.10+ |
| **Tested on** | macOS 14 (Apple Silicon), Windows 11 |
