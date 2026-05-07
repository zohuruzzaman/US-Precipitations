# Weather Data Explorer

An interactive Streamlit app for browsing, filtering, refreshing, and exporting US weather station data across multiple NOAA observation networks.

Built by **CREATE Lab - Jackson State University**.

## Quick Start

Requirement: Python 3.10+ or Miniconda/Anaconda must be installed.

| Platform | File to run |
|----------|-------------|
| macOS | `launch.command` |
| Windows | `launch.bat` |

The launcher:

1. Detects Python automatically.
2. Installs packages from `scripts/requirements.txt`.
3. Starts the app at `http://localhost:8501`.

macOS may require right-clicking `launch.command` and choosing **Open** the first time.

## What's Included

```text
WeatherData_v2/
├── README.md
├── launch.command
├── launch.bat
├── scripts/
│   ├── app.py
│   ├── refresh_data.py
│   ├── build_file_inventory.py
│   ├── build_data_profile.py
│   ├── build_master_station_list.py
│   ├── requirements.txt
│   ├── logo.svg
│   └── supporting download / processing scripts
└── data/
    └── recon/
        ├── file_inventory.duckdb
        ├── master_station_list.csv
        ├── master_station_list.parquet
        ├── station_inventory_daily.parquet
        ├── station_inventory_hourly.parquet
        ├── station_inventory_15min.parquet
        └── station_inventory_1min.parquet
```

Raw observation data is intentionally not committed. The local full archive is about 76 GB and can be rebuilt or refreshed from NOAA. The committed `data/recon/` files let the map, filters, and station metadata load immediately.

## App Features

- Interactive station map with state, source, and year filters.
- Station selection by marker click or drawn map shapes.
- Download queue with date range and variable selection.
- CSV export for selected stations and observations.
- In-app **What's New** refresh checks and background refresh execution.
- Refresh pause, resume, stop, and progress tracking.
- Optional coverage map export when `cartopy` is installed.

## Active App Sources

These are the sources exposed in the app refresh UI and `refresh_data.py --all`:

| App source | Resolution | Local data path | NOAA source |
|------------|------------|-----------------|-------------|
| `GHCN_daily` | Daily | `data/GHCNd/daily/` | `https://www.ncei.noaa.gov/pub/data/ghcn/daily/` |
| `ASOS_ISD` | Hourly | `data/GHCNh/hourly/` | `https://www.ncei.noaa.gov/data/global-hourly/access/` |
| `NCEI_15min` | 15 minutes | `data/ASOS/15min/` | `https://www.ncei.noaa.gov/pub/data/hpd/auto/v2/beta/15min/all_csv/` |
| `ASOS_1min` | 1 minute | `data/ASOS/1min/parquet_final/` | `https://www.ncei.noaa.gov/data/automated-surface-observing-system-one-minute-pg2/access/` |

`HPD_1hr` exists in the refresh script as a CLI-supported source, but it is not currently exposed in the app refresh selector.

## Refreshing Data

Use the app sidebar **What's New** controls for normal refresh checks. The same logic can be run from the command line:

```bash
# Check all app-visible sources without downloading
python scripts/refresh_data.py --all --dry-run

# Check one source
python scripts/refresh_data.py --source ASOS_1min --dry-run

# Refresh all app-visible sources
python scripts/refresh_data.py --all

# Refresh one source
python scripts/refresh_data.py --source ASOS_1min
```

ASOS 1-minute refresh uses NOAA Page 2 (`pg2`) files and writes monthly parquet files named like:

```text
asos-1min-pg2-KABE-202401.parquet
```

## Rebuilding Indexes

After adding or refreshing raw data, rebuild metadata with:

```bash
python scripts/build_file_inventory.py --all
python scripts/build_data_profile.py --station-inventory-only
python scripts/build_master_station_list.py
python scripts/build_data_profile.py --coverage-tables-only
```

The app reads these metadata files from `data/recon/`:

- `file_inventory.duckdb`
- `master_station_list.parquet`
- `station_inventory_daily.parquet`
- `station_inventory_hourly.parquet`
- `station_inventory_15min.parquet`
- `station_inventory_1min.parquet`

## Dependencies

Install with:

```bash
pip install -r scripts/requirements.txt
```

Main dependencies:

- `streamlit`
- `streamlit-folium`
- `folium`
- `duckdb`
- `pandas`
- `pyarrow`
- `numpy`
- `httpx`
- `requests`
- `matplotlib`
- `shapely`
- `cartopy` optional for coverage map export

## Development Notes

- `scripts/app.py` is the Streamlit UI.
- `scripts/refresh_data.py` is the authoritative incremental refresh path used by the app.
- `scripts/build_file_inventory.py` recognizes current ASOS Page 2 filenames.
- Raw `data/` folders, refresh logs, local state files, caches, and `.DS_Store` files are ignored by Git.
