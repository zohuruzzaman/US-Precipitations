# US Weather Data Archive — Project Summary

## Overview

A unified, queryable archive of US weather observations from 5 NOAA data sources, covering **96,226 stations** with data spanning **1836–2026**. All data is stored as **parquet files (~90 GB)** with a crosswalk system that maps every station across all ID systems (WBAN, ICAO, GHCND, COOP, FAA, etc.), enabling lookup by location, state, or any station identifier.

**Total: ~10.1 billion rows of observation data across 510K+ files.**

---

## Data Sources

### 1. ASOS 1-Minute (Automated Surface Observing System)
- **What it is:** High-resolution (1-minute interval) precipitation and temperature readings from US airports and weather stations
- **Resolution:** 1 minute
- **Variables:** precipitation (inches), temperature (°F), precipitation type ID
- **Stations:** ~958 (primarily major airports)
- **Date range:** 2000–2022
- **Files:** ~185,690 parquets in `ASOS/asos_1min_parquet_master/`
- **Size:** ~66 GB
- **Schema:** `wban, icao, datetime_utc, precip_id, precip_1min_in, temp_F`
- **Station ID format:** WBAN (5-digit) + ICAO (4-letter) encoded in filename
- **NOAA source:** https://www.ncei.noaa.gov/data/automated-surface-observing-system-one-minute-pg1/

### 2. ASOS ISD (Integrated Surface Data — Hourly)
- **What it is:** Comprehensive hourly weather observations from surface stations worldwide. Our archive contains US-only processed data from `noa_parser.py`
- **Resolution:** Hourly
- **Variables:** Multi-variable — precipitation (groups AA1-AA4), temperature, wind, pressure, visibility, and more (15 columns)
- **Stations:** ~4,876
- **Date range:** 1930–2025
- **Files:** ~210,881 parquets in `ASOS/processed_parquet/`
- **Size:** ~17 GB
- **Station ID format:** USAF (6-digit) + WBAN (5-digit) in filename. Many have WBAN=99999 (military/remote sites)
- **NOAA source:** https://www.ncei.noaa.gov/data/global-hourly/access/

### 3. HPD 1-Hour (COOP Hourly Precipitation Dataset v2)
- **What it is:** Hourly precipitation totals from the NOAA Cooperative Observer Program. Long historical record, primarily precipitation-focused
- **Resolution:** 1 hour
- **Variables:** Precipitation (131 columns in wide format — hourly values per day)
- **Stations:** ~2,329
- **Date range:** 1940–2026
- **Files:** ~2,081 parquets in `HPD_1hr/raw/HPD_1hr/` (converted from original CSVs — 41x compression)
- **Size:** ~0.5 GB (was 21 GB as CSV)
- **Station ID format:** GHCND (e.g., USC00010008, USW00013874)
- **NOAA source:** https://www.ncei.noaa.gov/data/coop-hourly-precipitation/v2/access/

### 4. NCEI 15-Minute Precipitation
- **What it is:** 15-minute precipitation accumulations. Legacy network — mostly historical, declining station count
- **Resolution:** 15 minutes
- **Variables:** Precipitation (491 columns in wide format for the .15m.parquet files; 4-column raw text in .dat.parquet files)
- **Stations:** ~2,332
- **Date range:** 1970–2026
- **Files:** ~35,574 parquets in `NCEI_15min_raw/raw_parquet/`
  - `hpd15_station_parquet/` — 2,080 per-station .15m.parquet files (primary, parsed)
  - `legacy_state_parquet/` — 33,450 state-level parquets
  - `legacy_monthly_parquet/` — 36 monthly raw-text parquets (unparsed legacy)
- **Size:** ~1.6 GB
- **Station ID format:** GHCND
- **NOAA source:** https://www.ncei.noaa.gov/data/15-min-precipitation/

### 5. GHCN Daily (Global Historical Climatology Network — Daily)
- **What it is:** The largest and most comprehensive daily weather observation dataset. Includes COOP stations, CoCoRaHS volunteer observers, airports, and more. Our archive contains PRCP, TMAX, TMIN, and TAVG only (other elements filtered out during conversion)
- **Resolution:** Daily
- **Variables:** Precipitation (tenths of mm), max temperature (tenths of °C), min temperature (tenths of °C), average temperature (tenths of °C), plus quality flags
- **Stations:** ~76,493 (by far the largest network — includes CoCoRaHS volunteers)
- **Date range:** 1836–2025
- **Files:**
  - `GHCN_daily/ghcnd_all_parquet/` — 76,198 per-station parquets (converted from .dly fixed-width)
  - `GHCN_daily/state_parquet/` — 51 state-level PRCP-only parquets (from collaborator's pre-processed state outputs)
- **Size:** ~4.3 GB (per-station) + ~2.1 GB (state parquets)
- **Schema (per-station):** `station_id, date, element, value, mflag, qflag, sflag` (long format)
- **Schema (state):** `station_id, date, year, month, day, element, value_raw, missing, mflag, qflag, sflag, prcp_mm, state, station_name, lat, lon, elevation` (17 columns, PRCP only)
- **Station ID format:** GHCND (e.g., US1MSHR0045 for CoCoRaHS, USC00227840 for COOP, USW00013874 for ASOS)
- **NOAA source:** https://www.ncei.noaa.gov/pub/data/ghcn/daily/

---

## Coverage Summary

| Resolution | Source | Stations | Files | Total Rows | Size (GB) | Date Range |
|-----------|--------|----------|-------|------------|-----------|------------|
| 1-minute | ASOS | ~958 | 185,690 | ~6.6 billion | 66 | 2000–2022 |
| 15-minute | NCEI | ~2,332 | 35,574 | ~28.6 million | 1.6 | 1970–2026 |
| Hourly (ISD) | ASOS | ~4,876 | 210,881 | ~2.9 billion | 17 | 1930–2025 |
| Hourly (Precip) | HPD | ~2,329 | 2,081 | ~41 million | 0.5 | 1940–2026 |
| Daily | GHCN | ~76,493 | 78,545 | ~1.0 billion | 6.4 | 1836–2025 |
| **Total** | | **~96,226** | **~510,000** | **~10.1 billion** | **~91** | **1836–2026** |

---

## Station Crosswalk (HOMR Backbone)

The master station list uses NOAA's **HOMR (Historical Observing Metadata Repository)** as the backbone for cross-referencing station IDs across all networks.

**Source files:**
- `HOMR/processed/stations_master.parquet` — 227,149 rows × 21 columns (temporal — multiple rows per station for historical changes). Contains: ncei_id, station_name, state, lat, lon, elevation, country_code, icao_id, wban_id, ghcnd_id, coop_id, hpd_id, wmo_id, and more
- `HOMR/processed/stations_crosswalk.parquet` — 497,375 rows × 6 columns (long format: ncei_id → id_type, id_value). ID types: GHCND, COOP, NWSLI, HPD, WBAN, FAA, ICAO, WMO
- `HOMR/emshr_lite.txt` — Raw fixed-width NOAA master file (308K lines)

**How the crosswalk works:**
1. HOMR `stations_master` is collapsed to one row per unique ncei_id (US only)
2. All ID columns (icao_id, wban_id, ghcnd_id, etc.) are already in the master table
3. Each data source's files are matched to the master list via the appropriate ID:
   - ASOS 1-min → icao_id (4-letter code, extracted from filename by stripping leading digit)
   - ASOS ISD → wban_id (digits 7–11 of filename) or usaf_id (digits 1–6)
   - HPD → ghcnd_id (GHCND station ID from filename)
   - NCEI → ghcnd_id
   - GHCN Daily → ghcnd_id
4. Stations not in HOMR were recovered from source-specific metadata (HPD stationinv CSV, ASOS master CSV) and added as orphans
5. 4,945 USAF-only ASOS stations were matched to HOMR via coordinate proximity (≤0.01°)

---

## Pipeline Scripts (in execution order)

### Core Pipeline

| Script | Purpose |
|--------|---------|
| `recon/build_file_inventory.py` | Walks all data folders, extracts station IDs from filenames, stores in DuckDB. Supports `--all` (full rebuild) or `--source X` (incremental). **Run first.** |
| `recon/build_master_station_list.py` | Builds the 96K-station crosswalk using HOMR backbone + file inventory. Joins all ID systems, flags data availability per source. Outputs parquet + CSV. |
| `recon/build_data_profile.py` | Profiles every parquet file via `parquet_metadata()` (reads file footer only, zero data rows). Extracts row counts, date ranges, quality flags. |
| `recon/plot_coverage.py` | Generates 5 static coverage visualizations (station maps, temporal depth, density, timeline, state bars). |

### Conversion Scripts

| Script | Purpose |
|--------|---------|
| `recon/convert_hpd_csv_to_parquet.py` | Converted 2,088 HPD CSVs → parquet (41x compression, 21 GB → 0.5 GB) |
| `recon/convert_dly_to_parquet.py` | Converted 76,198 GHCN .dly fixed-width files → parquet (PRCP/TMAX/TMIN/TAVG only, 4.5x compression) |

### Deletion/Cleanup Manifests

| Script | Purpose |
|--------|---------|
| `recon/build_deletion_manifest.py` | Identified 500,531 international ASOS files for deletion (18.6 GB reclaimed) |

### Existing Processing Scripts (pre-project)

| Script | Purpose |
|--------|---------|
| `ASOS/asos1min.py` | Downloads and processes ASOS 1-minute data |
| `ASOS/noa_parser.py` | Parses ISD raw data into processed parquet (the ASOS hourly pipeline) |
| `download_emshr.py` | Downloads HOMR emshr_lite.txt from NOAA |
| `process_emshr.py` | Parses fixed-width emshr_lite.txt into stations_master and stations_crosswalk parquets |

---

## Key Files

### Metadata (recon/)
| File | Size | Description |
|------|------|-------------|
| `recon/file_inventory.duckdb` | ~100 MB | DuckDB database with tables: file_inventory (510K rows), group_schemas (~15 rows), data_profile (510K rows), station_coverage, station_coverage_geo, anomalies |
| `recon/master_station_list.parquet` | 4.6 MB | 96,226 US stations with all cross-reference IDs + data availability flags |
| `recon/master_station_list.csv` | 14.3 MB | Same as above, human-readable |

### Coverage Maps (recon/)
| File | Description |
|------|-------------|
| `map_stations_by_type.png` | Station locations colored by best available data resolution |
| `map_temporal_depth.png` | Years of record per station, per resolution (5 panels) |
| `map_data_density.png` | log(total rows) per 0.5° grid cell (5 panels) |
| `chart_timeline.png` | Active stations per year per network (1900–2026) |
| `chart_states.png` | Station count by state, stacked by resolution |

---

## DuckDB Tables Reference

### file_inventory
One row per file on disk. ~510K rows.
```
filepath, filename, extension, file_size_bytes, source, subfolder,
station_id_raw, id_format, date_part, is_data_file, schema_group, anomaly_flag
```

### data_profile
One row per profiled parquet file. ~510K rows. Created by `parquet_metadata()` — no data rows read.
```
filepath, source, station_id_raw, id_format, total_rows, min_datetime, max_datetime, has_data, error
```

### station_coverage
Aggregated per station per source per resolution.
```
station_id_raw, id_format, source, temporal_resolution, file_count, total_rows,
total_gb, earliest_date, latest_date, years_span, empty_files, error_files
```

### station_coverage_geo
station_coverage joined with master station list for lat/lon/state.
```
[all station_coverage columns] + station_name, state, lat, lon
```

### group_schemas
One row per unique (source, subfolder, extension) combo. ~15 rows.
```
group_id, source, subfolder, extension, sample_filepath, file_count,
total_size_gb, column_names, column_types, column_count, row_count_sample, header_raw, notes
```

---

## Useful DuckDB Queries

```sql
-- Connect to the inventory
-- duckdb recon/file_inventory.duckdb

-- Find stations near a point (Jackson MS: 32.3, -90.2)
SELECT station_name, state, lat, lon,
    ROUND(2 * 6371 * ASIN(SQRT(
        POWER(SIN(RADIANS(lat - 32.3) / 2), 2) +
        COS(RADIANS(32.3)) * COS(RADIANS(lat)) *
        POWER(SIN(RADIANS(lon - (-90.2)) / 2), 2)
    )), 1) as dist_km
FROM read_parquet('recon/master_station_list.parquet')
WHERE state = 'MS'
ORDER BY dist_km LIMIT 20;

-- Stations with multiple data sources
SELECT station_name, state,
    has_asos_1min, has_asos_isd, has_hpd_1hr, has_ncei_15min, has_ghcn_daily
FROM read_parquet('recon/master_station_list.parquet')
WHERE (has_asos_1min::INT + has_hpd_1hr::INT + has_ncei_15min::INT + has_ghcn_daily::INT) >= 3
ORDER BY state;

-- Files for a specific station
SELECT * FROM file_inventory WHERE station_id_raw = 'USW00013874';

-- Data volume by state
SELECT sc.state, sc.temporal_resolution,
    COUNT(*) as stations, SUM(sc.total_rows) as total_rows
FROM station_coverage_geo sc
WHERE sc.state IS NOT NULL
GROUP BY sc.state, sc.temporal_resolution
ORDER BY sc.state, sc.temporal_resolution;
```

---

## What Happened (Chronological)

1. **Downloaded raw data** — ASOS (1-min + ISD hourly), HOMR (station master), HPD (hourly precip), NCEI (15-min precip), GHCN (daily) from various NOAA endpoints. ~118 GB across mixed formats (.parquet, .csv, .gz, .dly, .txt, fixed-width).

2. **Built file inventory** — Walked 934K files, extracted station IDs from filenames via regex. Stored in DuckDB. Iteratively fixed patterns for USAF_WBAN, ICAO_ASOS, GHCND, CoCoRaHS, and territory prefixes. Achieved 99.9% ID extraction rate.

3. **Built station crosswalk** — Used HOMR as backbone (227K temporal rows → 92,773 unique stations). Pivoted crosswalk IDs. Joined file inventory to master list via ICAO/WBAN/GHCND IDs. Recovered orphans from source metadata. Added 5,548 USAF-only ASOS stations via coordinate matching. Added 1,800 GHCN orphans.

4. **Cleaned international data** — Identified and deleted 500,531 international ASOS files (18.6 GB). Filtered GHCN .dly files at index time (53,369 international skipped).

5. **Unified formats** — Converted 2,088 HPD CSVs → parquet (41x compression). Converted 76,198 GHCN .dly → parquet (PRCP/TMAX/TMIN/TAVG only, 4.5x compression). Also converted 51 state PRCP .csv.gz → parquet. Deleted originals after verification.

6. **Profiled all data** — Read parquet_metadata() on 510K files (no data rows loaded). Extracted row counts, date ranges, quality flags. Built station_coverage summary tables.

7. **Generated coverage maps** — 5 visualizations showing spatial coverage, temporal depth, data density, network growth timeline, and per-state breakdowns.

---

## Current Disk Layout (~90 GB)

```
WeatherData/                              ~90 GB total
├── ASOS/
│   ├── asos_1min_parquet_master/         185,690 parquets    66 GB   (1-min precip+temp)
│   ├── processed_parquet/                210,881 parquets    17 GB   (hourly ISD)
│   ├── master_station_inventory.csv      Station metadata    55 MB
│   ├── asos1min.py                       Download script
│   └── noa_parser.py                     ISD parser
├── HOMR/
│   ├── processed/
│   │   ├── stations_master.parquet       227K rows           Crosswalk backbone
│   │   └── stations_crosswalk.parquet    497K rows           Long-format ID map
│   └── emshr_lite.txt                    Raw NOAA master     162 MB
├── HPD_1hr/
│   └── raw/HPD_1hr/
│       ├── *.parquet                     2,081 files         0.5 GB  (hourly precip)
│       └── HPD_v02r02_stationinv_*.csv   Station inventory
├── NCEI_15min_raw/
│   ├── raw_parquet/
│   │   ├── hpd15_station_parquet/        2,080 .15m.parquet  1.0 GB  (15-min precip)
│   │   ├── legacy_state_parquet/         33,450 parquets     0.5 GB
│   │   └── legacy_monthly_parquet/       36 parquets         36 MB
│   └── NCEI_15min_metadata/              Station metadata CSVs
├── GHCN_daily/
│   ├── ghcnd_all_parquet/                76,198 parquets     4.3 GB  (daily PRCP/T)
│   └── state_parquet/                    51 parquets         2.1 GB  (state PRCP)
├── recon/                                Metadata + scripts
│   ├── file_inventory.duckdb             All metadata tables
│   ├── master_station_list.parquet       96K station crosswalk
│   ├── master_station_list.csv           Human-readable version
│   ├── build_file_inventory.py           File walk + indexing
│   ├── build_master_station_list.py      Crosswalk builder
│   ├── build_data_profile.py             Parquet metadata profiler
│   ├── plot_coverage.py                  Coverage visualization
│   ├── convert_*.py                      Format converters
│   └── *.png                             Coverage maps
├── scripts/
│   └── refresh_data.py                   NOAA data updater (planned)
├── app.py                                Streamlit explorer (WIP)
└── download_emshr.py, process_emshr.py   HOMR download/parse
```

---

## Next Steps

- [ ] **Refresh script** — `scripts/refresh_data.py` to pull latest data from NOAA (incremental, per-source)
- [ ] **Interactive explorer** — Streamlit prototype → FastAPI + React production app
- [ ] **County overlay** — Point-in-polygon with census shapefiles for county-level station lookup
- [ ] **Unified data store** — State-partitioned parquet for optimal query performance
- [ ] **Docker deployment** — One-command deploy for lab PC serving the team
- [ ] **Data quality audit** — Validate actual precipitation/temperature values, not just metadata
