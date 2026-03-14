# WeatherData Pipeline — Metadata Reference

## Overview

| Stat | Value |
|------|-------|
| Master stations | 96,226 |
| Parquet files profiled | 510,468 |
| Total data on disk | ~87 GB |
| Total rows across all files | ~10.1 billion |
| Sources | ASOS, GHCN-Daily, HPD-1hr, NCEI-15min, HOMR |

All data lives under `~/Documents/WeatherData/`. The recon pipeline builds a
queryable DuckDB inventory (`recon/file_inventory.duckdb`) — no data files are
committed to git; the DB is rebuilt from the scripts below.

---

## Directory Layout

```
WeatherData/
├── ASOS/
│   ├── asos_1min_parquet_master/YYYY/   185,668 parquets — 1-min precip+temp (~62 GB)
│   └── processed_parquet/YYYY/          210,880 parquets — hourly ISD (~16 GB)
├── GHCN_daily/
│   ├── ghcnd_all_parquet/               76,198 parquets — daily PRCP/TMAX/TMIN/TAVG (~4.1 GB)
│   └── state_parquet/                   51 parquets — state-level daily PRCP (~2.0 GB)
├── HOMR/
│   └── processed/                       stations_master + crosswalk parquets (~0.2 GB)
├── HPD_1hr/raw/HPD_v02r02_POR_.../     2,080 parquets — hourly precip (~0.8 GB)
├── NCEI_15min_raw/raw_parquet/
│   ├── hpd15_station_parquet/           2,080 parquets — 15-min precip (~1.2 GB)
│   ├── legacy_monthly_parquet/          36 raw-text parquets
│   └── legacy_state_parquet/            33,450 raw-text parquets
├── recon/                               ← scripts + inventory DB (this dir)
├── download_emshr.py
├── process_emshr.py
├── explorer.ipynb
└── PrecipAnalysis.ipynb
```

---

## Scripts — Execution Order

### Stage 0: Station metadata

| Script | What it does |
|--------|-------------|
| `download_emshr.py` | Downloads HOMR EMSHR-Lite fixed-width file from NCEI |
| `process_emshr.py` | Parses EMSHR → `HOMR/processed/stations_master.parquet` + `stations_crosswalk.parquet` (227K stations, 21 cols each) |

### Stage 1: Raw → Parquet conversion

| Script | What it does |
|--------|-------------|
| `recon/convert_dly_to_parquet.py` | Converts GHCN-D `.dly` files → per-station parquets (PRCP/TMAX/TMIN/TAVG only). Also converts state-level `PRCP.csv.gz` files → `state_parquet/`. 4.5× compression. |
| `recon/convert_hpd_csv_to_parquet.py` | Converts HPD hourly CSV files → parquet (one per station). |
| `ASOS/asos1min.py` | Downloads and stages raw ASOS 1-min data from NCDC |
| `ASOS/noa_parser.py` | Parses ISD hourly data → `ASOS/processed_parquet/` |

### Stage 2: Inventory + master station list

| Script | What it does |
|--------|-------------|
| `recon/build_file_inventory.py` | **Pass 1–3 recon.** Walks all of WeatherData/, extracts station IDs from filenames via regex, builds `file_inventory`, `group_schemas`, `anomalies`, `station_id_parse_failures` tables. ~5 min for 510K files. |
| `recon/build_master_station_list.py` | Joins all source-specific station IDs (ICAO, GHCND, WBAN, HPD, NCEI) to HOMR master. Outputs `recon/master_station_list.parquet` and `.csv` (96,226 stations). |
| `recon/build_deletion_manifest.py` | Scans inventory for non-US/international files and writes a deletion manifest. Run after inventory is built; review before executing deletes. |

### Stage 3: Profiling + visualization

| Script | What it does |
|--------|-------------|
| `recon/build_data_profile.py` | Reads parquet footer metadata (zero data rows) for all 510K files. Builds `data_profile`, `station_coverage`, `station_coverage_geo`. ~12 min with Pool(8). |
| `recon/plot_coverage.py` | Generates 5 coverage visualizations (maps + charts) from `station_coverage_geo`. Requires `cartopy` (`pip install cartopy`). ~7 seconds. |

---

## Database: `recon/file_inventory.duckdb`

### Table: `file_inventory` — 510,489 rows

One row per file in the WeatherData directory tree.

| Column | Type | Description |
|--------|------|-------------|
| `filepath` | VARCHAR PK | Absolute path |
| `filename` | VARCHAR | Basename |
| `extension` | VARCHAR | `.parquet`, `.15m.parquet`, `.dat.parquet`, etc. |
| `file_size_bytes` | BIGINT | File size |
| `source` | VARCHAR | `ASOS`, `GHCN_daily`, `HPD_1hr`, `NCEI_15min_raw`, `HOMR` |
| `subfolder` | VARCHAR | Subdirectory within source |
| `schema_group` | VARCHAR | `source\|subfolder\|extension` — grouping key for schema sampling |
| `station_id_raw` | VARCHAR | Station ID extracted from filename (nullable) |
| `id_format` | VARCHAR | `ICAO_ASOS`, `USAF_WBAN`, `GHCND_DLY`, `GHCND_HPD`, `ASOS_A` |
| `date_part` | VARCHAR | Date extracted from filename if present (e.g. `2000-02`) |
| `is_data_file` | BOOLEAN | True for `.parquet`, `.csv`, `.txt`, `.dat`; false for `.py`, `.pdf`, etc. |
| `anomaly_flag` | VARCHAR | Populated by Pass 3 anomaly detection (currently all NULL) |

**Source breakdown:**

| source | files | GB |
|--------|-------|----|
| ASOS | 396,573 | 82.95 |
| GHCN_daily | 76,250 | 4.49 |
| NCEI_15min_raw | 35,575 | 1.58 |
| HPD_1hr | 2,085 | 0.83 |
| HOMR | 6 | 0.23 |

---

### Table: `data_profile` — 510,468 rows

One row per parquet file. All stats read from parquet footer metadata — zero data rows scanned.

| Column | Type | Description |
|--------|------|-------------|
| `filepath` | VARCHAR PK | Matches `file_inventory.filepath` |
| `source` | VARCHAR | Source label |
| `station_id_raw` | VARCHAR | Station ID from inventory |
| `id_format` | VARCHAR | ID format from inventory |
| `total_rows` | BIGINT | Row count from `parquet_file_metadata()` |
| `min_datetime` | VARCHAR | Earliest date/timestamp from row-group stats |
| `max_datetime` | VARCHAR | Latest date/timestamp from row-group stats |
| `has_data` | BOOLEAN | `total_rows > 0` |
| `error` | VARCHAR | Error message if profiling failed (all NULL = 0 errors) |

**Datetime columns by group:**

| Source | Subfolder | Datetime column |
|--------|-----------|----------------|
| ASOS | asos_1min_parquet_master | `datetime_utc` |
| ASOS | processed_parquet | `date` |
| GHCN_daily | ghcnd_all_parquet | `date` |
| GHCN_daily | state_parquet | `date` |
| HPD_1hr | raw | `DATE` |
| NCEI_15min_raw | hpd15_station_parquet | `DATE` |
| NCEI_15min_raw | legacy_* | None (raw text) |

---

### Table: `station_coverage` — 88,825 rows

Aggregated per (station_id_raw, id_format, source, temporal_resolution).

| Column | Type | Description |
|--------|------|-------------|
| `station_id_raw` | VARCHAR | Station ID |
| `id_format` | VARCHAR | ID format |
| `source` | VARCHAR | Data source |
| `temporal_resolution` | VARCHAR | `1min`, `15min`, `hourly`, `hourly_precip`, `daily`, `other` |
| `file_count` | BIGINT | Number of parquet files |
| `total_rows` | BIGINT | Sum of rows across all files |
| `total_gb` | DOUBLE | Sum of file sizes in GB |
| `earliest_date` | VARCHAR | MIN(min_datetime) across files |
| `latest_date` | VARCHAR | MAX(max_datetime) across files |
| `years_span` | INT | `latest_year - earliest_year` |
| `empty_files` | BIGINT | Files with 0 rows |
| `error_files` | BIGINT | Files that failed profiling |

**Network summary:**

| Resolution | Stations | Total Rows | Date Range |
|-----------|---------|-----------|-----------|
| 1-min | 958 | 6.6B | 2000–2022 |
| 15-min | 2,080 | 25.5M | 1970–2026 |
| hourly ISD | 7,509 | 2.0B | 1930–2025 |
| hourly precip | 2,080 | 36.3M | 1940–2026 |
| daily | 76,198 | 914M | 1836–2025 |

---

### Table: `station_coverage_geo` — 88,825 rows

`station_coverage` joined to `master_station_list.parquet` for lat/lon. 81,282 rows have coordinates.

Adds columns: `station_name`, `state`, `county`, `lat`, `lon`, `elev_m`.

**Join keys used:**
- `GHCND_DLY` / `GHCND_HPD` → `station_id_raw = master.ghcnd_ids`
- `ICAO_ASOS` → `SUBSTRING(station_id_raw, 2, 4) = master.icao_ids`  *(station_id_raw = `0KDWH` → ICAO = `KDWH`)*
- `USAF_WBAN` / `ASOS_A` → `SUBSTRING(station_id_raw, 1, 6) = master.usaf_id`

Master list is deduplicated per join key (ROW_NUMBER, prefer rows with coordinates) before joining to prevent fan-out.

---

### Table: `group_schemas` — 114 rows

One row per distinct `schema_group` (source|subfolder|extension). Populated during Pass 2 of `build_file_inventory.py`.

| Column | Description |
|--------|-------------|
| `schema_group` | PK — `source\|subfolder\|extension` |
| `sample_filepath` | Path of the file sampled for schema |
| `column_names` | Column names as a string list |
| `row_count` | Row count of the sampled file |
| `notes` | E.g. "compressed CSV — not opened", "fixed-width format" |
| `sampled_at` | Timestamp |

---

### Table: `anomalies` — 0 rows

Populated by Pass 3 anomaly detection in `build_file_inventory.py`. Currently empty (all files consistent within groups).

---

### Table: `station_id_parse_failures` — 1,610 rows

Files whose filenames didn't match any station ID regex pattern. Mostly `legacy_monthly_parquet` and `legacy_state_parquet` entries which intentionally have no station ID.

---

## Master Station List: `recon/master_station_list.parquet`

96,226 rows. One row per unique station (de-duplicated from HOMR EMSHR-Lite).

Key columns: `ncei_id`, `station_name`, `state`, `county`, `lat_dec`, `lon_dec`, `elev_m`, `station_type`, `is_active`, `has_coordinates`, `icao_ids`, `ghcnd_ids`, `wban_ids`, `coop_ids`, `hpd_ids`, `usaf_id`, `has_asos_1min`, `has_asos_isd`, `has_ghcn_daily`, `has_hpd_1hr`, `has_ncei_15min`.

---

## Rebuilding the Pipeline from Scratch

```bash
# 1. Get station metadata
python download_emshr.py
python process_emshr.py

# 2. Convert raw data to parquet (if .dly / HPD CSVs present)
python recon/convert_dly_to_parquet.py
python recon/convert_hpd_csv_to_parquet.py

# 3. Build file inventory (~5 min)
python recon/build_file_inventory.py

# 4. Build master station list
python recon/build_master_station_list.py

# 5. Profile all parquet files (~12 min, Pool(8))
python recon/build_data_profile.py

# 6. Generate coverage maps (~7 sec)
pip install cartopy   # if not installed
python recon/plot_coverage.py
```

Output maps saved to `recon/`: `map_stations_by_type.png`, `map_temporal_depth.png`,
`map_data_density.png`, `chart_timeline.png`, `chart_states.png`.
