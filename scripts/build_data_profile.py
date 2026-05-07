"""
build_data_profile.py
Profile all parquet files via footer metadata (zero data rows read).
Steps 1-4: datetime mapping → profile → station_coverage → stats.
"""

import os
import sys
import time
import ast
from multiprocessing import Pool, cpu_count
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE        = str(Path(__file__).resolve().parent.parent)   # scripts/ → project root
RECON       = os.path.join(BASE, "data", "recon")
INV_DB      = os.path.join(RECON, "file_inventory.duckdb")
MASTER_PQ   = os.path.join(RECON, "master_station_list.parquet")

# ---------------------------------------------------------------------------
# STEP 1: Datetime column mapping per (source, subfolder, extension)
# ---------------------------------------------------------------------------
DATETIME_COL_MAP = {
    # ASOS 1-min: per-station files in parquet_final/YEAR/
    ('ASOS',  '1min',   '.parquet'): 'timestamp_utc',
    # ASOS 15-min: Hive-partitioned year=X/state=X/hash.parquet
    ('ASOS',  '15min',  '.parquet'): 'date',
    # GHCNd: year-based parquets (no station ID in filename — profiled but station_id_raw=NULL)
    ('GHCNd', 'daily',  '.parquet'): 'DATE',
    ('GHCNd', 'root',   '.parquet'): None,   # ghcnd_us_stations.parquet — metadata, not time series
    # GHCNh: Hive-partitioned year=X/state=X/hash.parquet
    ('GHCNh', 'hourly', '.parquet'): 'date',
    # HOMR metadata
    ('HOMR',  'processed', '.parquet'): 'begin_date',
}

def print_datetime_mapping():
    print("=" * 80)
    print("STEP 1: Datetime column mapping")
    print("=" * 80)
    print(f"  {'Source':<20s} {'Subfolder':<35s} {'Extension':<18s} {'Datetime Column'}")
    print("  " + "-" * 92)
    for (src, sub, ext), col in DATETIME_COL_MAP.items():
        print(f"  {src:<20s} {sub:<35s} {ext:<18s} {str(col)}")
    print()


# ---------------------------------------------------------------------------
# Worker: profile one parquet file (module-level for pickling)
# ---------------------------------------------------------------------------
def _profile_one(args):
    filepath, source, station_id_raw, id_format, datetime_col = args
    fp = filepath.replace("'", "''")   # escape single quotes
    con = None
    try:
        con = duckdb.connect()

        # Row count from parquet footer (instant, no data read)
        r = con.execute(f"SELECT SUM(num_rows) FROM parquet_file_metadata('{fp}')").fetchone()
        total_rows = int(r[0]) if r[0] is not None else 0

        min_dt = max_dt = None
        if datetime_col:
            dc = datetime_col.replace("'", "''")
            # Try row-group stats (fastest path)
            try:
                r2 = con.execute(f"""
                    SELECT MIN(stats_min_value), MAX(stats_max_value)
                    FROM parquet_metadata('{fp}')
                    WHERE path_in_schema = '{dc}'
                """).fetchone()
                if r2 and r2[0] is not None:
                    min_dt = str(r2[0])
                    max_dt = str(r2[1]) if r2[1] is not None else None
            except Exception:
                pass

            # Fallback: read actual min/max from data column
            if min_dt is None and total_rows > 0:
                try:
                    r3 = con.execute(f"""
                        SELECT MIN("{datetime_col}"), MAX("{datetime_col}")
                        FROM read_parquet('{fp}')
                    """).fetchone()
                    if r3 and r3[0] is not None:
                        min_dt = str(r3[0])
                        max_dt = str(r3[1]) if r3[1] is not None else None
                except Exception:
                    pass

        return (filepath, source, station_id_raw, id_format,
                total_rows, min_dt, max_dt, total_rows > 0, None)
    except Exception as e:
        return (filepath, source, station_id_raw, id_format,
                0, None, None, False, str(e)[:200])
    finally:
        if con:
            con.close()


# ---------------------------------------------------------------------------
# STEP 2: Profile all parquet files
# ---------------------------------------------------------------------------
INSERT_SQL = """
    INSERT OR REPLACE INTO data_profile
      (filepath, source, station_id_raw, id_format,
       total_rows, min_datetime, max_datetime, has_data, error)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

def step2_profile(n_workers: int = 8):
    con = duckdb.connect(INV_DB)

    # Create table
    con.execute("""
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
    """)

    # Already-profiled filepaths (resume-safe)
    done_set = {r[0] for r in con.execute("SELECT filepath FROM data_profile").fetchall()}
    print(f"  Already profiled: {len(done_set):,}")

    # Gather tasks: all parquet files in inventory
    rows = con.execute("""
        SELECT filepath, source, station_id_raw, id_format, subfolder, extension
        FROM file_inventory
        WHERE extension IN ('.parquet', '.15m.parquet', '.dat.parquet')
    """).fetchall()

    tasks = []
    for fp, src, sid, idfmt, sub, ext in rows:
        if fp in done_set:
            continue
        dt_col = DATETIME_COL_MAP.get((src, sub, ext))
        tasks.append((fp, src, sid, idfmt, dt_col))

    total = len(tasks)
    print(f"  Files to profile: {total:,}  (workers={n_workers})")
    if total == 0:
        print("  All files already profiled.")
        con.close()
        return

    t0 = time.time()
    processed = errors = 0
    batch = []

    BATCH_SIZE   = 500
    PRINT_EVERY  = 25_000
    chunksize    = max(1, min(200, total // (n_workers * 8)))

    with Pool(n_workers) as pool:
        for result in pool.imap_unordered(_profile_one, tasks, chunksize=chunksize):
            batch.append(result)
            processed += 1
            if result[8] is not None:
                errors += 1

            if len(batch) >= BATCH_SIZE:
                con.executemany(INSERT_SQL, batch)
                batch.clear()

            if processed % PRINT_EVERY == 0:
                elapsed = time.time() - t0
                rate = processed / elapsed
                eta  = (total - processed) / rate if rate > 0 else 0
                print(f"  [{processed:>8,}/{total:,}]  errors={errors}  "
                      f"elapsed={elapsed:.0f}s  eta={eta:.0f}s  rate={rate:.0f}/s",
                      flush=True)

    if batch:
        con.executemany(INSERT_SQL, batch)

    elapsed = time.time() - t0
    total_done = con.execute("SELECT COUNT(*) FROM data_profile").fetchone()[0]
    print(f"\n  Phase complete in {elapsed:.1f}s  ({total:,} profiled, {errors} errors)")
    print(f"  data_profile total rows: {total_done:,}")
    con.close()


# ---------------------------------------------------------------------------
# STEP 3: Build station_coverage
# ---------------------------------------------------------------------------
RESOLUTION_CASE = """
    CASE
        WHEN fi.source = 'ASOS' AND fi.subfolder = '1min'   THEN '1min'
        WHEN fi.source = 'ASOS' AND fi.subfolder = '15min'  THEN '15min'
        WHEN fi.source = 'GHCNd'                            THEN 'daily'
        WHEN fi.source = 'GHCNh'                            THEN 'hourly'
        ELSE 'other'
    END
"""

def step3_station_coverage():
    con = duckdb.connect(INV_DB)
    con.execute("DROP TABLE IF EXISTS station_coverage")
    con.execute(f"""
        CREATE TABLE station_coverage AS
        SELECT
            fi.station_id_raw,
            fi.id_format,
            fi.source,
            {RESOLUTION_CASE}                                AS temporal_resolution,
            COUNT(*)                                         AS file_count,
            SUM(dp.total_rows)                               AS total_rows,
            ROUND(SUM(fi.file_size_bytes) / 1e9, 3)         AS total_gb,
            MIN(dp.min_datetime)                             AS earliest_date,
            MAX(dp.max_datetime)                             AS latest_date,
            TRY_CAST(LEFT(MAX(dp.max_datetime), 4) AS INT)
              - TRY_CAST(LEFT(MIN(dp.min_datetime), 4) AS INT) AS years_span,
            SUM(CASE WHEN NOT dp.has_data  THEN 1 ELSE 0 END) AS empty_files,
            SUM(CASE WHEN dp.error IS NOT NULL THEN 1 ELSE 0 END) AS error_files
        FROM file_inventory fi
        JOIN data_profile dp ON fi.filepath = dp.filepath
        WHERE fi.is_data_file
          AND fi.station_id_raw IS NOT NULL
          AND fi.station_id_raw <> ''
        GROUP BY
            fi.station_id_raw, fi.id_format, fi.source,
            {RESOLUTION_CASE}
    """)
    n = con.execute("SELECT COUNT(*) FROM station_coverage").fetchone()[0]
    print(f"  station_coverage: {n:,} rows")
    con.close()


# ---------------------------------------------------------------------------
# STEP 3b: Build station_coverage_geo (join to master station list)
# ---------------------------------------------------------------------------
def step3b_geo():
    con = duckdb.connect(INV_DB)

    con.execute("DROP TABLE IF EXISTS station_coverage_geo")
    con.execute(f"""
        CREATE TABLE station_coverage_geo AS
        WITH sc AS (SELECT * FROM station_coverage),
        -- Deduplicate master per join key: prefer rows with coordinates,
        -- then pick the first ncei_id to avoid fan-out.
        master_by_ghcnd AS (
            SELECT ghcnd_ids AS join_key, station_name, state, county,
                   lat_dec AS lat, lon_dec AS lon, elev_m
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY ghcnd_ids
                    ORDER BY (lat_dec IS NOT NULL) DESC, ncei_id
                ) AS rn
                FROM read_parquet('{MASTER_PQ}')
                WHERE ghcnd_ids IS NOT NULL AND ghcnd_ids <> ''
            ) WHERE rn = 1
        ),
        master_by_icao AS (
            -- station_id_raw for ICAO_ASOS is 5-char: '0KDWH' → ICAO = chars 2-5
            SELECT icao_ids AS join_key, station_name, state, county,
                   lat_dec AS lat, lon_dec AS lon, elev_m
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY icao_ids
                    ORDER BY (lat_dec IS NOT NULL) DESC, ncei_id
                ) AS rn
                FROM read_parquet('{MASTER_PQ}')
                WHERE icao_ids IS NOT NULL AND icao_ids <> ''
            ) WHERE rn = 1
        ),
        master_by_usaf AS (
            SELECT usaf_id AS join_key, station_name, state, county,
                   lat_dec AS lat, lon_dec AS lon, elev_m
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY usaf_id
                    ORDER BY (lat_dec IS NOT NULL) DESC, ncei_id
                ) AS rn
                FROM read_parquet('{MASTER_PQ}')
                WHERE usaf_id IS NOT NULL AND usaf_id <> ''
            ) WHERE rn = 1
        )
        SELECT
            sc.*,
            COALESCE(m1.station_name, m2.station_name, m3.station_name) AS station_name,
            COALESCE(m1.state,        m2.state,        m3.state)        AS state,
            COALESCE(m1.county,       m2.county,       m3.county)       AS county,
            COALESCE(m1.lat,          m2.lat,          m3.lat)          AS lat,
            COALESCE(m1.lon,          m2.lon,          m3.lon)          AS lon,
            COALESCE(m1.elev_m,       m2.elev_m,       m3.elev_m)       AS elev_m
        FROM sc
        -- Join 1: GHCND ID match (GHCN_daily, HPD, NCEI 15min)
        LEFT JOIN master_by_ghcnd m1 ON (
            sc.id_format IN ('GHCND_DLY', 'GHCND_HPD', 'GHCND')
            AND sc.station_id_raw = m1.join_key
        )
        -- Join 2: ICAO match (ASOS 1-min; station_id_raw='0KDWH' → ICAO='KDWH')
        LEFT JOIN master_by_icao m2 ON (
            sc.id_format = 'ICAO_ASOS'
            AND SUBSTRING(sc.station_id_raw, 2, 4) = m2.join_key
        )
        -- Join 3: USAF match (ASOS processed: first 6 chars = USAF code)
        LEFT JOIN master_by_usaf m3 ON (
            sc.id_format IN ('USAF_WBAN', 'ASOS_A')
            AND SUBSTRING(sc.station_id_raw, 1, 6) = m3.join_key
        )
    """)
    n     = con.execute("SELECT COUNT(*) FROM station_coverage_geo").fetchone()[0]
    n_geo = con.execute("SELECT COUNT(*) FROM station_coverage_geo WHERE lat IS NOT NULL").fetchone()[0]
    print(f"  station_coverage_geo: {n:,} rows  ({n_geo:,} with coordinates)")
    con.close()


# ---------------------------------------------------------------------------
# STEP 4: Print comprehensive stats
# ---------------------------------------------------------------------------
def step4_stats():
    con = duckdb.connect(INV_DB)

    print("\n" + "=" * 80)
    print("STEP 4: DATA PROFILE RESULTS")
    print("=" * 80)

    # Overall profile counts
    r = con.execute("""
        SELECT COUNT(*),
               SUM(total_rows),
               SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END),
               SUM(CASE WHEN NOT has_data THEN 1 ELSE 0 END)
        FROM data_profile
    """).fetchone()
    print(f"\n  Profiled files:  {r[0]:>10,}")
    print(f"  Total rows:      {r[1]:>10,}")
    print(f"  Errors:          {r[2]:>10,}")
    print(f"  Empty files:     {r[3]:>10,}")

    # By resolution
    print("\n  Resolution summary (station_coverage_geo):")
    print(f"  {'Resolution':<16s} {'Source':<20s} {'Stations':>10s} {'Files':>10s} {'Total Rows':>14s} {'GB':>8s} {'Date Range'}")
    print("  " + "-" * 100)
    rows = con.execute("""
        SELECT temporal_resolution, source,
               COUNT(DISTINCT station_id_raw) AS stations,
               SUM(file_count)               AS files,
               SUM(total_rows)               AS total_rows,
               ROUND(SUM(total_gb), 2)       AS gb,
               MIN(earliest_date)            AS earliest,
               MAX(latest_date)              AS latest
        FROM station_coverage_geo
        GROUP BY temporal_resolution, source
        ORDER BY temporal_resolution, source
    """).fetchall()
    for r in rows:
        date_range = f"{str(r[6])[:10]} – {str(r[7])[:10]}" if r[6] else "N/A"
        print(f"  {str(r[0]):<16s} {r[1]:<20s} {r[2]:>10,} {r[3]:>10,} "
              f"{(r[4] or 0):>14,} {r[5]:>8.2f} {date_range}")

    # Data quality
    print("\n  Data quality by source:")
    print(f"  {'Source':<20s} {'Total':>8s} {'Empty':>8s} {'Errors':>8s} {'<1yr span':>10s}")
    print("  " + "-" * 58)
    rows = con.execute("""
        SELECT source,
               COUNT(DISTINCT station_id_raw) AS stations,
               SUM(empty_files)               AS empty,
               SUM(error_files)               AS errors,
               SUM(CASE WHEN years_span IS NOT NULL AND years_span < 1 THEN 1 ELSE 0 END) AS short
        FROM station_coverage_geo
        GROUP BY source ORDER BY source
    """).fetchall()
    for r in rows:
        print(f"  {r[0]:<20s} {r[1]:>8,} {r[2]:>8,} {r[3]:>8,} {r[4]:>10,}")

    # State coverage
    print("\n  State coverage (top 20 by total stations):")
    print(f"  {'State':<8s} {'1min':>8s} {'15min':>8s} {'hourly':>8s} {'hrly_prcp':>10s} {'daily':>8s} {'Total':>8s}")
    print("  " + "-" * 60)
    rows = con.execute("""
        SELECT state,
               SUM(CASE WHEN temporal_resolution='1min'         THEN 1 ELSE 0 END) AS s1min,
               SUM(CASE WHEN temporal_resolution='15min'        THEN 1 ELSE 0 END) AS s15min,
               SUM(CASE WHEN temporal_resolution='hourly'       THEN 1 ELSE 0 END) AS shourly,
               SUM(CASE WHEN temporal_resolution='hourly_precip'THEN 1 ELSE 0 END) AS shrlyp,
               SUM(CASE WHEN temporal_resolution='daily'        THEN 1 ELSE 0 END) AS sdaily,
               COUNT(*) AS total
        FROM station_coverage_geo
        WHERE state IS NOT NULL AND state <> ''
        GROUP BY state
        ORDER BY total DESC
        LIMIT 20
    """).fetchall()
    for r in rows:
        print(f"  {str(r[0]):<8s} {r[1]:>8,} {r[2]:>8,} {r[3]:>8,} {r[4]:>10,} {r[5]:>8,} {r[6]:>8,}")

    # Temporal density by decade
    print("\n  Temporal density by decade (stations with data spanning that decade):")
    print(f"  {'Decade':<10s} {'1min':>8s} {'15min':>8s} {'hourly':>8s} {'daily':>8s}")
    print("  " + "-" * 42)
    decades = list(range(1900, 2030, 10))
    for dec in decades:
        dec_end = dec + 9
        rows = con.execute(f"""
            SELECT
                SUM(CASE WHEN temporal_resolution='1min'   THEN 1 ELSE 0 END),
                SUM(CASE WHEN temporal_resolution='15min'  THEN 1 ELSE 0 END),
                SUM(CASE WHEN temporal_resolution='hourly' THEN 1 ELSE 0 END),
                SUM(CASE WHEN temporal_resolution='daily'  THEN 1 ELSE 0 END)
            FROM station_coverage_geo
            WHERE TRY_CAST(LEFT(earliest_date, 4) AS INT) <= {dec_end}
              AND TRY_CAST(LEFT(latest_date,   4) AS INT) >= {dec}
        """).fetchone()
        if any(v and v > 0 for v in rows):
            print(f"  {dec}s{'':<5s} {rows[0]:>8,} {rows[1]:>8,} {rows[2]:>8,} {rows[3]:>8,}")

    # Validation cross-checks
    print("\n  Cross-checks:")
    inv_n = con.execute("""
        SELECT COUNT(*) FROM file_inventory fi
        WHERE fi.is_data_file
          AND (fi.extension='.parquet' OR fi.extension LIKE '%.parquet')
          AND NOT EXISTS (SELECT 1 FROM data_profile dp WHERE dp.filepath=fi.filepath)
    """).fetchone()[0]
    print(f"    Files in inventory but NOT profiled: {inv_n}")

    top_errors = con.execute("""
        SELECT filepath, error FROM data_profile
        WHERE error IS NOT NULL LIMIT 5
    """).fetchall()
    if top_errors:
        print(f"    Top errors:")
        for r in top_errors:
            print(f"      {os.path.basename(r[0])}: {r[1][:80]}")

    con.close()


# ---------------------------------------------------------------------------
# STEP 5: Build per-resolution station inventory parquets
# ---------------------------------------------------------------------------
DATA_DIR  = os.path.join(BASE, "data")
HOMR_PQ   = os.path.join(DATA_DIR, "HOMR", "processed", "stations_master.parquet")
GHCND_STA = os.path.join(DATA_DIR, "GHCNd", "ghcnd_us_stations.parquet")
GHCND_DIR = os.path.join(DATA_DIR, "GHCNd", "daily")
GHCNH_DIR = os.path.join(DATA_DIR, "GHCNh", "hourly")
MIN15_DIR = os.path.join(DATA_DIR, "ASOS", "15min")
MIN1_DIR  = os.path.join(DATA_DIR, "ASOS", "1min", "parquet_final")

INVENTORY_OUT = {
    "daily":  os.path.join(RECON, "station_inventory_daily.parquet"),
    "hourly": os.path.join(RECON, "station_inventory_hourly.parquet"),
    "15min":  os.path.join(RECON, "station_inventory_15min.parquet"),
    "1min":   os.path.join(RECON, "station_inventory_1min.parquet"),
}


def _escape(p: str) -> str:
    return p.replace("'", "''")


def _build_daily_inventory(con):
    """GHCNd: year-based parquets joined to ghcnd_us_stations.parquet for coords."""
    out = INVENTORY_OUT["daily"]
    if not os.path.isdir(GHCND_DIR):
        print("  [SKIP] GHCNd daily dir not found:", GHCND_DIR)
        return
    if not os.path.exists(GHCND_STA):
        print("  [SKIP] ghcnd_us_stations.parquet not found:", GHCND_STA)
        return
    print("  Building daily inventory (GHCNd)...")
    t0 = time.time()
    con.execute(f"""
        COPY (
            SELECT
                d.STATION   AS station_id,
                s.LAT       AS lat,
                s.LON       AS lon,
                s.STATE     AS state,
                MIN(CAST(d.DATE AS DATE)) AS earliest_dt,
                MAX(CAST(d.DATE AS DATE)) AS latest_dt,
                COUNT(*)                  AS row_count,
                COUNT(DISTINCT YEAR(CAST(d.DATE AS DATE))) AS file_count
            FROM read_parquet('{_escape(GHCND_DIR)}/*.parquet', union_by_name=true) d
            JOIN read_parquet('{_escape(GHCND_STA)}') s ON d.STATION = s.STATION
            GROUP BY d.STATION, s.LAT, s.LON, s.STATE
        ) TO '{_escape(out)}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{_escape(out)}')").fetchone()[0]
    print(f"    daily inventory: {n:,} stations  ({time.time()-t0:.1f}s)")


def _build_hourly_inventory(con):
    """GHCNh: Hive-partitioned; join to HOMR by WBAN for coords."""
    out = INVENTORY_OUT["hourly"]
    if not os.path.isdir(GHCNH_DIR):
        print("  [SKIP] GHCNh hourly dir not found:", GHCNH_DIR)
        return
    print("  Building hourly inventory (GHCNh)...")
    t0 = time.time()
    # HOMR dedup: one lat/lon per wban_id, prefer rows with coords
    homr_exists = os.path.exists(HOMR_PQ)
    if homr_exists:
        homr_cte = f"""
        homr AS (
            SELECT wban_id, FIRST(lat_dec) AS lat, FIRST(lon_dec) AS lon
            FROM (
                SELECT wban_id, lat_dec, lon_dec
                FROM read_parquet('{_escape(HOMR_PQ)}')
                WHERE wban_id IS NOT NULL AND lat_dec IS NOT NULL
                ORDER BY (lat_dec IS NOT NULL) DESC
            )
            GROUP BY wban_id
        ),"""
        lat_expr = "h.lat"
        lon_expr = "h.lon"
        join_clause = f"LEFT JOIN homr h ON SUBSTRING(g.station_id, 7, 5) = h.wban_id"
    else:
        homr_cte = ""
        lat_expr = "NULL::DOUBLE"
        lon_expr = "NULL::DOUBLE"
        join_clause = ""

    con.execute(f"""
        COPY (
            WITH ghcnh AS (
                SELECT station_id, state,
                       MIN(CAST(date AS DATE)) AS earliest_dt,
                       MAX(CAST(date AS DATE)) AS latest_dt,
                       COUNT(*)                AS row_count,
                       COUNT(DISTINCT year)    AS file_count
                FROM read_parquet('{_escape(GHCNH_DIR)}/**/*.parquet',
                                  hive_partitioning=true, union_by_name=true)
                WHERE station_id IS NOT NULL
                GROUP BY station_id, state
            ),
            {homr_cte}
            dummy AS (SELECT 1)
            SELECT g.station_id, {lat_expr} AS lat, {lon_expr} AS lon, g.state,
                   g.earliest_dt, g.latest_dt, g.row_count, g.file_count
            FROM ghcnh g
            {join_clause}
        ) TO '{_escape(out)}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{_escape(out)}')").fetchone()[0]
    print(f"    hourly inventory: {n:,} stations  ({time.time()-t0:.1f}s)")


def _build_15min_inventory(con):
    """15-min ASOS: Hive-partitioned; station_id is GHCND format.

    Parquet schema: station_id, station_name, lat, lon, elevation, element,
                    month, day, time, precip_in, flag_measurement, flag_quality,
                    state, year  (year/state are Hive partition columns).
    Date is reconstructed from year/month/day — there is no single 'date' column.
    lat/lon are embedded so no external join is needed.
    """
    out = INVENTORY_OUT["15min"]
    if not os.path.isdir(MIN15_DIR):
        print("  [SKIP] ASOS 15min dir not found:", MIN15_DIR)
        return
    print("  Building 15-min inventory (ASOS/15min)...")
    t0 = time.time()

    con.execute(f"""
        COPY (
            WITH raw AS (
                SELECT station_id,
                       FIRST(lat)  AS lat,
                       FIRST(lon)  AS lon,
                       state,
                       MIN(MAKE_DATE(year::INT, month::INT, day::INT)) AS earliest_dt,
                       MAX(MAKE_DATE(year::INT, month::INT, day::INT)) AS latest_dt,
                       COUNT(*)    AS row_count,
                       COUNT(DISTINCT year) AS file_count
                FROM read_parquet('{_escape(MIN15_DIR)}/**/*.parquet',
                                  hive_partitioning=true, union_by_name=true)
                WHERE station_id IS NOT NULL
                GROUP BY station_id, state
            )
            SELECT station_id, lat, lon, state,
                   earliest_dt, latest_dt, row_count, file_count
            FROM raw
        ) TO '{_escape(out)}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{_escape(out)}')").fetchone()[0]
    print(f"    15-min inventory: {n:,} stations  ({time.time()-t0:.1f}s)")


def _build_1min_inventory(con):
    """ASOS 1-min: per-station parquets; ICAO from filename; coords from HOMR."""
    out = INVENTORY_OUT["1min"]
    if not os.path.isdir(MIN1_DIR):
        print("  [SKIP] ASOS 1min parquet_final dir not found:", MIN1_DIR)
        return
    if not os.path.exists(INV_DB):
        print("  [SKIP] file_inventory.duckdb not found — run build_file_inventory.py first")
        return
    print("  Building 1-min inventory (ASOS/1min)...")
    t0 = time.time()
    # Pull ICAO + date range from file inventory
    con.execute(f"ATTACH '{_escape(INV_DB)}' AS inv (READ_ONLY)")
    homr_join = ""
    lat_expr = "NULL::DOUBLE"
    lon_expr = "NULL::DOUBLE"
    state_expr = "NULL::VARCHAR"
    if os.path.exists(HOMR_PQ):
        homr_join = f"""
        LEFT JOIN (
            SELECT DISTINCT icao_id, FIRST(lat_dec) AS lat, FIRST(lon_dec) AS lon,
                            FIRST(state) AS state
            FROM read_parquet('{_escape(HOMR_PQ)}')
            WHERE icao_id IS NOT NULL AND lat_dec IS NOT NULL
            GROUP BY icao_id
        ) h ON fi_icao.icao_id = h.icao_id"""
        lat_expr = "h.lat"
        lon_expr = "h.lon"
        state_expr = "h.state"

    con.execute(f"""
        COPY (
            WITH fi_icao AS (
                SELECT SUBSTRING(station_id_raw, 2, 4) AS icao_id,
                       MIN(date_part)               AS earliest_raw,
                       MAX(date_part)               AS latest_raw,
                       COUNT(*)                     AS file_count
                FROM inv.file_inventory
                WHERE source='ASOS' AND subfolder='1min' AND id_format='ICAO_ASOS'
                  AND station_id_raw IS NOT NULL
                GROUP BY SUBSTRING(station_id_raw, 2, 4)
            )
            SELECT fi_icao.icao_id AS station_id,
                   {lat_expr} AS lat, {lon_expr} AS lon, {state_expr} AS state,
                   TRY_CAST(earliest_raw || '-01' AS DATE) AS earliest_dt,
                   TRY_CAST(latest_raw   || '-01' AS DATE) AS latest_dt,
                   0::BIGINT AS row_count,
                   fi_icao.file_count
            FROM fi_icao{homr_join}
        ) TO '{_escape(out)}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    con.execute("DETACH inv")
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{_escape(out)}')").fetchone()[0]
    print(f"    1-min inventory: {n:,} stations  ({time.time()-t0:.1f}s)")


def step5_station_inventories(sources=None):
    """
    Build per-resolution station inventory parquets in data/recon/.
    Each file has: station_id, lat, lon, state, earliest_dt, latest_dt, row_count, file_count.

    Args:
        sources: list of resolution names to build, or None for all.
                 Valid values: 'daily', 'hourly', '15min', '1min'
    """
    os.makedirs(RECON, exist_ok=True)
    all_sources = ['daily', 'hourly', '15min', '1min']
    targets = sources if sources else all_sources

    print("=" * 80)
    print("STEP 5: Building station inventory parquets")
    print("=" * 80)
    print(f"  Targets: {targets}")
    print(f"  Output:  {RECON}/")

    t0 = time.time()
    con = duckdb.connect()   # in-memory; each sub-function uses it
    try:
        if 'daily'  in targets: _build_daily_inventory(con)
        if 'hourly' in targets: _build_hourly_inventory(con)
        if '15min'  in targets: _build_15min_inventory(con)
        if '1min'   in targets: _build_1min_inventory(con)
    finally:
        con.close()

    print(f"\n  Step 5 complete in {time.time()-t0:.1f}s")
    print("  Files written:")
    for res, path in INVENTORY_OUT.items():
        if res in targets and os.path.exists(path):
            mb = os.path.getsize(path) / 1e6
            print(f"    {res:8s} → {path}  ({mb:.1f} MB)")


# ---------------------------------------------------------------------------
# STEP 6: Build station_coverage + station_coverage_geo from inventory parquets
# (used when --station-inventory-only was run and step2/step3 were skipped)
# ---------------------------------------------------------------------------
def step6_coverage_tables():
    """Build station_coverage and station_coverage_geo from the 4 inventory parquets."""
    inv = INVENTORY_OUT  # dict of resolution → parquet path
    res_map = [
        ("1min",   inv["1min"]),
        ("15min",  inv["15min"]),
        ("hourly", inv["hourly"]),
        ("daily",  inv["daily"]),
    ]

    con = duckdb.connect(INV_DB)

    # station_coverage_geo — one row per station with lat/lon
    union_parts_geo = []
    for res_label, path in res_map:
        if os.path.exists(path):
            union_parts_geo.append(
                f"SELECT '{res_label}' AS temporal_resolution, lat, lon "
                f"FROM read_parquet('{_escape(path)}') WHERE lat IS NOT NULL AND lon IS NOT NULL"
            )
    if union_parts_geo:
        geo_sql = " UNION ALL ".join(union_parts_geo)
        con.execute(f"CREATE OR REPLACE TABLE station_coverage_geo AS {geo_sql}")
        n = con.execute("SELECT COUNT(*) FROM station_coverage_geo").fetchone()[0]
        print(f"  station_coverage_geo: {n:,} rows")
    else:
        print("  [WARN] No inventory parquets found — station_coverage_geo not built")

    # station_coverage — aggregate per resolution
    union_parts_cov = []
    for res_label, path in res_map:
        if os.path.exists(path):
            union_parts_cov.append(
                f"SELECT '{res_label}' AS temporal_resolution, "
                f"earliest_dt, latest_dt, row_count "
                f"FROM read_parquet('{_escape(path)}')"
            )
    if union_parts_cov:
        src_sql = " UNION ALL ".join(union_parts_cov)
        con.execute(f"""
            CREATE OR REPLACE TABLE station_coverage AS
            SELECT temporal_resolution,
                   MIN(earliest_dt)  AS earliest_date,
                   MAX(latest_dt)    AS latest_date,
                   COUNT(*)          AS n_stations,
                   SUM(row_count)    AS total_rows
            FROM ({src_sql})
            GROUP BY temporal_resolution
        """)
        n = con.execute("SELECT COUNT(*) FROM station_coverage").fetchone()[0]
        print(f"  station_coverage: {n:,} rows")
    else:
        print("  [WARN] No inventory parquets found — station_coverage not built")

    con.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--step3-only", action="store_true",
        help="Skip file profiling; only rebuild station_coverage and station_coverage_geo",
    )
    parser.add_argument(
        "--station-inventory-only", action="store_true",
        help="Skip file profiling; only build station_inventory_*.parquet per resolution",
    )
    parser.add_argument(
        "--coverage-tables-only", action="store_true",
        help="Build station_coverage + station_coverage_geo from existing inventory parquets",
    )
    parser.add_argument(
        "--source", choices=["daily", "hourly", "15min", "1min"],
        action="append", dest="sources",
        help="Limit --station-inventory-only to specific resolution(s); can repeat",
    )
    args = parser.parse_args()

    n_workers = min(cpu_count(), 8)
    t_total = time.time()

    if args.coverage_tables_only:
        print("=" * 80)
        print("STEP 6: Building station_coverage + station_coverage_geo from inventory parquets")
        print("=" * 80)
        step6_coverage_tables()
    elif args.station_inventory_only:
        step5_station_inventories(sources=args.sources)
    elif args.step3_only:
        print("=" * 80)
        print("STEP 3 ONLY: Rebuilding station_coverage + station_coverage_geo")
        print("=" * 80)
        step3_station_coverage()
        step3b_geo()
    else:
        print_datetime_mapping()

        print("=" * 80)
        print("STEP 2: Profiling parquet files")
        print("=" * 80)
        step2_profile(n_workers)

        print("\n" + "=" * 80)
        print("STEP 3: Building station_coverage")
        print("=" * 80)
        step3_station_coverage()
        step3b_geo()

        step4_stats()

        print("\n" + "=" * 80)
        print("STEP 5: Building station inventories")
        print("=" * 80)
        step5_station_inventories()

    elapsed = time.time() - t_total
    print(f"\nTotal elapsed: {elapsed:.1f}s")


if __name__ == '__main__':
    main()
