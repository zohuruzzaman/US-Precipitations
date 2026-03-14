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

import duckdb

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE        = os.path.expanduser("~/Documents/WeatherData")
RECON       = os.path.join(BASE, "recon")
INV_DB      = os.path.join(RECON, "file_inventory.duckdb")
MASTER_PQ   = os.path.join(RECON, "master_station_list.parquet")

# ---------------------------------------------------------------------------
# STEP 1: Datetime column mapping per (source, subfolder, extension)
# ---------------------------------------------------------------------------
DATETIME_COL_MAP = {
    ('ASOS',           'asos_1min_parquet_master', '.parquet'):     'datetime_utc',
    ('ASOS',           'processed_parquet',         '.parquet'):     'date',
    ('GHCN_daily',     'ghcnd_all_parquet',          '.parquet'):     'date',
    ('GHCN_daily',     'state_parquet',              '.parquet'):     'date',
    ('HOMR',           'processed',                  '.parquet'):     'begin_date',
    ('HPD_1hr',        'raw',                        '.parquet'):     'DATE',
    ('NCEI_15min_raw', 'raw_parquet',                '.15m.parquet'): 'DATE',
    ('NCEI_15min_raw', 'raw_parquet',                '.dat.parquet'): None,
    ('NCEI_15min_raw', 'raw_parquet',                '.parquet'):     None,
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
        WHEN fi.source = 'ASOS' AND fi.id_format = 'ICAO_ASOS'                THEN '1min'
        WHEN fi.source = 'ASOS' AND fi.id_format IN ('USAF_WBAN', 'ASOS_A')   THEN 'hourly'
        WHEN fi.source = 'HPD_1hr'                                             THEN 'hourly_precip'
        WHEN fi.source = 'NCEI_15min_raw' AND fi.extension = '.15m.parquet'    THEN '15min'
        WHEN fi.source = 'GHCN_daily'                                          THEN 'daily'
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
# Main
# ---------------------------------------------------------------------------
def main():
    n_workers = min(cpu_count(), 8)
    t_total = time.time()

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

    elapsed = time.time() - t_total
    print(f"\nTotal elapsed: {elapsed:.1f}s")


if __name__ == '__main__':
    main()
