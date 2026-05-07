"""
build_master_station_list.py
Build a master station crosswalk.

If HOMR backbone data exists (data/HOMR/processed/stations_master.parquet):
  → Full build: HOMR + file inventory joins + USAF coord-matching.

If HOMR data is absent (common during initial data recovery):
  → Fallback build: extract station metadata directly from HPD/NCEI parquet
    files (which embed STATION, NAME, LATITUDE, LONGITUDE, ELEVATION per row)
    and augment with GHCN station IDs from file_inventory.
"""

import os
import sys
import time
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE        = str(Path(__file__).resolve().parent.parent)   # scripts/ → project root
DATA        = os.path.join(BASE, "data")
RECON       = os.path.join(DATA, "recon")
INV_DB      = os.path.join(RECON, "file_inventory.duckdb")
OUT_PARQUET = os.path.join(RECON, "master_station_list.parquet")
OUT_CSV     = os.path.join(RECON, "master_station_list.csv")

STATIONS_MASTER = os.path.join(DATA, "HOMR", "processed", "stations_master.parquet")
ASOS_MASTER_CSV = os.path.join(DATA, "ASOS", "master_station_inventory.csv")

# Pre-computed station inventory parquets (built by build_data_profile.py --station-inventory-only)
INV_DAILY  = os.path.join(RECON, "station_inventory_daily.parquet")
INV_HOURLY = os.path.join(RECON, "station_inventory_hourly.parquet")
INV_15MIN  = os.path.join(RECON, "station_inventory_15min.parquet")
INV_1MIN   = os.path.join(RECON, "station_inventory_1min.parquet")

COORD_TOL = 0.01  # degrees; ~1.1 km at equator


# ---------------------------------------------------------------------------
# Fallback build: extract coordinates from HPD/NCEI parquet metadata
# ---------------------------------------------------------------------------
def build_fallback(con):
    """Build minimal master_station_list.parquet without HOMR data.
    Uses station inventory parquets (GHCNd daily, GHCNh hourly, 15-min) as coord sources.
    """

    print("=" * 60)
    print("FALLBACK MODE: Building from station inventory parquets")
    print("=" * 60)

    # ── GHCNd daily stations (have coords from ghcnd_us_stations.parquet) ──
    has_daily  = os.path.exists(INV_DAILY)
    has_hourly = os.path.exists(INV_HOURLY)
    has_15min  = os.path.exists(INV_15MIN)

    print(f"  station_inventory_daily.parquet:  {'found' if has_daily  else 'MISSING'}")
    print(f"  station_inventory_hourly.parquet: {'found' if has_hourly else 'MISSING'}")
    print(f"  station_inventory_15min.parquet:  {'found' if has_15min  else 'MISSING'}")

    sources = []

    if has_daily:
        con.execute(f"""
            CREATE TEMP TABLE daily_meta AS
            SELECT station_id, lat AS lat_dec, lon AS lon_dec,
                   NULL::DOUBLE AS elev_m, state, NULL::VARCHAR AS station_name
            FROM read_parquet('{INV_DAILY}')
            WHERE station_id IS NOT NULL
        """)
        n = con.execute("SELECT COUNT(*) FROM daily_meta").fetchone()[0]
        print(f"  GHCNd stations: {n:,}")
        sources.append("GHCNd")

    if has_15min:
        con.execute(f"""
            CREATE TEMP TABLE meta_15min AS
            SELECT station_id, lat AS lat_dec, lon AS lon_dec,
                   NULL::DOUBLE AS elev_m, state, NULL::VARCHAR AS station_name
            FROM read_parquet('{INV_15MIN}')
            WHERE station_id IS NOT NULL
        """)
        n = con.execute("SELECT COUNT(*) FROM meta_15min").fetchone()[0]
        print(f"  15-min stations: {n:,}")
        sources.append("15min")

    # ── GHCN station IDs — now from daily inventory ─────────────────────────
    if has_daily:
        con.execute("""
            CREATE TEMP TABLE ghcn_ids AS
            SELECT DISTINCT station_id FROM daily_meta
        """)
    else:
        con.execute("CREATE TEMP TABLE ghcn_ids (station_id VARCHAR)")
    n_ghcn = con.execute("SELECT COUNT(*) FROM ghcn_ids").fetchone()[0]
    print(f"  GHCN unique station IDs: {n_ghcn:,}")

    # ── Combine: daily (GHCNd) is primary; 15min fills gaps ─────────────────
    if has_daily and has_15min:
        con.execute("""
            CREATE TEMP TABLE combined_meta AS
            SELECT
                COALESCE(d.station_id, n.station_id) AS station_id,
                COALESCE(d.station_name, n.station_name) AS station_name,
                COALESCE(d.state, n.state)            AS state,
                COALESCE(d.lat_dec, n.lat_dec)        AS lat_dec,
                COALESCE(d.lon_dec, n.lon_dec)        AS lon_dec,
                COALESCE(d.elev_m,  n.elev_m)         AS elev_m,
                (d.station_id IS NOT NULL)             AS has_hpd,
                (n.station_id IS NOT NULL)             AS has_ncei
            FROM daily_meta d
            FULL OUTER JOIN meta_15min n USING (station_id)
        """)
    elif has_daily:
        con.execute("""
            CREATE TEMP TABLE combined_meta AS
            SELECT station_id, station_name, state, lat_dec, lon_dec, elev_m,
                   TRUE AS has_hpd, FALSE AS has_ncei
            FROM daily_meta
        """)
    elif has_15min:
        con.execute("""
            CREATE TEMP TABLE combined_meta AS
            SELECT station_id, station_name, state, lat_dec, lon_dec, elev_m,
                   FALSE AS has_hpd, TRUE AS has_ncei
            FROM meta_15min
        """)
    else:
        print("  WARNING: No station inventory parquets found. Creating empty master.")
        con.execute("""
            CREATE TEMP TABLE combined_meta (
                station_id VARCHAR, station_name VARCHAR, state VARCHAR,
                lat_dec DOUBLE, lon_dec DOUBLE, elev_m DOUBLE,
                has_hpd BOOLEAN, has_ncei BOOLEAN
            )
        """)

    # Add GHCN-only stations not already in combined_meta
    con.execute("""
        INSERT INTO combined_meta
        SELECT g.station_id,
               NULL AS station_name,
               NULL AS state,
               NULL AS lat_dec,
               NULL AS lon_dec,
               NULL AS elev_m,
               FALSE AS has_hpd,
               FALSE AS has_ncei
        FROM ghcn_ids g
        WHERE NOT EXISTS (SELECT 1 FROM combined_meta m WHERE m.station_id = g.station_id)
    """)

    n_total = con.execute("SELECT COUNT(*) FROM combined_meta").fetchone()[0]
    n_coords = con.execute("SELECT COUNT(*) FROM combined_meta WHERE lat_dec IS NOT NULL").fetchone()[0]
    print(f"\n  Combined: {n_total:,} total stations | {n_coords:,} with coordinates")

    # ── Write output ─────────────────────────────────────────────────────────
    # Schema must match what step3b_geo() expects:
    #   ghcnd_ids, icao_ids, usaf_id, ncei_id, station_name, state, county,
    #   lat_dec, lon_dec, elev_m, ...
    con.execute(f"""
        COPY (
            SELECT
                station_id   AS ncei_id,
                station_name,
                state,
                NULL::VARCHAR AS county,
                lat_dec,
                lon_dec,
                elev_m,
                NULL::VARCHAR AS station_type,
                NULL::BOOLEAN AS is_active,
                (lat_dec IS NOT NULL AND lon_dec IS NOT NULL) AS has_coordinates,
                NULL::VARCHAR AS wban_ids,
                NULL::VARCHAR AS icao_ids,
                station_id    AS ghcnd_ids,   -- station_id IS the GHCND ID
                NULL::VARCHAR AS coop_ids,
                NULL::VARCHAR AS hpd_ids,
                NULL::VARCHAR AS wmo_ids,
                NULL::VARCHAR AS faa_ids,
                NULL::VARCHAR AS nwsli_ids,
                NULL::BOOLEAN AS has_asos_1min,
                NULL::BIGINT  AS asos_1min_files,
                NULL::DOUBLE  AS asos_1min_gb,
                NULL::VARCHAR AS asos_1min_start,
                NULL::VARCHAR AS asos_1min_end,
                NULL::BOOLEAN AS has_asos_isd,
                NULL::BIGINT  AS asos_isd_files,
                NULL::DOUBLE  AS asos_isd_gb,
                FALSE         AS has_hpd_1hr,
                NULL::BIGINT  AS hpd_files,
                NULL::DOUBLE  AS hpd_data_gb,
                has_ncei      AS has_ncei_15min,
                NULL::BIGINT  AS ncei_files,
                NULL::DOUBLE  AS ncei_data_gb,
                has_hpd       AS has_ghcn_daily,
                NULL::BIGINT  AS ghcn_daily_files,
                NULL::DOUBLE  AS ghcn_daily_data_gb,
                FALSE         AS homr_matched,
                NULL::VARCHAR AS usaf_id,
                'FALLBACK'    AS source_if_orphan
            FROM combined_meta
            ORDER BY state NULLS LAST, station_name NULLS LAST
        ) TO '{OUT_PARQUET}' (FORMAT PARQUET)
    """)
    con.execute(f"""
        COPY (SELECT * FROM read_parquet('{OUT_PARQUET}'))
        TO '{OUT_CSV}' (HEADER, DELIMITER ',')
    """)

    pq_mb = os.path.getsize(OUT_PARQUET) / 1e6
    csv_mb = os.path.getsize(OUT_CSV) / 1e6
    print(f"\n  Written: {OUT_PARQUET} ({pq_mb:.1f} MB)")
    print(f"  Written: {OUT_CSV} ({csv_mb:.1f} MB)")
    print(f"\n  Summary:")
    print(f"    Total stations:          {n_total:,}")
    print(f"    With coordinates:        {n_coords:,}")
    print(f"    GHCN-only (no coords):   {n_total - n_coords:,}")


# ---------------------------------------------------------------------------
# Full HOMR-based build (original logic, path-fixed)
# ---------------------------------------------------------------------------
def build_full(con):
    t0 = time.time()

    con.execute(f"ATTACH '{INV_DB}' AS inv (READ_ONLY)")

    print("=" * 60)
    print("PHASE 1: Schema verification")
    print("=" * 60)

    r = con.execute(f"""
        SELECT COUNT(*) as rows, COUNT(DISTINCT ncei_id) as unique_stations
        FROM read_parquet('{STATIONS_MASTER}')
        WHERE country_code = 'US'
    """).fetchone()
    print(f"  stations_master (US): {r[0]:,} rows → {r[1]:,} unique ncei_id")

    r = con.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE icao_id IS NOT NULL) as has_icao,
            COUNT(*) FILTER (WHERE wban_id IS NOT NULL) as has_wban,
            COUNT(*) FILTER (WHERE ghcnd_id IS NOT NULL) as has_ghcnd
        FROM (
            SELECT DISTINCT ncei_id, icao_id, wban_id, ghcnd_id
            FROM read_parquet('{STATIONS_MASTER}')
            WHERE country_code='US'
        )
    """).fetchone()
    print(f"  US stations with icao_id: {r[0]:,} | wban_id: {r[1]:,} | ghcnd_id: {r[2]:,}")

    print("\n" + "=" * 60)
    print("PHASE 2: Distinct stations from file inventory")
    print("=" * 60)

    # ASOS 1-min: per-station files — station_id in filename (ICAO_ASOS format)
    con.execute("""
        CREATE TEMP VIEW asos_icao AS
        SELECT SUBSTRING(station_id_raw, 2, 4) AS icao,
               COUNT(*) AS file_count,
               ROUND(SUM(file_size_bytes)/1e9, 3) AS data_gb,
               MIN(date_part) AS earliest_date,
               MAX(date_part) AS latest_date
        FROM inv.file_inventory
        WHERE source = 'ASOS' AND subfolder = '1min' AND id_format = 'ICAO_ASOS'
        GROUP BY SUBSTRING(station_id_raw, 2, 4)
    """)

    # USAF-only ASOS (for orphan coord matching) — still from ASOS/1min inventory
    # Note: GHCNh (hourly) replaces the old ASOS/processed_parquet — it uses ISD station_id not USAF_WBAN files
    con.execute("""
        CREATE TEMP TABLE asos_usaf_only (
            usaf VARCHAR, file_count BIGINT, data_gb DOUBLE
        )
    """)

    # GHCNh hourly inventory (replaces old asos_wban / processed_parquet)
    if os.path.exists(INV_HOURLY):
        con.execute(f"""
            CREATE TEMP VIEW ghcnh_stations AS
            SELECT station_id,
                   SUBSTRING(station_id, 7, 5) AS wban,
                   lat, lon,
                   file_count,
                   ROUND(file_count * 0.001, 3) AS data_gb,
                   earliest_dt AS earliest_date,
                   latest_dt   AS latest_date
            FROM read_parquet('{INV_HOURLY}')
            WHERE station_id IS NOT NULL
        """)
        n_hourly = con.execute("SELECT COUNT(*) FROM ghcnh_stations").fetchone()[0]
    else:
        con.execute("""
            CREATE TEMP VIEW ghcnh_stations AS
            SELECT NULL::VARCHAR AS station_id, NULL::VARCHAR AS wban,
                   NULL::DOUBLE AS lat, NULL::DOUBLE AS lon,
                   0::BIGINT AS file_count, 0.0::DOUBLE AS data_gb,
                   NULL::VARCHAR AS earliest_date, NULL::VARCHAR AS latest_date
            WHERE FALSE
        """)
        n_hourly = 0

    # HPD: no longer available — empty placeholder keeps downstream SQL intact
    con.execute("""
        CREATE TEMP VIEW hpd_stations AS
        SELECT NULL::VARCHAR AS ghcnd_id, 0::BIGINT AS file_count, 0.0::DOUBLE AS data_gb
        WHERE FALSE
    """)

    # 15-min station inventory (replaces old ncei_stations from NCEI_15min_raw)
    if os.path.exists(INV_15MIN):
        con.execute(f"""
            CREATE TEMP VIEW ncei_stations AS
            SELECT station_id AS ghcnd_id,
                   file_count,
                   ROUND(file_count * 0.001, 3) AS data_gb
            FROM read_parquet('{INV_15MIN}')
            WHERE station_id IS NOT NULL
        """)
        n_15min = con.execute("SELECT COUNT(*) FROM ncei_stations").fetchone()[0]
    else:
        con.execute("""
            CREATE TEMP VIEW ncei_stations AS
            SELECT NULL::VARCHAR AS ghcnd_id, 0::BIGINT AS file_count, 0.0::DOUBLE AS data_gb
            WHERE FALSE
        """)
        n_15min = 0

    # GHCNd daily inventory (replaces old per-station ghcnd_all_parquet)
    if os.path.exists(INV_DAILY):
        con.execute(f"""
            CREATE TEMP VIEW ghcnd_stations AS
            SELECT station_id AS ghcnd_id,
                   file_count,
                   ROUND(file_count * 0.0001, 3) AS data_gb
            FROM read_parquet('{INV_DAILY}')
            WHERE station_id IS NOT NULL
        """)
        n_daily = con.execute("SELECT COUNT(*) FROM ghcnd_stations").fetchone()[0]
    else:
        con.execute("""
            CREATE TEMP VIEW ghcnd_stations AS
            SELECT NULL::VARCHAR AS ghcnd_id, 0::BIGINT AS file_count, 0.0::DOUBLE AS data_gb
            WHERE FALSE
        """)
        n_daily = 0

    r  = con.execute("SELECT COUNT(*) FROM asos_icao").fetchone()[0]
    print(f"  ASOS 1-min unique ICAO stations: {r:,}")
    print(f"  GHCNh hourly stations:           {n_hourly:,}")
    print(f"  15-min stations:                 {n_15min:,}")
    print(f"  GHCNd daily stations:            {n_daily:,}")

    print("\n" + "=" * 60)
    print("PHASE 3: Collapsing stations_master (temporal → one row per ncei_id)")
    print("=" * 60)

    con.execute(f"""
        CREATE TEMP VIEW homr_collapsed AS
        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY ncei_id
                    ORDER BY
                        CASE WHEN is_active THEN 0 ELSE 1 END,
                        end_date DESC NULLS LAST
                ) AS rn
            FROM read_parquet('{STATIONS_MASTER}')
            WHERE country_code = 'US'
        ),
        base AS (
            SELECT ncei_id, station_name, state, county, country_code,
                   lat_dec, lon_dec, elev_m, station_type, is_active, has_coordinates
            FROM ranked WHERE rn = 1
        ),
        ids AS (
            SELECT ncei_id,
                STRING_AGG(DISTINCT wban_id,  '|') FILTER (WHERE wban_id  IS NOT NULL) AS wban_ids,
                STRING_AGG(DISTINCT icao_id,  '|') FILTER (WHERE icao_id  IS NOT NULL) AS icao_ids,
                STRING_AGG(DISTINCT ghcnd_id, '|') FILTER (WHERE ghcnd_id IS NOT NULL) AS ghcnd_ids,
                STRING_AGG(DISTINCT coop_id,  '|') FILTER (WHERE coop_id  IS NOT NULL) AS coop_ids,
                STRING_AGG(DISTINCT hpd_id,   '|') FILTER (WHERE hpd_id   IS NOT NULL) AS hpd_ids,
                STRING_AGG(DISTINCT wmo_id,   '|') FILTER (WHERE wmo_id   IS NOT NULL) AS wmo_ids,
                STRING_AGG(DISTINCT faa_id,   '|') FILTER (WHERE faa_id   IS NOT NULL) AS faa_ids,
                STRING_AGG(DISTINCT nwsli_id, '|') FILTER (WHERE nwsli_id IS NOT NULL) AS nwsli_ids
            FROM read_parquet('{STATIONS_MASTER}')
            WHERE country_code = 'US'
            GROUP BY ncei_id
        )
        SELECT b.*, i.wban_ids, i.icao_ids, i.ghcnd_ids, i.coop_ids,
               i.hpd_ids, i.wmo_ids, i.faa_ids, i.nwsli_ids
        FROM base b JOIN ids i USING (ncei_id)
    """)

    r = con.execute("SELECT COUNT(*) FROM homr_collapsed").fetchone()[0]
    print(f"  homr_collapsed: {r:,} unique US stations")
    r2 = con.execute("SELECT COUNT(DISTINCT ncei_id) FROM homr_collapsed").fetchone()[0]
    if r != r2:
        print(f"  ERROR: ncei_id not unique! rows={r:,} distinct={r2:,}. Stopping.")
        sys.exit(1)
    print(f"  ncei_id uniqueness: OK ({r:,} = {r2:,})")

    print("\n" + "=" * 60)
    print("PHASE 4: Building master join")
    print("=" * 60)

    con.execute("""
        CREATE TEMP VIEW master AS
        SELECT
            h.ncei_id, h.station_name, h.state, h.county,
            h.lat_dec, h.lon_dec, h.elev_m, h.station_type, h.is_active, h.has_coordinates,
            h.wban_ids, h.icao_ids, h.ghcnd_ids, h.coop_ids,
            h.hpd_ids, h.wmo_ids, h.faa_ids, h.nwsli_ids,
            -- ASOS 1-min (per-station files, ICAO match)
            (a_icao.icao IS NOT NULL)   AS has_asos_1min,
            a_icao.file_count           AS asos_1min_files,
            a_icao.data_gb              AS asos_1min_gb,
            a_icao.earliest_date        AS asos_1min_start,
            a_icao.latest_date          AS asos_1min_end,
            -- GHCNh hourly (replaces old ASOS ISD processed_parquet; WBAN match)
            (gh.wban IS NOT NULL)       AS has_asos_isd,
            gh.file_count               AS asos_isd_files,
            gh.data_gb                  AS asos_isd_gb,
            -- HPD 1hr: not available
            FALSE                       AS has_hpd_1hr,
            0::BIGINT                   AS hpd_files,
            0.0::DOUBLE                 AS hpd_data_gb,
            -- 15-min (Hive-partitioned ASOS/15min; GHCND match)
            (ncei.ghcnd_id IS NOT NULL) AS has_ncei_15min,
            ncei.file_count             AS ncei_files,
            ncei.data_gb                AS ncei_data_gb,
            -- GHCNd daily (year-based; GHCND match)
            (gd.ghcnd_id IS NOT NULL)   AS has_ghcn_daily,
            gd.file_count               AS ghcn_daily_files,
            gd.data_gb                  AS ghcn_daily_data_gb,
            TRUE          AS homr_matched,
            NULL::VARCHAR AS usaf_id,
            NULL::VARCHAR AS source_if_orphan
        FROM homr_collapsed h
        -- 1-min: match ICAO
        LEFT JOIN asos_icao a_icao
            ON h.icao_ids IS NOT NULL
            AND (h.icao_ids = a_icao.icao
              OR h.icao_ids LIKE a_icao.icao || '|%'
              OR h.icao_ids LIKE '%|' || a_icao.icao
              OR h.icao_ids LIKE '%|' || a_icao.icao || '|%')
        -- GHCNh hourly: match WBAN (extracted from ISD station_id chars 7-11)
        LEFT JOIN ghcnh_stations gh
            ON h.wban_ids IS NOT NULL
            AND (h.wban_ids = gh.wban
              OR h.wban_ids LIKE gh.wban || '|%'
              OR h.wban_ids LIKE '%|' || gh.wban
              OR h.wban_ids LIKE '%|' || gh.wban || '|%')
        -- 15-min: match GHCND ID
        LEFT JOIN ncei_stations ncei
            ON h.ghcnd_ids IS NOT NULL
            AND (h.ghcnd_ids = ncei.ghcnd_id
              OR h.ghcnd_ids LIKE ncei.ghcnd_id || '|%'
              OR h.ghcnd_ids LIKE '%|' || ncei.ghcnd_id
              OR h.ghcnd_ids LIKE '%|' || ncei.ghcnd_id || '|%')
        -- GHCNd daily: match GHCND ID
        LEFT JOIN ghcnd_stations gd
            ON h.ghcnd_ids IS NOT NULL
            AND (h.ghcnd_ids = gd.ghcnd_id
              OR h.ghcnd_ids LIKE gd.ghcnd_id || '|%'
              OR h.ghcnd_ids LIKE '%|' || gd.ghcnd_id
              OR h.ghcnd_ids LIKE '%|' || gd.ghcnd_id || '|%')
        ORDER BY h.state NULLS LAST, h.station_name
    """)

    total_rows = con.execute("SELECT COUNT(*) FROM master").fetchone()[0]
    distinct_ncei = con.execute("SELECT COUNT(DISTINCT ncei_id) FROM master").fetchone()[0]
    print(f"  master rows: {total_rows:,} | distinct ncei_id: {distinct_ncei:,}")

    if total_rows != distinct_ncei:
        print("  WARNING: cartesian join — deduplicating...")
        con.execute("DROP VIEW master")
        con.execute("""
            CREATE TEMP VIEW master AS
            WITH ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY ncei_id ORDER BY
                        CASE WHEN has_asos_1min  THEN 0 ELSE 1 END,
                        CASE WHEN has_asos_isd   THEN 0 ELSE 1 END,
                        CASE WHEN has_ncei_15min THEN 0 ELSE 1 END
                    ) AS rn
                FROM (
                    SELECT h.ncei_id, h.station_name, h.state, h.county,
                           h.lat_dec, h.lon_dec, h.elev_m, h.station_type,
                           h.is_active, h.has_coordinates,
                           h.wban_ids, h.icao_ids, h.ghcnd_ids, h.coop_ids,
                           h.hpd_ids, h.wmo_ids, h.faa_ids, h.nwsli_ids,
                           (a_icao.icao IS NOT NULL)   AS has_asos_1min,
                           a_icao.file_count           AS asos_1min_files,
                           a_icao.data_gb              AS asos_1min_gb,
                           a_icao.earliest_date        AS asos_1min_start,
                           a_icao.latest_date          AS asos_1min_end,
                           (gh.wban IS NOT NULL)       AS has_asos_isd,
                           gh.file_count               AS asos_isd_files,
                           gh.data_gb                  AS asos_isd_gb,
                           FALSE                       AS has_hpd_1hr,
                           0::BIGINT                   AS hpd_files,
                           0.0::DOUBLE                 AS hpd_data_gb,
                           (ncei.ghcnd_id IS NOT NULL) AS has_ncei_15min,
                           ncei.file_count             AS ncei_files,
                           ncei.data_gb                AS ncei_data_gb,
                           (gd.ghcnd_id IS NOT NULL)   AS has_ghcn_daily,
                           gd.file_count               AS ghcn_daily_files,
                           gd.data_gb                  AS ghcn_daily_data_gb,
                           TRUE AS homr_matched, NULL::VARCHAR AS usaf_id,
                           NULL::VARCHAR AS source_if_orphan
                    FROM homr_collapsed h
                    LEFT JOIN asos_icao a_icao ON h.icao_ids IS NOT NULL
                        AND (h.icao_ids = a_icao.icao OR h.icao_ids LIKE a_icao.icao||'|%'
                          OR h.icao_ids LIKE '%|'||a_icao.icao OR h.icao_ids LIKE '%|'||a_icao.icao||'|%')
                    LEFT JOIN ghcnh_stations gh ON h.wban_ids IS NOT NULL
                        AND (h.wban_ids = gh.wban OR h.wban_ids LIKE gh.wban||'|%'
                          OR h.wban_ids LIKE '%|'||gh.wban OR h.wban_ids LIKE '%|'||gh.wban||'|%')
                    LEFT JOIN ncei_stations ncei ON h.ghcnd_ids IS NOT NULL
                        AND (h.ghcnd_ids = ncei.ghcnd_id OR h.ghcnd_ids LIKE ncei.ghcnd_id||'|%'
                          OR h.ghcnd_ids LIKE '%|'||ncei.ghcnd_id OR h.ghcnd_ids LIKE '%|'||ncei.ghcnd_id||'|%')
                    LEFT JOIN ghcnd_stations gd ON h.ghcnd_ids IS NOT NULL
                        AND (h.ghcnd_ids = gd.ghcnd_id OR h.ghcnd_ids LIKE gd.ghcnd_id||'|%'
                          OR h.ghcnd_ids LIKE '%|'||gd.ghcnd_id OR h.ghcnd_ids LIKE '%|'||gd.ghcnd_id||'|%')
                )
            )
            SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1
            ORDER BY state NULLS LAST, station_name
        """)
        total_rows = con.execute("SELECT COUNT(*) FROM master").fetchone()[0]
        distinct_ncei = con.execute("SELECT COUNT(DISTINCT ncei_id) FROM master").fetchone()[0]
        print(f"  After dedup: {total_rows:,} rows = {distinct_ncei:,} unique stations")

    print(f"  Row count check: OK ({total_rows:,} rows)")

    print("\n" + "=" * 60)
    print("PHASE 5: Orphan analysis + recovery")
    print("=" * 60)

    hpd_orphans = con.execute("""
        SELECT h.ghcnd_id, h.file_count FROM hpd_stations h
        WHERE NOT EXISTS (
            SELECT 1 FROM homr_collapsed hc WHERE hc.ghcnd_ids IS NOT NULL
              AND (hc.ghcnd_ids = h.ghcnd_id OR hc.ghcnd_ids LIKE h.ghcnd_id||'|%'
                OR hc.ghcnd_ids LIKE '%|'||h.ghcnd_id OR hc.ghcnd_ids LIKE '%|'||h.ghcnd_id||'|%')
        )
    """).fetchall()

    ncei_orphans = con.execute("""
        SELECT n.ghcnd_id, n.file_count FROM ncei_stations n
        WHERE NOT EXISTS (
            SELECT 1 FROM homr_collapsed hc WHERE hc.ghcnd_ids IS NOT NULL
              AND (hc.ghcnd_ids = n.ghcnd_id OR hc.ghcnd_ids LIKE n.ghcnd_id||'|%'
                OR hc.ghcnd_ids LIKE '%|'||n.ghcnd_id OR hc.ghcnd_ids LIKE '%|'||n.ghcnd_id||'|%')
        )
    """).fetchall()

    hpd_total  = con.execute("SELECT COUNT(*) FROM hpd_stations").fetchone()[0]
    ncei_total = con.execute("SELECT COUNT(*) FROM ncei_stations").fetchone()[0]
    print(f"  HPD orphans:  {len(hpd_orphans):,} / {hpd_total:,}")
    print(f"  NCEI orphans: {len(ncei_orphans):,} / {ncei_total:,}")

    orphan_rows = []
    all_orphan_ghcnd = set(o[0] for o in hpd_orphans) | set(o[0] for o in ncei_orphans)

    if ncei_orphans:
        # Recover lat/lon for NCEI 15-min orphans from the inventory parquet
        orphan_ids_sql = ", ".join(f"'{o[0]}'" for o in ncei_orphans)
        recovered = con.execute(f"""
            SELECT inv.station_id                  AS ncei_id,
                   NULL::VARCHAR                   AS station_name,
                   inv.state,
                   NULL::VARCHAR                   AS county,
                   inv.lat                         AS lat_dec,
                   inv.lon                         AS lon_dec,
                   NULL::DOUBLE                    AS elev_m,
                   NULL::VARCHAR                   AS station_type,
                   NULL::BOOLEAN                   AS is_active,
                   (inv.lat IS NOT NULL)            AS has_coordinates,
                   NULL::VARCHAR                   AS wban_ids,
                   NULL::VARCHAR                   AS icao_ids,
                   inv.station_id                  AS ghcnd_ids,
                   NULL::VARCHAR                   AS coop_ids,
                   NULL::VARCHAR                   AS hpd_ids,
                   NULL::VARCHAR                   AS wmo_ids,
                   NULL::VARCHAR                   AS faa_ids,
                   NULL::VARCHAR                   AS nwsli_ids,
                   NULL::BOOLEAN                   AS has_asos_1min,
                   NULL::BIGINT                    AS asos_1min_files,
                   NULL::DOUBLE                    AS asos_1min_gb,
                   NULL::VARCHAR                   AS asos_1min_start,
                   NULL::VARCHAR                   AS asos_1min_end,
                   NULL::BOOLEAN                   AS has_asos_isd,
                   NULL::BIGINT                    AS asos_isd_files,
                   NULL::DOUBLE                    AS asos_isd_gb,
                   FALSE                           AS has_hpd_1hr,
                   0::BIGINT                       AS hpd_files,
                   0.0::DOUBLE                     AS hpd_data_gb,
                   TRUE                            AS has_ncei_15min,
                   inv.file_count                  AS ncei_files,
                   NULL::DOUBLE                    AS ncei_data_gb,
                   FALSE                           AS has_ghcn_daily,
                   0::BIGINT                       AS ghcn_daily_files,
                   NULL::DOUBLE                    AS ghcn_daily_data_gb,
                   FALSE                           AS homr_matched,
                   NULL::VARCHAR                   AS usaf_id,
                   'NCEI_15MIN_FALLBACK'           AS source_if_orphan
            FROM read_parquet('{INV_15MIN}') inv
            WHERE inv.station_id IN ({orphan_ids_sql})
        """).fetchall()
        orphan_rows.extend(recovered)
        print(f"  Recovered {len(recovered):,} NCEI orphan stations from 15-min inventory")

    # PHASE 5b: USAF US stations
    usaf_total = 0
    n_coord_matched = 0
    if os.path.exists(ASOS_MASTER_CSV):
        print("\n  Processing USAF US stations...")
        con.execute(f"""
            CREATE TEMP TABLE usaf_us AS
            WITH asos_dedup AS (
                SELECT DISTINCT ON (station_id::VARCHAR)
                    station_id::VARCHAR AS station_id_11,
                    LEFT(station_id::VARCHAR, 6) AS usaf_code,
                    station_name,
                    REGEXP_EXTRACT(station_name, '.*, ([A-Z]{{2}}) US$', 1) AS state,
                    latitude, longitude, elevation_m
                FROM read_csv_auto('{ASOS_MASTER_CSV}')
                WHERE REGEXP_MATCHES(COALESCE(station_name::VARCHAR, ''), '.*, [A-Z]{{2}} US$')
                ORDER BY station_id::VARCHAR, year DESC
            ),
            file_counts AS (
                SELECT station_id_raw AS usaf_code,
                    COUNT(*) AS file_count,
                    ROUND(SUM(file_size_bytes)/1e9, 3) AS data_gb
                FROM inv.file_inventory
                WHERE id_format = 'USAF_WBAN' AND SUBSTRING(filename, 7, 5) = '99999'
                GROUP BY station_id_raw
            )
            SELECT a.station_id_11, a.usaf_code, a.station_name, a.state,
                   a.latitude, a.longitude, a.elevation_m,
                   f.file_count, f.data_gb
            FROM asos_dedup a JOIN file_counts f ON a.usaf_code = f.usaf_code
        """)
        usaf_total = con.execute("SELECT COUNT(*) FROM usaf_us").fetchone()[0]

        con.execute(f"""
            CREATE TEMP TABLE usaf_coord_matches AS
            SELECT u.usaf_code, u.station_id_11, h.ncei_id AS matched_ncei_id,
                   ABS(h.lat_dec - u.latitude) + ABS(h.lon_dec - u.longitude) AS coord_dist
            FROM usaf_us u
            JOIN LATERAL (
                SELECT ncei_id, lat_dec, lon_dec FROM homr_collapsed
                WHERE lat_dec IS NOT NULL AND lon_dec IS NOT NULL
                  AND ABS(lat_dec - u.latitude)  <= {COORD_TOL}
                  AND ABS(lon_dec - u.longitude) <= {COORD_TOL}
                ORDER BY ABS(lat_dec - u.latitude) + ABS(lon_dec - u.longitude)
                LIMIT 1
            ) h ON TRUE
        """)
        n_coord_matched = con.execute("SELECT COUNT(*) FROM usaf_coord_matches").fetchone()[0]
        n_usaf_orphans  = usaf_total - n_coord_matched
        print(f"  USAF matched via coords: {n_coord_matched:,} | orphans: {n_usaf_orphans:,}")

        usaf_orphan_rows = con.execute(f"""
            SELECT u.station_id_11 AS ncei_id, u.station_name, u.state,
                   NULL::VARCHAR AS county, u.latitude AS lat_dec, u.longitude AS lon_dec,
                   u.elevation_m AS elev_m, NULL::VARCHAR AS station_type,
                   NULL::BOOLEAN AS is_active,
                   (u.latitude IS NOT NULL AND u.longitude IS NOT NULL) AS has_coordinates,
                   NULL::VARCHAR AS wban_ids, NULL::VARCHAR AS icao_ids,
                   NULL::VARCHAR AS ghcnd_ids, NULL::VARCHAR AS coop_ids,
                   NULL::VARCHAR AS hpd_ids, NULL::VARCHAR AS wmo_ids,
                   NULL::VARCHAR AS faa_ids, NULL::VARCHAR AS nwsli_ids,
                   NULL::BOOLEAN AS has_asos_1min, NULL::BIGINT AS asos_1min_files,
                   NULL::DOUBLE AS asos_1min_gb, NULL::VARCHAR AS asos_1min_start,
                   NULL::VARCHAR AS asos_1min_end,
                   TRUE AS has_asos_isd, u.file_count AS asos_isd_files,
                   u.data_gb AS asos_isd_gb,
                   NULL::BOOLEAN AS has_hpd_1hr, NULL::BIGINT AS hpd_files,
                   NULL::DOUBLE AS hpd_data_gb,
                   NULL::BOOLEAN AS has_ncei_15min, NULL::BIGINT AS ncei_files,
                   NULL::DOUBLE AS ncei_data_gb,
                   FALSE AS homr_matched, u.usaf_code AS usaf_id, 'ASOS_USAF' AS source_if_orphan
            FROM usaf_us u
            WHERE u.usaf_code NOT IN (SELECT usaf_code FROM usaf_coord_matches)
        """).fetchall()
        orphan_rows.extend(usaf_orphan_rows)
        print(f"  USAF orphan rows added: {len(usaf_orphan_rows):,}")
    else:
        # No ASOS CSV — create empty usaf_coord_matches for the base_select below
        con.execute("CREATE TEMP TABLE usaf_coord_matches (usaf_code VARCHAR, station_id_11 VARCHAR, matched_ncei_id VARCHAR, coord_dist DOUBLE)")

    print("\n" + "=" * 60)
    print("PHASE 6: Writing output")
    print("=" * 60)

    master_cols = [d[0] for d in con.execute("SELECT * FROM master LIMIT 0").description]
    col_list = ", ".join(master_cols)
    placeholders = ", ".join(["?" for _ in master_cols])

    master_cols_with_usaf = ", ".join(
        f"COALESCE(ucm.usaf_code, m.usaf_id) AS usaf_id" if c == "usaf_id" else f"m.{c}"
        for c in master_cols
    )
    base_select = f"""
        SELECT {master_cols_with_usaf}
        FROM master m
        LEFT JOIN usaf_coord_matches ucm ON m.ncei_id = ucm.matched_ncei_id
    """

    if orphan_rows:
        con.execute("CREATE TEMP TABLE orphan_tbl AS SELECT * FROM master WHERE 1=0")
        con.executemany(f"INSERT INTO orphan_tbl VALUES ({placeholders})", orphan_rows)
        final_sql = f"""
            {base_select}
            UNION ALL
            SELECT {col_list} FROM orphan_tbl
            ORDER BY homr_matched DESC, state NULLS LAST, station_name
        """
    else:
        final_sql = f"{base_select} ORDER BY homr_matched DESC, state NULLS LAST, station_name"

    con.execute(f"COPY ({final_sql}) TO '{OUT_PARQUET}' (FORMAT PARQUET)")
    con.execute(f"COPY ({final_sql}) TO '{OUT_CSV}' (HEADER, DELIMITER ',')")

    pq_mb = os.path.getsize(OUT_PARQUET) / 1e6
    csv_mb = os.path.getsize(OUT_CSV) / 1e6
    print(f"  Written: {OUT_PARQUET} ({pq_mb:.1f} MB)")
    print(f"  Written: {OUT_CSV} ({csv_mb:.1f} MB)")

    out = con.execute(f"""
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE homr_matched),
               COUNT(*) FILTER (WHERE NOT homr_matched),
               COUNT(*) FILTER (WHERE has_hpd_1hr),
               COUNT(*) FILTER (WHERE has_ncei_15min),
               COUNT(*) FILTER (WHERE lat_dec IS NOT NULL)
        FROM read_parquet('{OUT_PARQUET}')
    """).fetchone()
    print(f"\n  Total:         {out[0]:,}")
    print(f"  HOMR-matched:  {out[1]:,}")
    print(f"  Orphans:       {out[2]:,}")
    print(f"  Has HPD:       {out[3]:,}")
    print(f"  Has NCEI:      {out[4]:,}")
    print(f"  With coords:   {out[5]:,}")

    elapsed = time.time() - t0
    print(f"\n  Total elapsed: {elapsed:.1f}s")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    os.makedirs(RECON, exist_ok=True)
    t0 = time.time()

    con = duckdb.connect()

    if os.path.exists(STATIONS_MASTER):
        print(f"HOMR data found: {STATIONS_MASTER}")
        build_full(con)
    else:
        print(f"HOMR data not found ({STATIONS_MASTER})")
        print("Using fallback mode (HPD/NCEI parquet metadata).")
        build_fallback(con)

    con.close()
    print(f"\nDone in {time.time() - t0:.1f}s")
    print(f"Output: {OUT_PARQUET}")


if __name__ == "__main__":
    main()
