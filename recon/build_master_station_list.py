"""
build_master_station_list.py
Build a master station crosswalk from HOMR backbone + file inventory data flags.

Key schema facts (from Phase 1 recon):
  stations_master:  temporal (multiple rows per ncei_id), already has all ID cols
                    country_code='US', icao_id, wban_id, ghcnd_id, coop_id, hpd_id, wmo_id
  stations_crosswalk: long-format, id_types = GHCND,COOP,NWSLI,HPD,WBAN,FAA,ICAO,WMO
  USAF_WBAN format:  filename stem = USAF(6)+WBAN(5); WBAN=99999 means no real WBAN
  ASOS_A format:     processed_parquet/ internal IDs — not matchable to HOMR
  USAF US stations:  2,753 US stations in ASOS/master_station_inventory.csv with WBAN=99999;
                     matched to HOMR via coordinate proximity (<=0.01 deg), else orphan.
"""

import os
import sys
import time

import duckdb

BASE = os.path.expanduser("~/Documents/WeatherData")
RECON = os.path.join(BASE, "recon")
INV_DB = os.path.join(RECON, "file_inventory.duckdb")
OUT_PARQUET = os.path.join(RECON, "master_station_list.parquet")
OUT_CSV = os.path.join(RECON, "master_station_list.csv")

STATIONS_MASTER = os.path.join(BASE, "HOMR/processed/stations_master.parquet")
HPD_STATIONINV  = os.path.join(BASE, "HPD_1hr/raw/HPD_1hr/HPD_v02r02_stationinv_c20260309.csv")
NCEI_STATIONINV = os.path.join(BASE, "NCEI_15min_raw/NCEI_15min_metadata/hpd15_station_inventory.csv")
ASOS_MASTER_CSV = os.path.join(BASE, "ASOS/master_station_inventory.csv")

COORD_TOL = 0.01  # degrees; ~1.1 km at equator


def main():
    t0 = time.time()
    con = duckdb.connect()

    # Attach file inventory as read-only
    con.execute(f"ATTACH '{INV_DB}' AS inv (READ_ONLY)")

    # -------------------------------------------------------------------------
    # Phase 1 recap (print key facts)
    # -------------------------------------------------------------------------
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

    # -------------------------------------------------------------------------
    # Phase 2: Distinct stations from file inventory
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("PHASE 2: Distinct stations from file inventory")
    print("=" * 60)

    # ASOS ICAO_ASOS stations
    # station_id_raw is 5 chars e.g. '0KABE' (last WBAN digit + 4-char ICAO).
    # HOMR stores 4-char ICAO only, so strip the leading digit.
    con.execute("""
        CREATE TEMP VIEW asos_icao AS
        SELECT SUBSTRING(station_id_raw, 2, 4) AS icao,
               COUNT(*) AS file_count,
               ROUND(SUM(file_size_bytes)/1e9, 3) AS data_gb,
               MIN(date_part) AS earliest_date,
               MAX(date_part) AS latest_date
        FROM inv.file_inventory
        WHERE source = 'ASOS' AND id_format = 'ICAO_ASOS'
        GROUP BY SUBSTRING(station_id_raw, 2, 4)
    """)

    # ASOS USAF_WBAN: extract WBAN from filename (chars 7-11 of 11-char stem before .parquet)
    # Only keep real WBANs (not 99999), pad to 5 chars
    con.execute("""
        CREATE TEMP VIEW asos_wban AS
        SELECT
            SUBSTRING(filename, 7, 5) AS wban,
            station_id_raw AS usaf,
            COUNT(*) AS file_count,
            ROUND(SUM(file_size_bytes)/1e9, 3) AS data_gb,
            MIN(date_part) AS earliest_date,
            MAX(date_part) AS latest_date
        FROM inv.file_inventory
        WHERE source = 'ASOS' AND id_format = 'USAF_WBAN'
          AND SUBSTRING(filename, 7, 5) != '99999'
        GROUP BY SUBSTRING(filename, 7, 5), station_id_raw
    """)

    # ASOS USAF_WBAN orphans (WBAN=99999, can't join to HOMR)
    con.execute("""
        CREATE TEMP VIEW asos_usaf_only AS
        SELECT station_id_raw AS usaf,
               COUNT(*) AS file_count,
               ROUND(SUM(file_size_bytes)/1e9, 3) AS data_gb
        FROM inv.file_inventory
        WHERE source = 'ASOS' AND id_format = 'USAF_WBAN'
          AND SUBSTRING(filename, 7, 5) = '99999'
        GROUP BY station_id_raw
    """)

    # HPD stations
    con.execute("""
        CREATE TEMP VIEW hpd_stations AS
        SELECT station_id_raw AS ghcnd_id,
               COUNT(*) AS file_count,
               ROUND(SUM(file_size_bytes)/1e9, 3) AS data_gb
        FROM inv.file_inventory
        WHERE source = 'HPD_1hr' AND id_format = 'GHCND_HPD'
        GROUP BY station_id_raw
    """)

    # NCEI 15-min stations
    con.execute("""
        CREATE TEMP VIEW ncei_stations AS
        SELECT station_id_raw AS ghcnd_id,
               COUNT(*) AS file_count,
               ROUND(SUM(file_size_bytes)/1e9, 3) AS data_gb
        FROM inv.file_inventory
        WHERE source = 'NCEI_15min_raw' AND id_format = 'GHCND'
        GROUP BY station_id_raw
    """)

    r = con.execute("SELECT COUNT(*) FROM asos_icao").fetchone()[0]
    r2 = con.execute("SELECT COUNT(*) FROM asos_wban").fetchone()[0]
    r3 = con.execute("SELECT COUNT(*) FROM asos_usaf_only").fetchone()[0]
    r4 = con.execute("SELECT COUNT(*) FROM hpd_stations").fetchone()[0]
    r5 = con.execute("SELECT COUNT(*) FROM ncei_stations").fetchone()[0]
    print(f"  ASOS unique stations: ICAO={r:,} | USAF+realWBAN={r2:,} | USAF-only(no WBAN)={r3:,}")
    print(f"  HPD unique stations:  {r4:,}")
    print(f"  NCEI unique stations: {r5:,}")

    # -------------------------------------------------------------------------
    # Phase 3: Collapse stations_master to one row per ncei_id (US only)
    # Uses STRING_AGG for IDs that vary across time periods.
    # Coordinates: take the row with the most recent non-null values.
    # -------------------------------------------------------------------------
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
            -- Most current record per ncei_id for name/state/coords
            SELECT ncei_id, station_name, state, county, country_code,
                   lat_dec, lon_dec, elev_m, station_type, is_active, has_coordinates
            FROM ranked WHERE rn = 1
        ),
        ids AS (
            -- Aggregate all IDs across time periods
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

    # Sanity: check for ncei_id duplication
    r2 = con.execute("SELECT COUNT(DISTINCT ncei_id) FROM homr_collapsed").fetchone()[0]
    if r != r2:
        print(f"  ERROR: ncei_id not unique! rows={r:,} distinct={r2:,}. Stopping.")
        sys.exit(1)
    print(f"  ncei_id uniqueness: OK ({r:,} = {r2:,})")

    # -------------------------------------------------------------------------
    # Phase 4: Join everything
    # ICAO join: match each icao in icao_ids (may be pipe-separated)
    # WBAN join: match wban in wban_ids
    # GHCND join: match ghcnd in ghcnd_ids
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("PHASE 4: Building master join")
    print("=" * 60)

    con.execute("""
        CREATE TEMP VIEW master AS
        SELECT
            h.ncei_id,
            h.station_name,
            h.state,
            h.county,
            h.lat_dec,
            h.lon_dec,
            h.elev_m,
            h.station_type,
            h.is_active,
            h.has_coordinates,
            -- All cross-reference IDs
            h.wban_ids,
            h.icao_ids,
            h.ghcnd_ids,
            h.coop_ids,
            h.hpd_ids,
            h.wmo_ids,
            h.faa_ids,
            h.nwsli_ids,
            -- ASOS 1-min (ICAO match)
            (a_icao.icao IS NOT NULL) AS has_asos_1min,
            a_icao.file_count        AS asos_1min_files,
            a_icao.data_gb           AS asos_1min_gb,
            a_icao.earliest_date     AS asos_1min_start,
            a_icao.latest_date       AS asos_1min_end,
            -- ASOS ISD (WBAN match)
            (a_wban.wban IS NOT NULL) AS has_asos_isd,
            a_wban.file_count         AS asos_isd_files,
            a_wban.data_gb            AS asos_isd_gb,
            -- HPD 1-hr
            (hpd.ghcnd_id IS NOT NULL) AS has_hpd_1hr,
            hpd.file_count             AS hpd_files,
            hpd.data_gb                AS hpd_data_gb,
            -- NCEI 15-min
            (ncei.ghcnd_id IS NOT NULL) AS has_ncei_15min,
            ncei.file_count             AS ncei_files,
            ncei.data_gb                AS ncei_data_gb,
            -- Summary flag
            TRUE AS homr_matched,
            -- Populated later via coord match or left as NULL
            NULL::VARCHAR AS usaf_id,
            NULL::VARCHAR AS source_if_orphan

        FROM homr_collapsed h

        -- ASOS via ICAO: join on any ICAO in the pipe-separated list
        LEFT JOIN asos_icao a_icao
            ON h.icao_ids IS NOT NULL
            AND (
                h.icao_ids = a_icao.icao
                OR h.icao_ids LIKE a_icao.icao || '|%'
                OR h.icao_ids LIKE '%|' || a_icao.icao
                OR h.icao_ids LIKE '%|' || a_icao.icao || '|%'
            )

        -- ASOS via WBAN: join on any WBAN in the pipe-separated list
        LEFT JOIN asos_wban a_wban
            ON h.wban_ids IS NOT NULL
            AND (
                h.wban_ids = a_wban.wban
                OR h.wban_ids LIKE a_wban.wban || '|%'
                OR h.wban_ids LIKE '%|' || a_wban.wban
                OR h.wban_ids LIKE '%|' || a_wban.wban || '|%'
            )

        -- HPD via GHCND
        LEFT JOIN hpd_stations hpd
            ON h.ghcnd_ids IS NOT NULL
            AND (
                h.ghcnd_ids = hpd.ghcnd_id
                OR h.ghcnd_ids LIKE hpd.ghcnd_id || '|%'
                OR h.ghcnd_ids LIKE '%|' || hpd.ghcnd_id
                OR h.ghcnd_ids LIKE '%|' || hpd.ghcnd_id || '|%'
            )

        -- NCEI via GHCND
        LEFT JOIN ncei_stations ncei
            ON h.ghcnd_ids IS NOT NULL
            AND (
                h.ghcnd_ids = ncei.ghcnd_id
                OR h.ghcnd_ids LIKE ncei.ghcnd_id || '|%'
                OR h.ghcnd_ids LIKE '%|' || ncei.ghcnd_id
                OR h.ghcnd_ids LIKE '%|' || ncei.ghcnd_id || '|%'
            )

        ORDER BY h.state NULLS LAST, h.station_name
    """)

    # Verify no cartesian explosion before writing
    total_rows = con.execute("SELECT COUNT(*) FROM master").fetchone()[0]
    distinct_ncei = con.execute("SELECT COUNT(DISTINCT ncei_id) FROM master").fetchone()[0]
    print(f"  master rows: {total_rows:,} | distinct ncei_id: {distinct_ncei:,}")
    if total_rows != distinct_ncei:
        print("  WARNING: cartesian join detected — debugging...")
        # Find culprits
        dups = con.execute("""
            SELECT ncei_id, COUNT(*) as n FROM master GROUP BY ncei_id HAVING n > 1
            ORDER BY n DESC LIMIT 10
        """).fetchall()
        print("  Top duplicate ncei_ids:")
        for d in dups:
            info = con.execute(f"SELECT icao_ids, wban_ids, ghcnd_ids FROM master WHERE ncei_id='{d[0]}' LIMIT 1").fetchone()
            print(f"    ncei_id={d[0]}  n={d[1]}  icao={info[0]}  wban={info[1]}  ghcnd={info[2]}")
        print("  Fixing: using DISTINCT or deduplication...")

        # Rebuild master with DISTINCT ncei_id (take first match per station)
        con.execute("DROP VIEW master")
        con.execute("""
            CREATE TEMP VIEW master AS
            WITH ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY ncei_id ORDER BY
                        CASE WHEN has_asos_1min THEN 0 ELSE 1 END,
                        CASE WHEN has_hpd_1hr   THEN 0 ELSE 1 END,
                        CASE WHEN has_ncei_15min THEN 0 ELSE 1 END
                    ) AS rn
                FROM (
                    SELECT
                        h.ncei_id, h.station_name, h.state, h.county,
                        h.lat_dec, h.lon_dec, h.elev_m, h.station_type,
                        h.is_active, h.has_coordinates,
                        h.wban_ids, h.icao_ids, h.ghcnd_ids, h.coop_ids,
                        h.hpd_ids, h.wmo_ids, h.faa_ids, h.nwsli_ids,
                        (a_icao.icao IS NOT NULL) AS has_asos_1min,
                        a_icao.file_count        AS asos_1min_files,
                        a_icao.data_gb           AS asos_1min_gb,
                        a_icao.earliest_date     AS asos_1min_start,
                        a_icao.latest_date       AS asos_1min_end,
                        (a_wban.wban IS NOT NULL) AS has_asos_isd,
                        a_wban.file_count         AS asos_isd_files,
                        a_wban.data_gb            AS asos_isd_gb,
                        (hpd.ghcnd_id IS NOT NULL) AS has_hpd_1hr,
                        hpd.file_count             AS hpd_files,
                        hpd.data_gb                AS hpd_data_gb,
                        (ncei.ghcnd_id IS NOT NULL) AS has_ncei_15min,
                        ncei.file_count             AS ncei_files,
                        ncei.data_gb                AS ncei_data_gb,
                        TRUE AS homr_matched,
                        NULL::VARCHAR AS usaf_id,
                        NULL::VARCHAR AS source_if_orphan
                    FROM homr_collapsed h
                    LEFT JOIN asos_icao a_icao
                        ON h.icao_ids IS NOT NULL
                        AND (h.icao_ids = a_icao.icao
                          OR h.icao_ids LIKE a_icao.icao || '|%'
                          OR h.icao_ids LIKE '%|' || a_icao.icao
                          OR h.icao_ids LIKE '%|' || a_icao.icao || '|%')
                    LEFT JOIN asos_wban a_wban
                        ON h.wban_ids IS NOT NULL
                        AND (h.wban_ids = a_wban.wban
                          OR h.wban_ids LIKE a_wban.wban || '|%'
                          OR h.wban_ids LIKE '%|' || a_wban.wban
                          OR h.wban_ids LIKE '%|' || a_wban.wban || '|%')
                    LEFT JOIN hpd_stations hpd
                        ON h.ghcnd_ids IS NOT NULL
                        AND (h.ghcnd_ids = hpd.ghcnd_id
                          OR h.ghcnd_ids LIKE hpd.ghcnd_id || '|%'
                          OR h.ghcnd_ids LIKE '%|' || hpd.ghcnd_id
                          OR h.ghcnd_ids LIKE '%|' || hpd.ghcnd_id || '|%')
                    LEFT JOIN ncei_stations ncei
                        ON h.ghcnd_ids IS NOT NULL
                        AND (h.ghcnd_ids = ncei.ghcnd_id
                          OR h.ghcnd_ids LIKE ncei.ghcnd_id || '|%'
                          OR h.ghcnd_ids LIKE '%|' || ncei.ghcnd_id
                          OR h.ghcnd_ids LIKE '%|' || ncei.ghcnd_id || '|%')
                )
            )
            SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1
            ORDER BY state NULLS LAST, station_name
        """)
        total_rows = con.execute("SELECT COUNT(*) FROM master").fetchone()[0]
        distinct_ncei = con.execute("SELECT COUNT(DISTINCT ncei_id) FROM master").fetchone()[0]
        print(f"  After dedup: master rows={total_rows:,} distinct ncei_id={distinct_ncei:,}")
        assert total_rows == distinct_ncei, "Still duplicate ncei_ids after dedup!"

    print(f"  Row count check: OK ({total_rows:,} rows = {distinct_ncei:,} unique stations)")

    # -------------------------------------------------------------------------
    # Phase 5: Orphan analysis
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("PHASE 5: Orphan analysis")
    print("=" * 60)

    # ASOS ICAO orphans (not matched by any HOMR station)
    asos_icao_orphans = con.execute("""
        SELECT a.icao, a.file_count, a.data_gb
        FROM asos_icao a
        WHERE NOT EXISTS (
            SELECT 1 FROM homr_collapsed h
            WHERE h.icao_ids IS NOT NULL
              AND (h.icao_ids = a.icao
                OR h.icao_ids LIKE a.icao || '|%'
                OR h.icao_ids LIKE '%|' || a.icao
                OR h.icao_ids LIKE '%|' || a.icao || '|%')
        )
        ORDER BY a.file_count DESC
    """).fetchall()

    # ASOS WBAN orphans
    asos_wban_orphans = con.execute("""
        SELECT a.wban, a.usaf, a.file_count
        FROM asos_wban a
        WHERE NOT EXISTS (
            SELECT 1 FROM homr_collapsed h
            WHERE h.wban_ids IS NOT NULL
              AND (h.wban_ids = a.wban
                OR h.wban_ids LIKE a.wban || '|%'
                OR h.wban_ids LIKE '%|' || a.wban
                OR h.wban_ids LIKE '%|' || a.wban || '|%')
        )
        ORDER BY a.file_count DESC LIMIT 20
    """).fetchall()

    # HPD orphans
    hpd_orphans = con.execute("""
        SELECT h.ghcnd_id, h.file_count
        FROM hpd_stations h
        WHERE NOT EXISTS (
            SELECT 1 FROM homr_collapsed hc
            WHERE hc.ghcnd_ids IS NOT NULL
              AND (hc.ghcnd_ids = h.ghcnd_id
                OR hc.ghcnd_ids LIKE h.ghcnd_id || '|%'
                OR hc.ghcnd_ids LIKE '%|' || h.ghcnd_id
                OR hc.ghcnd_ids LIKE '%|' || h.ghcnd_id || '|%')
        )
    """).fetchall()

    # NCEI orphans
    ncei_orphans = con.execute("""
        SELECT n.ghcnd_id, n.file_count
        FROM ncei_stations n
        WHERE NOT EXISTS (
            SELECT 1 FROM homr_collapsed hc
            WHERE hc.ghcnd_ids IS NOT NULL
              AND (hc.ghcnd_ids = n.ghcnd_id
                OR hc.ghcnd_ids LIKE n.ghcnd_id || '|%'
                OR hc.ghcnd_ids LIKE '%|' || n.ghcnd_id
                OR hc.ghcnd_ids LIKE '%|' || n.ghcnd_id || '|%')
        )
    """).fetchall()

    asos_icao_total = con.execute("SELECT COUNT(*) FROM asos_icao").fetchone()[0]
    asos_wban_total = con.execute("SELECT COUNT(*) FROM asos_wban").fetchone()[0]
    hpd_total = con.execute("SELECT COUNT(*) FROM hpd_stations").fetchone()[0]
    ncei_total = con.execute("SELECT COUNT(*) FROM ncei_stations").fetchone()[0]
    usaf_only_total = con.execute("SELECT COUNT(*) FROM asos_usaf_only").fetchone()[0]

    print(f"  ASOS ICAO orphans: {len(asos_icao_orphans):,} / {asos_icao_total:,} "
          f"({100*len(asos_icao_orphans)/max(asos_icao_total,1):.1f}%)")
    if asos_icao_orphans[:5]:
        for o in asos_icao_orphans[:5]:
            print(f"    icao={o[0]}  files={o[1]:,}  gb={o[2]:.3f}")
    print(f"  ASOS WBAN orphans:  {len(asos_wban_orphans):,} / {asos_wban_total:,} (sample only)")
    print(f"  ASOS USAF-only (WBAN=99999, unmatched): {usaf_only_total:,} stations (international/no WBAN)")
    print(f"  HPD orphans:  {len(hpd_orphans):,} / {hpd_total:,} "
          f"({100*len(hpd_orphans)/max(hpd_total,1):.1f}%)")
    print(f"  NCEI orphans: {len(ncei_orphans):,} / {ncei_total:,} "
          f"({100*len(ncei_orphans)/max(ncei_total,1):.1f}%)")

    # Recover HPD/NCEI orphans from stationinv CSV
    orphan_rows = []
    if hpd_orphans or ncei_orphans:
        print("\n  Recovering HPD/NCEI orphans from station inventory CSVs...")

        all_orphan_ghcnd = set(o[0] for o in hpd_orphans) | set(o[0] for o in ncei_orphans)

        recovered = con.execute(f"""
            SELECT DISTINCT
                si.StnID AS ncei_id,
                si.Name  AS station_name,
                si."State/Province" AS state,
                NULL     AS county,
                si.Lat   AS lat_dec,
                si.Lon   AS lon_dec,
                si.Elev  AS elev_m,
                NULL     AS station_type,
                NULL     AS is_active,
                (si.Lat IS NOT NULL AND si.Lon IS NOT NULL) AS has_coordinates,
                NULL AS wban_ids,
                NULL AS icao_ids,
                si.StnID AS ghcnd_ids,
                NULL AS coop_ids,
                NULL AS hpd_ids,
                si.WMO_ID::VARCHAR AS wmo_ids,
                NULL AS faa_ids,
                NULL AS nwsli_ids,
                -- data flags
                NULL AS has_asos_1min, NULL AS asos_1min_files, NULL AS asos_1min_gb,
                NULL AS asos_1min_start, NULL AS asos_1min_end,
                NULL AS has_asos_isd, NULL AS asos_isd_files, NULL AS asos_isd_gb,
                (hpd.ghcnd_id IS NOT NULL) AS has_hpd_1hr,
                hpd.file_count AS hpd_files, hpd.data_gb AS hpd_data_gb,
                (nc.ghcnd_id IS NOT NULL) AS has_ncei_15min,
                nc.file_count AS ncei_files, nc.data_gb AS ncei_data_gb,
                FALSE AS homr_matched,
                NULL::VARCHAR AS usaf_id,
                'HPD_STATIONINV' AS source_if_orphan
            FROM read_csv_auto('{HPD_STATIONINV}') si
            LEFT JOIN hpd_stations  hpd ON si.StnID = hpd.ghcnd_id
            LEFT JOIN ncei_stations nc  ON si.StnID = nc.ghcnd_id
            WHERE si.StnID IN ({','.join(["'" + g + "'" for g in all_orphan_ghcnd])})
              AND (hpd.ghcnd_id IS NOT NULL OR nc.ghcnd_id IS NOT NULL)
        """).fetchall()

        orphan_rows = list(recovered)
        print(f"  Recovered {len(orphan_rows):,} HPD/NCEI orphan stations")

    # -------------------------------------------------------------------------
    # Phase 5b: USAF US stations (WBAN=99999, sourced from ASOS master CSV)
    # Coordinate proximity match against HOMR (within COORD_TOL degrees).
    # Matched → contribute usaf_id to the existing HOMR row (via coord_matches table).
    # Unmatched → new orphan rows with homr_matched=False, source_if_orphan='ASOS_USAF'.
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("PHASE 5b: USAF US stations — coord proximity match")
    print("=" * 60)

    # Build deduplicated US USAF station table from ASOS master CSV
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
        FROM asos_dedup a
        JOIN file_counts f ON a.usaf_code = f.usaf_code
    """)

    usaf_us_total = con.execute("SELECT COUNT(*) FROM usaf_us").fetchone()[0]
    print(f"  US USAF stations to process: {usaf_us_total:,}")

    # Coordinate proximity match: for each USAF station find the nearest HOMR
    # station within COORD_TOL degrees. LATERAL gives us at most one match per USAF.
    con.execute(f"""
        CREATE TEMP TABLE usaf_coord_matches AS
        SELECT
            u.usaf_code,
            u.station_id_11,
            h.ncei_id   AS matched_ncei_id,
            ABS(h.lat_dec - u.latitude) + ABS(h.lon_dec - u.longitude) AS coord_dist
        FROM usaf_us u
        JOIN LATERAL (
            SELECT ncei_id, lat_dec, lon_dec
            FROM homr_collapsed
            WHERE lat_dec IS NOT NULL AND lon_dec IS NOT NULL
              AND ABS(lat_dec - u.latitude)  <= {COORD_TOL}
              AND ABS(lon_dec - u.longitude) <= {COORD_TOL}
            ORDER BY ABS(lat_dec - u.latitude) + ABS(lon_dec - u.longitude)
            LIMIT 1
        ) h ON TRUE
    """)

    n_coord_matched = con.execute("SELECT COUNT(*) FROM usaf_coord_matches").fetchone()[0]
    n_usaf_orphans  = usaf_us_total - n_coord_matched
    print(f"  Matched via coordinates (≤{COORD_TOL}°): {n_coord_matched:,}")
    print(f"  Remaining USAF orphans:                  {n_usaf_orphans:,}")

    if n_coord_matched:
        sample = con.execute("""
            SELECT u.station_name, u.usaf_code, m.matched_ncei_id, m.coord_dist
            FROM usaf_coord_matches m
            JOIN usaf_us u ON m.usaf_code = u.usaf_code
            ORDER BY m.coord_dist LIMIT 5
        """).fetchall()
        print("  Sample coord matches (closest first):")
        for s in sample:
            print(f"    {s[1]}  {s[0]:<35} → ncei_id={s[2]}  dist={s[3]:.5f}°")

    # Build USAF orphan rows (no coord match) — added as new stations
    usaf_orphan_rows = con.execute(f"""
        SELECT
            u.station_id_11 AS ncei_id,   -- use 11-digit ID as synthetic key
            u.station_name,
            u.state,
            NULL::VARCHAR   AS county,
            u.latitude      AS lat_dec,
            u.longitude     AS lon_dec,
            u.elevation_m   AS elev_m,
            NULL::VARCHAR   AS station_type,
            NULL::BOOLEAN   AS is_active,
            (u.latitude IS NOT NULL AND u.longitude IS NOT NULL) AS has_coordinates,
            NULL::VARCHAR AS wban_ids,
            NULL::VARCHAR AS icao_ids,
            NULL::VARCHAR AS ghcnd_ids,
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
            TRUE          AS has_asos_isd,   -- they DO have processed_parquet files
            u.file_count  AS asos_isd_files,
            u.data_gb     AS asos_isd_gb,
            NULL::BOOLEAN AS has_hpd_1hr,
            NULL::BIGINT  AS hpd_files,
            NULL::DOUBLE  AS hpd_data_gb,
            NULL::BOOLEAN AS has_ncei_15min,
            NULL::BIGINT  AS ncei_files,
            NULL::DOUBLE  AS ncei_data_gb,
            FALSE         AS homr_matched,
            u.usaf_code   AS usaf_id,
            'ASOS_USAF'   AS source_if_orphan
        FROM usaf_us u
        WHERE u.usaf_code NOT IN (SELECT usaf_code FROM usaf_coord_matches)
        ORDER BY u.state, u.station_name
    """).fetchall()
    orphan_rows.extend(usaf_orphan_rows)
    print(f"  USAF orphan rows added to output: {len(usaf_orphan_rows):,}")

    # -------------------------------------------------------------------------
    # Phase 6: Write output
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("PHASE 6: Writing output")
    print("=" * 60)

    # Get column list from master (includes usaf_id, source_if_orphan)
    master_cols = [d[0] for d in con.execute("SELECT * FROM master LIMIT 0").description]
    col_list = ", ".join(master_cols)
    placeholders = ", ".join(["?" for _ in master_cols])

    # Base SELECT: master rows, with usaf_id filled in where coord-matched
    # COALESCE(ucm.usaf_code, master.usaf_id) so coord-matched HOMR rows get the USAF code
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

    parquet_mb = os.path.getsize(OUT_PARQUET) / 1e6
    csv_mb = os.path.getsize(OUT_CSV) / 1e6
    print(f"  Written: {OUT_PARQUET} ({parquet_mb:.1f} MB)")
    print(f"  Written: {OUT_CSV} ({csv_mb:.1f} MB)")

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    final = con.execute(f"SELECT * FROM read_parquet('{OUT_PARQUET}')").fetchdf() \
        if False else None  # avoid loading 90K row df; use SQL

    out = con.execute(f"""
        SELECT
            COUNT(*)                                                       AS total,
            COUNT(*) FILTER (WHERE homr_matched)                           AS homr_matched,
            COUNT(*) FILTER (WHERE NOT homr_matched)                       AS orphans,
            COUNT(*) FILTER (WHERE source_if_orphan = 'ASOS_USAF')        AS usaf_orphans,
            COUNT(*) FILTER (WHERE source_if_orphan = 'HPD_STATIONINV')   AS hpd_orphans,
            COUNT(*) FILTER (WHERE usaf_id IS NOT NULL AND homr_matched)   AS usaf_coord_linked,
            COUNT(*) FILTER (WHERE has_asos_1min)                          AS has_asos_1min,
            COUNT(*) FILTER (WHERE has_asos_isd)                           AS has_asos_isd,
            COUNT(*) FILTER (WHERE has_hpd_1hr)                            AS has_hpd,
            COUNT(*) FILTER (WHERE has_ncei_15min)                         AS has_ncei,
            COUNT(*) FILTER (WHERE
                (CAST(COALESCE(has_asos_1min,false) AS INT) +
                 CAST(COALESCE(has_asos_isd,false)  AS INT) +
                 CAST(COALESCE(has_hpd_1hr,false)   AS INT) +
                 CAST(COALESCE(has_ncei_15min,false) AS INT)) >= 2)        AS has_2plus,
            COUNT(*) FILTER (WHERE
                (CAST(COALESCE(has_asos_1min,false) AS INT) +
                 CAST(COALESCE(has_asos_isd,false)  AS INT) +
                 CAST(COALESCE(has_hpd_1hr,false)   AS INT) +
                 CAST(COALESCE(has_ncei_15min,false) AS INT)) >= 3)        AS has_3plus
        FROM read_parquet('{OUT_PARQUET}')
    """).fetchone()
    (total_stations, homr_matched_n, orphans_n, usaf_orphans_n, hpd_orphans_n,
     usaf_coord_linked_n, has_asos_1min, has_asos_isd, has_hpd, has_ncei,
     has_2plus, has_3plus) = out

    top_states = con.execute(f"""
        SELECT state, COUNT(*) AS cnt
        FROM read_parquet('{OUT_PARQUET}')
        WHERE state IS NOT NULL
        GROUP BY state ORDER BY cnt DESC LIMIT 10
    """).fetchall()

    elapsed = time.time() - t0

    print()
    print("=" * 60)
    print("=== MASTER STATION LIST ===")
    print("=" * 60)
    print(f"  Total US stations:                   {total_stations:,}")
    print(f"  Matched via HOMR (backbone):         {homr_matched_n:,}")
    print(f"    of which USAF coord-linked:        {usaf_coord_linked_n:,}")
    print(f"  Orphans added:")
    print(f"    USAF (no HOMR coord match):        {usaf_orphans_n:,}")
    print(f"    HPD/NCEI stationinv:               {hpd_orphans_n:,}")
    print(f"  International USAF (excluded):       {usaf_only_total - usaf_us_total:,}")
    print(f"\n  Data coverage:")
    print(f"    Has ASOS 1-min:                    {has_asos_1min:,}")
    print(f"    Has ASOS ISD (processed):          {has_asos_isd:,}")
    print(f"    Has HPD 1-hr:                      {has_hpd:,}")
    print(f"    Has NCEI 15-min:                   {has_ncei:,}")
    print(f"    Has 2+ sources:                    {has_2plus:,}")
    print(f"    Has 3+ sources:                    {has_3plus:,}")
    print(f"\n  Remaining orphans (no HOMR match, not recovered):")
    print(f"    ASOS ICAO:                         {len(asos_icao_orphans):,}")
    print(f"    ASOS WBAN (sample, ≤20 shown):     {len(asos_wban_orphans):,}")
    print(f"\n  Top 10 states by station count:")
    for s in top_states:
        print(f"    {s[0]:<4} {s[1]:>6,}")
    print(f"\n  Output files:")
    print(f"    {OUT_PARQUET} ({parquet_mb:.1f} MB)")
    print(f"    {OUT_CSV} ({csv_mb:.1f} MB)")
    print(f"\n  Total elapsed: {elapsed:.1f}s")
    print("=== DONE ===")

    con.close()


if __name__ == "__main__":
    main()
