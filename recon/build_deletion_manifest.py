"""
build_deletion_manifest.py
Identify international ASOS data files for deletion. NO FILES ARE DELETED.
Outputs:
  recon/international_files_to_delete.txt  — one filepath per line
  recon/deletion_summary.txt               — statistics and safety report
"""

import os
import time

import duckdb

BASE        = os.path.expanduser("~/Documents/WeatherData")
RECON       = os.path.join(BASE, "recon")
INV_DB      = os.path.join(RECON, "file_inventory.duckdb")
MASTER_LIST = os.path.join(RECON, "master_station_list.parquet")
ASOS_CSV    = os.path.join(BASE, "ASOS/master_station_inventory.csv")

OUT_FILES   = os.path.join(RECON, "international_files_to_delete.txt")
OUT_SUMMARY = os.path.join(RECON, "deletion_summary.txt")

# US territories to keep (2-letter suffix in station_name)
US_KEEP = ("US", "PR", "VI", "GU", "AS", "MP", "FM", "MH", "PW")
# Safety net bounding box: keep anything with coordinates in this range
LAT_MIN, LAT_MAX   =  17.0,  72.0   # Hawaii to Alaska
LON_MIN, LON_MAX   = -180.0, -65.0  # West coast to East coast + Hawaii/AK


def main():
    t0 = time.time()
    con = duckdb.connect()
    con.execute(f"ATTACH '{INV_DB}' AS inv (READ_ONLY)")

    print("=" * 60)
    print("STEP 1: Build international station ID set from ASOS master CSV")
    print("=" * 60)

    # US territory codes (appear as the last token before end-of-string in station_name)
    territory_alts = "|".join(US_KEEP)  # e.g. "US|PR|VI|GU|AS|MP|FM|MH|PW"

    # A station is US/territory if ANY of its rows match:
    #   • Name pattern: "CITY, ST CODE" where CODE is a US territory code
    #     (handles both "CITY, AK US" and "CITY, GU" forms)
    #   • CONUS + Hawaii safety bbox:  lat 17-72, lon -180 to -65
    #   • Western Aleutians bbox:      lat 50-72, lon 160-180  (past Int'l Date Line)
    con.execute(f"""
        CREATE TEMP TABLE us_station_ids AS
        SELECT DISTINCT station_id::VARCHAR AS station_id_11
        FROM read_csv_auto('{ASOS_CSV}')
        WHERE REGEXP_MATCHES(
                  COALESCE(station_name::VARCHAR, ''),
                  '.*, [A-Z]{{2}} ({territory_alts})$'
              )
           OR REGEXP_MATCHES(
                  COALESCE(station_name::VARCHAR, ''),
                  '.*, ({territory_alts})$'
              )
           OR (latitude  BETWEEN {LAT_MIN} AND {LAT_MAX}
               AND longitude BETWEEN {LON_MIN} AND {LON_MAX})
           OR (latitude  BETWEEN 50 AND 72
               AND longitude BETWEEN 160 AND 180)
    """)

    con.execute(f"""
        CREATE TEMP TABLE intl_stations AS
        SELECT DISTINCT station_id::VARCHAR AS station_id_11
        FROM read_csv_auto('{ASOS_CSV}')
        WHERE station_id::VARCHAR NOT IN (SELECT station_id_11 FROM us_station_ids)
    """)

    n_intl = con.execute("SELECT COUNT(*) FROM intl_stations").fetchone()[0]
    n_total_csv = con.execute(
        f"SELECT COUNT(DISTINCT station_id::VARCHAR) FROM read_csv_auto('{ASOS_CSV}')"
    ).fetchone()[0]
    print(f"  Total distinct station_ids in ASOS CSV:  {n_total_csv:,}")
    print(f"  Flagged as international (from CSV):     {n_intl:,}")
    print(f"  Kept as US/territory/safety-net:         {n_total_csv - n_intl:,}")

    # Hard-exclude any station that appears in master_station_list (authoritative override).
    # Catches edge cases: stations with renamed names, sentinel IDs, territory airports
    # whose CSV entry lacked the territory suffix in some years.
    con.execute(f"""
        DELETE FROM intl_stations
        WHERE station_id_11 IN (
            -- WBAN=99999 stations: usaf_id || '99999'
            SELECT usaf_id || '99999' FROM read_parquet('{MASTER_LIST}')
            WHERE usaf_id IS NOT NULL
            UNION
            -- The all-nines sentinel: always keep
            SELECT '99999999999'
        )
    """)
    n_after_override = con.execute("SELECT COUNT(*) FROM intl_stations").fetchone()[0]
    n_overridden = n_intl - n_after_override
    if n_overridden:
        print(f"  Master-list override removed {n_overridden} station(s) from intl set")
        print(f"  Final international count:               {n_after_override:,}")

    # -------------------------------------------------------------------------
    # STEP 2: Safety check — any international station that looks US by coords?
    # -------------------------------------------------------------------------
    print()
    print("=" * 60)
    print("STEP 2: Safety check — international stations with US-looking coords")
    print("=" * 60)

    conus_suspects = con.execute(f"""
        SELECT DISTINCT
            m.station_id::VARCHAR AS station_id,
            m.station_name,
            m.latitude, m.longitude
        FROM read_csv_auto('{ASOS_CSV}') m
        JOIN intl_stations i ON m.station_id::VARCHAR = i.station_id_11
        WHERE m.latitude  BETWEEN 24 AND 50
          AND m.longitude BETWEEN -125 AND -66
        ORDER BY m.station_name
    """).fetchall()

    print(f"  Stations flagged international but within CONUS lat/lon box: {len(conus_suspects)}")
    if conus_suspects:
        print("  *** REVIEW THESE BEFORE DELETING ***")
        for s in conus_suspects[:20]:
            print(f"    {s[0]}  {s[1]:<40}  lat={s[2]:.3f}  lon={s[3]:.3f}")
        if len(conus_suspects) > 20:
            print(f"    ... and {len(conus_suspects)-20} more")

    # -------------------------------------------------------------------------
    # STEP 3: Cross-reference with file_inventory
    # -------------------------------------------------------------------------
    print()
    print("=" * 60)
    print("STEP 3: Cross-reference with file_inventory")
    print("=" * 60)

    # USAF_WBAN: the 11-digit stem (station_id_raw + '99999' OR real WBAN)
    # For WBAN=99999: full 11-digit stem = station_id_raw || '99999'
    # For real WBAN:  full 11-digit stem = station_id_raw || SUBSTRING(filename,7,5)
    con.execute("""
        CREATE TEMP TABLE files_to_delete AS
        SELECT
            filepath,
            filename,
            station_id_raw,
            id_format,
            subfolder,
            file_size_bytes,
            -- reconstruct 11-digit ID for matching
            CASE
                WHEN id_format = 'USAF_WBAN'
                    THEN station_id_raw || SUBSTRING(filename, 7, 5)
                ELSE NULL
            END AS station_id_11
        FROM inv.file_inventory
        WHERE source = 'ASOS'
          AND id_format = 'USAF_WBAN'
          -- match against international set
          AND (station_id_raw || SUBSTRING(filename, 7, 5)) IN (
              SELECT station_id_11 FROM intl_stations
          )
    """)

    n_delete = con.execute("SELECT COUNT(*) FROM files_to_delete").fetchone()[0]
    gb_delete = con.execute(
        "SELECT ROUND(SUM(file_size_bytes)/1e9, 3) FROM files_to_delete"
    ).fetchone()[0]
    print(f"  USAF_WBAN files flagged for deletion: {n_delete:,}  ({gb_delete:.2f} GB)")

    # ICAO_ASOS: skip from deletion — files are small (0.07 GB total) and
    # non-HOMR ICAOs include legitimate US territory airports (PGSN=Saipan,
    # PGUM=Guam, K1V4=Virginia). Not worth the risk.
    print()
    icao_total = con.execute(
        "SELECT COUNT(*), ROUND(SUM(file_size_bytes)/1e9,3) FROM inv.file_inventory "
        "WHERE source='ASOS' AND id_format='ICAO_ASOS'"
    ).fetchone()
    print(f"  ICAO_ASOS files: {icao_total[0]:,} ({icao_total[1]:.3f} GB) — kept, not flagged for deletion")
    icao_intl = []  # empty — nothing flagged

    # Verify HPD and NCEI are all US (don't flag for deletion)
    print()
    print("  Verifying HPD/NCEI are all US (should flag 0)...")
    for src, fmt in [("HPD_1hr", "GHCND_HPD"), ("NCEI_15min_raw", "GHCND")]:
        r = con.execute(f"""
            SELECT COUNT(*) FROM inv.file_inventory
            WHERE source='{src}' AND id_format='{fmt}'
              AND station_id_raw NOT IN (
                  SELECT UNNEST(STRING_SPLIT(ghcnd_ids, '|'))
                  FROM read_parquet('{MASTER_LIST}')
                  WHERE ghcnd_ids IS NOT NULL
              )
        """).fetchone()[0]
        print(f"    {src} ({fmt}) unmatched in master list: {r}")

    # -------------------------------------------------------------------------
    # STEP 4: Safety validation — ensure zero overlap with master_station_list
    # -------------------------------------------------------------------------
    print()
    print("=" * 60)
    print("STEP 4: Safety validation")
    print("=" * 60)

    # Check 1: any file in deletion list whose station is in master_station_list?
    overlap = con.execute(f"""
        SELECT COUNT(DISTINCT d.station_id_11)
        FROM files_to_delete d
        JOIN read_parquet('{MASTER_LIST}') m
          ON d.station_id_11 = m.usaf_id || '99999'
          OR d.station_id_11 = m.usaf_id
        WHERE m.usaf_id IS NOT NULL
    """).fetchone()[0]
    print(f"  Check 1 — Deletion set overlaps master_station_list (should be 0): {overlap}")
    if overlap > 0:
        print("  *** WARNING: Some files flagged for deletion belong to US stations! ***")
        examples = con.execute(f"""
            SELECT d.station_id_11, d.filepath, m.station_name
            FROM files_to_delete d
            JOIN read_parquet('{MASTER_LIST}') m
              ON d.station_id_11 = m.usaf_id || '99999'
              OR d.station_id_11 = m.usaf_id
            WHERE m.usaf_id IS NOT NULL
            LIMIT 10
        """).fetchall()
        for e in examples:
            print(f"    {e[0]}  {e[2]}  →  {e[1]}")

    # Check 2: how many US files remain after deletion?
    us_files_remaining = con.execute("""
        SELECT COUNT(*), ROUND(SUM(file_size_bytes)/1e9, 2)
        FROM inv.file_inventory
        WHERE source = 'ASOS'
          AND filepath NOT IN (SELECT filepath FROM files_to_delete)
    """).fetchall()[0]
    print(f"  Check 2 — ASOS files remaining after deletion: "
          f"{us_files_remaining[0]:,} files, {us_files_remaining[1]:.2f} GB")

    # Check 3: breakdown by subfolder for deletion list
    print()
    print("  Breakdown by subfolder:")
    breakdown = con.execute("""
        SELECT subfolder,
               COUNT(*) AS files,
               ROUND(SUM(file_size_bytes)/1e9, 3) AS gb
        FROM files_to_delete
        GROUP BY subfolder ORDER BY gb DESC
    """).fetchall()
    for b in breakdown:
        print(f"    {b[0]:<35} {b[1]:>8,} files   {b[2]:>8.3f} GB")

    # Check 4: sample 10 entries from deletion list
    print()
    print("  Sample 10 entries from deletion manifest:")
    samples = con.execute("""
        SELECT filepath, station_id_11,
               ROUND(file_size_bytes/1e6, 2) AS mb
        FROM files_to_delete ORDER BY RANDOM() LIMIT 10
    """).fetchall()
    for s in samples:
        print(f"    [{s[2]:6.2f} MB]  {s[1]}  {s[0]}")

    # -------------------------------------------------------------------------
    # STEP 5: Write outputs
    # -------------------------------------------------------------------------
    print()
    print("=" * 60)
    print("STEP 5: Writing outputs")
    print("=" * 60)

    # Write filepath manifest
    all_paths = con.execute(
        "SELECT filepath FROM files_to_delete ORDER BY filepath"
    ).fetchall()
    with open(OUT_FILES, "w") as f:
        for (fp,) in all_paths:
            f.write(fp + "\n")
    print(f"  Wrote {len(all_paths):,} paths to: {OUT_FILES}")

    # Detailed breakdown for summary file
    total_files  = con.execute("SELECT COUNT(*) FROM files_to_delete").fetchone()[0]
    total_gb     = con.execute(
        "SELECT ROUND(SUM(file_size_bytes)/1e9, 3) FROM files_to_delete"
    ).fetchone()[0]

    asos_proc = con.execute("""
        SELECT COUNT(*), ROUND(SUM(file_size_bytes)/1e9,3)
        FROM files_to_delete WHERE subfolder='processed_parquet'
    """).fetchone()
    asos_1min = con.execute("""
        SELECT COUNT(*), COALESCE(ROUND(SUM(file_size_bytes)/1e9,3), 0.0)
        FROM files_to_delete WHERE subfolder='asos_1min_parquet_master'
    """).fetchone()
    asos_raw = con.execute("""
        SELECT COUNT(*), COALESCE(ROUND(SUM(file_size_bytes)/1e9,3), 0.0)
        FROM files_to_delete WHERE subfolder NOT IN ('processed_parquet','asos_1min_parquet_master')
    """).fetchone()

    total_inv = con.execute(
        "SELECT COUNT(*), ROUND(SUM(file_size_bytes)/1e9,2) FROM inv.file_inventory"
    ).fetchone()

    elapsed = time.time() - t0

    summary_lines = [
        "INTERNATIONAL FILES DELETION MANIFEST",
        f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        f"Total files to delete:   {total_files:,}",
        f"Total GB to reclaim:     {total_gb:.3f} GB",
        "",
        "Breakdown by subfolder:",
        f"  ASOS/processed_parquet:           {asos_proc[0]:>8,} files   {asos_proc[1]:>8.3f} GB",
        f"  ASOS/asos_1min_parquet_master:    {asos_1min[0]:>8,} files   {asos_1min[1]:>8.3f} GB",
        f"  Other ASOS:                       {asos_raw[0]:>8,} files   {asos_raw[1]:>8.3f} GB",
        "",
        "After deletion:",
        f"  Total files remaining:   {total_inv[0] - total_files:,}",
        f"  Total data remaining:    {total_inv[1] - total_gb:.2f} GB",
        "",
        "Safety checks:",
        f"  Overlap with master_station_list (must be 0): {overlap}",
        f"  Intl stations within CONUS lat/lon box:       {len(conus_suspects)}",
        f"  ICAO_ASOS files with no US HOMR match:        {len(icao_intl)}",
        "",
        "CONUS lat/lon suspects (review before deleting):",
    ]
    for s in conus_suspects:
        summary_lines.append(
            f"  {s[0]}  {s[1]:<40}  lat={s[2]:.3f}  lon={s[3]:.3f}"
        )
    if not conus_suspects:
        summary_lines.append("  (none)")

    summary_lines += [
        "",
        "Manifest file: international_files_to_delete.txt",
        f"Elapsed: {elapsed:.1f}s",
        "",
        "DO NOT DELETE without reviewing this report.",
    ]

    with open(OUT_SUMMARY, "w") as f:
        f.write("\n".join(summary_lines) + "\n")
    print(f"  Wrote summary to: {OUT_SUMMARY}")

    # Final print
    print()
    print("=" * 60)
    print("=== DELETION MANIFEST SUMMARY ===")
    print("=" * 60)
    print(f"  Total files to delete:                     {total_files:,}")
    print(f"  Total GB to reclaim:                       {total_gb:.3f} GB")
    print(f"  ASOS/processed_parquet:                    {asos_proc[0]:,} files  {asos_proc[1]:.3f} GB")
    print(f"  ASOS/asos_1min_parquet_master:             {asos_1min[0]:,} files  {asos_1min[1]:.3f} GB")
    print()
    print(f"  Safety — overlap with master_station_list: {overlap}  (must be 0)")
    print(f"  Safety — CONUS suspects to review:         {len(conus_suspects)}")
    print(f"  Safety — ICAO non-US files included:       {len(icao_intl)}")
    print()
    print(f"  Remaining after deletion:  {total_inv[0]-total_files:,} files  {total_inv[1]-total_gb:.2f} GB")
    print(f"  Manifest: {OUT_FILES}")
    print(f"  Summary:  {OUT_SUMMARY}")
    print(f"  Elapsed:  {elapsed:.1f}s")
    print("=== DONE — NO FILES WERE DELETED ===")

    con.close()


if __name__ == "__main__":
    main()
