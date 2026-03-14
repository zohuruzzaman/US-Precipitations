"""
build_file_inventory.py
Incremental file inventory builder for WeatherData directory.
Output: ~/Documents/WeatherData/recon/file_inventory.duckdb

Usage:
  python3 recon/build_file_inventory.py --all           # full rebuild (drop + recreate)
  python3 recon/build_file_inventory.py --source ASOS   # replace only ASOS rows
  python3 recon/build_file_inventory.py --source GHCN_daily
"""

import argparse
import os
import re
import time

import duckdb

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE     = os.path.expanduser("~/Documents/WeatherData")
RECON    = os.path.join(BASE, "recon")
DB_PATH  = os.path.join(RECON, "file_inventory.duckdb")

# ---------------------------------------------------------------------------
# Source definitions
# ---------------------------------------------------------------------------
# Each entry maps a source label → directory path(s) to walk.
# 'paths' is a list of paths relative to BASE.
# 'skip_subdirs': subdirectory names to prune during os.walk for this source.
# 'us_filter_ext': if set, only index files with this ext that pass is_us_ghcn().
SOURCES = {
    'ASOS': {
        'paths': ['ASOS'],
        'skip_subdirs': {'asos_raw_downloads', 'raw_downloads', 'temp_extracted'},
    },
    'HPD_1hr': {
        'paths': ['HPD_1hr'],
    },
    'NCEI_15min_raw': {
        'paths': ['NCEI_15min_raw'],
    },
    'HOMR': {
        'paths': ['HOMR'],
    },
    'GHCN_daily': {
        # ghcnd_all.tar.gz is a macOS-mounted directory containing 129K .dly files.
        # GHCN_state_outputs_all.zip is a macOS-mounted directory with state folders.
        # Skip the actual compressed blobs (.tar.gz.gz, .zip.zip).
        'paths': [
            os.path.join('GHCN_daily', 'ghcnd_all.tar.gz', 'ghcnd_all'),
            os.path.join('GHCN_daily', 'GHCN_state_outputs_all.zip'),
        ],
        'us_filter_ext': '.dly',   # only keep US-prefix .dly files; count/skip rest
    },
}

# ---------------------------------------------------------------------------
# US GHCN prefix filter
# ---------------------------------------------------------------------------
# Two-letter country codes that map to US territories in GHCN-D naming.
US_GHCND_PREFIXES = frozenset({
    'US',  # CONUS, Alaska, Hawaii
    'RQ',  # Puerto Rico
    'VQ',  # US Virgin Islands
    'CQ',  # Commonwealth of Northern Mariana Islands
    'GQ',  # Guam
    'AQ',  # American Samoa
    'MQ',  # Midway Atoll
    'FQ',  # Baker / Howland Islands
    'SQ',  # misc Pacific (Johnston, Wake)
    'RW',  # Marshall Islands (compact of free association)
})


def is_us_ghcn(filename: str) -> bool:
    """Return True if a .dly filename belongs to a US or territory station."""
    return filename[:2] in US_GHCND_PREFIXES


# ---------------------------------------------------------------------------
# Always-skip directory names (any source)
# ---------------------------------------------------------------------------
GLOBAL_SKIP_DIRS = {'.DS_Store', '__pycache__', '.git', '.claude', 'recon'}

# ---------------------------------------------------------------------------
# Data extensions
# ---------------------------------------------------------------------------
DATA_EXTENSIONS = frozenset({
    '.parquet', '.csv', '.txt', '.dat',
    '.dat.parquet', '.15m.parquet',
    '.dly', '.csv.gz',
})

# ---------------------------------------------------------------------------
# Extension parser
# ---------------------------------------------------------------------------
_COMPOUND_EXTS = ('.dat.parquet', '.15m.parquet', '.csv.gz', '.tar.gz', '.tar.gz.gz')


def get_extension(filename: str) -> str:
    fn = filename.lower()
    for c in _COMPOUND_EXTS:
        if fn.endswith(c):
            return c
    _, ext = os.path.splitext(filename)
    return ext.lower()


# ---------------------------------------------------------------------------
# Station ID extraction
# ---------------------------------------------------------------------------
STATION_ID_PATTERNS = [
    # ── HPD / NCEI 15-min station parquets and CSVs ──────────────────────
    # e.g. USC00010008.parquet, AQC00914594.parquet
    (re.compile(
        r'^((?:AQC|CQC|FQC|GQC|MQC|RQC|RQW|SQC|USC|USW|VQC)\d{8})'
        r'(?:\.csv|\.parquet|\.15m\.parquet)$'
    ), 'GHCND_HPD'),

    # ── ASOS 1-min: 6406x(ICAO5)(YYYYMM).parquet ─────────────────────────
    # e.g. 64060KABE200002.parquet → ICAO = KABE
    (re.compile(r'^6406([A-Z0-9]{5})(\d{6})\.parquet$'), 'ICAO_ASOS'),

    # ── ASOS ISD (processed_parquet): USAF(6)WBAN(5).parquet ─────────────
    # e.g. 72509014733.parquet
    (re.compile(r'^(\d{6})(\d{5})\.parquet$'), 'USAF_WBAN'),

    # ── ASOS A-prefix: A + 10 digits ─────────────────────────────────────
    (re.compile(r'^(A\d{10})\.parquet$'), 'ASOS_A'),

    # ── GHCN-Daily .dly: 11-char GHCND ID ────────────────────────────────
    # e.g. USC00010008.dly, US1MSNS0004.dly, RQ000011630.dly
    # Format: country(2 alpha) + 9 alphanumeric chars (letters in pos 4-5
    # for community-collaborative networks like US1, US2, etc.)
    (re.compile(r'^([A-Z]{2}[A-Z0-9]{9})\.dly$'), 'GHCND_DLY'),

    # ── GHCN state-output aggregates: TX_raw_daily_PRCP.csv.gz ───────────
    (re.compile(r'^([A-Z]{2})_raw_daily_\w+\.csv\.gz$'), 'STATE_AGGREGATE'),
    (re.compile(r'^([A-Z]{2})_station_locations\.csv$'), 'STATE_METADATA'),
    (re.compile(r'^([A-Z]{2})_station_summary\.csv$'), 'STATE_METADATA'),
    (re.compile(r'^([A-Z]{2})_station_year_summary\.csv$'), 'STATE_METADATA'),
    (re.compile(r'^([A-Z]{2})_yearly_summary\.csv$'), 'STATE_METADATA'),

    # ── Known aggregate / named files (no station ID) ─────────────────────
    (re.compile(r'^(?:stations_master|stations_crosswalk|raw_file_inventory'
                r'|hpd15_station|master_station|ALL_STATES)', re.IGNORECASE), None),

    # ── NCEI legacy aggregates ────────────────────────────────────────────
    # Monthly: 3260apr2011.dat.parquet
    (re.compile(r'^3260\w+\.dat\.parquet$', re.IGNORECASE), None),
    # State: 3260_423611_por-1998.parquet  OR  3260_\d+_YYYY-YYYY.parquet
    (re.compile(r'^3260_\d+_(?:por-)?\d{4}(?:-\d{4})?\.parquet$'), None),

    # ── Year-only parquet (ASOS processed): 1951.parquet ─────────────────
    (re.compile(r'^(\d{4})\.parquet$'), None),
]


def extract_station_id(filename: str):
    """Return (station_id_raw, id_format, parse_failure)."""
    for pattern, fmt in STATION_ID_PATTERNS:
        m = pattern.match(filename)
        if m:
            if fmt is None:
                return None, None, False      # known aggregate, no station ID
            if fmt in ('ICAO_ASOS', 'USAF_WBAN'):
                return m.group(1), fmt, False  # group(1) = primary code
            return m.group(1), fmt, False
    return None, None, True                    # parse failure


def extract_date_part(filename: str):
    """Pull YYYYMM or YYYY from ASOS filenames."""
    m = re.search(r'(\d{6})\.parquet$', filename)
    if m:
        raw = m.group(1)
        return f"{raw[:4]}-{raw[4:6]}"
    m = re.search(r'^(\d{4})\.parquet$', filename)
    if m:
        return m.group(1)
    return None


def get_subfolder(filepath: str, source_root: str) -> str:
    """Return the immediate subdirectory under the source root, or 'root'."""
    rel = os.path.relpath(filepath, source_root)
    parts = rel.split(os.sep)
    return parts[0] if len(parts) >= 2 else 'root'


# ---------------------------------------------------------------------------
# DB schema
# ---------------------------------------------------------------------------
DDL_TABLES = """
CREATE TABLE IF NOT EXISTS file_inventory (
    filepath            TEXT PRIMARY KEY,
    filename            TEXT,
    extension           TEXT,
    file_size_bytes     BIGINT,
    source              TEXT,
    subfolder           TEXT,
    schema_group        TEXT,
    station_id_raw      TEXT,
    id_format           TEXT,
    date_part           TEXT,
    is_data_file        BOOLEAN,
    anomaly_flag        TEXT
);

CREATE TABLE IF NOT EXISTS group_schemas (
    schema_group        TEXT PRIMARY KEY,
    sample_filepath     TEXT,
    column_names        TEXT,
    row_count           BIGINT,
    notes               TEXT,
    sampled_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS anomalies (
    id                  INTEGER,
    filepath            TEXT,
    schema_group        TEXT,
    anomaly_type        TEXT,
    detail              TEXT,
    detected_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS station_id_parse_failures (
    filepath            TEXT,
    filename            TEXT,
    source              TEXT,
    subfolder           TEXT,
    extension           TEXT
);
"""


def create_tables(con):
    con.execute(DDL_TABLES)


def drop_all_tables(con):
    for tbl in ('file_inventory', 'group_schemas', 'anomalies', 'station_id_parse_failures'):
        con.execute(f"DROP TABLE IF EXISTS {tbl}")


# ---------------------------------------------------------------------------
# Walk one source
# ---------------------------------------------------------------------------

def walk_source(source_name: str, cfg: dict, intl_skipped_ref: list):
    """
    Generator: yields 11-tuples:
      (filepath, filename, ext, size, source, subfolder,
       schema_group, station_id, id_fmt, date_part, is_data)

    intl_skipped_ref is a single-element list used as a mutable counter for
    international .dly files that are filtered out.
    """
    source_skip = cfg.get('skip_subdirs', set()) | GLOBAL_SKIP_DIRS
    us_filter_ext = cfg.get('us_filter_ext')   # e.g. '.dly'

    for rel_path in cfg['paths']:
        root = os.path.join(BASE, rel_path)
        if not os.path.isdir(root):
            print(f"  [WARN] path not found, skipping: {root}")
            continue

        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [
                d for d in dirnames
                if d not in source_skip and not d.startswith('.')
            ]

            for filename in filenames:
                if filename.startswith('.'):
                    continue

                ext = get_extension(filename)

                # US-only filter for GHCN .dly files
                if us_filter_ext and ext == us_filter_ext:
                    if not is_us_ghcn(filename):
                        intl_skipped_ref[0] += 1
                        continue

                filepath = os.path.join(dirpath, filename)
                try:
                    size = os.path.getsize(filepath)
                except OSError:
                    size = 0

                rel = os.path.relpath(dirpath, root)
                parts = rel.split(os.sep)
                subfolder = parts[0] if parts[0] != '.' else 'root'

                schema_group = f"{source_name}|{subfolder}|{ext}"
                station_id, id_fmt, _ = extract_station_id(filename)
                date_part = extract_date_part(filename)
                is_data = ext in DATA_EXTENSIONS

                yield (
                    filepath, filename, ext, size,
                    source_name, subfolder, schema_group,
                    station_id, id_fmt, date_part, is_data,
                )


# ---------------------------------------------------------------------------
# Pass 1: inventory walk
# ---------------------------------------------------------------------------

def pass1_walk(con, sources_to_run: list):
    print("\n" + "="*60)
    print("PASS 1: File walk")
    print(f"Sources: {sources_to_run}")
    print("="*60)

    t0 = time.time()
    grand_total = 0
    intl_skipped_ref = [0]   # mutable counter shared with walk_source generator

    INSERT_SQL = """
        INSERT OR IGNORE INTO file_inventory
          (filepath, filename, extension, file_size_bytes,
           source, subfolder, schema_group,
           station_id_raw, id_format, date_part,
           is_data_file, anomaly_flag)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,NULL)
    """
    FAIL_SQL = "INSERT INTO station_id_parse_failures VALUES (?,?,?,?,?)"

    for source_name in sources_to_run:
        cfg = SOURCES[source_name]
        batch = []
        fail_batch = []
        source_total = 0
        source_parse_failures = 0
        print(f"\n  Scanning {source_name} ...", flush=True)
        st = time.time()

        for row in walk_source(source_name, cfg, intl_skipped_ref):
            (filepath, filename, ext, size,
             src, subfolder, schema_group,
             station_id, id_fmt, date_part, is_data) = row

            # Track parse failures (file has no matched station ID pattern and
            # is a data file that should have one)
            if station_id is None and id_fmt is None and is_data:
                # Check if it's a genuinely unknown filename (not a known aggregate)
                _, _, parse_fail = extract_station_id(filename)
                if parse_fail:
                    fail_batch.append((filepath, filename, src, subfolder, ext))
                    source_parse_failures += 1

            batch.append((
                filepath, filename, ext, size,
                src, subfolder, schema_group,
                station_id, id_fmt, date_part, is_data,
            ))
            source_total += 1
            grand_total += 1

            if len(batch) >= 10_000:
                con.executemany(INSERT_SQL, batch)
                batch.clear()
            if fail_batch and len(fail_batch) >= 1_000:
                con.executemany(FAIL_SQL, fail_batch)
                fail_batch.clear()

            if grand_total % 100_000 == 0:
                print(f"    {grand_total:,} files so far ({time.time()-t0:.0f}s)...",
                      flush=True)

        if batch:
            con.executemany(INSERT_SQL, batch)
        if fail_batch:
            con.executemany(FAIL_SQL, fail_batch)

        elapsed_src = time.time() - st
        print(f"    {source_name}: {source_total:,} files in {elapsed_src:.1f}s"
              f"  (parse failures: {source_parse_failures:,})")

    elapsed = time.time() - t0
    print(f"\nPass 1 complete: {grand_total:,} files in {elapsed:.1f}s")
    if intl_skipped_ref[0]:
        print(f"  GHCN international .dly skipped: {intl_skipped_ref[0]:,}")

    # Summary
    print("\n--- Source breakdown ---")
    rows = con.execute("""
        SELECT source, COUNT(*) as files,
               ROUND(SUM(file_size_bytes)/1e9, 2) as gb,
               COUNT(DISTINCT id_format) as id_fmts,
               STRING_AGG(DISTINCT COALESCE(id_format,'NULL'), '|') as formats
        FROM file_inventory
        GROUP BY source ORDER BY files DESC
    """).fetchall()
    print(f"  {'Source':<20} {'Files':>10}  {'GB':>7}  Formats")
    for r in rows:
        print(f"  {r[0]:<20} {r[1]:>10,}  {r[2]:>7.2f}  {r[4]}")

    print("\n--- ID format distribution ---")
    rows = con.execute("""
        SELECT id_format, COUNT(*) as files,
               COUNT(DISTINCT station_id_raw) as unique_stations
        FROM file_inventory
        GROUP BY id_format ORDER BY files DESC
    """).fetchall()
    print(f"  {'id_format':<20} {'Files':>10}  {'Unique IDs':>12}")
    for r in rows:
        fmt = str(r[0]) if r[0] is not None else 'NULL'
        print(f"  {fmt:<20} {r[1]:>10,}  {r[2]:>12,}")

    pf_count = con.execute(
        "SELECT COUNT(*) FROM station_id_parse_failures"
    ).fetchone()[0]
    print(f"\n  Station ID parse failures: {pf_count:,}")
    if pf_count > 0:
        samples = con.execute(
            "SELECT source, extension, filename FROM station_id_parse_failures LIMIT 10"
        ).fetchall()
        for s in samples:
            print(f"    {s[0]} | {s[1]} | {s[2]}")

    return grand_total


# ---------------------------------------------------------------------------
# Pass 2: Schema sampling (lightweight — one file per group)
# ---------------------------------------------------------------------------

def get_schema(filepath: str, ext: str, con) -> dict:
    result = {'columns': None, 'row_count': None, 'notes': ''}
    try:
        if ext in ('.parquet', '.dat.parquet', '.15m.parquet'):
            cols = con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{filepath}') LIMIT 0"
            ).fetchall()
            result['columns'] = [c[0] for c in cols]
            rc = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{filepath}')"
            ).fetchone()[0]
            result['row_count'] = rc

        elif ext in ('.csv',):
            cols = con.execute(
                f"DESCRIBE SELECT * FROM read_csv_auto('{filepath}', ignore_errors=true) LIMIT 0"
            ).fetchall()
            result['columns'] = [c[0] for c in cols]
            rc = con.execute(
                f"SELECT COUNT(*) FROM read_csv_auto('{filepath}', ignore_errors=true)"
            ).fetchone()[0]
            result['row_count'] = rc

        elif ext == '.dly':
            # Fixed-width text: read first 5 lines as sample
            with open(filepath, 'r', errors='replace') as f:
                lines = [f.readline().rstrip('\n') for _ in range(3)]
            result['columns'] = ['station_id(1-11)', 'year(12-15)', 'month(16-17)',
                                  'element(18-21)', 'day_values(22-269)']
            result['notes'] = 'fixed-width GHCN-D format; sample: ' + lines[0][:60]

        elif ext == '.csv.gz':
            result['notes'] = 'compressed CSV — not opened in schema pass'

        elif ext in ('.gz', '.tar.gz', '.tar.gz.gz', '.zip'):
            result['notes'] = 'archive — not opened'

        elif ext == '.pdf':
            result['notes'] = 'PDF — not opened'

        elif ext == '.txt':
            try:
                cols = con.execute(
                    f"DESCRIBE SELECT * FROM read_csv_auto('{filepath}', ignore_errors=true) LIMIT 0"
                ).fetchall()
                result['columns'] = [c[0] for c in cols]
            except Exception:
                with open(filepath, 'r', errors='replace') as f:
                    lines = [f.readline().rstrip('\n') for _ in range(5)]
                result['columns'] = ['raw_line']
                result['notes'] = 'unstructured: ' + lines[0][:80]

        else:
            result['notes'] = f'unhandled extension: {ext}'

    except Exception as e:
        result['notes'] = f'ERROR: {str(e)[:120]}'

    return result


def pass2_schema(con, sources_to_run: list):
    print("\n" + "="*60)
    print("PASS 2: Schema sampling")
    print("="*60)

    # Only sample groups belonging to the sources we're rebuilding
    source_list = "', '".join(sources_to_run)
    groups = con.execute(f"""
        SELECT DISTINCT schema_group
        FROM file_inventory
        WHERE is_data_file = true AND source IN ('{source_list}')
        ORDER BY schema_group
    """).fetchall()
    groups = [g[0] for g in groups]
    print(f"  {len(groups)} data file groups to sample\n")

    for sg in groups:
        candidates = con.execute("""
            SELECT filepath FROM file_inventory
            WHERE schema_group = ? AND is_data_file = true
            ORDER BY RANDOM() LIMIT 3
        """, [sg]).fetchall()

        ext = sg.split('|')[-1]
        schema = None
        used_fp = None
        for (fp,) in candidates:
            schema = get_schema(fp, ext, con)
            if (schema['columns'] is not None
                    or 'archive' in schema['notes']
                    or 'PDF' in schema['notes']
                    or 'compressed' in schema['notes']
                    or 'fixed-width' in schema['notes']):
                used_fp = fp
                break

        if schema is None:
            schema = {'columns': None, 'row_count': None, 'notes': 'no candidates'}

        col_str = str(schema['columns']) if schema['columns'] else None
        col_count = len(schema['columns']) if schema['columns'] else 0

        con.execute("""
            INSERT OR REPLACE INTO group_schemas
              (schema_group, sample_filepath, column_names, row_count, notes)
            VALUES (?, ?, ?, ?, ?)
        """, [sg, used_fp, col_str, schema['row_count'], schema['notes']])

        print(f"  [{sg}]")
        if schema['columns']:
            print(f"    cols ({col_count}): {schema['columns'][:5]}"
                  f"{'...' if col_count > 5 else ''}")
        if schema['row_count'] is not None:
            print(f"    rows: {schema['row_count']:,}")
        if schema['notes']:
            print(f"    notes: {schema['notes'][:100]}")
        print()

    gs_count = con.execute("SELECT COUNT(*) FROM group_schemas").fetchone()[0]
    print(f"Pass 2 complete: {gs_count} groups sampled")


# ---------------------------------------------------------------------------
# Final summary
# ---------------------------------------------------------------------------

def print_summary(con, elapsed_total: float):
    print("\n" + "="*60)
    print("=== FILE INVENTORY COMPLETE ===")
    print("="*60)

    total = con.execute("SELECT COUNT(*) FROM file_inventory").fetchone()[0]
    print(f"\n  Total files indexed : {total:,}")
    print(f"  Elapsed             : {elapsed_total:.1f}s")
    print(f"  DB                  : {DB_PATH}\n")

    print("  Source breakdown:")
    rows = con.execute("""
        SELECT source,
               COUNT(*) as files,
               COUNT(DISTINCT station_id_raw) - 1 as unique_stations,
               ROUND(SUM(file_size_bytes)/1e9, 2) as gb,
               STRING_AGG(DISTINCT extension, ', ') as exts
        FROM file_inventory
        GROUP BY source ORDER BY files DESC
    """).fetchall()
    print(f"  {'Source':<22} {'Files':>9}  {'Stations':>9}  {'GB':>7}  Extensions")
    print(f"  {'-'*22} {'-'*9}  {'-'*9}  {'-'*7}  ----------")
    for r in rows:
        stn = r[2] if r[2] > 0 else '—'
        print(f"  {r[0]:<22} {r[1]:>9,}  {str(stn):>9}  {r[3]:>7.2f}  {r[4][:50]}")

    print("\n  ID format distribution:")
    rows = con.execute("""
        SELECT COALESCE(id_format, 'NULL') as fmt,
               COUNT(*) as files,
               COUNT(DISTINCT station_id_raw) as unique_ids,
               STRING_AGG(DISTINCT source, '+') as sources
        FROM file_inventory
        GROUP BY id_format ORDER BY files DESC
    """).fetchall()
    print(f"  {'id_format':<20} {'Files':>9}  {'Unique IDs':>11}  Source(s)")
    for r in rows:
        print(f"  {r[0]:<20} {r[1]:>9,}  {r[2]:>11,}  {r[3]}")

    pf_count = con.execute(
        "SELECT COUNT(*) FROM station_id_parse_failures"
    ).fetchone()[0]
    print(f"\n  Parse failures: {pf_count:,}")
    if pf_count > 0:
        rows = con.execute("""
            SELECT source, extension, COUNT(*) as n
            FROM station_id_parse_failures
            GROUP BY source, extension ORDER BY n DESC LIMIT 10
        """).fetchall()
        for r in rows:
            print(f"    {r[0]} | {r[1]} | {r[2]:,} files")

    gs_count = con.execute("SELECT COUNT(*) FROM group_schemas").fetchone()[0]
    an_count = con.execute("SELECT COUNT(*) FROM anomalies").fetchone()[0]
    print(f"\n  group_schemas : {gs_count} rows")
    print(f"  anomalies     : {an_count} rows")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Build/update WeatherData file inventory")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--all', action='store_true',
                       help='Full rebuild: drop all tables and re-walk every source')
    group.add_argument('--source', metavar='SOURCE',
                       choices=list(SOURCES.keys()),
                       help=f'Replace rows for one source only. Choices: {list(SOURCES.keys())}')
    args = parser.parse_args()

    os.makedirs(RECON, exist_ok=True)

    t_start = time.time()

    if args.all:
        sources_to_run = list(SOURCES.keys())
        print(f"Mode: FULL REBUILD — dropping all tables")
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
        con = duckdb.connect(DB_PATH)
        create_tables(con)
    else:
        sources_to_run = [args.source]
        print(f"Mode: INCREMENTAL — replacing source '{args.source}'")
        con = duckdb.connect(DB_PATH)
        create_tables(con)   # no-op if tables exist
        # Delete existing rows for this source
        deleted = con.execute(
            "DELETE FROM file_inventory WHERE source = ?", [args.source]
        ).rowcount
        con.execute(
            "DELETE FROM station_id_parse_failures WHERE source = ?", [args.source]
        )
        # Remove group_schemas entries that belong only to this source
        con.execute("""
            DELETE FROM group_schemas
            WHERE schema_group LIKE ?
        """, [f"{args.source}|%"])
        print(f"  Deleted {deleted:,} existing rows for '{args.source}'")

    # Pass 1
    pass1_walk(con, sources_to_run)

    # Pass 2 (schema sampling for new groups only)
    pass2_schema(con, sources_to_run)

    # Final summary
    elapsed = time.time() - t_start
    print_summary(con, elapsed)

    con.close()
    print(f"\nDone. DB: {DB_PATH}")


if __name__ == '__main__':
    main()
