"""
convert_dly_to_parquet.py
Convert US GHCN-D .dly files to snappy-compressed parquet (PRCP/TMAX/TMIN/TAVG only).
Also converts state PRCP .csv.gz files to parquet.

Output:
  GHCN_daily/ghcnd_all_parquet/{STATION_ID}.parquet   (per-station, long format)
  GHCN_daily/state_parquet/{STATE}_prcp.parquet        (per-state PRCP)
"""

import datetime
import os
import sys
import time
from multiprocessing import Pool, cpu_count

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE         = os.path.expanduser("~/Documents/WeatherData")
DLY_DIR      = os.path.join(BASE, "GHCN_daily", "ghcnd_all.tar.gz", "ghcnd_all")
OUT_DIR      = os.path.join(BASE, "GHCN_daily", "ghcnd_all_parquet")
STATE_ROOT   = os.path.join(BASE, "GHCN_daily", "GHCN_state_outputs_all.zip")
STATE_OUT    = os.path.join(BASE, "GHCN_daily", "state_parquet")
RECON        = os.path.join(BASE, "recon")
INV_DB       = os.path.join(RECON, "file_inventory.duckdb")

# Elements to extract — skip everything else (SNOW, SNWD, AWND, WT*, WV*, etc.)
KEEP_ELEMENTS = frozenset({'PRCP', 'TMAX', 'TMIN', 'TAVG'})

# US station prefixes (same as inventory filter)
US_PREFIXES = frozenset({
    'US', 'RQ', 'VQ', 'CQ', 'GQ', 'AQ', 'MQ', 'FQ', 'SQ', 'RW',
})

# PyArrow schema — defined once at module level (inherited by workers)
PQ_SCHEMA = pa.schema([
    ('station_id', pa.string()),
    ('date',       pa.date32()),
    ('element',    pa.string()),
    ('value',      pa.int32()),
    ('mflag',      pa.string()),
    ('qflag',      pa.string()),
    ('sflag',      pa.string()),
])

WRITE_OPTIONS = pq.ParquetWriter  # referenced by name in worker


# ---------------------------------------------------------------------------
# Worker: parse one .dly file → write one .parquet
# ---------------------------------------------------------------------------

def convert_dly(args):
    """
    Returns ('ok', path, rows) | ('skipped', path) | ('error', path, msg)
    """
    dly_path, out_path = args

    station_ids = []
    dates        = []
    elements     = []
    values       = []
    mflags       = []
    qflags       = []
    sflags       = []

    try:
        with open(dly_path, 'r', errors='replace') as fh:
            for line in fh:
                # Minimum: header (21) + at least one day group (8)
                if len(line) < 29:
                    continue
                element = line[17:21]
                if element not in KEEP_ELEMENTS:
                    continue

                station_id = line[0:11]
                try:
                    year  = int(line[11:15])
                    month = int(line[15:17])
                except ValueError:
                    continue

                for day in range(1, 32):
                    offset = 21 + (day - 1) * 8
                    if offset + 8 > len(line):
                        break

                    try:
                        value = int(line[offset:offset + 5])
                    except ValueError:
                        continue

                    mf = line[offset + 5:offset + 6].strip()
                    qf = line[offset + 6:offset + 7].strip()
                    sf = line[offset + 7:offset + 8].strip()

                    # Missing with no flags → skip entirely
                    if value == -9999 and not mf and not qf and not sf:
                        continue

                    try:
                        d = datetime.date(year, month, day)
                    except ValueError:
                        continue  # invalid date (e.g. Feb 30)

                    station_ids.append(station_id)
                    dates.append(d)
                    elements.append(element)
                    values.append(None if value == -9999 else value)
                    mflags.append(mf or None)
                    qflags.append(qf or None)
                    sflags.append(sf or None)

        if not dates:
            return ('skipped', dly_path)

        table = pa.table(
            {
                'station_id': pa.array(station_ids, type=pa.string()),
                'date':       pa.array(dates,        type=pa.date32()),
                'element':    pa.array(elements,     type=pa.string()),
                'value':      pa.array(values,       type=pa.int32()),
                'mflag':      pa.array(mflags,       type=pa.string()),
                'qflag':      pa.array(qflags,       type=pa.string()),
                'sflag':      pa.array(sflags,       type=pa.string()),
            },
            schema=PQ_SCHEMA,
        )
        pq.write_table(table, out_path, compression='snappy')
        return ('ok', dly_path, len(dates))

    except Exception as exc:
        return ('error', dly_path, str(exc)[:120])


# ---------------------------------------------------------------------------
# Phase 1: Convert .dly → parquet (Pool(N_WORKERS))
# ---------------------------------------------------------------------------

def phase1_dly(n_workers: int):
    os.makedirs(OUT_DIR, exist_ok=True)

    # Gather US .dly files from DLY_DIR
    dly_files = []
    for fn in os.listdir(DLY_DIR):
        if not fn.endswith('.dly'):
            continue
        if fn[:2].upper() not in US_PREFIXES:
            continue          # skip non-US (shouldn't exist after inventory filter, but be safe)
        stem     = os.path.splitext(fn)[0].upper()   # normalise case
        out_path = os.path.join(OUT_DIR, stem + '.parquet')
        if os.path.exists(out_path):
            continue          # resume-safe: skip already done
        dly_files.append((os.path.join(DLY_DIR, fn), out_path))

    total = len(dly_files)
    print(f"  .dly files to convert: {total:,}  (workers={n_workers})")
    if total == 0:
        print("  All files already converted.")
        return 0, 0, 0, 0

    t0 = time.time()
    done = skipped = errors = total_rows = 0
    log_errors = []

    # Use chunksize ~50 so workers stay busy without memory bloat
    chunksize = max(1, min(100, total // (n_workers * 4)))

    with Pool(n_workers) as pool:
        for result in pool.imap_unordered(convert_dly, dly_files, chunksize=chunksize):
            if result[0] == 'ok':
                done += 1
                total_rows += result[2]
            elif result[0] == 'skipped':
                skipped += 1
            else:
                errors += 1
                log_errors.append(result)

            processed = done + skipped + errors
            if processed % 5_000 == 0:
                elapsed = time.time() - t0
                rate    = processed / elapsed if elapsed > 0 else 0
                eta     = (total - processed) / rate if rate > 0 else 0
                print(f"  [{processed:>6,}/{total:,}]  converted={done:,}  "
                      f"skipped={skipped:,}  errors={errors}  "
                      f"elapsed={elapsed:.0f}s  eta={eta:.0f}s",
                      flush=True)

    elapsed = time.time() - t0
    print(f"\n  Phase 1 complete in {elapsed:.0f}s  "
          f"({done+skipped+errors:,} processed, {errors} errors)")
    if log_errors:
        for e in log_errors[:5]:
            print(f"    ERROR: {os.path.basename(e[1])}: {e[2]}")

    return done, skipped, errors, total_rows


# ---------------------------------------------------------------------------
# Phase 2: Convert state PRCP .csv.gz → parquet
# ---------------------------------------------------------------------------

def phase2_state_prcp():
    os.makedirs(STATE_OUT, exist_ok=True)
    con = duckdb.connect()

    states = sorted(
        d for d in os.listdir(STATE_ROOT)
        if os.path.isdir(os.path.join(STATE_ROOT, d)) and len(d) == 2
    )

    converted = 0
    total_rows = 0
    t0 = time.time()

    for state in states:
        gz_path  = os.path.join(STATE_ROOT, state, f"{state}_raw_daily_PRCP.csv.gz")
        out_path = os.path.join(STATE_OUT, f"{state}_prcp.parquet")

        if not os.path.exists(gz_path):
            continue
        if os.path.exists(out_path):
            continue   # resume-safe

        try:
            # Force mflag/qflag/sflag as VARCHAR — DuckDB auto-detects BOOLEAN
            # from the first 20K sample rows (where mflag is empty/null), then
            # fails when it encounters 'P', 'T', etc. later in the file.
            con.execute(f"""
                COPY (
                    SELECT station_id,
                           date::DATE          AS date,
                           value_raw,
                           missing,
                           mflag::VARCHAR      AS mflag,
                           qflag::VARCHAR      AS qflag,
                           sflag::VARCHAR      AS sflag,
                           prcp_mm,
                           state,
                           station_name,
                           latitude,
                           longitude,
                           elevation
                    FROM read_csv_auto(
                        '{gz_path}',
                        types={{'mflag': 'VARCHAR', 'qflag': 'VARCHAR', 'sflag': 'VARCHAR'}}
                    )
                )
                TO '{out_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
            """)
            rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_path}')").fetchone()[0]
            total_rows += rows
            converted += 1
            print(f"  {state}: {rows:,} rows → {os.path.basename(out_path)}")
        except Exception as e:
            print(f"  {state}: ERROR — {e}")

    con.close()
    elapsed = time.time() - t0
    print(f"\n  Phase 2 complete: {converted} states, {total_rows:,} rows in {elapsed:.1f}s")
    return converted, total_rows


# ---------------------------------------------------------------------------
# Phase 3: Update file_inventory
# ---------------------------------------------------------------------------

def phase3_update_inventory(dly_done: int):
    print("\nUpdating file_inventory.duckdb ...")
    con = duckdb.connect(INV_DB)

    # Walk output dirs and collect new parquets
    new_pq = []
    for fn in os.listdir(OUT_DIR):
        if fn.endswith('.parquet'):
            fp = os.path.join(OUT_DIR, fn)
            new_pq.append((fp, fn, os.path.getsize(fp)))

    for fn in os.listdir(STATE_OUT):
        if fn.endswith('.parquet'):
            fp = os.path.join(STATE_OUT, fn)
            new_pq.append((fp, fn, os.path.getsize(fp)))

    # Insert new parquet rows (skip if already present)
    INSERT_SQL = """
        INSERT OR IGNORE INTO file_inventory
          (filepath, filename, extension, file_size_bytes,
           source, subfolder, schema_group,
           station_id_raw, id_format, date_part,
           is_data_file, anomaly_flag)
        VALUES (?, ?, '.parquet', ?, 'GHCN_daily', ?, ?, ?, 'GHCND_DLY', NULL, true, NULL)
    """
    batch = []
    for fp, fn, size in new_pq:
        stem      = os.path.splitext(fn)[0]
        subfolder = 'ghcnd_all_parquet' if 'ghcnd_all_parquet' in fp else 'state_parquet'
        sg        = f"GHCN_daily|{subfolder}|.parquet"
        station   = stem if not stem.endswith('_prcp') else None
        batch.append((fp, fn, size, subfolder, sg, station))

    if batch:
        con.executemany(INSERT_SQL, batch)

    total = con.execute("SELECT COUNT(*) FROM file_inventory").fetchone()[0]
    ghcn  = con.execute(
        "SELECT COUNT(*) FROM file_inventory WHERE source='GHCN_daily'"
    ).fetchone()[0]
    con.close()

    print(f"  Added {len(batch):,} parquet entries to inventory")
    print(f"  Total inventory rows: {total:,}  (GHCN_daily: {ghcn:,})")
    return len(batch)


# ---------------------------------------------------------------------------
# Final disk summary
# ---------------------------------------------------------------------------

def print_summary(done, skipped, errors, dly_rows,
                  state_converted, state_rows, inv_added):
    print("\n" + "=" * 60)
    print("=== GHCN-D CONVERSION COMPLETE ===")
    print("=" * 60)

    # .dly sizes
    dly_gb = 0.0
    for fn in os.listdir(DLY_DIR):
        if fn.endswith('.dly') and fn[:2].upper() in US_PREFIXES:
            dly_gb += os.path.getsize(os.path.join(DLY_DIR, fn))
    dly_gb /= 1e9

    # parquet sizes
    pq_gb = sum(
        os.path.getsize(os.path.join(OUT_DIR, fn))
        for fn in os.listdir(OUT_DIR) if fn.endswith('.parquet')
    ) / 1e9

    state_gb = sum(
        os.path.getsize(os.path.join(STATE_OUT, fn))
        for fn in os.listdir(STATE_OUT) if fn.endswith('.parquet')
    ) / 1e9

    ratio = dly_gb / pq_gb if pq_gb > 0 else 0

    print(f"\n  .dly → parquet:")
    print(f"    Converted:         {done:>8,} files  ({dly_rows/1e6:.1f}M rows)")
    print(f"    Skipped (no data): {skipped:>8,} files")
    print(f"    Errors:            {errors:>8,} files")
    print(f"    Original .dly:      {dly_gb:>7.2f} GB  (US stations)")
    print(f"    New parquet:        {pq_gb:>7.2f} GB  (PRCP/TMAX/TMIN/TAVG only)")
    print(f"    Compression ratio:  {ratio:>7.1f}x")
    print(f"    Space to reclaim:   {dly_gb - pq_gb:>7.2f} GB  (if .dly deleted)")
    print(f"\n  State PRCP parquets:")
    print(f"    States converted:  {state_converted:>8,}")
    print(f"    Total rows:        {state_rows:>8,}")
    print(f"    Parquet size:       {state_gb:>7.3f} GB")
    print(f"\n  Inventory: {inv_added:,} new rows added")
    print(f"\n  NOTE: .dly files NOT deleted. Delete {dly_gb:.2f} GB when ready.")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    n_workers = min(cpu_count(), 10)   # M4 Pro has 10 cores — use all
    print(f"GHCN-D .dly → parquet conversion")
    print(f"Workers: {n_workers}  |  Output: {OUT_DIR}")
    print(f"Elements kept: {sorted(KEEP_ELEMENTS)}\n")

    # Phase 1: .dly → parquet (parallel)
    print("=" * 60)
    print("PHASE 1: Converting .dly files")
    print("=" * 60)
    done, skipped, errors, dly_rows = phase1_dly(n_workers)

    # Phase 2: state PRCP .csv.gz → parquet (sequential, only 51 files)
    print("\n" + "=" * 60)
    print("PHASE 2: Converting state PRCP .csv.gz files")
    print("=" * 60)
    state_converted, state_rows = phase2_state_prcp()

    # Phase 3: update inventory
    print("\n" + "=" * 60)
    print("PHASE 3: Updating file_inventory")
    print("=" * 60)
    inv_added = phase3_update_inventory(done)

    # Summary
    print_summary(done, skipped, errors, dly_rows,
                  state_converted, state_rows, inv_added)


if __name__ == '__main__':
    main()
