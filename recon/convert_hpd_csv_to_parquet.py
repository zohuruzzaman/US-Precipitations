"""
convert_hpd_csv_to_parquet.py
Convert HPD 1-hr CSV files to snappy-compressed parquet, one at a time.
For each file:  read CSV → write .parquet → verify row count → delete CSV.
Updates file_inventory.duckdb on completion.
"""

import os
import sys
import time
import duckdb

BASE    = os.path.expanduser("~/Documents/WeatherData")
RECON   = os.path.join(BASE, "recon")
INV_DB  = os.path.join(RECON, "file_inventory.duckdb")
LOG     = os.path.join(RECON, "hpd_conversion_errors.txt")


def main():
    t0 = time.time()

    # ── Gather file list from file_inventory ──────────────────────────────────
    inv = duckdb.connect(INV_DB, read_only=True)
    csv_files = inv.execute("""
        SELECT filepath, file_size_bytes
        FROM file_inventory
        WHERE source = 'HPD_1hr' AND extension = '.csv'
        ORDER BY filepath
    """).fetchall()
    inv.close()

    total_files = len(csv_files)
    total_csv_bytes = sum(r[1] for r in csv_files)
    print(f"HPD CSVs to convert: {total_files:,}  ({total_csv_bytes/1e9:.3f} GB)")
    print(f"Log: {LOG}\n")

    errors = []
    converted = 0
    skipped = 0
    total_parquet_bytes = 0
    total_deleted_bytes = 0

    con = duckdb.connect()  # in-memory for conversions

    for i, (csv_path, csv_size) in enumerate(csv_files, 1):
        stem     = os.path.splitext(csv_path)[0]
        pq_path  = stem + ".parquet"

        # Skip if parquet already exists (resume-safe)
        if os.path.exists(pq_path) and not os.path.exists(csv_path):
            skipped += 1
            continue

        if not os.path.exists(csv_path):
            skipped += 1
            continue

        try:
            # Step 1: count CSV rows
            csv_rows = con.execute(
                f"SELECT COUNT(*) FROM read_csv_auto('{csv_path}', ignore_errors=true)"
            ).fetchone()[0]

            # Step 2: write parquet (snappy)
            con.execute(f"""
                COPY (SELECT * FROM read_csv_auto('{csv_path}', ignore_errors=true))
                TO '{pq_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
            """)

            # Step 3: verify row count
            pq_rows = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{pq_path}')"
            ).fetchone()[0]

            if pq_rows != csv_rows:
                msg = f"ROW MISMATCH {csv_path}: csv={csv_rows} pq={pq_rows}"
                errors.append(msg)
                os.remove(pq_path)  # clean up bad parquet
                with open(LOG, "a") as lf:
                    lf.write(msg + "\n")
                continue

            # Step 4: delete CSV, record sizes
            pq_size = os.path.getsize(pq_path)
            os.remove(csv_path)

            total_parquet_bytes += pq_size
            total_deleted_bytes += csv_size
            converted += 1

        except Exception as e:
            msg = f"ERROR {csv_path}: {e}"
            errors.append(msg)
            with open(LOG, "a") as lf:
                lf.write(msg + "\n")
            # Remove partial parquet if it exists
            if os.path.exists(pq_path):
                try:
                    os.remove(pq_path)
                except OSError:
                    pass
            continue

        if i % 100 == 0 or i == total_files:
            elapsed = time.time() - t0
            rate = converted / elapsed if elapsed > 0 else 0
            eta = (total_files - i) / rate if rate > 0 else 0
            print(f"  [{i:>5}/{total_files}]  converted={converted:,}  "
                  f"errors={len(errors)}  "
                  f"elapsed={elapsed:.0f}s  eta={eta:.0f}s")

    con.close()

    # ── Update file_inventory ─────────────────────────────────────────────────
    print("\nUpdating file_inventory.duckdb...")
    inv = duckdb.connect(INV_DB)

    # Walk the HPD directory to find all new parquets
    hpd_dir = os.path.join(BASE, "HPD_1hr")
    new_pq = []
    for dirpath, _, filenames in os.walk(hpd_dir):
        for fn in filenames:
            if fn.endswith(".parquet"):
                fp = os.path.join(dirpath, fn)
                # Only update rows that previously had .csv extension
                new_pq.append((fp, os.path.getsize(fp)))

    # Update extension and file_size for converted files
    updated = 0
    for pq_path, pq_size in new_pq:
        csv_path = pq_path.replace(".parquet", ".csv")
        n = inv.execute("""
            UPDATE file_inventory
            SET extension='.parquet', file_size_bytes=?, filepath=?
            WHERE filepath=?
        """, [pq_size, pq_path, csv_path]).rowcount
        updated += n

    # Any parquets not already in inventory (shouldn't happen, but safety net)
    existing = set(
        r[0] for r in inv.execute(
            "SELECT filepath FROM file_inventory WHERE source='HPD_1hr' AND extension='.parquet'"
        ).fetchall()
    )
    inv.close()

    elapsed = time.time() - t0
    saved = total_deleted_bytes - total_parquet_bytes
    ratio = total_deleted_bytes / total_parquet_bytes if total_parquet_bytes else 0

    print(f"\n{'='*55}")
    print(f"=== HPD CSV → PARQUET CONVERSION COMPLETE ===")
    print(f"{'='*55}")
    print(f"  Files converted:    {converted:,}")
    print(f"  Files skipped:      {skipped:,}  (already done)")
    print(f"  Errors:             {len(errors):,}")
    print(f"  Original CSV size:  {total_deleted_bytes/1e9:.3f} GB")
    print(f"  New parquet size:   {total_parquet_bytes/1e9:.3f} GB")
    print(f"  Space saved:        {saved/1e9:.3f} GB  ({ratio:.1f}x compression)")
    print(f"  Inventory updated:  {updated:,} rows")
    print(f"  Elapsed:            {elapsed:.1f}s")
    if errors:
        print(f"\n  Errors logged to: {LOG}")
        for e in errors[:5]:
            print(f"    {e}")
    print("=== NO CSVs REMAIN — ALL CONVERTED ===")


if __name__ == "__main__":
    main()
