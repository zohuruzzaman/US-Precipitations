"""
process_emshr.py
----------------
Parse, filter, clean, and save the NOAA HOMR EMSHR-Lite fixed-width file.

Outputs:
    data/processed/homr/stations_master.parquet
    data/processed/homr/stations_master.csv
    data/processed/homr/stations_crosswalk.parquet
    data/processed/homr/stations_crosswalk.csv
    logs/homr_summary.txt

Usage:
    python scripts/process_emshr.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT        = Path(__file__).resolve().parent.parent
RAW_FILE    = ROOT / "WEATHERDATA" / "HOMR" / "emshr_lite.txt"
PROC_DIR    = ROOT / "WEATHERDATA" / "HOMR" / "processed"
LOG_DIR     = ROOT / "WEATHERDATA" / "HOMR" / "logs"

# ---------------------------------------------------------------------------
# EMSHR-Lite column specs (0-indexed, half-open)
# ---------------------------------------------------------------------------
COLSPECS = [
    (0,   8),   # NCDC / ncei_id
    (9,  17),   # BEG_DT
    (18, 26),   # END_DT
    (27, 33),   # COOP
    (34, 39),   # WBAN
    (40, 44),   # ICAO
    (45, 50),   # FAA
    (51, 56),   # NWSLI
    (57, 62),   # WMO
    (74, 85),   # GHCND
    (86, 186),  # STATION_NAME
    (187, 189), # CC
    (226, 228), # ST
    (229, 264), # COUNTY
    (272, 281), # LAT_DEC
    (282, 292), # LON_DEC
    (342, 350), # EL_GR_M
    (369, 469), # TYPE
    (525, 536), # HPD
]

RAW_NAMES = [
    "ncdc_id", "begin_date", "end_date",
    "coop_id", "wban_id", "icao_id", "faa_id", "nwsli_id", "wmo_id",
    "ghcnd_id",
    "station_name", "country_code", "state", "county",
    "lat_dec", "lon_dec", "elev_m",
    "station_type", "hpd_id",
]

# Columns whose string values should be treated as IDs (preserved as str)
ID_COLS = ["ncdc_id", "coop_id", "wban_id", "icao_id", "faa_id",
           "nwsli_id", "wmo_id", "ghcnd_id", "hpd_id"]

# US territories to keep even when CC != 'US'
US_TERRITORIES = {"PR", "VI", "GU", "AS", "MP"}


def hdr(text: str) -> None:
    """Print a section header."""
    print(f"\n{'='*60}")
    print(f"  {text}")
    print(f"{'='*60}")


# ---------------------------------------------------------------------------
# Step 0 — Guard
# ---------------------------------------------------------------------------
def step0_guard() -> None:
    hdr("Step 0 — Guard")
    if not RAW_FILE.exists():
        print(f"ERROR: Raw file not found:\n  {RAW_FILE}\n"
              f"Run first:  python scripts/download_emshr.py")
        sys.exit(1)
    PROC_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Raw file : {RAW_FILE}")
    print(f"Proc dir : {PROC_DIR}")


# ---------------------------------------------------------------------------
# Step 1 — Parse fixed-width file
# ---------------------------------------------------------------------------
def step1_parse() -> pd.DataFrame:
    hdr("Step 1 — Parse fixed-width file")
    df = pd.read_fwf(
        RAW_FILE,
        colspecs=COLSPECS,
        names=RAW_NAMES,
        header=None,
        skiprows=1,
        encoding="latin-1",
        dtype=str,
    )

    # Strip whitespace from all string columns; replace empty strings with NaN
    for col in df.columns:
        df[col] = df[col].str.strip()
        df[col] = df[col].replace("", np.nan)

    print(f"Parsed shape: {df.shape}")
    return df


# ---------------------------------------------------------------------------
# Step 2 — Filter USA
# ---------------------------------------------------------------------------
def step2_filter_usa(df: pd.DataFrame) -> pd.DataFrame:
    hdr("Step 2 — Filter USA")
    before = len(df)
    mask = (df["country_code"] == "US") | (df["state"].isin(US_TERRITORIES))
    df = df[mask].copy()
    after = len(df)
    print(f"Rows before filter : {before:,}")
    print(f"Rows after  filter : {after:,}  (kept {after/before:.1%})")
    return df


# ---------------------------------------------------------------------------
# Step 3 — Clean & standardize
# ---------------------------------------------------------------------------
def step3_clean(df: pd.DataFrame) -> pd.DataFrame:
    hdr("Step 3 — Clean & standardize")

    # Rename ncdc_id → ncei_id for clarity
    df = df.rename(columns={"ncdc_id": "ncei_id"})

    # Parse dates (format YYYYMMDD); sentinel 00010101 → NaT
    for col in ("begin_date", "end_date"):
        df[col] = df[col].replace("00010101", np.nan)
        df[col] = pd.to_datetime(df[col], format="%Y%m%d", errors="coerce")

    # Active flag (end_date year == 9999)
    df["is_active"] = df["end_date"].dt.year == 9999

    # Numeric columns
    for col in ("lat_dec", "lon_dec", "elev_m"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Coordinate flag
    df["has_coordinates"] = df["lat_dec"].notna() & df["lon_dec"].notna()

    # Report null counts for ID columns
    id_cols_present = [c for c in ["ncei_id", "ghcnd_id", "coop_id",
                                    "wban_id", "icao_id", "hpd_id"]
                       if c in df.columns]
    print("\nNull counts for key ID columns:")
    for col in id_cols_present:
        n_null = df[col].isna().sum()
        print(f"  {col:<15} {n_null:>8,} null  ({n_null/len(df):.1%})")

    return df


# ---------------------------------------------------------------------------
# Step 4 — Save master table
# ---------------------------------------------------------------------------
def step4_save_master(df: pd.DataFrame) -> None:
    hdr("Step 4 — Save master table")
    parquet_path = PROC_DIR / "stations_master.parquet"
    csv_path     = PROC_DIR / "stations_master.csv"
    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)
    print(f"Saved: {parquet_path}")
    print(f"Saved: {csv_path}")
    print(f"Shape: {df.shape}")


# ---------------------------------------------------------------------------
# Step 5 — Build crosswalk
# ---------------------------------------------------------------------------
def step5_crosswalk(df: pd.DataFrame) -> pd.DataFrame:
    hdr("Step 5 — Build crosswalk")

    # ID columns to melt (exclude ncei_id — it's used as id_var)
    melt_cols = [c for c in ["ghcnd_id", "coop_id", "wban_id",
                              "icao_id", "faa_id", "nwsli_id", "wmo_id", "hpd_id"]
                 if c in df.columns]

    keep_cols = ["ncei_id", "station_name", "state", "is_active"]
    id_vars   = [c for c in keep_cols if c in df.columns]

    xwalk = df[id_vars + melt_cols].melt(
        id_vars=id_vars,
        value_vars=melt_cols,
        var_name="id_type",
        value_name="id_value",
    )

    # Drop rows where the alternate ID is missing
    xwalk = xwalk.dropna(subset=["id_value"])

    # Clean up id_type label: strip "_id" suffix, uppercase
    xwalk["id_type"] = (xwalk["id_type"]
                        .str.removesuffix("_id")
                        .str.upper())

    xwalk = xwalk.reset_index(drop=True)

    parquet_path = PROC_DIR / "stations_crosswalk.parquet"
    csv_path     = PROC_DIR / "stations_crosswalk.csv"
    xwalk.to_parquet(parquet_path, index=False)
    xwalk.to_csv(csv_path, index=False)
    print(f"Saved: {parquet_path}")
    print(f"Saved: {csv_path}")
    print(f"Shape: {xwalk.shape}")
    return xwalk


# ---------------------------------------------------------------------------
# Step 6 — Summary
# ---------------------------------------------------------------------------
def step6_summary(df: pd.DataFrame, xwalk: pd.DataFrame) -> None:
    hdr("Step 6 — Summary")

    lines: list[str] = []
    lines.append("HOMR EMSHR-Lite Processing Summary")
    lines.append("=" * 50)

    total = len(df)
    active = df["is_active"].sum()
    inactive = total - active
    no_coords = (~df["has_coordinates"]).sum()

    lines.append(f"\nTotal stations   : {total:>10,}")
    lines.append(f"Active           : {active:>10,}")
    lines.append(f"Inactive         : {inactive:>10,}")
    lines.append(f"Missing coords   : {no_coords:>10,}")

    lines.append("\nStations with key IDs:")
    for col, label in [("ghcnd_id", "GHCND"), ("coop_id", "COOP"),
                        ("wban_id", "WBAN"),  ("icao_id", "ICAO"),
                        ("hpd_id",  "HPD")]:
        if col in df.columns:
            n = df[col].notna().sum()
            lines.append(f"  {label:<8} {n:>10,}")

    lines.append("\nTop 20 states by station count:")
    top_states = (df.groupby("state", dropna=False)
                    .size()
                    .sort_values(ascending=False)
                    .head(20))
    for state, count in top_states.items():
        lines.append(f"  {str(state):<6} {count:>8,}")

    # Spot-check NYC Central Park (GHCND USW00094728)
    lines.append("\nSpot-check USW00094728 (NYC Central Park):")
    hit = df[df["ghcnd_id"] == "USW00094728"]
    if hit.empty:
        lines.append("  NOT FOUND — check GHCND column parsing")
    else:
        row = hit.iloc[0]
        lines.append(f"  name    : {row.get('station_name', 'N/A')}")
        lines.append(f"  ncei_id : {row.get('ncei_id', 'N/A')}")
        lines.append(f"  state   : {row.get('state', 'N/A')}")
        lines.append(f"  active  : {row.get('is_active', 'N/A')}")
        lines.append(f"  lat/lon : {row.get('lat_dec', 'N/A')} / {row.get('lon_dec', 'N/A')}")

    summary_text = "\n".join(lines)
    print(summary_text)

    summary_path = LOG_DIR / "homr_summary.txt"
    summary_path.write_text(summary_text, encoding="utf-8")
    print(f"\nSummary written: {summary_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    step0_guard()
    df    = step1_parse()
    df    = step2_filter_usa(df)
    df    = step3_clean(df)
    step4_save_master(df)
    xwalk = step5_crosswalk(df)
    step6_summary(df, xwalk)
    hdr("Done")
    print("All steps completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
