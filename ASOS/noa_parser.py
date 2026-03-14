import os
import shutil
import requests
import tarfile
import urllib.parse
import sys
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- CONFIGURATION ---
BASE_URL = "https://www.ncei.noaa.gov/data/global-hourly/archive/csv/"
DOWNLOAD_DIR = "raw_downloads"
PARQUET_ROOT_DIR = "processed_parquet"
TEMP_EXTRACT_DIR = "temp_extracted"
MASTER_INVENTORY_FILE = "master_station_inventory.csv"
MAX_CORES = 9  # Optimized for M4 Pro

# Every file will have exactly these 15 columns
STRICT_COLUMNS = [
    'station_id', 'station_name', 'date', 'latitude', 'longitude', 'elevation_m',
    'temperature_celsius',
    'precip_1_period_hrs', 'precip_1_depth_mm',
    'precip_2_period_hrs', 'precip_2_depth_mm',
    'precip_3_period_hrs', 'precip_3_depth_mm',
    'precip_4_period_hrs', 'precip_4_depth_mm'
]

def setup_directories():
    for folder in [DOWNLOAD_DIR, PARQUET_ROOT_DIR, TEMP_EXTRACT_DIR]:
        os.makedirs(folder, exist_ok=True)

def get_archive_links(url):
    print(f"Fetching archive links from {url}...")
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'lxml')
    return [urllib.parse.urljoin(url, a.get('href')) for a in soup.find_all('a') if a.get('href').endswith('.tar.gz')]

def process_single_csv(file_path, year_dir):
    """Worker function: Parses one CSV and saves it immediately to disk."""
    try:
        station_id_str = os.path.basename(file_path).replace('.csv', '')
        out_path = os.path.join(year_dir, f"{station_id_str}.parquet")
        
        # Skip if already processed
        if os.path.exists(out_path):
            return None 

        available_cols = pd.read_csv(file_path, nrows=0).columns.tolist()
        target_cols = ['STATION', 'DATE', 'LATITUDE', 'LONGITUDE', 'ELEVATION', 'NAME', 'TMP', 'AA1', 'AA2', 'AA3', 'AA4']
        usecols = [c for c in target_cols if c in available_cols]
        
        df = pd.read_csv(file_path, usecols=usecols, dtype=str)
        if df.empty: return None

        # 1. Temperature Parsing
        df['temperature_celsius'] = np.nan
        if 'TMP' in df.columns:
            tmp_split = df['TMP'].str.split(',', n=1, expand=True)
            df['temperature_celsius'] = pd.to_numeric(tmp_split[0], errors='coerce') / 10.0
            df.loc[df['temperature_celsius'] == 99.9, 'temperature_celsius'] = np.nan

        # 2. Precipitation Parsing (AA1-AA4)
        for i in range(1, 5):
            df[f'precip_{i}_period_hrs'] = np.nan
            df[f'precip_{i}_depth_mm'] = np.nan
            col = f'AA{i}'
            if col in df.columns:
                aa_split = df[col].str.split(',', expand=True)
                if aa_split.shape[1] >= 2:
                    df[f'precip_{i}_period_hrs'] = pd.to_numeric(aa_split[0], errors='coerce')
                    df[f'precip_{i}_depth_mm'] = pd.to_numeric(aa_split[1], errors='coerce') / 10.0
                    df.loc[df[f'precip_{i}_depth_mm'] == 999.9, f'precip_{i}_depth_mm'] = np.nan

        # 3. Rename and Normalize
        df = df.rename(columns={'STATION': 'station_id', 'NAME': 'station_name', 'DATE': 'date', 
                                'LATITUDE': 'latitude', 'LONGITUDE': 'longitude', 'ELEVATION': 'elevation_m'})
        
        # 4. Force Strict Schema
        for col in STRICT_COLUMNS:
            if col not in df.columns: df[col] = np.nan
        
        df = df[STRICT_COLUMNS]
        
        # 5. Save station file immediately to clear RAM
        df.to_parquet(out_path, index=False)
        
        # Return tiny metadata row for the master inventory
        return df[['station_id', 'station_name', 'latitude', 'longitude', 'elevation_m']].drop_duplicates().tail(1)

    except Exception:
        return None

def process_year_archive(tar_url):
    filename = os.path.basename(tar_url)
    year = filename.replace('.tar.gz', '')
    ############## delet this block after testing ##############
    year_parquet_dir = os.path.join(PARQUET_ROOT_DIR, year)
    if os.path.exists(year_parquet_dir) and len(os.listdir(year_parquet_dir)) > 0:
        print(f"[{year}] Year already processed. Skipping download and extraction.")
        return # This exits the function immediately for this year
    
    raw_tar_path = os.path.join(DOWNLOAD_DIR, filename)
    extract_path = os.path.join(TEMP_EXTRACT_DIR, year)
    year_parquet_dir = os.path.join(PARQUET_ROOT_DIR, year)
    os.makedirs(year_parquet_dir, exist_ok=True)

    # 1. Download
    if not os.path.exists(raw_tar_path):
        print(f"\n[{year}] Downloading...")
        with requests.get(tar_url, stream=True) as r:
            r.raise_for_status()
            with open(raw_tar_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024): f.write(chunk)

    # 2. Extract
    print(f"[{year}] Extracting archive...")
    os.makedirs(extract_path, exist_ok=True)
    with tarfile.open(raw_tar_path, "r:gz") as tar:
        tar.extractall(path=extract_path)

    # 3. Process Stations
    station_csvs = [os.path.join(root, f) for root, _, files in os.walk(extract_path) for f in files if f.endswith('.csv')]
    print(f"[{year}] Processing {len(station_csvs)} stations directly to disk...")
    
    inventory_list = []
    with ProcessPoolExecutor(max_workers=MAX_CORES) as executor:
        futures = {executor.submit(process_single_csv, csv, year_parquet_dir): csv for csv in station_csvs}
        for future in as_completed(futures):
            meta_df = future.result()
            if meta_df is not None:
                inventory_list.append(meta_df)

    # 4. Update Inventory
    if inventory_list:
        year_inv = pd.concat(inventory_list)
        year_inv['year'] = year
        write_header = not os.path.exists(MASTER_INVENTORY_FILE)
        year_inv.to_csv(MASTER_INVENTORY_FILE, mode='a', header=write_header, index=False)

    # 5. Cleanup
    shutil.rmtree(extract_path)
    if os.path.exists(raw_tar_path): os.remove(raw_tar_path)
    print(f"[{year}] Done.")

def main():
    setup_directories()
    links = get_archive_links(BASE_URL)
    for link in sorted(links):
        process_year_archive(link)
    print("\nAll years completed successfully!")

if __name__ == "__main__":
    main()