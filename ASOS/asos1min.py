import os
import requests
import urllib.parse
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- CONFIGURATION ---
START_YEAR = 2000
END_YEAR = 2022
BASE_ROOT_URL = "https://www.ncei.noaa.gov/pub/data/asos-onemin/"

DOWNLOAD_DIR = "asos_raw_downloads"
PARQUET_ROOT_DIR = "asos_1min_parquet_master"
MAX_CORES = 9  # Optimized for M4 Pro

def setup_base_directories():
    """Creates the main folders for downloads and output."""
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
    if not os.path.exists(PARQUET_ROOT_DIR):
        os.makedirs(PARQUET_ROOT_DIR)

def get_asos_links_for_year(year):
    """Fetches all .dat file links for a specific year's directory."""
    # NOAA folders are named 6406-YYYY
    url = f"{BASE_ROOT_URL}6406-{year}/"
    print(f"\n--- Checking {year} archive at {url} ---")
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        links = [urllib.parse.urljoin(url, a.get('href')) for a in soup.find_all('a') 
                 if a.get('href') and a.get('href').endswith('.dat')]
        return links
    except Exception as e:
        print(f"Skipping year {year}: Could not connect or folder not found. ({e})")
        return []

def process_single_asos(file_url, year_output_dir):
    """Parses one month-station file and saves it to the year's specific folder."""
    filename = file_url.split('/')[-1]
    local_path = os.path.join(DOWNLOAD_DIR, filename)
    out_file = os.path.join(year_output_dir, filename.replace('.dat', '.parquet'))
    
    # Checkpoint: Skip if this station-month is already done
    if os.path.exists(out_file):
        return None

    # 1. Download
    try:
        with requests.get(file_url, stream=True, timeout=15) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception:
        return f"Fail: {filename} (Download)"

    # 2. Parse (Space-delimited based on DSI-6406 documentation)
    parsed_data = []
    try:
        with open(local_path, 'r') as f:
            for line in f:
                parts = line.split()
                if len(parts) < 10: continue 
                
                try:
                    # IDs are in the first block, Timestamps in the second
                    wban, icao = parts[0][:5], parts[0][5:]
                    faa, date_str, time_utc = parts[1][:3], parts[1][3:11], parts[1][15:19]
                    
                    parsed_data.append([
                        wban, icao, faa, date_str, time_utc, 
                        parts[2], parts[3], parts[8], parts[9]
                    ])
                except Exception: continue
                    
        if not parsed_data:
            if os.path.exists(local_path): os.remove(local_path)
            return None

        cols = ['wban', 'icao', 'faa', 'date_str', 'time_utc', 'precip_id', 'precip_1min_in', 'temp_F', 'dew_F']
        df = pd.DataFrame(parsed_data, columns=cols)
        
        # Convert types and build timestamp
        df['datetime_utc'] = pd.to_datetime(df['date_str'] + df['time_utc'], format='%Y%m%d%H%M', errors='coerce')
        df['precip_1min_in'] = pd.to_numeric(df['precip_1min_in'], errors='coerce')
        df['temp_F'] = pd.to_numeric(df['temp_F'], errors='coerce')
        
        # 3. Save to specific Year Folder
        df[['wban', 'icao', 'datetime_utc', 'precip_id', 'precip_1min_in', 'temp_F']].to_parquet(out_file, index=False)
        
        if os.path.exists(local_path): os.remove(local_path)
        return filename
        
    except Exception as e:
        if os.path.exists(local_path): os.remove(local_path)
        return f"Error: {filename} - {e}"

def main():
    setup_base_directories()
    
    # Loop through every year from 2000 to 2022
    for year in range(START_YEAR, END_YEAR + 1):
        # Create a specific sub-folder for this year
        year_dir = os.path.join(PARQUET_ROOT_DIR, str(year))
        os.makedirs(year_dir, exist_ok=True)
        
        file_links = get_asos_links_for_year(year)
        if not file_links:
            continue
            
        print(f"Processing {len(file_links)} files for year {year}...")
        
        # Parallel processing for the current year
        with ProcessPoolExecutor(max_workers=MAX_CORES) as executor:
            # We pass the specific 'year_dir' to the worker
            futures = {executor.submit(process_single_asos, link, year_dir): link for link in file_links}
            
            count = 0
            for future in as_completed(futures):
                count += 1
                res = future.result()
                if count % 200 == 0:
                    print(f"[{year}] Progress: {count}/{len(file_links)} files finished.")

    print("\nALL YEARS (2000-2022) COMPLETED.")

if __name__ == '__main__':
    main()