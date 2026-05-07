import os
import requests
import urllib.parse
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- CONFIGURATION ---
START_YEAR = 2000
END_YEAR = 2026
BASE_ROOT_URL = "https://www.ncei.noaa.gov/pub/data/asos-onemin/"

# Directory structure
DOWNLOAD_DIR = "data/ASOS/1min/raw_temp"
PARQUET_ROOT_DIR = "data/ASOS/1min/parquet_final"

# Full capacity for M4 Pro
MAX_CORES = os.cpu_count() 

def setup_base_directories():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(PARQUET_ROOT_DIR, exist_ok=True)

def fetch_us_station_metadata():
    """Downloads master history, filters for US, and handles duplicate IDs."""
    print("Fetching and cleaning NOAA Master Station History for USA filtering...")
    url = "https://www1.ncdc.noaa.gov/pub/data/noaa/isd-history.csv"
    try:
        # We include 'END' to sort by the most recent record
        df = pd.read_csv(url, dtype={'WBAN': str, 'ICAO': str, 'CTRY': str}, 
                         usecols=['WBAN', 'ICAO', 'LAT', 'LON', 'CTRY', 'END'])
        
        # Filter for US stations
        df_us = df[df['CTRY'] == 'US'].copy()
        df_us['WBAN'] = df_us['WBAN'].astype(str).str.zfill(5)
        
        # Sort by 'END' date so the most recent station location is kept
        df_us = df_us.sort_values(by='END', ascending=False)
        
        # Drop duplicates to ensure unique keys for the dictionary
        # We prioritize ICAO for the main lookup used in filtering
        df_icao = df_us.dropna(subset=['ICAO', 'LAT', 'LON']).drop_duplicates(subset=['ICAO'])
        df_wban = df_us.dropna(subset=['WBAN', 'LAT', 'LON']).drop_duplicates(subset=['WBAN'])
        
        icao_map = df_icao.set_index('ICAO')[['LAT', 'LON']].to_dict('index')
        wban_map = df_wban.set_index('WBAN')[['LAT', 'LON']].to_dict('index')
        
        print(f"Metadata ready. Found {len(icao_map)} unique US stations.")
        return wban_map, icao_map
    except Exception as e:
        print(f"Metadata error: {e}. Filtering will be skipped.")
        # Add this at the end of fetch_us_station_metadata(), before the return:
        meta_df = df_us.dropna(subset=['WBAN','ICAO','LAT','LON']).drop_duplicates(subset=['ICAO'])
        meta_df.to_parquet("data/ASOS/1min/station_metadata.parquet", index=False)
        return {}, {}


def get_year_links(year):
    url = f"{BASE_ROOT_URL}6406-{year}/"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'lxml')
        return [urllib.parse.urljoin(url, a.get('href')) for a in soup.find_all('a') 
                if a.get('href') and a.get('href').endswith('.dat')]
    except Exception:
        return []

def process_file(file_url, year_dir, wban_map, icao_map):
    filename = file_url.split('/')[-1]
    
    # 1. Early Exit: US Only Filter
    # Identifies ICAO from filename (e.g., 64060KNYC...)
    icao_code = filename[5:9]
    if icao_map and icao_code not in icao_map:
        return None 

    local_path = os.path.join(DOWNLOAD_DIR, filename)
    out_file = os.path.join(year_dir, filename.replace('.dat', '.parquet'))
    
    if os.path.exists(out_file):
        return None

    try:
        # 2. Download
        with requests.get(file_url, stream=True, timeout=15) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=65536): 
                    f.write(chunk)
        
        # 3. Parse Page 2 Data
        data = []
        with open(local_path, 'r') as f:
            for line in f:
                p = line.split()
                if len(p) < 10: continue
                # p[0]: WBAN/ICAO, p[1]: Timestamp, p[2]: PrecipID, p[3]: PrecipAmt, p[8]: Temp, p[9]: DewPoint
                data.append([p[0][:5], p[0][5:], p[1][3:11], p[1][15:19], p[2], p[3], p[8], p[9]])

        if not data:
            if os.path.exists(local_path): os.remove(local_path)
            return None

        df = pd.DataFrame(data, columns=['wban', 'icao', 'date', 'time', 'precip_id', 'precip_in', 'temp_f', 'dew_f'])
        
        # 4. Map Coordinates
        def get_coords(row):
            if row['icao'] in icao_map:
                return pd.Series([icao_map[row['icao']]['LAT'], icao_map[row['icao']]['LON']])
            if row['wban'] in wban_map:
                return pd.Series([wban_map[row['wban']]['LAT'], wban_map[row['wban']]['LON']])
            return pd.Series([None, None])

        df[['lat', 'lon']] = df.apply(get_coords, axis=1)
        
        # 5. Conversion & Storage
        df['timestamp_utc'] = pd.to_datetime(df['date'] + df['time'], format='%Y%m%d%H%M', errors='coerce')
        for col in ['precip_in', 'temp_f', 'dew_f']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df[['wban', 'icao', 'lat', 'lon', 'timestamp_utc', 'precip_id', 'precip_in', 'temp_f', 'dew_f']].to_parquet(out_file)
        
        # 6. Cleanup
        if os.path.exists(local_path): os.remove(local_path)
        return filename

    except Exception:
        if os.path.exists(local_path): os.remove(local_path)
        return None

def main():
    setup_base_directories()
    wban_map, icao_map = fetch_us_station_metadata()
    
    for year in range(START_YEAR, END_YEAR + 1):
        year_dir = os.path.join(PARQUET_ROOT_DIR, str(year))
        os.makedirs(year_dir, exist_ok=True)
        
        links = get_year_links(year)
        if not links: continue
        
        # Pre-filter: skip files already downloaded
        todo = []
        for link in links:
            filename = link.split('/')[-1]
            out_file = os.path.join(year_dir, filename.replace('.dat', '.parquet'))
            if not os.path.exists(out_file):
                todo.append(link)
        
        if not todo:
            print(f"--- Year {year} --- All {len(links)} files done. Skipping.")
            continue
        
        print(f"\n--- Year {year} --- {len(todo)} new / {len(links)} total")
        
        with ProcessPoolExecutor(max_workers=MAX_CORES) as executor:
            futures = [executor.submit(process_file, link, year_dir, wban_map, icao_map) for link in todo]
            
            done = 0
            for future in as_completed(futures):
                done += 1
                if done % 500 == 0:
                    print(f"[{year}] Processed {done}/{len(todo)} files.")

    print("\nProcessing complete. Files are in data/ASOS/1min/parquet_final/")

if __name__ == "__main__":
    main()