from pathlib import Path
import json, requests
from datetime import datetime, date

BASE        = Path.home() / "Form5500"
DOWNLOADS   = BASE / "downloads"
OUTPUT      = BASE / "output"
LOGS        = BASE / "logs"
CACHE_FILE  = BASE / "cache_meta.json"

for d in [DOWNLOADS, OUTPUT, LOGS]:
    d.mkdir(parents=True, exist_ok=True)

CURRENT_YEAR = datetime.now().year
START_YEAR   = 2020

def dataset_url(year):
    return f"https://askebsa.dol.gov/FOIA Files/{year}/Latest/F_5500_{year}_Latest.zip"

def load_cache():
    return json.loads(CACHE_FILE.read_text()) if CACHE_FILE.exists() else {}

def save_cache(meta):
    CACHE_FILE.write_text(json.dumps(meta, indent=2))

def download_file(year):
    url      = dataset_url(year)
    zip_path = DOWNLOADS / f"form5500_{year}.zip"
    print(f"Downloading {year} from {url}")
    r = requests.get(url, stream=True, timeout=600)
    r.raise_for_status()
    with open(zip_path, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)
    print(f"  Saved: {zip_path}")
    return url

def run_pipeline():
    cache = load_cache()
    for year in range(START_YEAR, CURRENT_YEAR + 1):
        zip_path = DOWNLOADS / f"form5500_{year}.zip"
        if zip_path.exists():
            print(f"[{year}] Already downloaded, skipping.")
            continue
        print(f"Downloading {year}...")
        try:
            url = download_file(year)
            cache[str(year)] = {"downloaded_on": str(date.today()), "url": url}
        except Exception as e:
            print(f"[{year}] Failed: {e}")
    save_cache(cache)
    print("Done.")

if __name__ == "__main__":
    run_pipeline()
