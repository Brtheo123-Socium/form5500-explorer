import threading, time, requests, zipfile, sqlite3, pandas as pd, json, os
from pathlib import Path
from datetime import datetime, date

DB        = Path(os.environ.get("DB_PATH", "/data/form5500.db"))
BASE      = Path("/data")
DOWNLOADS = BASE / "downloads"
CACHE     = BASE / "cache_meta.json"

try:
    DOWNLOADS.mkdir(parents=True, exist_ok=True)
except:
    pass

def dataset_url(year):
    return f"https://askebsa.dol.gov/FOIA Files/{year}/Latest/F_5500_{year}_Latest.zip"

def load_cache():
    try:
        return json.loads(CACHE.read_text()) if CACHE.exists() else {}
    except:
        return {}

def save_cache(meta):
    try:
        CACHE.write_text(json.dumps(meta, indent=2))
    except:
        pass

def download_if_new(year):
    url      = dataset_url(year)
    zip_path = DOWNLOADS / f"form5500_{year}.zip"
    cache    = load_cache()
    entry    = cache.get(str(year), {})
    try:
        r = requests.head(url, timeout=15)
        remote_lm = r.headers.get("Last-Modified")
    except:
        return False
    if zip_path.exists():
        if year != datetime.now().year and remote_lm == entry.get("last_modified"):
            return False
        if year == datetime.now().year and entry.get("downloaded_on") == str(date.today()):
            return False
    print(f"[scheduler] Downloading {year}...")
    try:
        r = requests.get(url, stream=True, timeout=600)
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        cache[str(year)] = {"downloaded_on": str(date.today()), "last_modified": remote_lm}
        save_cache(cache)
        print(f"[scheduler] {year} downloaded.")
        return True
    except Exception as e:
        print(f"[scheduler] {year} failed: {e}")
        return False

def rebuild_db():
    print("[scheduler] Rebuilding database...")
    conn = sqlite3.connect(DB)
    current_year = datetime.now().year
    first = True
    for year in range(2020, current_year + 1):
        zip_path = DOWNLOADS / f"form5500_{year}.zip"
        if not zip_path.exists():
            continue
        try:
            with zipfile.ZipFile(zip_path) as zf:
                csv_files = [n for n in zf.namelist() if n.endswith(".csv") and "F_5500_" in n.upper()]
                if not csv_files:
                    csv_files = [n for n in zf.namelist() if n.endswith(".csv")][:1]
                if not csv_files:
                    continue
                with zf.open(csv_files[0]) as f:
                    df = pd.read_csv(f, dtype=str, low_memory=False, encoding="latin-1", on_bad_lines="skip")
            df["FORM_YEAR"] = year
            slim_cols = ["ACK_ID","FORM_YEAR","PLAN_NAME","SPONSOR_DFE_NAME","SPONS_DFE_EIN",
                         "SPONS_DFE_MAIL_US_CITY","SPONS_DFE_MAIL_US_STATE","SPONS_DFE_MAIL_US_ZIP",
                         "NET_ASSETS_EOY_AMT","TOT_PARTCP_BOY_CNT","TOT_ACTIVE_PARTCP_CNT",
                         "FILING_STATUS","DATE_RECEIVED","TYPE_PLAN_ENTITY_CD","BUSINESS_CODE"]
            cols = [c for c in slim_cols if c in df.columns]
            df   = df[cols]
            for c in ["NET_ASSETS_EOY_AMT","TOT_PARTCP_BOY_CNT","TOT_ACTIVE_PARTCP_CNT"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            df["FORM_YEAR"] = pd.to_numeric(df["FORM_YEAR"], errors="coerce").astype("Int16")
            df = df.where(pd.notnull(df), None)
            df.to_sql("filings", conn, if_exists="replace" if first else "append", index=False, chunksize=10000)
            first = False
            print(f"[scheduler] {year} loaded — {len(df):,} rows.")
        except Exception as e:
            print(f"[scheduler] {year} error: {e}")
    for idx, col in [("idx_plan","PLAN_NAME"),("idx_sponsor","SPONSOR_DFE_NAME"),
                     ("idx_state","SPONS_DFE_MAIL_US_STATE"),("idx_year","FORM_YEAR"),
                     ("idx_aum","NET_ASSETS_EOY_AMT"),("idx_parts","TOT_PARTCP_BOY_CNT")]:
        conn.execute(f"CREATE INDEX IF NOT EXISTS {idx} ON filings({col})")
    conn.commit()
    conn.close()
    print("[scheduler] Rebuild complete.")

def run_daily():
    current_year = datetime.now().year
    updated = any(download_if_new(y) for y in range(2020, current_year + 1))
    if updated:
        rebuild_db()
    else:
        print("[scheduler] All data up to date.")

def start_scheduler():
    def loop():
        time.sleep(60)
        while True:
            try:
                print(f"[scheduler] Daily refresh at {datetime.now()}")
                run_daily()
            except Exception as e:
                print(f"[scheduler] Error: {e}")
            time.sleep(86400)
    t = threading.Thread(target=loop, daemon=True)
    t.start()
    print("[scheduler] Started.")
