import requests, zipfile, pandas as pd
from pathlib import Path

BASE      = Path.home() / "Form5500"
DOWNLOADS = BASE / "downloads"

SCHEDULES = [
    "H", "H_Q5B", "I", "I_Q5B",
    "A", "A_PART1",
    "C", "C_PART1_ITEM1", "C_PART1_ITEM2", "C_PART1_ITEM2_SRVC_CODES",
    "C_PART1_ITEM3", "C_PART1_ITEM3_SRVC_CODES", "C_PART2", "C_PART2_SRVC_CODES", "C_PART3",
    "D", "D_PART1", "D_PART2",
    "DCG",
    "G", "G_PART1", "G_PART2", "G_PART3",
    "R_PARTS1_4_6", "R_PART5",
    "MB", "MB_Q3", "MB_Q7", "MB_ACTIVE_PARTICIPANT_DATA",
    "SB"
]

for year in range(2020, 2026):
    for sched in SCHEDULES:
        out_path = DOWNLOADS / f"form5500_{year}_sch{sched}.zip"
        if out_path.exists():
            print(f"[{year}] SCH_{sched} already exists, skipping.")
            continue
        url = f"https://askebsa.dol.gov/FOIA Files/{year}/Latest/F_SCH_{sched}_{year}_Latest.zip"
        print(f"Downloading [{year}] SCH_{sched}...")
        try:
            r = requests.get(url, stream=True, timeout=120)
            r.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
            print(f"  Saved.")
        except Exception as e:
            print(f"  Failed: {e}")

print("\nDownloads done! Now merging into master file...")

main = pd.read_parquet(BASE / "output" / "master_5500.parquet")
original_cols = [c for c in main.columns if not c.endswith(tuple([f"_SCH_{s}" for s in SCHEDULES]))]
main = main[original_cols]

for year in range(2020, 2026):
    for sched in SCHEDULES:
        zip_path = DOWNLOADS / f"form5500_{year}_sch{sched}.zip"
        if not zip_path.exists():
            continue
        try:
            with zipfile.ZipFile(zip_path) as zf:
                csv_files = [n for n in zf.namelist() if n.endswith(".csv")]
                if not csv_files:
                    continue
                with zf.open(csv_files[0]) as f:
                    df = pd.read_csv(f, dtype=str, low_memory=False,
                                     encoding="latin-1", on_bad_lines="skip")
            if "ACK_ID" not in df.columns:
                continue
            rename = {c: f"{c}_SCH_{sched}" for c in df.columns if c != "ACK_ID"}
            df = df.rename(columns=rename)
            main = main.merge(df, on="ACK_ID", how="left")
            print(f"  [{year}] SCH_{sched} merged — {len(df):,} rows.")
        except Exception as e:
            print(f"  [{year}] SCH_{sched} error: {e}")

# Coalesce key financial fields
for base_col, sources in [
    ("NET_ASSETS_EOY_AMT",   ["NET_ASSETS_EOY_AMT_SCH_H", "NET_ASSETS_EOY_AMT_SCH_I"]),
    ("TOTAL_ASSETS_EOY_AMT", ["TOTAL_ASSETS_EOY_AMT_SCH_H", "TOTAL_ASSETS_EOY_AMT_SCH_I"]),
    ("TOTAL_INCOME_AMT",     ["TOTAL_INCOME_AMT_SCH_H"]),
    ("TOTAL_EXPENSES_AMT",   ["TOTAL_EXPENSES_AMT_SCH_H"]),
]:
    for sc in sources:
        if sc in main.columns:
            main[sc] = pd.to_numeric(main[sc], errors="coerce")
    if base_col not in main.columns:
        for sc in sources:
            if sc in main.columns:
                main[base_col] = main[sc]
                break
    else:
        main[base_col] = pd.to_numeric(main[base_col], errors="coerce")
        for sc in sources:
            if sc in main.columns:
                main[base_col] = main[base_col].fillna(main[sc])

main.to_parquet(BASE / "output" / "master_5500.parquet", index=False)
print(f"\nDone! {len(main):,} rows, {len(main.columns)} total columns.")
