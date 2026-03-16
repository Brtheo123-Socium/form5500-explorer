import zipfile, pandas as pd
from pathlib import Path

BASE      = Path.home() / "Form5500"
DOWNLOADS = BASE / "downloads"

print("Loading main form data...")
main = pd.read_parquet(BASE / "output" / "master_5500.parquet")

# Keep only original columns, drop any previous schedule merges
main = main[[c for c in main.columns if "_SCH_" not in c]]
print(f"Main form: {len(main):,} rows, {len(main.columns)} cols")

# Only merge H and I — these have the AUM data
for year in range(2020, 2026):
    for sched, asset_col in [("H", "NET_ASSETS_EOY_AMT"), ("I", "NET_ASSETS_EOY_AMT")]:
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
            # Only keep the columns we need
            keep = ["ACK_ID","NET_ASSETS_EOY_AMT","TOTAL_ASSETS_EOY_AMT",
                    "TOTAL_INCOME_AMT","TOTAL_EXPENSES_AMT"]
            keep = [c for c in keep if c in df.columns]
            df = df[keep]
            for c in keep:
                if c != "ACK_ID":
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            suffix = f"_{sched}"
            df = df.rename(columns={c: c+suffix for c in keep if c != "ACK_ID"})
            main = main.merge(df, on="ACK_ID", how="left")

            # Coalesce into clean columns
            for col in ["NET_ASSETS_EOY_AMT","TOTAL_ASSETS_EOY_AMT",
                        "TOTAL_INCOME_AMT","TOTAL_EXPENSES_AMT"]:
                col_s = col + suffix
                if col_s not in main.columns:
                    continue
                if col not in main.columns:
                    main[col] = main[col_s]
                else:
                    main[col] = main[col].fillna(main[col_s])
                main = main.drop(columns=[col_s])

            print(f"  [{year}] SCH_{sched} merged.")
        except Exception as e:
            print(f"  [{year}] SCH_{sched} error: {e}")

main.to_parquet(BASE / "output" / "master_5500.parquet", index=False)
print(f"\nDone! {len(main):,} rows, {len(main.columns)} cols.")
print(f"AUM data filled: {main['NET_ASSETS_EOY_AMT'].notna().sum():,} filings")
