import zipfile, pandas as pd, sqlite3
from pathlib import Path

BASE      = Path.home() / "Form5500"
DOWNLOADS = BASE / "downloads"
DB        = BASE / "output" / "form5500.db"

SCHEDULES = [
    "H", "I", "A", "A_PART1",
    "C", "C_PART1_ITEM1", "C_PART1_ITEM2", "C_PART1_ITEM3",
    "C_PART2", "C_PART3",
    "D", "D_PART1", "D_PART2", "DCG",
    "G", "G_PART1", "G_PART2", "G_PART3",
    "MB", "SB"
]

conn = sqlite3.connect(DB)

for sched in SCHEDULES:
    all_dfs = []
    for year in range(2020, 2026):
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
            df["FORM_YEAR"] = year
            all_dfs.append(df)
            print(f"  [{year}] SCH_{sched}: {len(df):,} rows")
        except Exception as e:
            print(f"  [{year}] SCH_{sched} error: {e}")

    if not all_dfs:
        print(f"SCH_{sched}: no data found, skipping.")
        continue

    combined = pd.concat(all_dfs, ignore_index=True)
    table    = f"sch_{sched.lower()}"
    combined.to_sql(table, conn, if_exists="replace", index=False, chunksize=10000)

    # Index on ACK_ID for fast joins
    if "ACK_ID" in combined.columns:
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_ack ON {table}(ACK_ID)")
    conn.commit()
    print(f"SCH_{sched} saved to table '{table}' — {len(combined):,} total rows.")

conn.close()
print("\nAll schedules loaded into database!")
