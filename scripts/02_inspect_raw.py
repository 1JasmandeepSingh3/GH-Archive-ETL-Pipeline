import argparse
import gzip
import json
from datetime import datetime, timedelta
from pathlib import Path
import duckdb
import requests

RAW_DIR = Path("data/raw")
DB_PATH = Path("data/gh_archive.duckdb")

def get_connection():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute("""
        CREATE TABLE IF NOT EXISTS load_log (
            partition_hour TIMESTAMP PRIMARY KEY,
            loaded_at      TIMESTAMP,
            row_count      BIGINT,
            status         VARCHAR  -- 'success' or 'failed'
        )
    """)
    return con

def hour_range(start_date: str, end_date: str):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start
    while current <= end:
        for hour in range(24):
            yield current.strftime("%Y-%m-%d"), hour
        current += timedelta(days=1)

def already_loaded(con, partition_hour: datetime) -> bool:
    result = con.execute(
        "SELECT status FROM load_log WHERE partition_hour = ?",
        [partition_hour]
    ).fetchone()
    return result is not None and result[0] == "success"

def count_rows(gz_path: Path) -> int:
    count = 0
    with gzip.open(gz_path, "rt", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
    return count

def download_hour(date_str: str, hour: int) -> Path:
    filename = f"{date_str}-{hour}.json.gz"
    local_path = RAW_DIR / filename
    url = f"https://data.gharchive.org/{filename}"
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()
    with open(local_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    return local_path

def main(start_date: str, end_date: str):
    con = get_connection()
    total_hours = 0
    skipped = 0
    downloaded = 0
    failed = 0
    for date_str, hour in hour_range(start_date, end_date):
        total_hours += 1
        partition_hour = datetime.strptime(f"{date_str} {hour:02d}:00:00", "%Y-%m-%d %H:%M:%S")
        if already_loaded(con, partition_hour):
            print(f"  [skip] {date_str}-{hour} already loaded")
            skipped += 1
            continue
        try:
            print(f"  [download] {date_str}-{hour}")
            local_path = download_hour(date_str, hour)
            row_count = count_rows(local_path)
            con.execute("""
                INSERT INTO load_log (partition_hour, loaded_at, row_count, status)
                VALUES (?, ?, ?, 'success')
                ON CONFLICT (partition_hour) DO UPDATE SET
                    loaded_at = excluded.loaded_at,
                    row_count = excluded.row_count,
                    status = 'success'
            """, [partition_hour, datetime.now(), row_count])
            downloaded += 1
            print(f"    -> {row_count} rows, marked success")
        except Exception as e:
            print(f"    -> FAILED: {e}")
            con.execute("""
                INSERT INTO load_log (partition_hour, loaded_at, row_count, status)
                VALUES (?, ?, 0, 'failed')
                ON CONFLICT (partition_hour) DO UPDATE SET
                    loaded_at = excluded.loaded_at,
                    status = 'failed'
            """, [partition_hour, datetime.now()])
            failed += 1
    print(f"\nDone. {total_hours} hours checked | {downloaded} downloaded | "
          f"{skipped} already loaded (skipped) | {failed} failed")
    con.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()
    main(args.start_date, args.end_date)