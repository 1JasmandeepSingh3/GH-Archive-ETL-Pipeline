import argparse
import duckdb

DB_PATH = "data/gh_archive.duckdb"


def main(start_date: str, end_date: str):
    con = duckdb.connect(DB_PATH)

    before = con.execute(
        "SELECT COUNT(*) FROM load_log WHERE partition_hour >= ? AND partition_hour < ?::TIMESTAMP + INTERVAL 1 DAY",
        [start_date, end_date]
    ).fetchone()[0]

    con.execute(
        "DELETE FROM load_log WHERE partition_hour >= ? AND partition_hour < ?::TIMESTAMP + INTERVAL 1 DAY",
        [start_date, end_date]
    )

    print(f"Cleared {before} load_log entries from {start_date} through {end_date}")
    con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()
    main(args.start_date, args.end_date)