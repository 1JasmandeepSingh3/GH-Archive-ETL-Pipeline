from pathlib import Path
import duckdb
import importlib.util

DB_PATH = Path("data/gh_archive.duckdb")
SQL_DIR = Path("schema")

_quality_checks_path = Path(__file__).parent / "data_quality_check.py"
_spec = importlib.util.spec_from_file_location("quality_checks", _quality_checks_path)
quality_checks = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(quality_checks)

SQL_FILES_IN_ORDER = [
    "dim_date.sql",
    "dim_actor.sql",
    "dim_repo_scd2.sql",
    "fact_events.sql",
]

def main():
    con = duckdb.connect(str(DB_PATH))
    for filename in SQL_FILES_IN_ORDER:
        sql_path = SQL_DIR / filename
        if filename == "fact_events.sql":
            print("Running quarantine detection before building fact_events...")
            quarantine_result = quality_checks.validate_and_quarantine_partitions(
                con, "staged_events", "created_at"
            )
            print(f"  -> {quarantine_result['quarantined']} partition(s) quarantined, "
                  f"{quarantine_result['passed']} will be included\n")

        print(f"Running {sql_path}...")
        sql_text = sql_path.read_text()
        con.execute(sql_text)
        print(f"  -> done")
        tables = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main'
    """).fetchall()
    print(f"\nTables/views now in database: {[t[0] for t in tables]}")
    con.close()

if __name__ == "__main__":
    main()