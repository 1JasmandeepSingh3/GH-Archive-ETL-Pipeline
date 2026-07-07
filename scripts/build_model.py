from pathlib import Path
import duckdb
import importlib.util

DB_PATH = Path("data/gh_archive.duckdb")
SQL_DIR = Path("schema")

# Load 04_quality_checks.py by its actual file path relative to THIS
# file's location, rather than relying on sys.path + the current working
# directory. This avoids ModuleNotFoundError when the script is run from
# a different folder than expected, and sidesteps the fact that a module
# name starting with a digit can't be imported with a normal "import"
# statement anyway.
_quality_checks_path = Path(__file__).parent / "data_quality_check.py"
_spec = importlib.util.spec_from_file_location("quality_checks", _quality_checks_path)
quality_checks = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(quality_checks)

# Explicit order matters: dimensions must exist before fact_events can
# join to them. Listed here rather than relying on filename sort alone,
# so the dependency order is unambiguous even as more files get added.
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

        # Gate: run quarantine detection right before fact_events.sql,
        # since fact_events.sql excludes quarantined hours via a subquery
        # on quarantine_log. Dims don't need this -- only the fact table
        # reads from staged_events at event grain.
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

    # Quick sanity report after building everything
    tables = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main'
    """).fetchall()
    print(f"\nTables/views now in database: {[t[0] for t in tables]}")

    con.close()


if __name__ == "__main__":
    main()