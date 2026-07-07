from pathlib import Path
import duckdb

DB_PATH = Path("data/gh_archive.duckdb")
SQL_DIR = Path("schema")

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