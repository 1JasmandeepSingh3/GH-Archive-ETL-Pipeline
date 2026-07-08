from pathlib import Path
import duckdb

DB_PATH = Path("data/gh_archive.duckdb")

def atomic_refresh(con, final_table_name: str, build_sql: str):
    temp_name = f"{final_table_name}_new"
    old_name = f"{final_table_name}_old"
    con.execute(f"DROP TABLE IF EXISTS {temp_name}")
    con.execute(f"CREATE TABLE {temp_name} AS {build_sql}")
    new_count = con.execute(f"SELECT COUNT(*) FROM {temp_name}").fetchone()[0]
    if new_count == 0:
        con.execute(f"DROP TABLE {temp_name}")
        raise ValueError(f"Refresh aborted: {final_table_name} query came back empty, not swapping in.")
    table_exists = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{final_table_name}'
    """).fetchone()[0] > 0
    con.execute(f"DROP TABLE IF EXISTS {old_name}")
    if table_exists:
        con.execute(f"ALTER TABLE {final_table_name} RENAME TO {old_name}")
    con.execute(f"ALTER TABLE {temp_name} RENAME TO {final_table_name}")
    if table_exists:
        con.execute(f"DROP TABLE {old_name}")
    print(f"[atomic_refresh] {final_table_name} refreshed with {new_count} rows")

# weekly active repos
WEEKLY_ACTIVE_REPOS_SQL = """
SELECT
    DATE_TRUNC('week', event_timestamp) AS week_start,
    COUNT(DISTINCT repo_key) AS active_repo_count
FROM fact_events
WHERE repo_key IS NOT NULL
GROUP BY DATE_TRUNC('week', event_timestamp)
ORDER BY week_start
"""

# pr merge latency
PR_MERGE_LATENCY_SQL = """
WITH pr_opened AS (
    SELECT repo_key, pr_number, MIN(event_timestamp) AS opened_at
    FROM fact_events
    WHERE pr_action = 'opened' AND pr_number IS NOT NULL
    GROUP BY repo_key, pr_number
),
pr_closed AS (
    SELECT repo_key, pr_number, MIN(event_timestamp) AS closed_at
    FROM fact_events
    WHERE pr_action = 'closed' AND pr_number IS NOT NULL
    GROUP BY repo_key, pr_number
)
SELECT
    o.repo_key,
    o.pr_number,
    o.opened_at,
    c.closed_at,
    DATE_DIFF('hour', o.opened_at, c.closed_at) AS latency_hours
FROM pr_opened o
JOIN pr_closed c
    ON o.repo_key = c.repo_key AND o.pr_number = c.pr_number
WHERE c.closed_at >= o.opened_at  -- guards against out-of-order/bad data
"""

# Top even by volume
TOP_EVENT_TYPES_SQL = """
SELECT event_type,
COUNT(*) AS event_count
FROM fact_events
GROUP BY event_type
ORDER BY event_count DESC
"""

# Stars per day Trend
STARS_PER_DAY_SQL = """
SELECT
    CAST(event_timestamp AS DATE) AS event_date,
    COUNT(*) AS star_count
FROM fact_events
WHERE event_type = 'WatchEvent'
GROUP BY CAST(event_timestamp AS DATE)
ORDER BY event_date
"""

def main():
    con = duckdb.connect(str(DB_PATH))
    atomic_refresh(con, "weekly_active_repos", WEEKLY_ACTIVE_REPOS_SQL)
    atomic_refresh(con, "pr_merge_latency", PR_MERGE_LATENCY_SQL)
    atomic_refresh(con, "top_event_types", TOP_EVENT_TYPES_SQL)
    atomic_refresh(con, "stars_per_day", STARS_PER_DAY_SQL)
    print("\n--- Serving layer refresh complete ---")
    for table in ["weekly_active_repos", "pr_merge_latency", "top_event_types", "stars_per_day"]:
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count} rows")
    con.close()

if __name__ == "__main__":
    main()