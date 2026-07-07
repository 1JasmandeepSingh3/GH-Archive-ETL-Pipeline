"""
Diagnostic: check if dim_actor has more than one row per actor_id.

dim_actor was built as SELECT DISTINCT actor_id, actor_login - if a
GitHub user changed their username during the window (same actor_id,
different actor_login text), this creates TWO dim_actor rows for the
same actor_id. The fact_events join (ON actor_id = actor_id) would then
match BOTH rows for every event by that person, inflating the count -
exactly the kind of join fan-out we're chasing.

Run:
    python scripts/diagnose_actor_duplicates.py
"""

import duckdb

con = duckdb.connect("data/gh_archive.duckdb")

dupes = con.execute("""
    SELECT actor_id, COUNT(*) AS row_count, STRING_AGG(actor_login, ' | ') AS logins_seen
    FROM dim_actor
    GROUP BY actor_id
    HAVING COUNT(*) > 1
    ORDER BY row_count DESC
    LIMIT 10
""").fetchall()

print(f"Found {len(dupes)} actor_id values with multiple dim_actor rows (showing up to 10):")
for row in dupes:
    print(row)

# Also directly quantify how much fan-out this explains
total_extra = con.execute("""
    SELECT SUM(row_count - 1) FROM (
        SELECT actor_id, COUNT(*) AS row_count
        FROM dim_actor
        GROUP BY actor_id
        HAVING COUNT(*) > 1
    )
""").fetchone()[0]
print(f"\nIf every extra dim_actor row causes exactly one extra fact_events "
      f"match per affected event, this alone could account for roughly the "
      f"observed mismatch. Extra dim_actor rows beyond 1-per-actor_id: {total_extra}")

con.close()