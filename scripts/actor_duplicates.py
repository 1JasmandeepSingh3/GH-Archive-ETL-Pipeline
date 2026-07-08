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