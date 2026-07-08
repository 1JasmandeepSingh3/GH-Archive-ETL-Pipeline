import duckdb

con = duckdb.connect("data/gh_archive.duckdb")
overlaps = con.execute("""
    SELECT
        a.repo_id,
        a.repo_key AS key_a, a.repo_name AS name_a, a.valid_from AS from_a, a.valid_to AS to_a,
        b.repo_key AS key_b, b.repo_name AS name_b, b.valid_from AS from_b, b.valid_to AS to_b
    FROM dim_repo a
    JOIN dim_repo b
        ON a.repo_id = b.repo_id
        AND a.repo_key < b.repo_key
        AND a.valid_from < b.valid_to
        AND b.valid_from < a.valid_to
    LIMIT 10
""").fetchall()
print(f"Found {len(overlaps)} overlapping dim_repo version pairs (showing up to 10):")
for row in overlaps:
    print(row)
con.close()