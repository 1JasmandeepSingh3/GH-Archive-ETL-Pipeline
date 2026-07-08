import duckdb
con = duckdb.connect('data/gh_archive.duckdb')
for t in ['weekly_active_repos', 'pr_merge_latency', 'top_event_types', 'stars_per_day']:
    print(f'--- {t} ---')
    print(con.execute(f'SELECT * FROM {t} LIMIT 10').df())
    print()
con.close()