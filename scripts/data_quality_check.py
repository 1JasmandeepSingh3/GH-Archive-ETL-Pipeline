from pathlib import Path
import duckdb

DB_PATH = Path("data/gh_archive.duckdb")

def _ensure_log_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS reconciliation_log (
            run_ts TIMESTAMP DEFAULT current_timestamp,
            check_name STRING,
            source_count BIGINT,
            target_count BIGINT,
            row_diff BIGINT,
            status STRING
        )
    """)

def run_reconciliation_check(con, source_table: str, target_table: str, check_name: str,
                              timestamp_column: str = None) -> str:
    if timestamp_column:
        source_count = con.execute(f"""
            SELECT COUNT(*) FROM {source_table} se
            WHERE NOT EXISTS (
                SELECT 1 FROM quarantine_log ql
                WHERE ql.partition_hour = DATE_TRUNC('hour', se.{timestamp_column})
            )
        """).fetchone()[0]
    else:
        source_count = con.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()[0]
    target_count = con.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
    diff = source_count - target_count
    status = "PASS" if diff == 0 else "FAIL"
    print(f"[{check_name}] {source_table}(clean)={source_count}, {target_table}={target_count}, diff={diff}, status={status}")
    _ensure_log_table(con)
    con.execute(
        "INSERT INTO reconciliation_log VALUES (current_timestamp, ?, ?, ?, ?, ?)",
        [check_name, source_count, target_count, diff, status],
    )
    return status

def check_surrogate_key_uniqueness(con, table: str, key_column: str) -> str:
    dupes = con.execute(f"""
        SELECT {key_column}, COUNT(*) AS cnt
        FROM {table}
        GROUP BY {key_column}
        HAVING COUNT(*) > 1
    """).fetchall()
    status = "PASS" if len(dupes) == 0 else "FAIL"
    print(f"[uniqueness:{table}.{key_column}] duplicates found={len(dupes)}, status={status}")
    _ensure_log_table(con)
    con.execute(
        "INSERT INTO reconciliation_log VALUES (current_timestamp, ?, NULL, NULL, ?, ?)",
        [f"uniqueness_{table}_{key_column}", len(dupes), status],
    )
    return status

def check_scd2_no_overlap(con) -> str:
    overlaps = con.execute("""
        SELECT a.repo_id, a.repo_key AS key_a, b.repo_key AS key_b
        FROM dim_repo a
        JOIN dim_repo b
          ON a.repo_id = b.repo_id
          AND a.repo_key < b.repo_key
          AND a.valid_from < b.valid_to
          AND b.valid_from < a.valid_to
    """).fetchall()
    status = "PASS" if len(overlaps) == 0 else "FAIL"
    print(f"[scd2_overlap_check] overlapping pairs found={len(overlaps)}, status={status}")
    _ensure_log_table(con)
    con.execute(
        "INSERT INTO reconciliation_log VALUES (current_timestamp, ?, NULL, NULL, ?, ?)",
        ["scd2_overlap_check", len(overlaps), status],
    )
    return status

def check_freshness(con, log_table: str = "load_log", timestamp_column: str = "partition_hour",
                     max_allowed_lag_hours: int = 26) -> str:
    result = con.execute(f"""
        SELECT MAX({timestamp_column}) AS latest_loaded,
               current_timestamp AS now_ts,
               DATE_DIFF('hour', MAX({timestamp_column}), current_timestamp) AS lag_hours
        FROM {log_table}
    """).fetchone()
    latest_loaded, now_ts, lag_hours = result
    status = "PASS" if lag_hours is not None and lag_hours <= max_allowed_lag_hours else "FAIL"

    print(f"[freshness:{log_table}] latest_loaded={latest_loaded}, lag_hours={lag_hours}, "
          f"threshold={max_allowed_lag_hours}, status={status}")

    _ensure_log_table(con)
    con.execute(
        "INSERT INTO reconciliation_log VALUES (current_timestamp, ?, NULL, NULL, ?, ?)",
        ["freshness_check", lag_hours, status],
    )
    return status

def validate_and_quarantine_partitions(con, staging_table: str = "staged_events",
                                         timestamp_column: str = "created_at") -> dict:
    con.execute("""
        CREATE TABLE IF NOT EXISTS quarantine_log (
            partition_hour TIMESTAMP,
            reason STRING,
            detail_count BIGINT,
            quarantined_at TIMESTAMP DEFAULT current_timestamp
        )
    """)
    bad_partitions = con.execute(f"""
        SELECT partition_hour, COUNT(*) AS dup_count
        FROM (
            SELECT DATE_TRUNC('hour', {timestamp_column}) AS partition_hour, event_id
            FROM {staging_table}
            GROUP BY DATE_TRUNC('hour', {timestamp_column}), event_id
            HAVING COUNT(*) > 1
        )
        GROUP BY partition_hour
    """).fetchall()
    for partition_hour, dup_count in bad_partitions:
        already_logged = con.execute(
            "SELECT 1 FROM quarantine_log WHERE partition_hour = ?", [partition_hour]
        ).fetchone()
        if not already_logged:
            con.execute(
                "INSERT INTO quarantine_log VALUES (?, ?, ?, current_timestamp)",
                [partition_hour, "duplicate_event_id", dup_count],
            )
    total_partitions = con.execute(f"""
        SELECT COUNT(DISTINCT DATE_TRUNC('hour', {timestamp_column})) FROM {staging_table}
    """).fetchone()[0]
    quarantined_count = len(bad_partitions)
    passed_count = total_partitions - quarantined_count
    print(f"[quarantine] total={total_partitions}, passed={passed_count}, quarantined={quarantined_count}")
    return {"total": total_partitions, "passed": passed_count, "quarantined": quarantined_count}

def main():
    con = duckdb.connect(str(DB_PATH))
    results = {}
    results["quarantine"] = validate_and_quarantine_partitions(con, "staged_events", "created_at")
    results["staging_to_fact"] = run_reconciliation_check(con, "staged_events", "fact_events", "staging_to_fact", timestamp_column="created_at")
    results["uniqueness_repo_key"] = check_surrogate_key_uniqueness(con, "dim_repo", "repo_key")
    results["uniqueness_actor_key"] = check_surrogate_key_uniqueness(con, "dim_actor", "actor_key")
    results["uniqueness_date_key"] = check_surrogate_key_uniqueness(con, "dim_date", "date_key")
    results["scd2_overlap"] = check_scd2_no_overlap(con)
    results["freshness"] = check_freshness(con, "load_log", "partition_hour", max_allowed_lag_hours=26)

    print("\n--- Data Quality Summary ---")
    for name, status in results.items():
        print(f"  {name}: {status}")
    any_failed = any(status == "FAIL" for status in results.values())
    if any_failed:
        print("\nOne or more checks FAILED. Review reconciliation_log before trusting the model layer.")
    else:
        print("\nAll checks PASSED.")
    con.close()
    return results

if __name__ == "__main__":
    main()