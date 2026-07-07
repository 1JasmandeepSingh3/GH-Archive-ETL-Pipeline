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

def run_reconciliation_check(con, source_table: str, target_table: str, check_name: str) -> str:
    """Compares row counts between two layers (e.g. staged_events -> fact_events).
    A non-zero diff means rows were silently dropped or duplicated somewhere
    in the transform between them."""
    source_count = con.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()[0]
    target_count = con.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]

    diff = source_count - target_count
    status = "PASS" if diff == 0 else "FAIL"

    print(f"[{check_name}] {source_table}={source_count}, {target_table}={target_count}, diff={diff}, status={status}")

    _ensure_log_table(con)
    con.execute(
        "INSERT INTO reconciliation_log VALUES (current_timestamp, ?, ?, ?, ?, ?)",
        [check_name, source_count, target_count, diff, status],
    )
    return status

def check_surrogate_key_uniqueness(con, table: str, key_column: str) -> str:
    """Confirms every surrogate key in `table` appears exactly once. A
    duplicate means a fact row could fan out and join to more than one
    dimension row, silently inflating downstream counts."""
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
    """SCD-2 specific: for a given repo_id, no two valid_from/valid_to
    ranges in dim_repo should overlap. This is the automated version of
    the fan-out bug already caught manually in this project's history."""
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


def check_freshness(con, table: str, timestamp_column: str,
                     max_staleness_hours: float = 24.0,
                     reference_ts: str = None) -> str:
    """Confirms the most recent record in `table` (by `timestamp_column`)
    is no older than `max_staleness_hours` relative to a reference time.

    This catches the failure mode the other checks can't see: an upstream
    source that quietly stopped producing new partitions. Row counts can
    still reconcile, keys can still be unique, and SCD-2 ranges can still
    be clean, while the data itself has simply gone stale.

    `reference_ts` defaults to current_timestamp, but you can pass in the
    watermark from your load-log instead if you want "freshness relative
    to the last successful load" rather than "freshness relative to now"
    (useful in dev/backfill runs where wall-clock time isn't meaningful).
    """
    ref_expr = f"CAST('{reference_ts}' AS TIMESTAMP)" if reference_ts else "current_timestamp"

    result = con.execute(f"""
        SELECT
            MAX({timestamp_column}) AS latest_ts,
            {ref_expr} AS reference_ts,
            date_diff('hour', MAX({timestamp_column}), {ref_expr}) AS staleness_hours
        FROM {table}
    """).fetchone()

    latest_ts, reference_ts_val, staleness_hours = result

    # A NULL staleness_hours means the table is empty -- treat as a FAIL,
    # not a silent PASS.
    status = "PASS" if (staleness_hours is not None and staleness_hours <= max_staleness_hours) else "FAIL"

    print(f"[freshness:{table}.{timestamp_column}] latest={latest_ts}, "
          f"reference={reference_ts_val}, staleness_hours={staleness_hours}, status={status}")

    _ensure_log_table(con)
    con.execute(
        "INSERT INTO reconciliation_log VALUES (current_timestamp, ?, NULL, NULL, ?, ?)",
        [f"freshness_{table}_{timestamp_column}", staleness_hours, status],
    )
    return status


def main():
    con = duckdb.connect(str(DB_PATH))

    results = {}
    results["staging_to_fact"] = run_reconciliation_check(con, "staged_events", "fact_events", "staging_to_fact")
    results["uniqueness_repo_key"] = check_surrogate_key_uniqueness(con, "dim_repo", "repo_key")
    results["uniqueness_actor_key"] = check_surrogate_key_uniqueness(con, "dim_actor", "actor_key")
    results["uniqueness_date_key"] = check_surrogate_key_uniqueness(con, "dim_date", "date_key")
    results["scd2_overlap"] = check_scd2_no_overlap(con)

    latest_watermark = con.execute("SELECT MAX(partition_hour) FROM load_log").fetchone()[0]
    results["freshness_fact_events"] = check_freshness(
        con, "fact_events", "event_timestamp",
        max_staleness_hours=1.0,
        reference_ts=str(latest_watermark)
    )

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