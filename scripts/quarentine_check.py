from pathlib import Path
import duckdb

DB_PATH = Path("data/gh_archive.duckdb")


def _ensure_quarantine_tables(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS quarantine_events (
            event_id VARCHAR,
            event_type VARCHAR,
            actor_login VARCHAR,
            actor_id BIGINT,
            repo_name VARCHAR,
            repo_id BIGINT,
            created_at TIMESTAMP,
            push_commit_count INTEGER,
            pr_action VARCHAR,
            pr_number BIGINT,
            payload_json VARCHAR,
            quarantined_at TIMESTAMP DEFAULT current_timestamp
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS quarantine_log (
            run_ts TIMESTAMP DEFAULT current_timestamp,
            partition_hour TIMESTAMP,
            row_count BIGINT,
            reasons STRING,
            status STRING
        )
    """)


def validate_partition(con, staging_table: str, partition_hour: str):
    """Runs checks scoped to ONE incoming partition (one hour, derived from
    date_trunc('hour', created_at)). Returns a dict:
        {"null_key_count": int, "dupe_id_count": int, "row_count": int}
    so the caller can decide separately how to handle each failure type,
    rather than lumping every failure into one bucket.
    """
    row_count = con.execute(f"""
        SELECT COUNT(*) FROM {staging_table}
        WHERE date_trunc('hour', created_at) = CAST(? AS TIMESTAMP)
    """, [partition_hour]).fetchone()[0]

    null_keys = con.execute(f"""
        SELECT COUNT(*) FROM {staging_table}
        WHERE date_trunc('hour', created_at) = CAST(? AS TIMESTAMP)
          AND (event_id IS NULL OR actor_id IS NULL OR repo_id IS NULL)
    """, [partition_hour]).fetchone()[0]

    dupe_ids = con.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT event_id
            FROM {staging_table}
            WHERE date_trunc('hour', created_at) = CAST(? AS TIMESTAMP)
            GROUP BY event_id HAVING COUNT(*) > 1
        )
    """, [partition_hour]).fetchone()[0]

    return {"row_count": row_count, "null_key_count": null_keys, "dupe_id_count": dupe_ids}


def gate_partition(con, staging_table: str, partition_hour: str) -> str:
    """The actual gate for one hour. Three outcomes, each handled
    differently -- because they're not equally serious:

      - PASS: nothing wrong. Hour flows through untouched.
      - QUARANTINED: the hour has rows with a NULL event_id/actor_id/repo_id.
        This is treated as real corruption -- the whole hour is copied to
        quarantine_events and excluded from the model.
      - DEDUPE: the hour's only problem is duplicate event_ids (the same
        event recorded twice -- a known, low-volume GH Archive quirk, not
        corruption). The hour is NOT thrown out. It's logged as DEDUPE so
        get_clean_events_query() knows to keep it, just with one copy per
        event_id instead of two.

    Nothing is ever deleted from staging_table, since it may be a VIEW.
    Returns the status string: "PASS", "QUARANTINED", or "DEDUPE".
    """
    _ensure_quarantine_tables(con)
    stats = validate_partition(con, staging_table, partition_hour)
    row_count = stats["row_count"]

    if row_count == 0:
        status = "QUARANTINED"
        reason_text = "zero rows in partition"
    elif stats["null_key_count"] > 0:
        status = "QUARANTINED"
        reason_text = f"{stats['null_key_count']} rows with a null key column (event_id/actor_id/repo_id)"
        if stats["dupe_id_count"] > 0:
            reason_text += f"; also {stats['dupe_id_count']} duplicate event_id values"
    elif stats["dupe_id_count"] > 0:
        status = "DEDUPE"
        reason_text = f"{stats['dupe_id_count']} duplicate event_id values within the partition (deduped, hour kept)"
    else:
        status = "PASS"
        reason_text = None

    print(f"[quarantine_gate] partition={partition_hour} rows={row_count} status={status}"
          + (f" reasons={reason_text}" if reason_text else " -> proceeding to model"))

    if status == "QUARANTINED":
        con.execute(f"""
            INSERT INTO quarantine_events
            SELECT *, current_timestamp AS quarantined_at
            FROM {staging_table}
            WHERE date_trunc('hour', created_at) = CAST(? AS TIMESTAMP)
        """, [partition_hour])

    con.execute(
        "INSERT INTO quarantine_log VALUES (current_timestamp, ?, ?, ?, ?)",
        [partition_hour, row_count, reason_text, status],
    )
    return status


def gate_all_partitions(con, staging_table: str = "staged_events") -> dict:
    """Finds every distinct hour in staging_table and gates each one.
    Call this after your loader/flatten step and BEFORE build_model.py.
    Returns {partition_hour_str: status}.
    """
    hours = con.execute(f"""
        SELECT DISTINCT date_trunc('hour', created_at) AS partition_hour
        FROM {staging_table}
        ORDER BY partition_hour
    """).fetchall()

    if not hours:
        print(f"[gate_all_partitions] no rows found in {staging_table} -- nothing to gate")
        return {}

    results = {}
    for (hour,) in hours:
        hour_str = str(hour)
        results[hour_str] = gate_partition(con, staging_table, hour_str)

    passed = sum(1 for v in results.values() if v == "PASS")
    deduped = sum(1 for v in results.values() if v == "DEDUPE")
    quarantined = sum(1 for v in results.values() if v == "QUARANTINED")
    print(f"\n[gate_all_partitions] {len(results)} partitions checked -> "
          f"{passed} passed, {deduped} deduped, {quarantined} quarantined")

    return results


def get_clean_events_query(con, staging_table: str = "staged_events") -> str:
    """Returns a full SELECT statement to use in build_model.py wherever it
    currently reads from staged_events to build fact_events, e.g.:

        query = get_clean_events_query(con)
        con.execute(f"CREATE TABLE fact_events AS {query}")

    This excludes any hour logged as QUARANTINED (real corruption), and for
    hours logged as DEDUPE, keeps exactly one row per event_id (the
    earliest by created_at) instead of throwing the whole hour away.
    """
    return f"""
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY created_at) AS rn
            FROM {staging_table}
            WHERE date_trunc('hour', created_at) NOT IN (
                SELECT partition_hour FROM quarantine_log WHERE status = 'QUARANTINED'
            )
        )
        WHERE rn = 1
    """


if __name__ == "__main__":
    con = duckdb.connect(str(DB_PATH))
    gate_all_partitions(con, "staged_events")
    con.close()