CREATE OR REPLACE TABLE fact_events AS
SELECT
    se.event_id,
    se.event_type,
    se.created_at                              AS event_timestamp,
    da.actor_key,
    dr.repo_key,
    dd.date_key,
    se.push_commit_count,
    se.pr_action,
    se.pr_number
FROM staged_events se
LEFT JOIN dim_actor da
    ON se.actor_id = da.actor_id
LEFT JOIN dim_date dd
    ON dd.full_date = CAST(se.created_at AS DATE)
LEFT JOIN dim_repo dr
    ON se.repo_id = dr.repo_id
    AND se.created_at >= dr.valid_from
    AND se.created_at <  dr.valid_to
WHERE se.event_id IS NOT NULL
-- Exclude any hourly partition that failed validation (e.g. duplicate
-- event_ids) and was quarantined by data_quality_check.py. Using
-- NOT EXISTS instead of NOT IN deliberately -- NOT IN silently returns
-- zero rows for EVERYTHING if the subquery ever contains a NULL
-- partition_hour, which is a common, hard-to-spot bug. NOT EXISTS
-- doesn't have that failure mode.
AND NOT EXISTS (
    SELECT 1 FROM quarantine_log ql
    WHERE ql.partition_hour = DATE_TRUNC('hour', se.created_at)
);