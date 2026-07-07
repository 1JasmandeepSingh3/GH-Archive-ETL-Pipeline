CREATE OR REPLACE TABLE dim_repo AS
 
WITH ordered_events AS (
    SELECT DISTINCT repo_id, repo_name, created_at
    FROM staged_events
    WHERE repo_id IS NOT NULL
),
 
with_change_flag AS (
    SELECT
        repo_id,
        repo_name,
        created_at,
        repo_name != LAG(repo_name) OVER (
            PARTITION BY repo_id ORDER BY created_at
        ) AS name_changed
    FROM ordered_events
),
 
with_version AS (
    SELECT
        repo_id,
        repo_name,
        created_at,
        SUM(CASE WHEN name_changed THEN 1 ELSE 0 END) OVER (
            PARTITION BY repo_id ORDER BY created_at
            ROWS UNBOUNDED PRECEDING
        ) AS version_number
    FROM with_change_flag
),
 
versioned_repo AS (
    SELECT
        repo_id,
        repo_name,
        version_number,
        MIN(created_at) AS valid_from
    FROM with_version
    GROUP BY repo_id, repo_name, version_number
)
 
SELECT
    ROW_NUMBER() OVER (ORDER BY repo_id, valid_from) AS repo_key,
    repo_id,
    repo_name,
    valid_from,
    COALESCE(
        LEAD(valid_from) OVER (PARTITION BY repo_id ORDER BY valid_from),
        TIMESTAMP '9999-12-31'
    ) AS valid_to,
    (LEAD(valid_from) OVER (PARTITION BY repo_id ORDER BY valid_from) IS NULL)
        AS is_current
FROM versioned_repo;