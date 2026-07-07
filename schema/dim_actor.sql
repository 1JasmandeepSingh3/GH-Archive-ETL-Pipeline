CREATE OR REPLACE TABLE dim_actor AS
WITH latest_login_per_actor AS (
    SELECT
        actor_id,
        actor_login,
        ROW_NUMBER() OVER (
            PARTITION BY actor_id ORDER BY created_at DESC
        ) AS recency_rank
    FROM staged_events
    WHERE actor_id IS NOT NULL
)
SELECT
    ROW_NUMBER() OVER (ORDER BY actor_id)   AS actor_key,
    actor_id,
    actor_login,
    (actor_login LIKE '%[bot]')             AS is_bot
FROM latest_login_per_actor
WHERE recency_rank = 1;