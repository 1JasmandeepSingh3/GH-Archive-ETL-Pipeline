CREATE OR REPLACE TABLE dim_date AS
SELECT
    CAST(strftime(d, '%Y%m%d') AS INTEGER)     AS date_key,
    d                                           AS full_date,
    strftime(d, '%A')                           AS day_of_week,
    isodow(d)                                   AS day_of_week_num,
    weekofyear(d)                                AS week_number,
    strftime(d, '%B')                           AS month_name,
    MONTH(d)                                     AS month_number,
    YEAR(d)                                      AS year,
    (isodow(d) IN (6, 7))                        AS is_weekend
FROM range(
    DATE '2025-01-01',
    DATE '2025-01-08',
    INTERVAL 1 DAY
) AS t(d);



