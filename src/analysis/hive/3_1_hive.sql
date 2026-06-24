
-- DATABASE
CREATE DATABASE IF NOT EXISTS flights_db;
USE flights_db;

DROP TABLE IF EXISTS flights_cleaned_${hivevar:SIZE};
DROP TABLE IF EXISTS routes_${hivevar:SIZE};

-- INPUT TABLE
CREATE EXTERNAL TABLE flights_cleaned_${hivevar:SIZE} (
    op_unique_carrier STRING,
    origin            STRING,
    dest              STRING,
    month             INT,
    arr_delay         DOUBLE,
    cancelled         INT
)
STORED AS PARQUET
LOCATION '${hivevar:INPUT_DIR}';

-- ROUTE-LEVEL STATISTICS (FINAL OUTPUT)
CREATE TABLE routes_${hivevar:SIZE}
STORED AS PARQUET
AS
SELECT
    op_unique_carrier,
    CONCAT(origin, '-', dest)            AS route,
    COUNT(*)                             AS num_flights,
    MIN(arr_delay)                       AS min_arr_delay,
    MAX(arr_delay)                       AS max_arr_delay,
    ROUND(AVG(arr_delay), 2)             AS avg_arr_delay,
    ROUND(SUM(cancelled) / COUNT(*), 4)  AS cancel_rate,
    SORT_ARRAY(COLLECT_SET(month))       AS months
FROM flights_cleaned_${hivevar:SIZE}
GROUP BY op_unique_carrier, origin, dest
ORDER BY op_unique_carrier, route;

-- CLEANUP
DROP TABLE IF EXISTS flights_cleaned_${hivevar:SIZE};