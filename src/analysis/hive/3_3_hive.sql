CREATE DATABASE IF NOT EXISTS flights_db;
USE flights_db;

DROP TABLE IF EXISTS flights_cleaned_33_${hivevar:SIZE};
DROP TABLE IF EXISTS airline_stats_${hivevar:SIZE};
DROP TABLE IF EXISTS airport_avg_${hivevar:SIZE};
DROP TABLE IF EXISTS joined_stats_${hivevar:SIZE};
DROP TABLE IF EXISTS ranked_stats_${hivevar:SIZE};

-- INPUT TABLE
CREATE EXTERNAL TABLE flights_cleaned_33_${hivevar:SIZE} (
    origin STRING,
    op_unique_carrier STRING,
    dep_delay DOUBLE,
    arr_delay DOUBLE,
    cancelled INT
)
STORED AS PARQUET
LOCATION '${INPUT_DIR}';

-- STEP 1: AIRLINE STATS
CREATE TABLE airline_stats_${hivevar:SIZE} AS
SELECT
    origin,
    op_unique_carrier,
    COUNT(*) AS num_flights,
    ROUND(AVG(dep_delay), 2) AS avg_dep_delay,
    ROUND(AVG(arr_delay), 2) AS avg_arr_delay,
    ROUND(AVG(cancelled), 4) AS cancel_rate
FROM flights_cleaned_33_${hivevar:SIZE}
GROUP BY origin, op_unique_carrier;

-- STEP 2: AIRPORT BASELINE
CREATE TABLE airport_avg_${hivevar:SIZE} AS
SELECT
    origin,
    ROUND(AVG(dep_delay), 2) AS airport_avg_dep_delay
FROM flights_cleaned_33_${hivevar:SIZE}
GROUP BY origin;

-- STEP 3: JOIN
CREATE TABLE joined_stats_${hivevar:SIZE} AS
SELECT
    a.origin,
    a.op_unique_carrier,
    a.num_flights,
    a.avg_dep_delay,
    a.avg_arr_delay,
    a.cancel_rate,
    ROUND(a.avg_dep_delay - b.airport_avg_dep_delay, 2) AS dep_delay_diff
FROM airline_stats_${hivevar:SIZE} a
JOIN airport_avg_${hivevar:SIZE} b
ON a.origin = b.origin;

-- STEP 4: FINAL TABLE
CREATE TABLE ranked_stats_${hivevar:SIZE} (
    origin STRING,
    op_unique_carrier STRING,
    num_flights BIGINT,
    avg_dep_delay DOUBLE,
    avg_arr_delay DOUBLE,
    cancel_rate DOUBLE,
    dep_delay_diff DOUBLE,
    rank INT
)
STORED AS PARQUET;

INSERT OVERWRITE TABLE ranked_stats_${hivevar:SIZE}
SELECT
    origin,
    op_unique_carrier,
    num_flights,
    avg_dep_delay,
    avg_arr_delay,
    cancel_rate,
    dep_delay_diff,
    CAST(
        ROW_NUMBER() OVER (
            PARTITION BY origin
            ORDER BY avg_dep_delay ASC
        )
    AS INT) AS rank
FROM joined_stats_${hivevar:SIZE};

DROP TABLE IF EXISTS flights_cleaned_33_${hivevar:SIZE};
DROP TABLE IF EXISTS airline_stats_${hivevar:SIZE};
DROP TABLE IF EXISTS airport_avg_${hivevar:SIZE};
DROP TABLE IF EXISTS joined_stats_${hivevar:SIZE};