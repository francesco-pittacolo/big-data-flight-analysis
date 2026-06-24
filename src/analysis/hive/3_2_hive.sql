CREATE DATABASE IF NOT EXISTS flights_db;
USE flights_db;

DROP TABLE IF EXISTS flights_cleaned_32_${hivevar:SIZE};
DROP TABLE IF EXISTS band_stats_${hivevar:SIZE};
DROP TABLE IF EXISTS cause_counts_${hivevar:SIZE};
DROP TABLE IF EXISTS top3_causes_${hivevar:SIZE};
DROP TABLE IF EXISTS final_band_stats_${hivevar:SIZE};

-- INPUT TABLE
CREATE EXTERNAL TABLE flights_cleaned_32_${hivevar:SIZE} (
    origin               STRING,
    month                INT,
    dep_delay            DOUBLE,
    arr_delay            DOUBLE,
    cancelled            INT,
    cancellation_code    STRING,
    carrier_delay        DOUBLE,
    weather_delay        DOUBLE,
    nas_delay            DOUBLE,
    security_delay       DOUBLE,
    late_aircraft_delay  DOUBLE
)
STORED AS PARQUET
LOCATION '${INPUT_DIR}';

-- STEP 1: BAND STATS
CREATE TABLE band_stats_${hivevar:SIZE} AS
SELECT
    origin,
    month,
    CASE
        WHEN cancelled = 1 THEN 'cancelled'
        WHEN dep_delay IS NOT NULL AND dep_delay > 0 AND dep_delay < 15 THEN 'low'
        WHEN dep_delay IS NOT NULL AND dep_delay > 0 AND dep_delay <= 60 THEN 'medium'
        WHEN dep_delay IS NOT NULL AND dep_delay > 60 THEN 'high'
    END AS band,

    COUNT(*) AS num_flights,
    ROUND(AVG(dep_delay), 2) AS avg_dep_delay,
    ROUND(AVG(arr_delay), 2) AS avg_arr_delay

FROM flights_cleaned_32_${hivevar:SIZE}
WHERE cancelled = 1
   OR (dep_delay IS NOT NULL AND dep_delay > 0)

GROUP BY
    origin,
    month,
    CASE
        WHEN cancelled = 1 THEN 'cancelled'
        WHEN dep_delay IS NOT NULL AND dep_delay > 0 AND dep_delay < 15 THEN 'low'
        WHEN dep_delay IS NOT NULL AND dep_delay > 0 AND dep_delay <= 60 THEN 'medium'
        WHEN dep_delay IS NOT NULL AND dep_delay > 60 THEN 'high'
    END;

-- STEP 2: CAUSE COUNTS
CREATE TABLE cause_counts_${hivevar:SIZE} AS
SELECT
    origin,
    month,
    band,
    cause,
    COUNT(*) AS cause_count
FROM (

    -- DELAYED FLIGHTS

    SELECT origin, month,
        CASE
            WHEN dep_delay < 15 THEN 'low'
            WHEN dep_delay <= 60 THEN 'medium'
            ELSE 'high'
        END AS band,
        'carrier_delay' AS cause
    FROM flights_cleaned_32_${hivevar:SIZE}
    WHERE cancelled = 0
      AND dep_delay IS NOT NULL
      AND dep_delay > 0
      AND COALESCE(carrier_delay, 0) > 0

    UNION ALL

    SELECT origin, month,
        CASE
            WHEN dep_delay < 15 THEN 'low'
            WHEN dep_delay <= 60 THEN 'medium'
            ELSE 'high'
        END AS band,
        'weather_delay'
    FROM flights_cleaned_32_${hivevar:SIZE}
    WHERE cancelled = 0
      AND dep_delay IS NOT NULL
      AND dep_delay > 0
      AND COALESCE(weather_delay, 0) > 0

    UNION ALL

    SELECT origin, month,
        CASE
            WHEN dep_delay < 15 THEN 'low'
            WHEN dep_delay <= 60 THEN 'medium'
            ELSE 'high'
        END AS band,
        'nas_delay'
    FROM flights_cleaned_32_${hivevar:SIZE}
    WHERE cancelled = 0
      AND dep_delay IS NOT NULL
      AND dep_delay > 0
      AND COALESCE(nas_delay, 0) > 0

    UNION ALL

    SELECT origin, month,
        CASE
            WHEN dep_delay < 15 THEN 'low'
            WHEN dep_delay <= 60 THEN 'medium'
            ELSE 'high'
        END AS band,
        'security_delay'
    FROM flights_cleaned_32_${hivevar:SIZE}
    WHERE cancelled = 0
      AND dep_delay IS NOT NULL
      AND dep_delay > 0
      AND COALESCE(security_delay, 0) > 0

    UNION ALL

    SELECT origin, month,
        CASE
            WHEN dep_delay < 15 THEN 'low'
            WHEN dep_delay <= 60 THEN 'medium'
            ELSE 'high'
        END AS band,
        'late_aircraft_delay'
    FROM flights_cleaned_32_${hivevar:SIZE}
    WHERE cancelled = 0
      AND dep_delay IS NOT NULL
      AND dep_delay > 0
      AND COALESCE(late_aircraft_delay, 0) > 0

    UNION ALL

    -- CANCELLED FLIGHTS

    SELECT origin, month,
        'cancelled' AS band,
        CASE cancellation_code
            WHEN 'A' THEN 'carrier'
            WHEN 'B' THEN 'weather'
            WHEN 'C' THEN 'nas'
            WHEN 'D' THEN 'security'
        END AS cause
    FROM flights_cleaned_32_${hivevar:SIZE}
    WHERE cancelled = 1

) t
WHERE cause IS NOT NULL
GROUP BY origin, month, band, cause;

-- STEP 3: TOP 3 CAUSES
CREATE TABLE top3_causes_${hivevar:SIZE} AS
SELECT
    origin,
    month,
    band,
    COLLECT_LIST(
        NAMED_STRUCT('cause', cause, 'cause_count', cause_count)
    ) AS top_causes
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY origin, month, band
               ORDER BY cause_count DESC, cause ASC
           ) AS rn
    FROM cause_counts_${hivevar:SIZE}
) ranked
WHERE rn <= 3
GROUP BY origin, month, band;

-- STEP 4: FINAL TABLE
CREATE TABLE final_band_stats_${hivevar:SIZE} (
    origin STRING,
    month INT,
    band STRING,
    num_flights BIGINT,
    avg_dep_delay DOUBLE,
    avg_arr_delay DOUBLE,
    top_causes ARRAY<STRUCT<cause:STRING,cause_count:BIGINT>>
)
STORED AS PARQUET;

INSERT OVERWRITE TABLE final_band_stats_${hivevar:SIZE}
SELECT *
FROM (
    SELECT
        s.origin,
        s.month,
        s.band,
        s.num_flights,
        s.avg_dep_delay,
        s.avg_arr_delay,
        t.top_causes
    FROM band_stats_${hivevar:SIZE} s
    LEFT JOIN top3_causes_${hivevar:SIZE} t
        ON s.origin = t.origin
       AND s.month = t.month
       AND s.band = t.band
) x
ORDER BY
    origin,
    month,
    CASE band
        WHEN 'low' THEN 1
        WHEN 'medium' THEN 2
        WHEN 'high' THEN 3
        WHEN 'cancelled' THEN 4
    END;

DROP TABLE IF EXISTS flights_cleaned_32_${hivevar:SIZE};
DROP TABLE IF EXISTS band_stats_${hivevar:SIZE};
DROP TABLE IF EXISTS cause_counts_${hivevar:SIZE};
DROP TABLE IF EXISTS top3_causes_${hivevar:SIZE};