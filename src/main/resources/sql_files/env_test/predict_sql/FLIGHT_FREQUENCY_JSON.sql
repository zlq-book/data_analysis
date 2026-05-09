-- 乘机频次分布 - 合并24个CURRENT时间区间为一条数据
INSERT INTO DWS_TEST.T_DWS_FLIGHT_PREDICTION_DAILY (
    cache_key, departure_airport, arrival_airport, flight_number,
    airline_code, prediction_type, time_range_type, flight_frequency_json,
    created_time, updated_time
)
WITH 
-- 生成过去24个1天时间区间
date_ranges AS (
    SELECT 
        num,
        DATE_SUB(CURRENT_DATE(), INTERVAL (25 - num) DAY) as range_start_date,
        DATE_SUB(CURRENT_DATE(), INTERVAL (24 - num) DAY) as range_end_date
    FROM (
        SELECT 1 as num UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6
        UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12
        UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18
        UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24
    ) n
    ORDER BY num
),
-- 获取所有活跃的航班航线
active_flights AS (
    SELECT DISTINCT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE
    FROM DWD_TEST.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_TEST.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    WHERE f.DATA_ACTIVE = true
	  AND d.OPERAT_AIRLINE = 'SC'
      AND f.FK_DEPARTURES_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
      AND d.OPERAT_FLIGHT_NUM IS NOT NULL
      AND d.OPERAT_FLIGHT_NUM != ''
),
-- 计算每个航班在每个时间区间的乘机频次分布
flight_frequency_percentages AS (
    SELECT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE,
        dr.num as time_index,
        ROUND(SUM(CASE WHEN u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS = 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as ride_1,
        ROUND(SUM(CASE WHEN u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS BETWEEN 2 AND 4 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as ride_2_4,
        ROUND(SUM(CASE WHEN u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS BETWEEN 5 AND 7 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as ride_5_7,
        ROUND(SUM(CASE WHEN u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS BETWEEN 8 AND 10 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as ride_8_10,
        ROUND(SUM(CASE WHEN u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS > 10 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as ride_10_above
    FROM DWD_TEST.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_TEST.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    INNER JOIN DIM_TEST.T_DIM_USER_DIM u ON f.FK_PASSENGER_USER_TID = u.PK_ID
    INNER JOIN date_ranges dr ON f.FK_DEPARTURES_DATE >= dr.range_start_date 
                      AND f.FK_DEPARTURES_DATE < dr.range_end_date
    WHERE f.DATA_ACTIVE = true
        AND u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS IS NOT NULL
        AND d.OPERAT_FLIGHT_NUM IS NOT NULL
        AND d.OPERAT_FLIGHT_NUM != ''
    GROUP BY f.FK_DEPAIRPORT, f.FK_ARRIAIRPORT, d.OPERAT_FLIGHT_NUM, d.OPERAT_AIRLINE, dr.num
),
-- 为每个航班生成完整的24个时间区间数据，格式化数值
flight_time_series AS (
    SELECT 
        af.FK_DEPAIRPORT,
        af.FK_ARRIAIRPORT,
        af.OPERAT_FLIGHT_NUM,
        af.OPERAT_AIRLINE,
        dr.num as time_index,
        CASE 
            WHEN ffp.ride_1 IS NULL THEN '-1'
            ELSE CAST(ROUND(ffp.ride_1, 2) AS STRING)
        END as ride_1,
        CASE 
            WHEN ffp.ride_2_4 IS NULL THEN '-1'
            ELSE CAST(ROUND(ffp.ride_2_4, 2) AS STRING)
        END as ride_2_4,
        CASE 
            WHEN ffp.ride_5_7 IS NULL THEN '-1'
            ELSE CAST(ROUND(ffp.ride_5_7, 2) AS STRING)
        END as ride_5_7,
        CASE 
            WHEN ffp.ride_8_10 IS NULL THEN '-1'
            ELSE CAST(ROUND(ffp.ride_8_10, 2) AS STRING)
        END as ride_8_10,
        CASE 
            WHEN ffp.ride_10_above IS NULL THEN '-1'
            ELSE CAST(ROUND(ffp.ride_10_above, 2) AS STRING)
        END as ride_10_above
    FROM active_flights af
    CROSS JOIN date_ranges dr
    LEFT JOIN flight_frequency_percentages ffp ON af.FK_DEPAIRPORT = ffp.FK_DEPAIRPORT
                             AND af.FK_ARRIAIRPORT = ffp.FK_ARRIAIRPORT
                             AND af.OPERAT_FLIGHT_NUM = ffp.OPERAT_FLIGHT_NUM
                             AND af.OPERAT_AIRLINE = ffp.OPERAT_AIRLINE
                             AND dr.num = ffp.time_index
    ORDER BY af.FK_DEPAIRPORT, af.FK_ARRIAIRPORT, af.OPERAT_FLIGHT_NUM, dr.num
),
-- 按频次区间聚合24个时间序列值
frequency_aggregated AS (
    SELECT 
        fts.FK_DEPAIRPORT,
        fts.FK_ARRIAIRPORT,
        fts.OPERAT_FLIGHT_NUM,
        fts.OPERAT_AIRLINE,
        CONCAT(
            '{"ride_1":[', GROUP_CONCAT(fts.ride_1 ORDER BY fts.time_index), '],',
            '"ride_2_4":[', GROUP_CONCAT(fts.ride_2_4 ORDER BY fts.time_index), '],',
            '"ride_5_7":[', GROUP_CONCAT(fts.ride_5_7 ORDER BY fts.time_index), '],',
            '"ride_8_10":[', GROUP_CONCAT(fts.ride_8_10 ORDER BY fts.time_index), '],',
            '"ride_10_above":[', GROUP_CONCAT(fts.ride_10_above ORDER BY fts.time_index), ']}'
        ) as flight_frequency_json
    FROM flight_time_series fts
    GROUP BY fts.FK_DEPAIRPORT, fts.FK_ARRIAIRPORT, fts.OPERAT_FLIGHT_NUM, fts.OPERAT_AIRLINE
)
-- 最终插入
SELECT 
    CONCAT(fa.FK_DEPAIRPORT, '_', fa.FK_ARRIAIRPORT, '_', fa.OPERAT_FLIGHT_NUM, '_FREQ_CURRENT') as cache_key,
    fa.FK_DEPAIRPORT as departure_airport,
    fa.FK_ARRIAIRPORT as arrival_airport,
    fa.OPERAT_FLIGHT_NUM as flight_number,
    fa.OPERAT_AIRLINE as airline_code,
    'FLIGHT_FREQUENCY' as prediction_type,
    'CURRENT' as time_range_type,
    fa.flight_frequency_json,
    NOW() as created_time,
    NOW() as updated_time
FROM frequency_aggregated fa;




-- 乘机频次分布 - 合并24个DAY_3时间区间为一条数据
INSERT INTO DWS_TEST.T_DWS_FLIGHT_PREDICTION_DAILY (
    cache_key, departure_airport, arrival_airport, flight_number,
    airline_code, prediction_type, time_range_type, flight_frequency_json,
    created_time, updated_time
)
WITH 
-- 生成过去24个3天时间区间
date_ranges AS (
    SELECT 
        num,
        DATE_SUB(CURRENT_DATE(), INTERVAL (25 - num) * 3 DAY) as range_start_date,
        DATE_SUB(CURRENT_DATE(), INTERVAL (24 - num) * 3 DAY) as range_end_date
    FROM (
        SELECT 1 as num UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6
        UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12
        UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18
        UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24
    ) n
    ORDER BY num
),
-- 获取所有活跃的航班航线
active_flights AS (
    SELECT DISTINCT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE
    FROM DWD_TEST.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_TEST.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    WHERE f.DATA_ACTIVE = true
	  AND d.OPERAT_AIRLINE = 'SC'
      AND f.FK_DEPARTURES_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
      AND d.OPERAT_FLIGHT_NUM IS NOT NULL
      AND d.OPERAT_FLIGHT_NUM != ''
),
-- 计算每个航班在每个时间区间的乘机频次分布
flight_frequency_percentages AS (
    SELECT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE,
        dr.num as time_index,
        ROUND(SUM(CASE WHEN u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS = 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as ride_1,
        ROUND(SUM(CASE WHEN u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS BETWEEN 2 AND 4 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as ride_2_4,
        ROUND(SUM(CASE WHEN u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS BETWEEN 5 AND 7 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as ride_5_7,
        ROUND(SUM(CASE WHEN u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS BETWEEN 8 AND 10 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as ride_8_10,
        ROUND(SUM(CASE WHEN u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS > 10 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as ride_10_above
    FROM DWD_TEST.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_TEST.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    INNER JOIN DIM_TEST.T_DIM_USER_DIM u ON f.FK_PASSENGER_USER_TID = u.PK_ID
    INNER JOIN date_ranges dr ON f.FK_DEPARTURES_DATE >= dr.range_start_date 
                      AND f.FK_DEPARTURES_DATE < dr.range_end_date
    WHERE f.DATA_ACTIVE = true
        AND u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS IS NOT NULL
        AND d.OPERAT_FLIGHT_NUM IS NOT NULL
        AND d.OPERAT_FLIGHT_NUM != ''
    GROUP BY f.FK_DEPAIRPORT, f.FK_ARRIAIRPORT, d.OPERAT_FLIGHT_NUM, d.OPERAT_AIRLINE, dr.num
),
-- 为每个航班生成完整的24个时间区间数据，格式化数值
flight_time_series AS (
    SELECT 
        af.FK_DEPAIRPORT,
        af.FK_ARRIAIRPORT,
        af.OPERAT_FLIGHT_NUM,
        af.OPERAT_AIRLINE,
        dr.num as time_index,
        CASE 
            WHEN ffp.ride_1 IS NULL THEN '-1'
            ELSE CAST(ROUND(ffp.ride_1, 2) AS STRING)
        END as ride_1,
        CASE 
            WHEN ffp.ride_2_4 IS NULL THEN '-1'
            ELSE CAST(ROUND(ffp.ride_2_4, 2) AS STRING)
        END as ride_2_4,
        CASE 
            WHEN ffp.ride_5_7 IS NULL THEN '-1'
            ELSE CAST(ROUND(ffp.ride_5_7, 2) AS STRING)
        END as ride_5_7,
        CASE 
            WHEN ffp.ride_8_10 IS NULL THEN '-1'
            ELSE CAST(ROUND(ffp.ride_8_10, 2) AS STRING)
        END as ride_8_10,
        CASE 
            WHEN ffp.ride_10_above IS NULL THEN '-1'
            ELSE CAST(ROUND(ffp.ride_10_above, 2) AS STRING)
        END as ride_10_above
    FROM active_flights af
    CROSS JOIN date_ranges dr
    LEFT JOIN flight_frequency_percentages ffp ON af.FK_DEPAIRPORT = ffp.FK_DEPAIRPORT
                             AND af.FK_ARRIAIRPORT = ffp.FK_ARRIAIRPORT
                             AND af.OPERAT_FLIGHT_NUM = ffp.OPERAT_FLIGHT_NUM
                             AND af.OPERAT_AIRLINE = ffp.OPERAT_AIRLINE
                             AND dr.num = ffp.time_index
    ORDER BY af.FK_DEPAIRPORT, af.FK_ARRIAIRPORT, af.OPERAT_FLIGHT_NUM, dr.num
),
-- 按频次区间聚合24个时间序列值
frequency_aggregated AS (
    SELECT 
        fts.FK_DEPAIRPORT,
        fts.FK_ARRIAIRPORT,
        fts.OPERAT_FLIGHT_NUM,
        fts.OPERAT_AIRLINE,
        CONCAT(
            '{"ride_1":[', GROUP_CONCAT(fts.ride_1 ORDER BY fts.time_index), '],',
            '"ride_2_4":[', GROUP_CONCAT(fts.ride_2_4 ORDER BY fts.time_index), '],',
            '"ride_5_7":[', GROUP_CONCAT(fts.ride_5_7 ORDER BY fts.time_index), '],',
            '"ride_8_10":[', GROUP_CONCAT(fts.ride_8_10 ORDER BY fts.time_index), '],',
            '"ride_10_above":[', GROUP_CONCAT(fts.ride_10_above ORDER BY fts.time_index), ']}'
        ) as flight_frequency_json
    FROM flight_time_series fts
    GROUP BY fts.FK_DEPAIRPORT, fts.FK_ARRIAIRPORT, fts.OPERAT_FLIGHT_NUM, fts.OPERAT_AIRLINE
)
-- 最终插入
SELECT 
    CONCAT(fa.FK_DEPAIRPORT, '_', fa.FK_ARRIAIRPORT, '_', fa.OPERAT_FLIGHT_NUM, '_FREQ_DAY_3') as cache_key,
    fa.FK_DEPAIRPORT as departure_airport,
    fa.FK_ARRIAIRPORT as arrival_airport,
    fa.OPERAT_FLIGHT_NUM as flight_number,
    fa.OPERAT_AIRLINE as airline_code,
    'FLIGHT_FREQUENCY' as prediction_type,
    'DAY_3' as time_range_type,
    fa.flight_frequency_json,
    NOW() as created_time,
    NOW() as updated_time
FROM frequency_aggregated fa;




-- 乘机频次分布 - 合并24个DAY_7时间区间为一条数据
INSERT INTO DWS_TEST.T_DWS_FLIGHT_PREDICTION_DAILY (
    cache_key, departure_airport, arrival_airport, flight_number,
    airline_code, prediction_type, time_range_type, flight_frequency_json,
    created_time, updated_time
)
WITH 
-- 生成过去24个7天时间区间
date_ranges AS (
    SELECT 
        num,
        DATE_SUB(CURRENT_DATE(), INTERVAL (25 - num) * 7 DAY) as range_start_date,
        DATE_SUB(CURRENT_DATE(), INTERVAL (24 - num) * 7 DAY) as range_end_date
    FROM (
        SELECT 1 as num UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6
        UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12
        UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18
        UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24
    ) n
    ORDER BY num
),
-- 获取所有活跃的航班航线
active_flights AS (
    SELECT DISTINCT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE
    FROM DWD_TEST.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_TEST.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    WHERE f.DATA_ACTIVE = true
	  AND d.OPERAT_AIRLINE = 'SC'
      AND f.FK_DEPARTURES_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
      AND d.OPERAT_FLIGHT_NUM IS NOT NULL
      AND d.OPERAT_FLIGHT_NUM != ''
),
-- 计算每个航班在每个时间区间的乘机频次分布
flight_frequency_percentages AS (
    SELECT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE,
        dr.num as time_index,
        ROUND(SUM(CASE WHEN u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS = 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as ride_1,
        ROUND(SUM(CASE WHEN u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS BETWEEN 2 AND 4 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as ride_2_4,
        ROUND(SUM(CASE WHEN u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS BETWEEN 5 AND 7 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as ride_5_7,
        ROUND(SUM(CASE WHEN u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS BETWEEN 8 AND 10 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as ride_8_10,
        ROUND(SUM(CASE WHEN u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS > 10 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as ride_10_above
    FROM DWD_TEST.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_TEST.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    INNER JOIN DIM_TEST.T_DIM_USER_DIM u ON f.FK_PASSENGER_USER_TID = u.PK_ID
    INNER JOIN date_ranges dr ON f.FK_DEPARTURES_DATE >= dr.range_start_date 
                      AND f.FK_DEPARTURES_DATE < dr.range_end_date
    WHERE f.DATA_ACTIVE = true
        AND u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS IS NOT NULL
        AND d.OPERAT_FLIGHT_NUM IS NOT NULL
        AND d.OPERAT_FLIGHT_NUM != ''
    GROUP BY f.FK_DEPAIRPORT, f.FK_ARRIAIRPORT, d.OPERAT_FLIGHT_NUM, d.OPERAT_AIRLINE, dr.num
),
-- 为每个航班生成完整的24个时间区间数据，格式化数值
flight_time_series AS (
    SELECT 
        af.FK_DEPAIRPORT,
        af.FK_ARRIAIRPORT,
        af.OPERAT_FLIGHT_NUM,
        af.OPERAT_AIRLINE,
        dr.num as time_index,
        CASE 
            WHEN ffp.ride_1 IS NULL THEN '-1'
            ELSE CAST(ROUND(ffp.ride_1, 2) AS STRING)
        END as ride_1,
        CASE 
            WHEN ffp.ride_2_4 IS NULL THEN '-1'
            ELSE CAST(ROUND(ffp.ride_2_4, 2) AS STRING)
        END as ride_2_4,
        CASE 
            WHEN ffp.ride_5_7 IS NULL THEN '-1'
            ELSE CAST(ROUND(ffp.ride_5_7, 2) AS STRING)
        END as ride_5_7,
        CASE 
            WHEN ffp.ride_8_10 IS NULL THEN '-1'
            ELSE CAST(ROUND(ffp.ride_8_10, 2) AS STRING)
        END as ride_8_10,
        CASE 
            WHEN ffp.ride_10_above IS NULL THEN '-1'
            ELSE CAST(ROUND(ffp.ride_10_above, 2) AS STRING)
        END as ride_10_above
    FROM active_flights af
    CROSS JOIN date_ranges dr
    LEFT JOIN flight_frequency_percentages ffp ON af.FK_DEPAIRPORT = ffp.FK_DEPAIRPORT
                             AND af.FK_ARRIAIRPORT = ffp.FK_ARRIAIRPORT
                             AND af.OPERAT_FLIGHT_NUM = ffp.OPERAT_FLIGHT_NUM
                             AND af.OPERAT_AIRLINE = ffp.OPERAT_AIRLINE
                             AND dr.num = ffp.time_index
    ORDER BY af.FK_DEPAIRPORT, af.FK_ARRIAIRPORT, af.OPERAT_FLIGHT_NUM, dr.num
),
-- 按频次区间聚合24个时间序列值
frequency_aggregated AS (
    SELECT 
        fts.FK_DEPAIRPORT,
        fts.FK_ARRIAIRPORT,
        fts.OPERAT_FLIGHT_NUM,
        fts.OPERAT_AIRLINE,
        CONCAT(
            '{"ride_1":[', GROUP_CONCAT(fts.ride_1 ORDER BY fts.time_index), '],',
            '"ride_2_4":[', GROUP_CONCAT(fts.ride_2_4 ORDER BY fts.time_index), '],',
            '"ride_5_7":[', GROUP_CONCAT(fts.ride_5_7 ORDER BY fts.time_index), '],',
            '"ride_8_10":[', GROUP_CONCAT(fts.ride_8_10 ORDER BY fts.time_index), '],',
            '"ride_10_above":[', GROUP_CONCAT(fts.ride_10_above ORDER BY fts.time_index), ']}'
        ) as flight_frequency_json
    FROM flight_time_series fts
    GROUP BY fts.FK_DEPAIRPORT, fts.FK_ARRIAIRPORT, fts.OPERAT_FLIGHT_NUM, fts.OPERAT_AIRLINE
)
-- 最终插入
SELECT 
    CONCAT(fa.FK_DEPAIRPORT, '_', fa.FK_ARRIAIRPORT, '_', fa.OPERAT_FLIGHT_NUM, '_FREQ_DAY_7') as cache_key,
    fa.FK_DEPAIRPORT as departure_airport,
    fa.FK_ARRIAIRPORT as arrival_airport,
    fa.OPERAT_FLIGHT_NUM as flight_number,
    fa.OPERAT_AIRLINE as airline_code,
    'FLIGHT_FREQUENCY' as prediction_type,
    'DAY_7' as time_range_type,
    fa.flight_frequency_json,
    NOW() as created_time,
    NOW() as updated_time
FROM frequency_aggregated fa;




-- 乘机频次分布 - 合并24个DAY_30时间区间为一条数据
INSERT INTO DWS_TEST.T_DWS_FLIGHT_PREDICTION_DAILY (
    cache_key, departure_airport, arrival_airport, flight_number,
    airline_code, prediction_type, time_range_type, flight_frequency_json,
    created_time, updated_time
)
WITH 
-- 生成过去24个30天时间区间（按从最早到最近排序）
date_ranges AS (
    SELECT 
        num,
        DATE_SUB(CURRENT_DATE(), INTERVAL (25 - num) * 30 DAY) as range_start_date,
        DATE_SUB(CURRENT_DATE(), INTERVAL (24 - num) * 30 DAY) as range_end_date
    FROM (
        SELECT 1 as num UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6
        UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12
        UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18
        UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24
    ) n
    ORDER BY num
),
-- 获取所有活跃的航班航线（使用INNER JOIN）
active_flights AS (
    SELECT DISTINCT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE
    FROM DWD_TEST.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_TEST.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    WHERE f.DATA_ACTIVE = true
	  AND d.OPERAT_AIRLINE = 'SC'
      AND f.FK_DEPARTURES_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
      AND d.OPERAT_FLIGHT_NUM IS NOT NULL
      AND d.OPERAT_FLIGHT_NUM != ''
),
-- 计算每个航班在每个时间区间的乘机频次分布
flight_frequency_percentages AS (
    SELECT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE,
        dr.num as time_index,
        ROUND(SUM(CASE WHEN u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS = 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as ride_1,
        ROUND(SUM(CASE WHEN u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS BETWEEN 2 AND 4 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as ride_2_4,
        ROUND(SUM(CASE WHEN u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS BETWEEN 5 AND 7 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as ride_5_7,
        ROUND(SUM(CASE WHEN u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS BETWEEN 8 AND 10 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as ride_8_10,
        ROUND(SUM(CASE WHEN u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS > 10 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as ride_10_above
    FROM DWD_TEST.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_TEST.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    INNER JOIN DIM_TEST.T_DIM_USER_DIM u ON f.FK_PASSENGER_USER_TID = u.PK_ID
    INNER JOIN date_ranges dr ON f.FK_DEPARTURES_DATE >= dr.range_start_date 
                      AND f.FK_DEPARTURES_DATE < dr.range_end_date
    WHERE f.DATA_ACTIVE = true
        AND u.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS IS NOT NULL
        AND d.OPERAT_FLIGHT_NUM IS NOT NULL
        AND d.OPERAT_FLIGHT_NUM != ''
    GROUP BY f.FK_DEPAIRPORT, f.FK_ARRIAIRPORT, d.OPERAT_FLIGHT_NUM, d.OPERAT_AIRLINE, dr.num
),
-- 为每个航班生成完整的24个时间区间数据，格式化数值
flight_time_series AS (
    SELECT 
        af.FK_DEPAIRPORT,
        af.FK_ARRIAIRPORT,
        af.OPERAT_FLIGHT_NUM,
        af.OPERAT_AIRLINE,
        dr.num as time_index,
        CASE 
            WHEN ffp.ride_1 IS NULL THEN '-1'
            ELSE CAST(ROUND(ffp.ride_1, 2) AS STRING)
        END as ride_1,
        CASE 
            WHEN ffp.ride_2_4 IS NULL THEN '-1'
            ELSE CAST(ROUND(ffp.ride_2_4, 2) AS STRING)
        END as ride_2_4,
        CASE 
            WHEN ffp.ride_5_7 IS NULL THEN '-1'
            ELSE CAST(ROUND(ffp.ride_5_7, 2) AS STRING)
        END as ride_5_7,
        CASE 
            WHEN ffp.ride_8_10 IS NULL THEN '-1'
            ELSE CAST(ROUND(ffp.ride_8_10, 2) AS STRING)
        END as ride_8_10,
        CASE 
            WHEN ffp.ride_10_above IS NULL THEN '-1'
            ELSE CAST(ROUND(ffp.ride_10_above, 2) AS STRING)
        END as ride_10_above
    FROM active_flights af
    CROSS JOIN date_ranges dr
    LEFT JOIN flight_frequency_percentages ffp ON af.FK_DEPAIRPORT = ffp.FK_DEPAIRPORT
                             AND af.FK_ARRIAIRPORT = ffp.FK_ARRIAIRPORT
                             AND af.OPERAT_FLIGHT_NUM = ffp.OPERAT_FLIGHT_NUM
                             AND af.OPERAT_AIRLINE = ffp.OPERAT_AIRLINE
                             AND dr.num = ffp.time_index
    ORDER BY af.FK_DEPAIRPORT, af.FK_ARRIAIRPORT, af.OPERAT_FLIGHT_NUM, dr.num
),
-- 按频次区间聚合24个时间序列值
frequency_aggregated AS (
    SELECT 
        fts.FK_DEPAIRPORT,
        fts.FK_ARRIAIRPORT,
        fts.OPERAT_FLIGHT_NUM,
        fts.OPERAT_AIRLINE,
        CONCAT(
            '{"ride_1":[', GROUP_CONCAT(fts.ride_1 ORDER BY fts.time_index), '],',
            '"ride_2_4":[', GROUP_CONCAT(fts.ride_2_4 ORDER BY fts.time_index), '],',
            '"ride_5_7":[', GROUP_CONCAT(fts.ride_5_7 ORDER BY fts.time_index), '],',
            '"ride_8_10":[', GROUP_CONCAT(fts.ride_8_10 ORDER BY fts.time_index), '],',
            '"ride_10_above":[', GROUP_CONCAT(fts.ride_10_above ORDER BY fts.time_index), ']}'
        ) as flight_frequency_json
    FROM flight_time_series fts
    GROUP BY fts.FK_DEPAIRPORT, fts.FK_ARRIAIRPORT, fts.OPERAT_FLIGHT_NUM, fts.OPERAT_AIRLINE
)
-- 最终插入
SELECT 
    CONCAT(fa.FK_DEPAIRPORT, '_', fa.FK_ARRIAIRPORT, '_', fa.OPERAT_FLIGHT_NUM, '_FREQ_DAY_30') as cache_key,
    fa.FK_DEPAIRPORT as departure_airport,
    fa.FK_ARRIAIRPORT as arrival_airport,
    fa.OPERAT_FLIGHT_NUM as flight_number,
    fa.OPERAT_AIRLINE as airline_code,
    'FLIGHT_FREQUENCY' as prediction_type,
    'DAY_30' as time_range_type,
    fa.flight_frequency_json,
    NOW() as created_time,
    NOW() as updated_time
FROM frequency_aggregated fa;


--事务提交
commit;