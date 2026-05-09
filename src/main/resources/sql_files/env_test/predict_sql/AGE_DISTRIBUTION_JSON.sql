-- 年龄段分布 - 合并24个CURRENT时间区间为一条数据（按年龄段分组格式）
INSERT INTO DWS_TEST.T_DWS_FLIGHT_PREDICTION_DAILY (
    cache_key, departure_airport, arrival_airport, flight_number,
    airline_code, prediction_type, time_range_type, age_distribution_json,
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
-- 计算每个航班在每个时间区间的各年龄段占比
flight_age_percentages AS (
    SELECT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE,
        dr.num as time_index,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 0 AND f.PASSENGER_AGE <= 2 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_0_2,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 2 AND f.PASSENGER_AGE <= 12 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_2_12,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 12 AND f.PASSENGER_AGE <= 18 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_12_18,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 18 AND f.PASSENGER_AGE <= 25 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_18_25,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 25 AND f.PASSENGER_AGE <= 35 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_25_35,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 35 AND f.PASSENGER_AGE <= 45 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_35_45,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 45 AND f.PASSENGER_AGE <= 55 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_45_55,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 55 AND f.PASSENGER_AGE <= 80 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_55_80,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 80 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_80_above
    FROM DWD_TEST.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_TEST.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    INNER JOIN date_ranges dr ON f.FK_DEPARTURES_DATE >= dr.range_start_date 
                      AND f.FK_DEPARTURES_DATE < dr.range_end_date
    WHERE f.DATA_ACTIVE = true
        AND f.PASSENGER_AGE IS NOT NULL
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
            WHEN fap.age_0_2 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_0_2, 2) AS STRING)
        END as age_0_2,
        CASE 
            WHEN fap.age_2_12 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_2_12, 2) AS STRING)
        END as age_2_12,
        CASE 
            WHEN fap.age_12_18 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_12_18, 2) AS STRING)
        END as age_12_18,
        CASE 
            WHEN fap.age_18_25 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_18_25, 2) AS STRING)
        END as age_18_25,
        CASE 
            WHEN fap.age_25_35 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_25_35, 2) AS STRING)
        END as age_25_35,
        CASE 
            WHEN fap.age_35_45 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_35_45, 2) AS STRING)
        END as age_35_45,
        CASE 
            WHEN fap.age_45_55 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_45_55, 2) AS STRING)
        END as age_45_55,
        CASE 
            WHEN fap.age_55_80 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_55_80, 2) AS STRING)
        END as age_55_80,
        CASE 
            WHEN fap.age_80_above IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_80_above, 2) AS STRING)
        END as age_80_above
    FROM active_flights af
    CROSS JOIN date_ranges dr
    LEFT JOIN flight_age_percentages fap ON af.FK_DEPAIRPORT = fap.FK_DEPAIRPORT
                             AND af.FK_ARRIAIRPORT = fap.FK_ARRIAIRPORT
                             AND af.OPERAT_FLIGHT_NUM = fap.OPERAT_FLIGHT_NUM
                             AND af.OPERAT_AIRLINE = fap.OPERAT_AIRLINE
                             AND dr.num = fap.time_index
    ORDER BY af.FK_DEPAIRPORT, af.FK_ARRIAIRPORT, af.OPERAT_FLIGHT_NUM, dr.num
),
-- 按年龄段聚合24个时间序列值
age_aggregated AS (
    SELECT 
        fts.FK_DEPAIRPORT,
        fts.FK_ARRIAIRPORT,
        fts.OPERAT_FLIGHT_NUM,
        fts.OPERAT_AIRLINE,
        CONCAT(
            '{"age_0_2":[', GROUP_CONCAT(fts.age_0_2 ORDER BY fts.time_index), '],',
            '"age_2_12":[', GROUP_CONCAT(fts.age_2_12 ORDER BY fts.time_index), '],',
            '"age_12_18":[', GROUP_CONCAT(fts.age_12_18 ORDER BY fts.time_index), '],',
            '"age_18_25":[', GROUP_CONCAT(fts.age_18_25 ORDER BY fts.time_index), '],',
            '"age_25_35":[', GROUP_CONCAT(fts.age_25_35 ORDER BY fts.time_index), '],',
            '"age_35_45":[', GROUP_CONCAT(fts.age_35_45 ORDER BY fts.time_index), '],',
            '"age_45_55":[', GROUP_CONCAT(fts.age_45_55 ORDER BY fts.time_index), '],',
            '"age_55_80":[', GROUP_CONCAT(fts.age_55_80 ORDER BY fts.time_index), '],',
            '"age_80_above":[', GROUP_CONCAT(fts.age_80_above ORDER BY fts.time_index), ']}'
        ) as age_distribution_json
    FROM flight_time_series fts
    GROUP BY fts.FK_DEPAIRPORT, fts.FK_ARRIAIRPORT, fts.OPERAT_FLIGHT_NUM, fts.OPERAT_AIRLINE
)
-- 最终插入
SELECT 
    CONCAT(aa.FK_DEPAIRPORT, '_', aa.FK_ARRIAIRPORT, '_', aa.OPERAT_FLIGHT_NUM, '_AGE_CURRENT') as cache_key,
    aa.FK_DEPAIRPORT as departure_airport,
    aa.FK_ARRIAIRPORT as arrival_airport,
    aa.OPERAT_FLIGHT_NUM as flight_number,
    aa.OPERAT_AIRLINE as airline_code,
    'AGE_DISTRIBUTION' as prediction_type,
    'CURRENT' as time_range_type,
    aa.age_distribution_json,
    NOW() as created_time,
    NOW() as updated_time
FROM age_aggregated aa;




-- 年龄段分布 - 合并24个DAY_3时间区间为一条数据（按年龄段分组格式）
INSERT INTO DWS_TEST.T_DWS_FLIGHT_PREDICTION_DAILY (
    cache_key, departure_airport, arrival_airport, flight_number,
    airline_code, prediction_type, time_range_type, age_distribution_json,
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
-- 计算每个航班在每个时间区间的各年龄段占比
flight_age_percentages AS (
    SELECT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE,
        dr.num as time_index,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 0 AND f.PASSENGER_AGE <= 2 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_0_2,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 2 AND f.PASSENGER_AGE <= 12 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_2_12,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 12 AND f.PASSENGER_AGE <= 18 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_12_18,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 18 AND f.PASSENGER_AGE <= 25 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_18_25,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 25 AND f.PASSENGER_AGE <= 35 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_25_35,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 35 AND f.PASSENGER_AGE <= 45 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_35_45,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 45 AND f.PASSENGER_AGE <= 55 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_45_55,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 55 AND f.PASSENGER_AGE <= 80 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_55_80,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 80 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_80_above
    FROM DWD_TEST.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_TEST.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    INNER JOIN date_ranges dr ON f.FK_DEPARTURES_DATE >= dr.range_start_date 
                      AND f.FK_DEPARTURES_DATE < dr.range_end_date
    WHERE f.DATA_ACTIVE = true
        AND f.PASSENGER_AGE IS NOT NULL
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
            WHEN fap.age_0_2 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_0_2, 2) AS STRING)
        END as age_0_2,
        CASE 
            WHEN fap.age_2_12 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_2_12, 2) AS STRING)
        END as age_2_12,
        CASE 
            WHEN fap.age_12_18 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_12_18, 2) AS STRING)
        END as age_12_18,
        CASE 
            WHEN fap.age_18_25 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_18_25, 2) AS STRING)
        END as age_18_25,
        CASE 
            WHEN fap.age_25_35 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_25_35, 2) AS STRING)
        END as age_25_35,
        CASE 
            WHEN fap.age_35_45 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_35_45, 2) AS STRING)
        END as age_35_45,
        CASE 
            WHEN fap.age_45_55 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_45_55, 2) AS STRING)
        END as age_45_55,
        CASE 
            WHEN fap.age_55_80 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_55_80, 2) AS STRING)
        END as age_55_80,
        CASE 
            WHEN fap.age_80_above IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_80_above, 2) AS STRING)
        END as age_80_above
    FROM active_flights af
    CROSS JOIN date_ranges dr
    LEFT JOIN flight_age_percentages fap ON af.FK_DEPAIRPORT = fap.FK_DEPAIRPORT
                             AND af.FK_ARRIAIRPORT = fap.FK_ARRIAIRPORT
                             AND af.OPERAT_FLIGHT_NUM = fap.OPERAT_FLIGHT_NUM
                             AND af.OPERAT_AIRLINE = fap.OPERAT_AIRLINE
                             AND dr.num = fap.time_index
    ORDER BY af.FK_DEPAIRPORT, af.FK_ARRIAIRPORT, af.OPERAT_FLIGHT_NUM, dr.num
),
-- 按年龄段聚合24个时间序列值
age_aggregated AS (
    SELECT 
        fts.FK_DEPAIRPORT,
        fts.FK_ARRIAIRPORT,
        fts.OPERAT_FLIGHT_NUM,
        fts.OPERAT_AIRLINE,
        CONCAT(
            '{"age_0_2":[', GROUP_CONCAT(fts.age_0_2 ORDER BY fts.time_index), '],',
            '"age_2_12":[', GROUP_CONCAT(fts.age_2_12 ORDER BY fts.time_index), '],',
            '"age_12_18":[', GROUP_CONCAT(fts.age_12_18 ORDER BY fts.time_index), '],',
            '"age_18_25":[', GROUP_CONCAT(fts.age_18_25 ORDER BY fts.time_index), '],',
            '"age_25_35":[', GROUP_CONCAT(fts.age_25_35 ORDER BY fts.time_index), '],',
            '"age_35_45":[', GROUP_CONCAT(fts.age_35_45 ORDER BY fts.time_index), '],',
            '"age_45_55":[', GROUP_CONCAT(fts.age_45_55 ORDER BY fts.time_index), '],',
            '"age_55_80":[', GROUP_CONCAT(fts.age_55_80 ORDER BY fts.time_index), '],',
            '"age_80_above":[', GROUP_CONCAT(fts.age_80_above ORDER BY fts.time_index), ']}'
        ) as age_distribution_json
    FROM flight_time_series fts
    GROUP BY fts.FK_DEPAIRPORT, fts.FK_ARRIAIRPORT, fts.OPERAT_FLIGHT_NUM, fts.OPERAT_AIRLINE
)
-- 最终插入
SELECT 
    CONCAT(aa.FK_DEPAIRPORT, '_', aa.FK_ARRIAIRPORT, '_', aa.OPERAT_FLIGHT_NUM, '_AGE_DAY_3') as cache_key,
    aa.FK_DEPAIRPORT as departure_airport,
    aa.FK_ARRIAIRPORT as arrival_airport,
    aa.OPERAT_FLIGHT_NUM as flight_number,
    aa.OPERAT_AIRLINE as airline_code,
    'AGE_DISTRIBUTION' as prediction_type,
    'DAY_3' as time_range_type,
    aa.age_distribution_json,
    NOW() as created_time,
    NOW() as updated_time
FROM age_aggregated aa;




-- 年龄段分布 - 合并24个DAY_7时间区间为一条数据（按年龄段分组格式）
INSERT INTO DWS_TEST.T_DWS_FLIGHT_PREDICTION_DAILY (
    cache_key, departure_airport, arrival_airport, flight_number,
    airline_code, prediction_type, time_range_type, age_distribution_json,
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
-- 计算每个航班在每个时间区间的各年龄段占比
flight_age_percentages AS (
    SELECT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE,
        dr.num as time_index,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 0 AND f.PASSENGER_AGE <= 2 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_0_2,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 2 AND f.PASSENGER_AGE <= 12 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_2_12,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 12 AND f.PASSENGER_AGE <= 18 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_12_18,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 18 AND f.PASSENGER_AGE <= 25 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_18_25,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 25 AND f.PASSENGER_AGE <= 35 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_25_35,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 35 AND f.PASSENGER_AGE <= 45 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_35_45,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 45 AND f.PASSENGER_AGE <= 55 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_45_55,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 55 AND f.PASSENGER_AGE <= 80 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_55_80,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 80 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_80_above
    FROM DWD_TEST.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_TEST.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    INNER JOIN date_ranges dr ON f.FK_DEPARTURES_DATE >= dr.range_start_date 
                      AND f.FK_DEPARTURES_DATE < dr.range_end_date
    WHERE f.DATA_ACTIVE = true
        AND f.PASSENGER_AGE IS NOT NULL
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
            WHEN fap.age_0_2 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_0_2, 2) AS STRING)
        END as age_0_2,
        CASE 
            WHEN fap.age_2_12 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_2_12, 2) AS STRING)
        END as age_2_12,
        CASE 
            WHEN fap.age_12_18 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_12_18, 2) AS STRING)
        END as age_12_18,
        CASE 
            WHEN fap.age_18_25 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_18_25, 2) AS STRING)
        END as age_18_25,
        CASE 
            WHEN fap.age_25_35 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_25_35, 2) AS STRING)
        END as age_25_35,
        CASE 
            WHEN fap.age_35_45 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_35_45, 2) AS STRING)
        END as age_35_45,
        CASE 
            WHEN fap.age_45_55 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_45_55, 2) AS STRING)
        END as age_45_55,
        CASE 
            WHEN fap.age_55_80 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_55_80, 2) AS STRING)
        END as age_55_80,
        CASE 
            WHEN fap.age_80_above IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_80_above, 2) AS STRING)
        END as age_80_above
    FROM active_flights af
    CROSS JOIN date_ranges dr
    LEFT JOIN flight_age_percentages fap ON af.FK_DEPAIRPORT = fap.FK_DEPAIRPORT
                             AND af.FK_ARRIAIRPORT = fap.FK_ARRIAIRPORT
                             AND af.OPERAT_FLIGHT_NUM = fap.OPERAT_FLIGHT_NUM
                             AND af.OPERAT_AIRLINE = fap.OPERAT_AIRLINE
                             AND dr.num = fap.time_index
    ORDER BY af.FK_DEPAIRPORT, af.FK_ARRIAIRPORT, af.OPERAT_FLIGHT_NUM, dr.num
),
-- 按年龄段聚合24个时间序列值
age_aggregated AS (
    SELECT 
        fts.FK_DEPAIRPORT,
        fts.FK_ARRIAIRPORT,
        fts.OPERAT_FLIGHT_NUM,
        fts.OPERAT_AIRLINE,
        CONCAT(
            '{"age_0_2":[', GROUP_CONCAT(fts.age_0_2 ORDER BY fts.time_index), '],',
            '"age_2_12":[', GROUP_CONCAT(fts.age_2_12 ORDER BY fts.time_index), '],',
            '"age_12_18":[', GROUP_CONCAT(fts.age_12_18 ORDER BY fts.time_index), '],',
            '"age_18_25":[', GROUP_CONCAT(fts.age_18_25 ORDER BY fts.time_index), '],',
            '"age_25_35":[', GROUP_CONCAT(fts.age_25_35 ORDER BY fts.time_index), '],',
            '"age_35_45":[', GROUP_CONCAT(fts.age_35_45 ORDER BY fts.time_index), '],',
            '"age_45_55":[', GROUP_CONCAT(fts.age_45_55 ORDER BY fts.time_index), '],',
            '"age_55_80":[', GROUP_CONCAT(fts.age_55_80 ORDER BY fts.time_index), '],',
            '"age_80_above":[', GROUP_CONCAT(fts.age_80_above ORDER BY fts.time_index), ']}'
        ) as age_distribution_json
    FROM flight_time_series fts
    GROUP BY fts.FK_DEPAIRPORT, fts.FK_ARRIAIRPORT, fts.OPERAT_FLIGHT_NUM, fts.OPERAT_AIRLINE
)
-- 最终插入
SELECT 
    CONCAT(aa.FK_DEPAIRPORT, '_', aa.FK_ARRIAIRPORT, '_', aa.OPERAT_FLIGHT_NUM, '_AGE_DAY_7') as cache_key,
    aa.FK_DEPAIRPORT as departure_airport,
    aa.FK_ARRIAIRPORT as arrival_airport,
    aa.OPERAT_FLIGHT_NUM as flight_number,
    aa.OPERAT_AIRLINE as airline_code,
    'AGE_DISTRIBUTION' as prediction_type,
    'DAY_7' as time_range_type,
    aa.age_distribution_json,
    NOW() as created_time,
    NOW() as updated_time
FROM age_aggregated aa;




-- 年龄段分布 - 合并24个DAY_30时间区间为一条数据（按年龄段分组格式）
INSERT INTO DWS_TEST.T_DWS_FLIGHT_PREDICTION_DAILY (
    cache_key, departure_airport, arrival_airport, flight_number,
    airline_code, prediction_type, time_range_type, age_distribution_json,
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
-- 计算每个航班在每个时间区间的各年龄段占比（使用小数格式）
flight_age_percentages AS (
    SELECT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE,
        dr.num as time_index,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 0 AND f.PASSENGER_AGE <= 2 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_0_2,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 2 AND f.PASSENGER_AGE <= 12 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_2_12,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 12 AND f.PASSENGER_AGE <= 18 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_12_18,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 18 AND f.PASSENGER_AGE <= 25 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_18_25,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 25 AND f.PASSENGER_AGE <= 35 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_25_35,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 35 AND f.PASSENGER_AGE <= 45 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_35_45,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 45 AND f.PASSENGER_AGE <= 55 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_45_55,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 55 AND f.PASSENGER_AGE <= 80 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_55_80,
        ROUND(SUM(CASE WHEN f.PASSENGER_AGE > 80 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as age_80_above
    FROM DWD_TEST.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_TEST.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    INNER JOIN date_ranges dr ON f.FK_DEPARTURES_DATE >= dr.range_start_date 
                      AND f.FK_DEPARTURES_DATE < dr.range_end_date
    WHERE f.DATA_ACTIVE = true
        AND f.PASSENGER_AGE IS NOT NULL
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
            WHEN fap.age_0_2 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_0_2, 2) AS STRING)
        END as age_0_2,
        CASE 
            WHEN fap.age_2_12 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_2_12, 2) AS STRING)
        END as age_2_12,
        CASE 
            WHEN fap.age_12_18 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_12_18, 2) AS STRING)
        END as age_12_18,
        CASE 
            WHEN fap.age_18_25 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_18_25, 2) AS STRING)
        END as age_18_25,
        CASE 
            WHEN fap.age_25_35 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_25_35, 2) AS STRING)
        END as age_25_35,
        CASE 
            WHEN fap.age_35_45 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_35_45, 2) AS STRING)
        END as age_35_45,
        CASE 
            WHEN fap.age_45_55 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_45_55, 2) AS STRING)
        END as age_45_55,
        CASE 
            WHEN fap.age_55_80 IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_55_80, 2) AS STRING)
        END as age_55_80,
        CASE 
            WHEN fap.age_80_above IS NULL THEN '-1'
            ELSE CAST(ROUND(fap.age_80_above, 2) AS STRING)
        END as age_80_above
    FROM active_flights af
    CROSS JOIN date_ranges dr
    LEFT JOIN flight_age_percentages fap ON af.FK_DEPAIRPORT = fap.FK_DEPAIRPORT
                             AND af.FK_ARRIAIRPORT = fap.FK_ARRIAIRPORT
                             AND af.OPERAT_FLIGHT_NUM = fap.OPERAT_FLIGHT_NUM
                             AND af.OPERAT_AIRLINE = fap.OPERAT_AIRLINE
                             AND dr.num = fap.time_index
    ORDER BY af.FK_DEPAIRPORT, af.FK_ARRIAIRPORT, af.OPERAT_FLIGHT_NUM, dr.num
),
-- 按年龄段聚合24个时间序列值
age_aggregated AS (
    SELECT 
        fts.FK_DEPAIRPORT,
        fts.FK_ARRIAIRPORT,
        fts.OPERAT_FLIGHT_NUM,
        fts.OPERAT_AIRLINE,
        CONCAT(
            '{"age_0_2":[', GROUP_CONCAT(fts.age_0_2 ORDER BY fts.time_index), '],',
            '"age_2_12":[', GROUP_CONCAT(fts.age_2_12 ORDER BY fts.time_index), '],',
            '"age_12_18":[', GROUP_CONCAT(fts.age_12_18 ORDER BY fts.time_index), '],',
            '"age_18_25":[', GROUP_CONCAT(fts.age_18_25 ORDER BY fts.time_index), '],',
            '"age_25_35":[', GROUP_CONCAT(fts.age_25_35 ORDER BY fts.time_index), '],',
            '"age_35_45":[', GROUP_CONCAT(fts.age_35_45 ORDER BY fts.time_index), '],',
            '"age_45_55":[', GROUP_CONCAT(fts.age_45_55 ORDER BY fts.time_index), '],',
            '"age_55_80":[', GROUP_CONCAT(fts.age_55_80 ORDER BY fts.time_index), '],',
            '"age_80_above":[', GROUP_CONCAT(fts.age_80_above ORDER BY fts.time_index), ']}'
        ) as age_distribution_json
    FROM flight_time_series fts
    GROUP BY fts.FK_DEPAIRPORT, fts.FK_ARRIAIRPORT, fts.OPERAT_FLIGHT_NUM, fts.OPERAT_AIRLINE
)
-- 最终插入
SELECT 
    CONCAT(aa.FK_DEPAIRPORT, '_', aa.FK_ARRIAIRPORT, '_', aa.OPERAT_FLIGHT_NUM, '_AGE_DAY_30') as cache_key,
    aa.FK_DEPAIRPORT as departure_airport,
    aa.FK_ARRIAIRPORT as arrival_airport,
    aa.OPERAT_FLIGHT_NUM as flight_number,
    aa.OPERAT_AIRLINE as airline_code,
    'AGE_DISTRIBUTION' as prediction_type,
    'DAY_30' as time_range_type,
    aa.age_distribution_json,
    NOW() as created_time,
    NOW() as updated_time
FROM age_aggregated aa;


--事务提交
commit;