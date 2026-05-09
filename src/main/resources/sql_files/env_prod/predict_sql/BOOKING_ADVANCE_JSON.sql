-- 提前订票分布 - 合并24个CURRENT时间区间为一条数据
INSERT INTO DWS_PROD.T_DWS_FLIGHT_PREDICTION_DAILY (
    cache_key, departure_airport, arrival_airport, flight_number,
    airline_code, prediction_type, time_range_type, booking_advance_json,
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
    FROM DWD_PROD.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_PROD.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    WHERE f.DATA_ACTIVE = true
	  AND d.OPERAT_AIRLINE = 'SC'
      AND f.FK_DEPARTURES_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
      AND d.OPERAT_FLIGHT_NUM IS NOT NULL
      AND d.OPERAT_FLIGHT_NUM != ''
),
-- 计算每个航班在每个时间区间的提前订票分布
booking_advance_percentages AS (
    SELECT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE,
        dr.num as time_index,
        ROUND(SUM(CASE WHEN f.AK_ADVBOOK_DAY = 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as tkt_ahead_0,
        ROUND(SUM(CASE WHEN f.AK_ADVBOOK_DAY BETWEEN 1 AND 3 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as tkt_ahead_1_3,
        ROUND(SUM(CASE WHEN f.AK_ADVBOOK_DAY BETWEEN 4 AND 7 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as tkt_ahead_4_7,
        ROUND(SUM(CASE WHEN f.AK_ADVBOOK_DAY > 7 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as tkt_above_7
    FROM DWD_PROD.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_PROD.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    INNER JOIN date_ranges dr ON f.FK_DEPARTURES_DATE >= dr.range_start_date 
                      AND f.FK_DEPARTURES_DATE < dr.range_end_date
    WHERE f.DATA_ACTIVE = true
        AND f.AK_ADVBOOK_DAY IS NOT NULL
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
            WHEN bap.tkt_ahead_0 IS NULL THEN '-1'
            ELSE CAST(ROUND(bap.tkt_ahead_0, 2) AS STRING)
        END as tkt_ahead_0,
        CASE 
            WHEN bap.tkt_ahead_1_3 IS NULL THEN '-1'
            ELSE CAST(ROUND(bap.tkt_ahead_1_3, 2) AS STRING)
        END as tkt_ahead_1_3,
        CASE 
            WHEN bap.tkt_ahead_4_7 IS NULL THEN '-1'
            ELSE CAST(ROUND(bap.tkt_ahead_4_7, 2) AS STRING)
        END as tkt_ahead_4_7,
        CASE 
            WHEN bap.tkt_above_7 IS NULL THEN '-1'
            ELSE CAST(ROUND(bap.tkt_above_7, 2) AS STRING)
        END as tkt_above_7
    FROM active_flights af
    CROSS JOIN date_ranges dr
    LEFT JOIN booking_advance_percentages bap ON af.FK_DEPAIRPORT = bap.FK_DEPAIRPORT
                             AND af.FK_ARRIAIRPORT = bap.FK_ARRIAIRPORT
                             AND af.OPERAT_FLIGHT_NUM = bap.OPERAT_FLIGHT_NUM
                             AND af.OPERAT_AIRLINE = bap.OPERAT_AIRLINE
                             AND dr.num = bap.time_index
    ORDER BY af.FK_DEPAIRPORT, af.FK_ARRIAIRPORT, af.OPERAT_FLIGHT_NUM, dr.num
),
-- 按订票区间聚合24个时间序列值
booking_aggregated AS (
    SELECT 
        fts.FK_DEPAIRPORT,
        fts.FK_ARRIAIRPORT,
        fts.OPERAT_FLIGHT_NUM,
        fts.OPERAT_AIRLINE,
        CONCAT(
            '{"tkt_ahead_0":[', GROUP_CONCAT(fts.tkt_ahead_0 ORDER BY fts.time_index), '],',
            '"tkt_ahead_1_3":[', GROUP_CONCAT(fts.tkt_ahead_1_3 ORDER BY fts.time_index), '],',
            '"tkt_ahead_4_7":[', GROUP_CONCAT(fts.tkt_ahead_4_7 ORDER BY fts.time_index), '],',
            '"tkt_above_7":[', GROUP_CONCAT(fts.tkt_above_7 ORDER BY fts.time_index), ']}'
        ) as booking_advance_json
    FROM flight_time_series fts
    GROUP BY fts.FK_DEPAIRPORT, fts.FK_ARRIAIRPORT, fts.OPERAT_FLIGHT_NUM, fts.OPERAT_AIRLINE
)
-- 最终插入
SELECT 
    CONCAT(ba.FK_DEPAIRPORT, '_', ba.FK_ARRIAIRPORT, '_', ba.OPERAT_FLIGHT_NUM, '_BOOK_CURRENT') as cache_key,
    ba.FK_DEPAIRPORT as departure_airport,
    ba.FK_ARRIAIRPORT as arrival_airport,
    ba.OPERAT_FLIGHT_NUM as flight_number,
    ba.OPERAT_AIRLINE as airline_code,
    'BOOKING_ADVANCE' as prediction_type,
    'CURRENT' as time_range_type,
    ba.booking_advance_json,
    NOW() as created_time,
    NOW() as updated_time
FROM booking_aggregated ba;




-- 提前订票分布 - 合并24个DAY_3时间区间为一条数据
INSERT INTO DWS_PROD.T_DWS_FLIGHT_PREDICTION_DAILY (
    cache_key, departure_airport, arrival_airport, flight_number,
    airline_code, prediction_type, time_range_type, booking_advance_json,
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
    FROM DWD_PROD.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_PROD.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    WHERE f.DATA_ACTIVE = true
	  AND d.OPERAT_AIRLINE = 'SC'
      AND f.FK_DEPARTURES_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
      AND d.OPERAT_FLIGHT_NUM IS NOT NULL
      AND d.OPERAT_FLIGHT_NUM != ''
),
-- 计算每个航班在每个时间区间的提前订票分布
booking_advance_percentages AS (
    SELECT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE,
        dr.num as time_index,
        ROUND(SUM(CASE WHEN f.AK_ADVBOOK_DAY = 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as tkt_ahead_0,
        ROUND(SUM(CASE WHEN f.AK_ADVBOOK_DAY BETWEEN 1 AND 3 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as tkt_ahead_1_3,
        ROUND(SUM(CASE WHEN f.AK_ADVBOOK_DAY BETWEEN 4 AND 7 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as tkt_ahead_4_7,
        ROUND(SUM(CASE WHEN f.AK_ADVBOOK_DAY > 7 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as tkt_above_7
    FROM DWD_PROD.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_PROD.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    INNER JOIN date_ranges dr ON f.FK_DEPARTURES_DATE >= dr.range_start_date 
                      AND f.FK_DEPARTURES_DATE < dr.range_end_date
    WHERE f.DATA_ACTIVE = true
        AND f.AK_ADVBOOK_DAY IS NOT NULL
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
            WHEN bap.tkt_ahead_0 IS NULL THEN '-1'
            ELSE CAST(ROUND(bap.tkt_ahead_0, 2) AS STRING)
        END as tkt_ahead_0,
        CASE 
            WHEN bap.tkt_ahead_1_3 IS NULL THEN '-1'
            ELSE CAST(ROUND(bap.tkt_ahead_1_3, 2) AS STRING)
        END as tkt_ahead_1_3,
        CASE 
            WHEN bap.tkt_ahead_4_7 IS NULL THEN '-1'
            ELSE CAST(ROUND(bap.tkt_ahead_4_7, 2) AS STRING)
        END as tkt_ahead_4_7,
        CASE 
            WHEN bap.tkt_above_7 IS NULL THEN '-1'
            ELSE CAST(ROUND(bap.tkt_above_7, 2) AS STRING)
        END as tkt_above_7
    FROM active_flights af
    CROSS JOIN date_ranges dr
    LEFT JOIN booking_advance_percentages bap ON af.FK_DEPAIRPORT = bap.FK_DEPAIRPORT
                             AND af.FK_ARRIAIRPORT = bap.FK_ARRIAIRPORT
                             AND af.OPERAT_FLIGHT_NUM = bap.OPERAT_FLIGHT_NUM
                             AND af.OPERAT_AIRLINE = bap.OPERAT_AIRLINE
                             AND dr.num = bap.time_index
    ORDER BY af.FK_DEPAIRPORT, af.FK_ARRIAIRPORT, af.OPERAT_FLIGHT_NUM, dr.num
),
-- 按订票区间聚合24个时间序列值
booking_aggregated AS (
    SELECT 
        fts.FK_DEPAIRPORT,
        fts.FK_ARRIAIRPORT,
        fts.OPERAT_FLIGHT_NUM,
        fts.OPERAT_AIRLINE,
        CONCAT(
            '{"tkt_ahead_0":[', GROUP_CONCAT(fts.tkt_ahead_0 ORDER BY fts.time_index), '],',
            '"tkt_ahead_1_3":[', GROUP_CONCAT(fts.tkt_ahead_1_3 ORDER BY fts.time_index), '],',
            '"tkt_ahead_4_7":[', GROUP_CONCAT(fts.tkt_ahead_4_7 ORDER BY fts.time_index), '],',
            '"tkt_above_7":[', GROUP_CONCAT(fts.tkt_above_7 ORDER BY fts.time_index), ']}'
        ) as booking_advance_json
    FROM flight_time_series fts
    GROUP BY fts.FK_DEPAIRPORT, fts.FK_ARRIAIRPORT, fts.OPERAT_FLIGHT_NUM, fts.OPERAT_AIRLINE
)
-- 最终插入
SELECT 
    CONCAT(ba.FK_DEPAIRPORT, '_', ba.FK_ARRIAIRPORT, '_', ba.OPERAT_FLIGHT_NUM, '_BOOK_DAY_3') as cache_key,
    ba.FK_DEPAIRPORT as departure_airport,
    ba.FK_ARRIAIRPORT as arrival_airport,
    ba.OPERAT_FLIGHT_NUM as flight_number,
    ba.OPERAT_AIRLINE as airline_code,
    'BOOKING_ADVANCE' as prediction_type,
    'DAY_3' as time_range_type,
    ba.booking_advance_json,
    NOW() as created_time,
    NOW() as updated_time
FROM booking_aggregated ba;




-- 提前订票分布 - 合并24个DAY_7时间区间为一条数据
INSERT INTO DWS_PROD.T_DWS_FLIGHT_PREDICTION_DAILY (
    cache_key, departure_airport, arrival_airport, flight_number,
    airline_code, prediction_type, time_range_type, booking_advance_json,
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
    FROM DWD_PROD.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_PROD.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    WHERE f.DATA_ACTIVE = true
	  AND d.OPERAT_AIRLINE = 'SC'
      AND f.FK_DEPARTURES_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
      AND d.OPERAT_FLIGHT_NUM IS NOT NULL
      AND d.OPERAT_FLIGHT_NUM != ''
),
-- 计算每个航班在每个时间区间的提前订票分布
booking_advance_percentages AS (
    SELECT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE,
        dr.num as time_index,
        ROUND(SUM(CASE WHEN f.AK_ADVBOOK_DAY = 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as tkt_ahead_0,
        ROUND(SUM(CASE WHEN f.AK_ADVBOOK_DAY BETWEEN 1 AND 3 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as tkt_ahead_1_3,
        ROUND(SUM(CASE WHEN f.AK_ADVBOOK_DAY BETWEEN 4 AND 7 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as tkt_ahead_4_7,
        ROUND(SUM(CASE WHEN f.AK_ADVBOOK_DAY > 7 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as tkt_above_7
    FROM DWD_PROD.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_PROD.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    INNER JOIN date_ranges dr ON f.FK_DEPARTURES_DATE >= dr.range_start_date 
                      AND f.FK_DEPARTURES_DATE < dr.range_end_date
    WHERE f.DATA_ACTIVE = true
        AND f.AK_ADVBOOK_DAY IS NOT NULL
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
            WHEN bap.tkt_ahead_0 IS NULL THEN '-1'
            ELSE CAST(ROUND(bap.tkt_ahead_0, 2) AS STRING)
        END as tkt_ahead_0,
        CASE 
            WHEN bap.tkt_ahead_1_3 IS NULL THEN '-1'
            ELSE CAST(ROUND(bap.tkt_ahead_1_3, 2) AS STRING)
        END as tkt_ahead_1_3,
        CASE 
            WHEN bap.tkt_ahead_4_7 IS NULL THEN '-1'
            ELSE CAST(ROUND(bap.tkt_ahead_4_7, 2) AS STRING)
        END as tkt_ahead_4_7,
        CASE 
            WHEN bap.tkt_above_7 IS NULL THEN '-1'
            ELSE CAST(ROUND(bap.tkt_above_7, 2) AS STRING)
        END as tkt_above_7
    FROM active_flights af
    CROSS JOIN date_ranges dr
    LEFT JOIN booking_advance_percentages bap ON af.FK_DEPAIRPORT = bap.FK_DEPAIRPORT
                             AND af.FK_ARRIAIRPORT = bap.FK_ARRIAIRPORT
                             AND af.OPERAT_FLIGHT_NUM = bap.OPERAT_FLIGHT_NUM
                             AND af.OPERAT_AIRLINE = bap.OPERAT_AIRLINE
                             AND dr.num = bap.time_index
    ORDER BY af.FK_DEPAIRPORT, af.FK_ARRIAIRPORT, af.OPERAT_FLIGHT_NUM, dr.num
),
-- 按订票区间聚合24个时间序列值
booking_aggregated AS (
    SELECT 
        fts.FK_DEPAIRPORT,
        fts.FK_ARRIAIRPORT,
        fts.OPERAT_FLIGHT_NUM,
        fts.OPERAT_AIRLINE,
        CONCAT(
            '{"tkt_ahead_0":[', GROUP_CONCAT(fts.tkt_ahead_0 ORDER BY fts.time_index), '],',
            '"tkt_ahead_1_3":[', GROUP_CONCAT(fts.tkt_ahead_1_3 ORDER BY fts.time_index), '],',
            '"tkt_ahead_4_7":[', GROUP_CONCAT(fts.tkt_ahead_4_7 ORDER BY fts.time_index), '],',
            '"tkt_above_7":[', GROUP_CONCAT(fts.tkt_above_7 ORDER BY fts.time_index), ']}'
        ) as booking_advance_json
    FROM flight_time_series fts
    GROUP BY fts.FK_DEPAIRPORT, fts.FK_ARRIAIRPORT, fts.OPERAT_FLIGHT_NUM, fts.OPERAT_AIRLINE
)
-- 最终插入
SELECT 
    CONCAT(ba.FK_DEPAIRPORT, '_', ba.FK_ARRIAIRPORT, '_', ba.OPERAT_FLIGHT_NUM, '_BOOK_DAY_7') as cache_key,
    ba.FK_DEPAIRPORT as departure_airport,
    ba.FK_ARRIAIRPORT as arrival_airport,
    ba.OPERAT_FLIGHT_NUM as flight_number,
    ba.OPERAT_AIRLINE as airline_code,
    'BOOKING_ADVANCE' as prediction_type,
    'DAY_7' as time_range_type,
    ba.booking_advance_json,
    NOW() as created_time,
    NOW() as updated_time
FROM booking_aggregated ba;




-- 提前订票分布 - 合并24个DAY_30时间区间为一条数据
INSERT INTO DWS_PROD.T_DWS_FLIGHT_PREDICTION_DAILY (
    cache_key, departure_airport, arrival_airport, flight_number,
    airline_code, prediction_type, time_range_type, booking_advance_json,
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
    FROM DWD_PROD.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_PROD.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    WHERE f.DATA_ACTIVE = true
	  AND d.OPERAT_AIRLINE = 'SC'
      AND f.FK_DEPARTURES_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
      AND d.OPERAT_FLIGHT_NUM IS NOT NULL
      AND d.OPERAT_FLIGHT_NUM != ''
),
-- 计算每个航班在每个时间区间的提前订票分布
booking_advance_percentages AS (
    SELECT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE,
        dr.num as time_index,
        ROUND(SUM(CASE WHEN f.AK_ADVBOOK_DAY = 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as tkt_ahead_0,
        ROUND(SUM(CASE WHEN f.AK_ADVBOOK_DAY BETWEEN 1 AND 3 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as tkt_ahead_1_3,
        ROUND(SUM(CASE WHEN f.AK_ADVBOOK_DAY BETWEEN 4 AND 7 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as tkt_ahead_4_7,
        ROUND(SUM(CASE WHEN f.AK_ADVBOOK_DAY > 7 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as tkt_above_7
    FROM DWD_PROD.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_PROD.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    INNER JOIN date_ranges dr ON f.FK_DEPARTURES_DATE >= dr.range_start_date 
                      AND f.FK_DEPARTURES_DATE < dr.range_end_date
    WHERE f.DATA_ACTIVE = true
        AND f.AK_ADVBOOK_DAY IS NOT NULL
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
            WHEN bap.tkt_ahead_0 IS NULL THEN '-1'
            ELSE CAST(ROUND(bap.tkt_ahead_0, 2) AS STRING)
        END as tkt_ahead_0,
        CASE 
            WHEN bap.tkt_ahead_1_3 IS NULL THEN '-1'
            ELSE CAST(ROUND(bap.tkt_ahead_1_3, 2) AS STRING)
        END as tkt_ahead_1_3,
        CASE 
            WHEN bap.tkt_ahead_4_7 IS NULL THEN '-1'
            ELSE CAST(ROUND(bap.tkt_ahead_4_7, 2) AS STRING)
        END as tkt_ahead_4_7,
        CASE 
            WHEN bap.tkt_above_7 IS NULL THEN '-1'
            ELSE CAST(ROUND(bap.tkt_above_7, 2) AS STRING)
        END as tkt_above_7
    FROM active_flights af
    CROSS JOIN date_ranges dr
    LEFT JOIN booking_advance_percentages bap ON af.FK_DEPAIRPORT = bap.FK_DEPAIRPORT
                             AND af.FK_ARRIAIRPORT = bap.FK_ARRIAIRPORT
                             AND af.OPERAT_FLIGHT_NUM = bap.OPERAT_FLIGHT_NUM
                             AND af.OPERAT_AIRLINE = bap.OPERAT_AIRLINE
                             AND dr.num = bap.time_index
    ORDER BY af.FK_DEPAIRPORT, af.FK_ARRIAIRPORT, af.OPERAT_FLIGHT_NUM, dr.num
),
-- 按订票区间聚合24个时间序列值
booking_aggregated AS (
    SELECT 
        fts.FK_DEPAIRPORT,
        fts.FK_ARRIAIRPORT,
        fts.OPERAT_FLIGHT_NUM,
        fts.OPERAT_AIRLINE,
        CONCAT(
            '{"tkt_ahead_0":[', GROUP_CONCAT(fts.tkt_ahead_0 ORDER BY fts.time_index), '],',
            '"tkt_ahead_1_3":[', GROUP_CONCAT(fts.tkt_ahead_1_3 ORDER BY fts.time_index), '],',
            '"tkt_ahead_4_7":[', GROUP_CONCAT(fts.tkt_ahead_4_7 ORDER BY fts.time_index), '],',
            '"tkt_above_7":[', GROUP_CONCAT(fts.tkt_above_7 ORDER BY fts.time_index), ']}'
        ) as booking_advance_json
    FROM flight_time_series fts
    GROUP BY fts.FK_DEPAIRPORT, fts.FK_ARRIAIRPORT, fts.OPERAT_FLIGHT_NUM, fts.OPERAT_AIRLINE
)
-- 最终插入
SELECT 
    CONCAT(ba.FK_DEPAIRPORT, '_', ba.FK_ARRIAIRPORT, '_', ba.OPERAT_FLIGHT_NUM, '_BOOK_DAY_30') as cache_key,
    ba.FK_DEPAIRPORT as departure_airport,
    ba.FK_ARRIAIRPORT as arrival_airport,
    ba.OPERAT_FLIGHT_NUM as flight_number,
    ba.OPERAT_AIRLINE as airline_code,
    'BOOKING_ADVANCE' as prediction_type,
    'DAY_30' as time_range_type,
    ba.booking_advance_json,
    NOW() as created_time,
    NOW() as updated_time
FROM booking_aggregated ba;


--事务提交
commit;