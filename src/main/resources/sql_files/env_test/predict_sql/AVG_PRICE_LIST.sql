-- 平均票价 - 合并24个CURRENT时间区间为一条数据
INSERT INTO DWS_TEST.T_DWS_FLIGHT_PREDICTION_DAILY (
    cache_key, departure_airport, arrival_airport, flight_number,
    airline_code, prediction_type, time_range_type, avg_price_list,
    created_time, updated_time
)
WITH 
-- 生成过去24个1天时间区间
date_ranges AS (
    SELECT 
        num,
        DATE_SUB(CURRENT_DATE(), INTERVAL (25 - num) DAY) as range_start_date,
        DATE_SUB(CURRENT_DATE(), INTERVAL (24 - num) DAY) as range_end_date,
        DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL (25 - num) DAY), '%Y-%m-%d') as date_value
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
-- 计算每个航班在每个时间区间的平均票价
flight_prices AS (
    SELECT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE,
        dr.num as time_index,
        ROUND(AVG(f.SEG_TRANSPORT_TIK_PRICE), 2) as avg_price,
        COUNT(*) as price_data_count
    FROM DWD_TEST.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_TEST.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    INNER JOIN date_ranges dr ON f.FK_DEPARTURES_DATE >= dr.range_start_date 
                      AND f.FK_DEPARTURES_DATE < dr.range_end_date
    WHERE f.DATA_ACTIVE = true
        AND f.SEG_TRANSPORT_TIK_PRICE IS NOT NULL
        AND f.SEG_TRANSPORT_TIK_PRICE > 0
        AND d.OPERAT_FLIGHT_NUM IS NOT NULL
        AND d.OPERAT_FLIGHT_NUM != ''
    GROUP BY f.FK_DEPAIRPORT, f.FK_ARRIAIRPORT, d.OPERAT_FLIGHT_NUM, d.OPERAT_AIRLINE, dr.num
),
-- 为每个航班生成完整的24个时间区间数据
flight_time_series AS (
    SELECT 
        af.FK_DEPAIRPORT,
        af.FK_ARRIAIRPORT,
        af.OPERAT_FLIGHT_NUM,
        af.OPERAT_AIRLINE,
        dr.num as time_index,
        dr.date_value,
        CASE 
            WHEN fp.avg_price IS NULL THEN -1
            ELSE fp.avg_price
        END as average_price
    FROM active_flights af
    CROSS JOIN date_ranges dr
    LEFT JOIN flight_prices fp ON af.FK_DEPAIRPORT = fp.FK_DEPAIRPORT
                             AND af.FK_ARRIAIRPORT = fp.FK_ARRIAIRPORT
                             AND af.OPERAT_FLIGHT_NUM = fp.OPERAT_FLIGHT_NUM
                             AND af.OPERAT_AIRLINE = fp.OPERAT_AIRLINE
                             AND dr.num = fp.time_index
    ORDER BY af.FK_DEPAIRPORT, af.FK_ARRIAIRPORT, af.OPERAT_FLIGHT_NUM, dr.num
),
-- 构建JSON对象数组
json_aggregated AS (
    SELECT 
        fts.FK_DEPAIRPORT,
        fts.FK_ARRIAIRPORT,
        fts.OPERAT_FLIGHT_NUM,
        fts.OPERAT_AIRLINE,
        CONCAT(
            '[',
            GROUP_CONCAT(
                CONCAT(
                    '{"dateValue":"', fts.date_value, 
                    '","averagePrice":', 
                    CASE 
                        WHEN fts.average_price = -1 THEN '-1' 
                        ELSE CAST(fts.average_price AS STRING)
                    END, 
                    '}'
                )
                ORDER BY fts.time_index
            ),
            ']'
        ) as avg_price_json
    FROM flight_time_series fts
    GROUP BY fts.FK_DEPAIRPORT, fts.FK_ARRIAIRPORT, fts.OPERAT_FLIGHT_NUM, fts.OPERAT_AIRLINE
)
-- 合并24个时间区间的数据为一条
SELECT 
    CONCAT(ja.FK_DEPAIRPORT, '_', ja.FK_ARRIAIRPORT, '_', ja.OPERAT_FLIGHT_NUM, '_PRICE_CURRENT') as cache_key,
    ja.FK_DEPAIRPORT as departure_airport,
    ja.FK_ARRIAIRPORT as arrival_airport,
    ja.OPERAT_FLIGHT_NUM as flight_number,
    ja.OPERAT_AIRLINE as airline_code,
    'AVERAGE_PRICE' as prediction_type,
    'CURRENT' as time_range_type,
    ja.avg_price_json as avg_price_list,
    NOW() as created_time,
    NOW() as updated_time
FROM json_aggregated ja;




-- 平均票价 - 合并24个DAY_3时间区间为一条数据
INSERT INTO DWS_TEST.T_DWS_FLIGHT_PREDICTION_DAILY (
    cache_key, departure_airport, arrival_airport, flight_number,
    airline_code, prediction_type, time_range_type, avg_price_list,
    created_time, updated_time
)
WITH 
-- 生成过去24个3天时间区间
date_ranges AS (
    SELECT 
        num,
        DATE_SUB(CURRENT_DATE(), INTERVAL (25 - num) * 3 DAY) as range_start_date,
        DATE_SUB(CURRENT_DATE(), INTERVAL (24 - num) * 3 DAY) as range_end_date,
        CONCAT(
            DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL (25 - num) * 3 DAY), '%Y-%m-%d'),
            '~',
            DATE_FORMAT(DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL (24 - num) * 3 DAY), INTERVAL 1 DAY), '%Y-%m-%d')
        ) as date_value
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
-- 计算每个航班在每个时间区间的平均票价
flight_prices AS (
    SELECT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE,
        dr.num as time_index,
        ROUND(AVG(f.SEG_TRANSPORT_TIK_PRICE), 2) as avg_price,
        COUNT(*) as price_data_count
    FROM DWD_TEST.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_TEST.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    INNER JOIN date_ranges dr ON f.FK_DEPARTURES_DATE >= dr.range_start_date 
                      AND f.FK_DEPARTURES_DATE < dr.range_end_date
    WHERE f.DATA_ACTIVE = true
        AND f.SEG_TRANSPORT_TIK_PRICE IS NOT NULL
        AND f.SEG_TRANSPORT_TIK_PRICE > 0
        AND d.OPERAT_FLIGHT_NUM IS NOT NULL
        AND d.OPERAT_FLIGHT_NUM != ''
    GROUP BY f.FK_DEPAIRPORT, f.FK_ARRIAIRPORT, d.OPERAT_FLIGHT_NUM, d.OPERAT_AIRLINE, dr.num
),
-- 为每个航班生成完整的24个时间区间数据
flight_time_series AS (
    SELECT 
        af.FK_DEPAIRPORT,
        af.FK_ARRIAIRPORT,
        af.OPERAT_FLIGHT_NUM,
        af.OPERAT_AIRLINE,
        dr.num as time_index,
        dr.date_value,
        CASE 
            WHEN fp.avg_price IS NULL THEN -1
            ELSE fp.avg_price
        END as average_price
    FROM active_flights af
    CROSS JOIN date_ranges dr
    LEFT JOIN flight_prices fp ON af.FK_DEPAIRPORT = fp.FK_DEPAIRPORT
                             AND af.FK_ARRIAIRPORT = fp.FK_ARRIAIRPORT
                             AND af.OPERAT_FLIGHT_NUM = fp.OPERAT_FLIGHT_NUM
                             AND af.OPERAT_AIRLINE = fp.OPERAT_AIRLINE
                             AND dr.num = fp.time_index
    ORDER BY af.FK_DEPAIRPORT, af.FK_ARRIAIRPORT, af.OPERAT_FLIGHT_NUM, dr.num
),
-- 构建JSON对象数组
json_aggregated AS (
    SELECT 
        fts.FK_DEPAIRPORT,
        fts.FK_ARRIAIRPORT,
        fts.OPERAT_FLIGHT_NUM,
        fts.OPERAT_AIRLINE,
        CONCAT(
            '[',
            GROUP_CONCAT(
                CONCAT(
                    '{"dateValue":"', fts.date_value, 
                    '","averagePrice":', 
                    CASE 
                        WHEN fts.average_price = -1 THEN '-1' 
                        ELSE CAST(fts.average_price AS STRING)
                    END, 
                    '}'
                )
                ORDER BY fts.time_index
            ),
            ']'
        ) as avg_price_json
    FROM flight_time_series fts
    GROUP BY fts.FK_DEPAIRPORT, fts.FK_ARRIAIRPORT, fts.OPERAT_FLIGHT_NUM, fts.OPERAT_AIRLINE
)
-- 合并24个时间区间的数据为一条
SELECT 
    CONCAT(ja.FK_DEPAIRPORT, '_', ja.FK_ARRIAIRPORT, '_', ja.OPERAT_FLIGHT_NUM, '_PRICE_DAY_3') as cache_key,
    ja.FK_DEPAIRPORT as departure_airport,
    ja.FK_ARRIAIRPORT as arrival_airport,
    ja.OPERAT_FLIGHT_NUM as flight_number,
    ja.OPERAT_AIRLINE as airline_code,
    'AVERAGE_PRICE' as prediction_type,
    'DAY_3' as time_range_type,
    ja.avg_price_json as avg_price_list,
    NOW() as created_time,
    NOW() as updated_time
FROM json_aggregated ja;




-- 平均票价 - 合并24个DAY_7时间区间为一条数据
INSERT INTO DWS_TEST.T_DWS_FLIGHT_PREDICTION_DAILY (
    cache_key, departure_airport, arrival_airport, flight_number,
    airline_code, prediction_type, time_range_type, avg_price_list,
    created_time, updated_time
)
WITH 
-- 生成过去24个7天时间区间
date_ranges AS (
    SELECT 
        num,
        DATE_SUB(CURRENT_DATE(), INTERVAL (25 - num) * 7 DAY) as range_start_date,
        DATE_SUB(CURRENT_DATE(), INTERVAL (24 - num) * 7 DAY) as range_end_date,
        CONCAT(
            DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL (25 - num) * 7 DAY), '%Y-%m-%d'),
            '~',
            DATE_FORMAT(DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL (24 - num) * 7 DAY), INTERVAL 1 DAY), '%Y-%m-%d')
        ) as date_value
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
-- 计算每个航班在每个时间区间的平均票价
flight_prices AS (
    SELECT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE,
        dr.num as time_index,
        ROUND(AVG(f.SEG_TRANSPORT_TIK_PRICE), 2) as avg_price,
        COUNT(*) as price_data_count
    FROM DWD_TEST.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_TEST.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    INNER JOIN date_ranges dr ON f.FK_DEPARTURES_DATE >= dr.range_start_date 
                      AND f.FK_DEPARTURES_DATE < dr.range_end_date
    WHERE f.DATA_ACTIVE = true
        AND f.SEG_TRANSPORT_TIK_PRICE IS NOT NULL
        AND f.SEG_TRANSPORT_TIK_PRICE > 0
        AND d.OPERAT_FLIGHT_NUM IS NOT NULL
        AND d.OPERAT_FLIGHT_NUM != ''
    GROUP BY f.FK_DEPAIRPORT, f.FK_ARRIAIRPORT, d.OPERAT_FLIGHT_NUM, d.OPERAT_AIRLINE, dr.num
),
-- 为每个航班生成完整的24个时间区间数据
flight_time_series AS (
    SELECT 
        af.FK_DEPAIRPORT,
        af.FK_ARRIAIRPORT,
        af.OPERAT_FLIGHT_NUM,
        af.OPERAT_AIRLINE,
        dr.num as time_index,
        dr.date_value,
        CASE 
            WHEN fp.avg_price IS NULL THEN -1
            ELSE fp.avg_price
        END as average_price
    FROM active_flights af
    CROSS JOIN date_ranges dr
    LEFT JOIN flight_prices fp ON af.FK_DEPAIRPORT = fp.FK_DEPAIRPORT
                             AND af.FK_ARRIAIRPORT = fp.FK_ARRIAIRPORT
                             AND af.OPERAT_FLIGHT_NUM = fp.OPERAT_FLIGHT_NUM
                             AND af.OPERAT_AIRLINE = fp.OPERAT_AIRLINE
                             AND dr.num = fp.time_index
    ORDER BY af.FK_DEPAIRPORT, af.FK_ARRIAIRPORT, af.OPERAT_FLIGHT_NUM, dr.num
),
-- 构建JSON对象数组
json_aggregated AS (
    SELECT 
        fts.FK_DEPAIRPORT,
        fts.FK_ARRIAIRPORT,
        fts.OPERAT_FLIGHT_NUM,
        fts.OPERAT_AIRLINE,
        CONCAT(
            '[',
            GROUP_CONCAT(
                CONCAT(
                    '{"dateValue":"', fts.date_value, 
                    '","averagePrice":', 
                    CASE 
                        WHEN fts.average_price = -1 THEN '-1' 
                        ELSE CAST(fts.average_price AS STRING)
                    END, 
                    '}'
                )
                ORDER BY fts.time_index
            ),
            ']'
        ) as avg_price_json
    FROM flight_time_series fts
    GROUP BY fts.FK_DEPAIRPORT, fts.FK_ARRIAIRPORT, fts.OPERAT_FLIGHT_NUM, fts.OPERAT_AIRLINE
)
-- 合并24个时间区间的数据为一条
SELECT 
    CONCAT(ja.FK_DEPAIRPORT, '_', ja.FK_ARRIAIRPORT, '_', ja.OPERAT_FLIGHT_NUM, '_PRICE_DAY_7') as cache_key,
    ja.FK_DEPAIRPORT as departure_airport,
    ja.FK_ARRIAIRPORT as arrival_airport,
    ja.OPERAT_FLIGHT_NUM as flight_number,
    ja.OPERAT_AIRLINE as airline_code,
    'AVERAGE_PRICE' as prediction_type,
    'DAY_7' as time_range_type,
    ja.avg_price_json as avg_price_list,
    NOW() as created_time,
    NOW() as updated_time
FROM json_aggregated ja;






-- 平均票价 - 合并24个DAY_30时间区间为一条数据（JSON对象数组格式）
INSERT INTO DWS_TEST.T_DWS_FLIGHT_PREDICTION_DAILY (
    cache_key, departure_airport, arrival_airport, flight_number,
    airline_code, prediction_type, time_range_type, avg_price_list,
    created_time, updated_time
)
WITH 
-- 生成过去24个30天时间区间（按从最早到最近排序）
date_ranges AS (
    SELECT 
        num,
        DATE_SUB(CURRENT_DATE(), INTERVAL (25 - num) * 30 DAY) as range_start_date,
        DATE_SUB(CURRENT_DATE(), INTERVAL (24 - num) * 30 DAY) as range_end_date,
        CONCAT(
            DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL (25 - num) * 30 DAY), '%Y-%m-%d'),
            '~',
            DATE_FORMAT(DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL (24 - num) * 30 DAY), INTERVAL 1 DAY), '%Y-%m-%d')
        ) as date_value
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
-- 计算每个航班在每个时间区间的平均票价（使用INNER JOIN）
flight_prices AS (
    SELECT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE,
        dr.num as time_index,
        ROUND(AVG(f.SEG_TRANSPORT_TIK_PRICE), 2) as avg_price,
        COUNT(*) as price_data_count
    FROM DWD_TEST.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_TEST.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    INNER JOIN date_ranges dr ON f.FK_DEPARTURES_DATE >= dr.range_start_date 
                      AND f.FK_DEPARTURES_DATE < dr.range_end_date
    WHERE f.DATA_ACTIVE = true
        AND f.SEG_TRANSPORT_TIK_PRICE IS NOT NULL
        AND f.SEG_TRANSPORT_TIK_PRICE > 0
        AND d.OPERAT_FLIGHT_NUM IS NOT NULL
        AND d.OPERAT_FLIGHT_NUM != ''
    GROUP BY f.FK_DEPAIRPORT, f.FK_ARRIAIRPORT, d.OPERAT_FLIGHT_NUM, d.OPERAT_AIRLINE, dr.num
),
-- 为每个航班生成完整的24个时间区间数据
flight_time_series AS (
    SELECT 
        af.FK_DEPAIRPORT,
        af.FK_ARRIAIRPORT,
        af.OPERAT_FLIGHT_NUM,
        af.OPERAT_AIRLINE,
        dr.num as time_index,
        dr.date_value,
        CASE 
            WHEN fp.avg_price IS NULL THEN -1
            ELSE fp.avg_price
        END as average_price
    FROM active_flights af
    CROSS JOIN date_ranges dr
    LEFT JOIN flight_prices fp ON af.FK_DEPAIRPORT = fp.FK_DEPAIRPORT
                             AND af.FK_ARRIAIRPORT = fp.FK_ARRIAIRPORT
                             AND af.OPERAT_FLIGHT_NUM = fp.OPERAT_FLIGHT_NUM
                             AND af.OPERAT_AIRLINE = fp.OPERAT_AIRLINE
                             AND dr.num = fp.time_index
    ORDER BY af.FK_DEPAIRPORT, af.FK_ARRIAIRPORT, af.OPERAT_FLIGHT_NUM, dr.num
),
-- 构建JSON对象数组
json_aggregated AS (
    SELECT 
        fts.FK_DEPAIRPORT,
        fts.FK_ARRIAIRPORT,
        fts.OPERAT_FLIGHT_NUM,
        fts.OPERAT_AIRLINE,
        CONCAT(
            '[',
            GROUP_CONCAT(
                CONCAT(
                    '{"dateValue":"', fts.date_value, 
                    '","averagePrice":', 
                    CASE 
                        WHEN fts.average_price = -1 THEN '-1' 
                        ELSE CAST(fts.average_price AS STRING)
                    END, 
                    '}'
                )
                ORDER BY fts.time_index
            ),
            ']'
        ) as avg_price_json
    FROM flight_time_series fts
    GROUP BY fts.FK_DEPAIRPORT, fts.FK_ARRIAIRPORT, fts.OPERAT_FLIGHT_NUM, fts.OPERAT_AIRLINE
)
-- 合并24个时间区间的数据为一条
SELECT 
    CONCAT(ja.FK_DEPAIRPORT, '_', ja.FK_ARRIAIRPORT, '_', ja.OPERAT_FLIGHT_NUM, '_PRICE_DAY_30') as cache_key,
    ja.FK_DEPAIRPORT as departure_airport,
    ja.FK_ARRIAIRPORT as arrival_airport,
    ja.OPERAT_FLIGHT_NUM as flight_number,
    ja.OPERAT_AIRLINE as airline_code,
    'AVERAGE_PRICE' as prediction_type,
    'DAY_30' as time_range_type,
    ja.avg_price_json as avg_price_list,
    NOW() as created_time,
    NOW() as updated_time
FROM json_aggregated ja;

--事务提交
commit;