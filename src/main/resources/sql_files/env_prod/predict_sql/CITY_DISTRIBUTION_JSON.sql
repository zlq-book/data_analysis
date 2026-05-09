-- 城市分布 - 合并24个CURRENT时间区间为一条数据
INSERT INTO DWS_PROD.T_DWS_FLIGHT_PREDICTION_DAILY (
    cache_key, departure_airport, arrival_airport, flight_number,
    airline_code, prediction_type, time_range_type, city_distribution_json,
    created_time, updated_time
)
WITH 
-- 生成过去24个1天时间区间
date_ranges AS (
    SELECT 
        num,
        DATE_SUB(CURRENT_DATE(), INTERVAL (25 - num) * 1 DAY) as range_start_date,
        DATE_SUB(CURRENT_DATE(), INTERVAL (24 - num) * 1 DAY) as range_end_date
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
    WHERE (f.DATA_ACTIVE = true OR f.DATA_ACTIVE = 1)
      AND d.OPERAT_AIRLINE = 'SC'
      AND f.FK_DEPARTURES_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
      AND d.OPERAT_FLIGHT_NUM IS NOT NULL
      AND d.OPERAT_FLIGHT_NUM != ''
),
-- 合并计算航班-城市-时间区间数据，确保数据一致性
flight_city_data AS (
    SELECT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE,
        COALESCE(u.CITY, '未知') as city,
        dr.num as time_index,
        COUNT(*) as city_passengers,
        -- 同时计算总乘客数
        SUM(COUNT(*)) OVER (PARTITION BY f.FK_DEPAIRPORT, f.FK_ARRIAIRPORT, d.OPERAT_FLIGHT_NUM, d.OPERAT_AIRLINE, dr.num) as total_passengers
    FROM DWD_PROD.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_PROD.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    INNER JOIN DIM_PROD.T_DIM_USER_DIM u ON f.FK_PASSENGER_USER_TID = u.PK_ID
    INNER JOIN date_ranges dr ON f.FK_DEPARTURES_DATE >= dr.range_start_date 
                          AND f.FK_DEPARTURES_DATE < dr.range_end_date
    WHERE (f.DATA_ACTIVE = true OR f.DATA_ACTIVE = 1)
      AND u.CITY IS NOT NULL
      AND d.OPERAT_FLIGHT_NUM IS NOT NULL
      AND d.OPERAT_FLIGHT_NUM != ''
      AND d.OPERAT_AIRLINE = 'SC'
    GROUP BY f.FK_DEPAIRPORT, f.FK_ARRIAIRPORT, d.OPERAT_FLIGHT_NUM, d.OPERAT_AIRLINE, u.CITY, dr.num
),
-- 直接计算占比
city_percentages AS (
    SELECT 
        FK_DEPAIRPORT,
        FK_ARRIAIRPORT,
        OPERAT_FLIGHT_NUM,
        OPERAT_AIRLINE,
        city,
        time_index,
        CASE 
            WHEN total_passengers > 0 THEN
                ROUND(city_passengers * 1.0 / total_passengers, 2)
            ELSE 0
        END as city_percent
    FROM flight_city_data
),
-- 为每个航班生成完整的24个时间区间数据
flight_time_ranges AS (
    SELECT 
        af.FK_DEPAIRPORT,
        af.FK_ARRIAIRPORT,
        af.OPERAT_FLIGHT_NUM,
        af.OPERAT_AIRLINE,
        dr.num as time_index
    FROM active_flights af
    CROSS JOIN date_ranges dr
),
-- 获取所有实际出现的城市-航班组合
actual_combinations AS (
    SELECT DISTINCT
        FK_DEPAIRPORT,
        FK_ARRIAIRPORT,
        OPERAT_FLIGHT_NUM,
        OPERAT_AIRLINE,
        city
    FROM flight_city_data
),
-- 构建完整的时间序列
city_time_series AS (
    SELECT 
        ac.FK_DEPAIRPORT,
        ac.FK_ARRIAIRPORT,
        ac.OPERAT_FLIGHT_NUM,
        ac.OPERAT_AIRLINE,
        ac.city,
        ftr.time_index,
        -- 使用COALESCE处理NULL值，确保数据完整性
        COALESCE(CAST(cp.city_percent AS STRING), '0') as city_percent_value
    FROM actual_combinations ac
    INNER JOIN flight_time_ranges ftr ON ac.FK_DEPAIRPORT = ftr.FK_DEPAIRPORT
                                     AND ac.FK_ARRIAIRPORT = ftr.FK_ARRIAIRPORT
                                     AND ac.OPERAT_FLIGHT_NUM = ftr.OPERAT_FLIGHT_NUM
                                     AND ac.OPERAT_AIRLINE = ftr.OPERAT_AIRLINE
    LEFT JOIN city_percentages cp ON ac.FK_DEPAIRPORT = cp.FK_DEPAIRPORT
                                AND ac.FK_ARRIAIRPORT = cp.FK_ARRIAIRPORT
                                AND ac.OPERAT_FLIGHT_NUM = cp.OPERAT_FLIGHT_NUM
                                AND ac.OPERAT_AIRLINE = cp.OPERAT_AIRLINE
                                AND ac.city = cp.city
                                AND ftr.time_index = cp.time_index
),
-- 按城市聚合时间序列，过滤掉全是0的城市
city_aggregated AS (
    SELECT 
        FK_DEPAIRPORT,
        FK_ARRIAIRPORT,
        OPERAT_FLIGHT_NUM,
        OPERAT_AIRLINE,
        city,
        CONCAT('[', GROUP_CONCAT(city_percent_value ORDER BY time_index), ']') as city_time_series
    FROM city_time_series
    GROUP BY FK_DEPAIRPORT, FK_ARRIAIRPORT, OPERAT_FLIGHT_NUM, OPERAT_AIRLINE, city
    HAVING COUNT(CASE WHEN city_percent_value != '0' THEN 1 END) > 0
),
-- 构建最终的JSON
final_json AS (
    SELECT 
        FK_DEPAIRPORT,
        FK_ARRIAIRPORT,
        OPERAT_FLIGHT_NUM,
        OPERAT_AIRLINE,
        CONCAT(
            '{',
            GROUP_CONCAT(CONCAT('"', city, '":', city_time_series) ORDER BY city),
            '}'
        ) as city_distribution_json
    FROM city_aggregated
    GROUP BY FK_DEPAIRPORT, FK_ARRIAIRPORT, OPERAT_FLIGHT_NUM, OPERAT_AIRLINE
)
-- 最终插入
SELECT 
    CONCAT(fj.FK_DEPAIRPORT, '_', fj.FK_ARRIAIRPORT, '_', fj.OPERAT_FLIGHT_NUM, '_CITY_CURRENT') as cache_key,
    fj.FK_DEPAIRPORT as departure_airport,
    fj.FK_ARRIAIRPORT as arrival_airport,
    fj.OPERAT_FLIGHT_NUM as flight_number,
    fj.OPERAT_AIRLINE as airline_code,
    'CITY_DISTRIBUTION' as prediction_type,
    'CURRENT' as time_range_type,
    fj.city_distribution_json,
    NOW() as created_time,
    NOW() as updated_time
FROM final_json fj;




-- 城市分布 - 合并24个DAY_3时间区间为一条数据
INSERT INTO DWS_PROD.T_DWS_FLIGHT_PREDICTION_DAILY (
    cache_key, departure_airport, arrival_airport, flight_number,
    airline_code, prediction_type, time_range_type, city_distribution_json,
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
    WHERE (f.DATA_ACTIVE = true OR f.DATA_ACTIVE = 1)
      AND d.OPERAT_AIRLINE = 'SC'
      AND f.FK_DEPARTURES_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
      AND d.OPERAT_FLIGHT_NUM IS NOT NULL
      AND d.OPERAT_FLIGHT_NUM != ''
),
-- 合并计算航班-城市-时间区间数据，确保数据一致性
flight_city_data AS (
    SELECT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE,
        COALESCE(u.CITY, '未知') as city,
        dr.num as time_index,
        COUNT(*) as city_passengers,
        -- 同时计算总乘客数
        SUM(COUNT(*)) OVER (PARTITION BY f.FK_DEPAIRPORT, f.FK_ARRIAIRPORT, d.OPERAT_FLIGHT_NUM, d.OPERAT_AIRLINE, dr.num) as total_passengers
    FROM DWD_PROD.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_PROD.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    INNER JOIN DIM_PROD.T_DIM_USER_DIM u ON f.FK_PASSENGER_USER_TID = u.PK_ID
    INNER JOIN date_ranges dr ON f.FK_DEPARTURES_DATE >= dr.range_start_date 
                          AND f.FK_DEPARTURES_DATE < dr.range_end_date
    WHERE (f.DATA_ACTIVE = true OR f.DATA_ACTIVE = 1)
      AND u.CITY IS NOT NULL
      AND d.OPERAT_FLIGHT_NUM IS NOT NULL
      AND d.OPERAT_FLIGHT_NUM != ''
      AND d.OPERAT_AIRLINE = 'SC'
    GROUP BY f.FK_DEPAIRPORT, f.FK_ARRIAIRPORT, d.OPERAT_FLIGHT_NUM, d.OPERAT_AIRLINE, u.CITY, dr.num
),
-- 直接计算占比
city_percentages AS (
    SELECT 
        FK_DEPAIRPORT,
        FK_ARRIAIRPORT,
        OPERAT_FLIGHT_NUM,
        OPERAT_AIRLINE,
        city,
        time_index,
        CASE 
            WHEN total_passengers > 0 THEN
                ROUND(city_passengers * 1.0 / total_passengers, 2)
            ELSE 0
        END as city_percent
    FROM flight_city_data
),
-- 为每个航班生成完整的24个时间区间数据
flight_time_ranges AS (
    SELECT 
        af.FK_DEPAIRPORT,
        af.FK_ARRIAIRPORT,
        af.OPERAT_FLIGHT_NUM,
        af.OPERAT_AIRLINE,
        dr.num as time_index
    FROM active_flights af
    CROSS JOIN date_ranges dr
),
-- 获取所有实际出现的城市-航班组合
actual_combinations AS (
    SELECT DISTINCT
        FK_DEPAIRPORT,
        FK_ARRIAIRPORT,
        OPERAT_FLIGHT_NUM,
        OPERAT_AIRLINE,
        city
    FROM flight_city_data
),
-- 构建完整的时间序列
city_time_series AS (
    SELECT 
        ac.FK_DEPAIRPORT,
        ac.FK_ARRIAIRPORT,
        ac.OPERAT_FLIGHT_NUM,
        ac.OPERAT_AIRLINE,
        ac.city,
        ftr.time_index,
        -- 使用COALESCE处理NULL值，确保数据完整性
        COALESCE(CAST(cp.city_percent AS STRING), '0') as city_percent_value
    FROM actual_combinations ac
    INNER JOIN flight_time_ranges ftr ON ac.FK_DEPAIRPORT = ftr.FK_DEPAIRPORT
                                     AND ac.FK_ARRIAIRPORT = ftr.FK_ARRIAIRPORT
                                     AND ac.OPERAT_FLIGHT_NUM = ftr.OPERAT_FLIGHT_NUM
                                     AND ac.OPERAT_AIRLINE = ftr.OPERAT_AIRLINE
    LEFT JOIN city_percentages cp ON ac.FK_DEPAIRPORT = cp.FK_DEPAIRPORT
                                AND ac.FK_ARRIAIRPORT = cp.FK_ARRIAIRPORT
                                AND ac.OPERAT_FLIGHT_NUM = cp.OPERAT_FLIGHT_NUM
                                AND ac.OPERAT_AIRLINE = cp.OPERAT_AIRLINE
                                AND ac.city = cp.city
                                AND ftr.time_index = cp.time_index
),
-- 按城市聚合时间序列，过滤掉全是0的城市
city_aggregated AS (
    SELECT 
        FK_DEPAIRPORT,
        FK_ARRIAIRPORT,
        OPERAT_FLIGHT_NUM,
        OPERAT_AIRLINE,
        city,
        CONCAT('[', GROUP_CONCAT(city_percent_value ORDER BY time_index), ']') as city_time_series
    FROM city_time_series
    GROUP BY FK_DEPAIRPORT, FK_ARRIAIRPORT, OPERAT_FLIGHT_NUM, OPERAT_AIRLINE, city
    HAVING COUNT(CASE WHEN city_percent_value != '0' THEN 1 END) > 0
),
-- 构建最终的JSON
final_json AS (
    SELECT 
        FK_DEPAIRPORT,
        FK_ARRIAIRPORT,
        OPERAT_FLIGHT_NUM,
        OPERAT_AIRLINE,
        CONCAT(
            '{',
            GROUP_CONCAT(CONCAT('"', city, '":', city_time_series) ORDER BY city),
            '}'
        ) as city_distribution_json
    FROM city_aggregated
    GROUP BY FK_DEPAIRPORT, FK_ARRIAIRPORT, OPERAT_FLIGHT_NUM, OPERAT_AIRLINE
)
-- 最终插入
SELECT 
    CONCAT(fj.FK_DEPAIRPORT, '_', fj.FK_ARRIAIRPORT, '_', fj.OPERAT_FLIGHT_NUM, '_CITY_DAY_3') as cache_key,
    fj.FK_DEPAIRPORT as departure_airport,
    fj.FK_ARRIAIRPORT as arrival_airport,
    fj.OPERAT_FLIGHT_NUM as flight_number,
    fj.OPERAT_AIRLINE as airline_code,
    'CITY_DISTRIBUTION' as prediction_type,
    'DAY_3' as time_range_type,
    fj.city_distribution_json,
    NOW() as created_time,
    NOW() as updated_time
FROM final_json fj;




-- 城市分布 - 合并24个DAY_7时间区间为一条数据
INSERT INTO DWS_PROD.T_DWS_FLIGHT_PREDICTION_DAILY (
    cache_key, departure_airport, arrival_airport, flight_number,
    airline_code, prediction_type, time_range_type, city_distribution_json,
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
    WHERE (f.DATA_ACTIVE = true OR f.DATA_ACTIVE = 1)
      AND d.OPERAT_AIRLINE = 'SC'
      AND f.FK_DEPARTURES_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
      AND d.OPERAT_FLIGHT_NUM IS NOT NULL
      AND d.OPERAT_FLIGHT_NUM != ''
),
-- 合并计算航班-城市-时间区间数据，确保数据一致性
flight_city_data AS (
    SELECT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE,
        COALESCE(u.CITY, '未知') as city,
        dr.num as time_index,
        COUNT(*) as city_passengers,
        -- 同时计算总乘客数
        SUM(COUNT(*)) OVER (PARTITION BY f.FK_DEPAIRPORT, f.FK_ARRIAIRPORT, d.OPERAT_FLIGHT_NUM, d.OPERAT_AIRLINE, dr.num) as total_passengers
    FROM DWD_PROD.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_PROD.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    INNER JOIN DIM_PROD.T_DIM_USER_DIM u ON f.FK_PASSENGER_USER_TID = u.PK_ID
    INNER JOIN date_ranges dr ON f.FK_DEPARTURES_DATE >= dr.range_start_date 
                          AND f.FK_DEPARTURES_DATE < dr.range_end_date
    WHERE (f.DATA_ACTIVE = true OR f.DATA_ACTIVE = 1)
      AND u.CITY IS NOT NULL
      AND d.OPERAT_FLIGHT_NUM IS NOT NULL
      AND d.OPERAT_FLIGHT_NUM != ''
      AND d.OPERAT_AIRLINE = 'SC'
    GROUP BY f.FK_DEPAIRPORT, f.FK_ARRIAIRPORT, d.OPERAT_FLIGHT_NUM, d.OPERAT_AIRLINE, u.CITY, dr.num
),
-- 直接计算占比
city_percentages AS (
    SELECT 
        FK_DEPAIRPORT,
        FK_ARRIAIRPORT,
        OPERAT_FLIGHT_NUM,
        OPERAT_AIRLINE,
        city,
        time_index,
        CASE 
            WHEN total_passengers > 0 THEN
                ROUND(city_passengers * 1.0 / total_passengers, 2)
            ELSE 0
        END as city_percent
    FROM flight_city_data
),
-- 为每个航班生成完整的24个时间区间数据
flight_time_ranges AS (
    SELECT 
        af.FK_DEPAIRPORT,
        af.FK_ARRIAIRPORT,
        af.OPERAT_FLIGHT_NUM,
        af.OPERAT_AIRLINE,
        dr.num as time_index
    FROM active_flights af
    CROSS JOIN date_ranges dr
),
-- 获取所有实际出现的城市-航班组合
actual_combinations AS (
    SELECT DISTINCT
        FK_DEPAIRPORT,
        FK_ARRIAIRPORT,
        OPERAT_FLIGHT_NUM,
        OPERAT_AIRLINE,
        city
    FROM flight_city_data
),
-- 构建完整的时间序列
city_time_series AS (
    SELECT 
        ac.FK_DEPAIRPORT,
        ac.FK_ARRIAIRPORT,
        ac.OPERAT_FLIGHT_NUM,
        ac.OPERAT_AIRLINE,
        ac.city,
        ftr.time_index,
        -- 使用COALESCE处理NULL值，确保数据完整性
        COALESCE(CAST(cp.city_percent AS STRING), '0') as city_percent_value
    FROM actual_combinations ac
    INNER JOIN flight_time_ranges ftr ON ac.FK_DEPAIRPORT = ftr.FK_DEPAIRPORT
                                     AND ac.FK_ARRIAIRPORT = ftr.FK_ARRIAIRPORT
                                     AND ac.OPERAT_FLIGHT_NUM = ftr.OPERAT_FLIGHT_NUM
                                     AND ac.OPERAT_AIRLINE = ftr.OPERAT_AIRLINE
    LEFT JOIN city_percentages cp ON ac.FK_DEPAIRPORT = cp.FK_DEPAIRPORT
                                AND ac.FK_ARRIAIRPORT = cp.FK_ARRIAIRPORT
                                AND ac.OPERAT_FLIGHT_NUM = cp.OPERAT_FLIGHT_NUM
                                AND ac.OPERAT_AIRLINE = cp.OPERAT_AIRLINE
                                AND ac.city = cp.city
                                AND ftr.time_index = cp.time_index
),
-- 按城市聚合时间序列，过滤掉全是0的城市
city_aggregated AS (
    SELECT 
        FK_DEPAIRPORT,
        FK_ARRIAIRPORT,
        OPERAT_FLIGHT_NUM,
        OPERAT_AIRLINE,
        city,
        CONCAT('[', GROUP_CONCAT(city_percent_value ORDER BY time_index), ']') as city_time_series
    FROM city_time_series
    GROUP BY FK_DEPAIRPORT, FK_ARRIAIRPORT, OPERAT_FLIGHT_NUM, OPERAT_AIRLINE, city
    HAVING COUNT(CASE WHEN city_percent_value != '0' THEN 1 END) > 0
),
-- 构建最终的JSON
final_json AS (
    SELECT 
        FK_DEPAIRPORT,
        FK_ARRIAIRPORT,
        OPERAT_FLIGHT_NUM,
        OPERAT_AIRLINE,
        CONCAT(
            '{',
            GROUP_CONCAT(CONCAT('"', city, '":', city_time_series) ORDER BY city),
            '}'
        ) as city_distribution_json
    FROM city_aggregated
    GROUP BY FK_DEPAIRPORT, FK_ARRIAIRPORT, OPERAT_FLIGHT_NUM, OPERAT_AIRLINE
)
-- 最终插入
SELECT 
    CONCAT(fj.FK_DEPAIRPORT, '_', fj.FK_ARRIAIRPORT, '_', fj.OPERAT_FLIGHT_NUM, '_CITY_DAY_7') as cache_key,
    fj.FK_DEPAIRPORT as departure_airport,
    fj.FK_ARRIAIRPORT as arrival_airport,
    fj.OPERAT_FLIGHT_NUM as flight_number,
    fj.OPERAT_AIRLINE as airline_code,
    'CITY_DISTRIBUTION' as prediction_type,
    'DAY_7' as time_range_type,
    fj.city_distribution_json,
    NOW() as created_time,
    NOW() as updated_time
FROM final_json fj;




-- 城市分布 
INSERT INTO DWS_PROD.T_DWS_FLIGHT_PREDICTION_DAILY (
    cache_key, departure_airport, arrival_airport, flight_number,
    airline_code, prediction_type, time_range_type, city_distribution_json,
    created_time, updated_time
)
WITH 
-- 生成过去24个30天时间区间
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
-- 获取所有活跃的航班航线
active_flights AS (
    SELECT DISTINCT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE
    FROM DWD_PROD.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_PROD.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    WHERE (f.DATA_ACTIVE = true OR f.DATA_ACTIVE = 1)
      AND d.OPERAT_AIRLINE = 'SC'
      AND f.FK_DEPARTURES_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
      AND d.OPERAT_FLIGHT_NUM IS NOT NULL
      AND d.OPERAT_FLIGHT_NUM != ''
),
-- 合并计算航班-城市-时间区间数据，确保数据一致性
flight_city_data AS (
    SELECT 
        f.FK_DEPAIRPORT,
        f.FK_ARRIAIRPORT,
        d.OPERAT_FLIGHT_NUM,
        d.OPERAT_AIRLINE,
        COALESCE(u.CITY, '未知') as city,
        dr.num as time_index,
        COUNT(*) as city_passengers,
        -- 同时计算总乘客数
        SUM(COUNT(*)) OVER (PARTITION BY f.FK_DEPAIRPORT, f.FK_ARRIAIRPORT, d.OPERAT_FLIGHT_NUM, d.OPERAT_AIRLINE, dr.num) as total_passengers
    FROM DWD_PROD.T_DWD_DEPART_SEG_FACT f
    INNER JOIN DIM_PROD.T_DIM_SEG_DIM d ON f.FK_PNRD_SEG = d.SEGMENT_KEY
    INNER JOIN DIM_PROD.T_DIM_USER_DIM u ON f.FK_PASSENGER_USER_TID = u.PK_ID
    INNER JOIN date_ranges dr ON f.FK_DEPARTURES_DATE >= dr.range_start_date 
                          AND f.FK_DEPARTURES_DATE < dr.range_end_date
    WHERE (f.DATA_ACTIVE = true OR f.DATA_ACTIVE = 1)
      AND u.CITY IS NOT NULL
      AND d.OPERAT_FLIGHT_NUM IS NOT NULL
      AND d.OPERAT_FLIGHT_NUM != ''
      AND d.OPERAT_AIRLINE = 'SC'
    GROUP BY f.FK_DEPAIRPORT, f.FK_ARRIAIRPORT, d.OPERAT_FLIGHT_NUM, d.OPERAT_AIRLINE, u.CITY, dr.num
),
-- 直接计算占比
city_percentages AS (
    SELECT 
        FK_DEPAIRPORT,
        FK_ARRIAIRPORT,
        OPERAT_FLIGHT_NUM,
        OPERAT_AIRLINE,
        city,
        time_index,
        CASE 
            WHEN total_passengers > 0 THEN
                ROUND(city_passengers * 1.0 / total_passengers, 2)
            ELSE 0
        END as city_percent
    FROM flight_city_data
),
-- 为每个航班生成完整的24个时间区间数据
flight_time_ranges AS (
    SELECT 
        af.FK_DEPAIRPORT,
        af.FK_ARRIAIRPORT,
        af.OPERAT_FLIGHT_NUM,
        af.OPERAT_AIRLINE,
        dr.num as time_index
    FROM active_flights af
    CROSS JOIN date_ranges dr
),
-- 获取所有实际出现的城市-航班组合
actual_combinations AS (
    SELECT DISTINCT
        FK_DEPAIRPORT,
        FK_ARRIAIRPORT,
        OPERAT_FLIGHT_NUM,
        OPERAT_AIRLINE,
        city
    FROM flight_city_data
),
-- 构建完整的时间序列
city_time_series AS (
    SELECT 
        ac.FK_DEPAIRPORT,
        ac.FK_ARRIAIRPORT,
        ac.OPERAT_FLIGHT_NUM,
        ac.OPERAT_AIRLINE,
        ac.city,
        ftr.time_index,
        -- 修正：使用COALESCE处理NULL值，确保数据完整性
        COALESCE(CAST(cp.city_percent AS STRING), '0') as city_percent_value
    FROM actual_combinations ac
    INNER JOIN flight_time_ranges ftr ON ac.FK_DEPAIRPORT = ftr.FK_DEPAIRPORT
                                     AND ac.FK_ARRIAIRPORT = ftr.FK_ARRIAIRPORT
                                     AND ac.OPERAT_FLIGHT_NUM = ftr.OPERAT_FLIGHT_NUM
                                     AND ac.OPERAT_AIRLINE = ftr.OPERAT_AIRLINE
    LEFT JOIN city_percentages cp ON ac.FK_DEPAIRPORT = cp.FK_DEPAIRPORT
                                AND ac.FK_ARRIAIRPORT = cp.FK_ARRIAIRPORT
                                AND ac.OPERAT_FLIGHT_NUM = cp.OPERAT_FLIGHT_NUM
                                AND ac.OPERAT_AIRLINE = cp.OPERAT_AIRLINE
                                AND ac.city = cp.city
                                AND ftr.time_index = cp.time_index
),
-- 按城市聚合时间序列，过滤掉全是0的城市
city_aggregated AS (
    SELECT 
        FK_DEPAIRPORT,
        FK_ARRIAIRPORT,
        OPERAT_FLIGHT_NUM,
        OPERAT_AIRLINE,
        city,
        CONCAT('[', GROUP_CONCAT(city_percent_value ORDER BY time_index), ']') as city_time_series
    FROM city_time_series
    GROUP BY FK_DEPAIRPORT, FK_ARRIAIRPORT, OPERAT_FLIGHT_NUM, OPERAT_AIRLINE, city
    HAVING COUNT(CASE WHEN city_percent_value != '0' THEN 1 END) > 0
),
-- 构建最终的JSON
final_json AS (
    SELECT 
        FK_DEPAIRPORT,
        FK_ARRIAIRPORT,
        OPERAT_FLIGHT_NUM,
        OPERAT_AIRLINE,
        CONCAT(
            '{',
            GROUP_CONCAT(CONCAT('"', city, '":', city_time_series) ORDER BY city),
            '}'
        ) as city_distribution_json
    FROM city_aggregated
    GROUP BY FK_DEPAIRPORT, FK_ARRIAIRPORT, OPERAT_FLIGHT_NUM, OPERAT_AIRLINE
)
-- 最终插入
SELECT 
    CONCAT(fj.FK_DEPAIRPORT, '_', fj.FK_ARRIAIRPORT, '_', fj.OPERAT_FLIGHT_NUM, '_CITY_DAY_30') as cache_key,
    fj.FK_DEPAIRPORT as departure_airport,
    fj.FK_ARRIAIRPORT as arrival_airport,
    fj.OPERAT_FLIGHT_NUM as flight_number,
    fj.OPERAT_AIRLINE as airline_code,
    'CITY_DISTRIBUTION' as prediction_type,
    'DAY_30' as time_range_type,
    fj.city_distribution_json,
    NOW() as created_time,
    NOW() as updated_time
FROM final_json fj;


--事务提交
commit;