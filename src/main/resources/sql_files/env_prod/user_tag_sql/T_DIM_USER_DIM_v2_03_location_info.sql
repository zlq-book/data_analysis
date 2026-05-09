-- v2 - 03 国家及客源地
-- 目标表：DIM_PROD.T_DIM_USER_DIM

INSERT INTO DIM_PROD.T_DIM_USER_DIM (
    PK_ID,
    COUNTRY_VISITED_COUNT,              -- 去过的国家数量
    CITY_VISITED_COUNT,                 -- 去过的城市数量
    TRAVELER_SOURCE_LOCATION,           -- 旅客客源地
    PREFERENCE_DOMESTIC_TOP_CITY,       -- 山航出行城市偏好（国内）
    PREFERENCE_INTL_TOP_CITY,            -- 山航出行城市偏好（国际）
    UPDATE_TIME
)
WITH
-- 1. 基础层：拆解航段起降，统一标准化国家名称
base_flight_log AS (
    SELECT
        f.FK_PASSENGER_USER_TID AS user_id,
        f.FK_DEPARTURES_DATE AS flight_date,
        -- 0 代表起飞(DEP)，1 代表到达(ARR)
        CASE WHEN f.type = 'ARR' THEN 1 ELSE 0 END AS is_arr,
        dad.CITY AS city_name,

        -- 用于“计数”的国家字段 (港澳台 -> 中国)
        CASE
            WHEN dad.COUNTRY IN ('中国台湾省', '中国香港', '中国澳门') THEN '中国'
            ELSE dad.COUNTRY
        END AS standard_country,

        -- 用于“偏好分类”的原始国家字段 (港澳台判定为国际/地区)
        dad.COUNTRY AS original_country
    FROM (
        SELECT FK_PASSENGER_USER_TID, FK_DEPAIRPORT AS airport_key, FK_DEPARTURES_DATE, 'DEP' as type
        FROM DWD_PROD.T_DWD_DEPART_SEG_FACT WHERE FK_PASSENGER_USER_TID IS NOT NULL
        UNION ALL
        SELECT FK_PASSENGER_USER_TID, FK_ARRIAIRPORT AS airport_key, FK_DEPARTURES_DATE, 'ARR' as type
        FROM DWD_PROD.T_DWD_DEPART_SEG_FACT WHERE FK_PASSENGER_USER_TID IS NOT NULL
    ) f
    JOIN DWQ_PROD.T_DIM_ADDRESS_DIM dad ON f.airport_key = dad.AIRPORTKEY
),

-- 2. 聚合层：计算每个城市的统计指标
user_city_stats AS (
    SELECT
        user_id,
        city_name,
        standard_country,
        -- 偏好类型判断：仅大陆算 D，港澳台及国外算 I
        CASE
            WHEN original_country = '中国' THEN 'D'
            ELSE 'I'
        END AS geo_type,

        COUNT(*) AS visit_count,
        -- 【关键点】：专门计算作为“起飞机场”时的最早日期
        MIN(CASE WHEN is_arr = 0 THEN flight_date ELSE NULL END) AS first_dep_date,
        -- 整体最早日期 (含到达)
        MIN(flight_date) AS first_visit_date,
        MAX(flight_date) AS last_visit_date,
        SUM(is_arr) AS arr_score,
        SUM(1 - is_arr) AS dep_score
    FROM base_flight_log
    GROUP BY
        user_id,
        city_name,
        standard_country,
        CASE WHEN original_country = '中国' THEN 'D' ELSE 'I' END
),

-- 3. 排名计算层：计算客源地 (TRAVELER_SOURCE_LOCATION)
-- 逻辑：次数最多 > 起飞日期最早 > 城市名
ranked_cities AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY
                visit_count DESC,
                first_dep_date ASC NULLS LAST,  -- 次数相同时选最早起飞的城市，只有一次记录则起飞日期在前，null在后
                city_name ASC
        ) AS rn_source
    FROM user_city_stats
),

-- 4. 计算偏好：国内/国际 Top1 城市
-- 注意：偏好通常排除客源地城市 (rn_source <> 1)
preference_calculation AS (
    SELECT
        user_id,
        geo_type,
        city_name,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, geo_type
            ORDER BY
                visit_count DESC,
                last_visit_date DESC,
                arr_score DESC,
                city_name ASC
        ) AS rn_final_pref
    FROM ranked_cities
    WHERE rn_source <> 1
),

-- 5. 统计聚合层：计算国家数和城市数
user_aggs AS (
    SELECT
        user_id,
        COUNT(DISTINCT standard_country) AS country_cnt,
        COUNT(DISTINCT city_name) AS city_cnt
    FROM user_city_stats
    GROUP BY user_id
)

-- 主查询：合并结果
SELECT
    tdud.pk_id,
    COALESCE(ua.country_cnt, 0) AS COUNTRY_VISITED_COUNT,
    COALESCE(ua.city_cnt, 0) AS CITY_VISITED_COUNT,
    src.city_name AS TRAVELER_SOURCE_LOCATION,
    dom.city_name AS PREFERENCE_DOMESTIC_TOP_CITY,
    intl.city_name AS PREFERENCE_INTL_TOP_CITY,
    now()
FROM DIM_PROD.T_DIM_USER_DIM tdud
LEFT JOIN user_aggs ua ON tdud.pk_id = ua.user_id
LEFT JOIN ranked_cities src ON tdud.pk_id = src.user_id AND src.rn_source = 1
LEFT JOIN preference_calculation dom ON tdud.pk_id = dom.user_id AND dom.geo_type = 'D' AND dom.rn_final_pref = 1
LEFT JOIN preference_calculation intl ON tdud.pk_id = intl.user_id AND intl.geo_type = 'I' AND intl.rn_final_pref = 1
WHERE tdud.IS_VALID_USER = 1;