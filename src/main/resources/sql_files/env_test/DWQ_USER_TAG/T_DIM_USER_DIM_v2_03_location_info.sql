SET enable_unique_key_partial_update = true;

-- v2 - 03 国家及客源地
-- 目标表：DWQ_TEST.T_DIM_USER_DIM

INSERT INTO DWQ_TEST.T_DIM_USER_DIM (
    PK_ID,
    COUNTRY_VISITED_COUNT,              -- 去过的国家数量
    CITY_VISITED_COUNT,                 -- 去过的城市数量
    TRAVELER_SOURCE_LOCATION,           -- 旅客客源地
    PREFERENCE_DOMESTIC_TOP_CITY,       -- 山航出行城市偏好（国内）
    PREFERENCE_INTL_TOP_CITY            -- 山航出行城市偏好（国际）
)
WITH
-- 1.基础层
base_flight_log AS (
    SELECT
        f.FK_PASSENGER_USER_TID AS user_id,
        f.FK_DEPARTURES_DATE AS flight_date,
        CASE WHEN f.type = 'ARR' THEN 1 ELSE 0 END AS is_arr,
        dad.CITY AS city_name,

        -- 【修改点1】用于“计数”的国家字段 (港澳台 -> 中国)
        CASE
            WHEN dad.COUNTRY IN ('中国台湾省', '中国香港', '中国澳门') THEN '中国'
            ELSE dad.COUNTRY
        END AS standard_country,

        -- 【修改点2】用于“偏好分类”的原始国家字段 (保持港澳台原名，或者直接在这里算 geo_type)
        -- 这里我们保留原始国家名，以便下一层做逻辑判断
        dad.COUNTRY AS original_country

    FROM (
        SELECT FK_PASSENGER_USER_TID, FK_DEPAIRPORT AS airport_key, FK_DEPARTURES_DATE, 'DEP' as type
        FROM DWQ_PROD.T_DWD_DEPART_SEG_FACT WHERE FK_PASSENGER_USER_TID IS NOT NULL
        UNION ALL
        SELECT FK_PASSENGER_USER_TID, FK_ARRIAIRPORT AS airport_key, FK_DEPARTURES_DATE, 'ARR' as type
        FROM DWQ_PROD.T_DWD_DEPART_SEG_FACT WHERE FK_PASSENGER_USER_TID IS NOT NULL
    ) f
    JOIN DWQ_PROD.T_DIM_ADDRESS_DIM dad ON f.airport_key = dad.AIRPORTKEY
),

-- 2. 聚合层
user_city_stats AS (
    SELECT
        user_id,
        city_name,
        standard_country, -- 用于后续计算去过多少个国家

        -- 【修改点3】：修改偏好类型的判断逻辑
        -- 逻辑：只有 '中国' (大陆) 算 Domestic (D)
        -- 港澳台 (在 original_country 中是具体名字) 和其他国家 算 International (I)
        CASE
            WHEN original_country = '中国' THEN 'D'
            ELSE 'I'
        END AS geo_type,

        COUNT(*) AS visit_count,
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

-- 3. 排名计算层
ranked_cities AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY visit_count DESC, first_visit_date ASC, dep_score DESC, city_name ASC
        ) AS rn_source
    FROM user_city_stats
),

-- 4. 计算偏好
preference_calculation AS (
    SELECT
        user_id,
        geo_type,
        city_name,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, geo_type
            ORDER BY visit_count DESC, last_visit_date DESC, arr_score DESC, city_name ASC
        ) AS rn_final_pref
    FROM ranked_cities
    WHERE rn_source <> 1
),

-- 5. 统计聚合层
user_aggs AS (
    SELECT
        user_id,
        -- 使用 standard_country，所以港澳台会被归并为1个“中国”，符合逻辑A
        COUNT(DISTINCT standard_country) AS country_cnt,
        COUNT(DISTINCT city_name) AS city_cnt
    FROM user_city_stats
    GROUP BY user_id
)

-- 主查询
SELECT
    tdud.pk_id,
    COALESCE(ua.country_cnt, 0) AS COUNTRY_VISITED_COUNT,
    COALESCE(ua.city_cnt, 0) AS CITY_VISITED_COUNT,
    COALESCE(src.city_name, '') AS TRAVELER_SOURCE_LOCATION,
    COALESCE(dom.city_name, '') AS PREFERENCE_DOMESTIC_TOP_CITY,
    COALESCE(intl.city_name, '') AS PREFERENCE_INTL_TOP_CITY
FROM DWQ_TEST.T_DIM_USER_DIM tdud
LEFT JOIN user_aggs ua ON tdud.pk_id = ua.user_id
LEFT JOIN ranked_cities src ON tdud.pk_id = src.user_id AND src.rn_source = 1
LEFT JOIN preference_calculation dom ON tdud.pk_id = dom.user_id AND dom.geo_type = 'D' AND dom.rn_final_pref = 1
LEFT JOIN preference_calculation intl ON tdud.pk_id = intl.user_id AND intl.geo_type = 'I' AND intl.rn_final_pref = 1
WHERE tdud.IS_VALID_USER = 1;