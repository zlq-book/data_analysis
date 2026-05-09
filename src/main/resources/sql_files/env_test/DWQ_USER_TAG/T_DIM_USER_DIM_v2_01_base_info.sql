SET enable_unique_key_partial_update = true;

-- 目标表：DWQ_TEST.T_DIM_USER_DIM_COPY
-- 依赖表：DWQ_TEST
INSERT INTO DWQ_TEST.T_DIM_USER_DIM_COPY (
    PK_ID,
    AGE,                                            -- 年龄
    IS_HIGH_VALUE_FREQUENT_TRAVELER_MEMBER,         -- 是否是常旅客高价值会员
    AVERAGE_ADVANCE_PURCHASE_TIME_LAST_12M,         -- 是否是常旅客高价值会员
    SEAT_PREFERENCE,                                -- 座位喜好
    ANCILLARY_PRODUCT_PREFERENCE_CATEGORY           -- 辅营产品偏好（大类）
)
WITH 
-- CTE 1: 平均提前出票时间
booking_stats AS (
    SELECT 
        FK_PASSENGER_USER_TID,
        SUM(AK_ADVBOOK_DAY) / SUM(TICKET_COUNT) AS AVERAGE_ADVANCE_PURCHASE_TIME_LAST_12M
    FROM DWQ_PROD.T_DWD_TICKING_TIC_FACT
    WHERE FK_TICKETING_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 12 MONTH)
    GROUP BY FK_PASSENGER_USER_TID
),

-- CTE 2: 座位偏好（使用窗口函数）
seat_pref AS (
    SELECT 
        FK_PASSENGER_USER_TID,
        SEAT_FEATURE AS seat_preference
    FROM (
        SELECT 
            cs.FK_PASSENGER_USER_TID,
            cs.SEAT_FEATURE,
            COUNT(*) AS feature_count,
            MAX(cs.FK_CHECKIN_DATE) AS latest_checkin_date,
            ROW_NUMBER() OVER (
                PARTITION BY cs.FK_PASSENGER_USER_TID 
                ORDER BY COUNT(*) DESC, MAX(cs.FK_CHECKIN_DATE) DESC
            ) AS rn
        FROM DWQ_PROD.T_DWD_CHECKIN_SEG_FACT cs
        WHERE cs.SEAT_FEATURE IS NOT NULL
        GROUP BY cs.FK_PASSENGER_USER_TID, cs.SEAT_FEATURE
    ) t
    WHERE rn = 1
),

-- CTE 3: 辅营产品偏好（使用窗口函数）
ancillary_pref AS (
    SELECT 
        FK_PASSENGER_USER_TID,
        category AS ANCILLARY_PRODUCT_PREFERENCE_CATEGORY
    FROM (
        SELECT 
            all_purch.FK_PASSENGER_USER_TID,
            all_purch.category,
            COUNT(*) AS purchase_count,
            MAX(all_purch.purchase_date) AS latest_purchase_date,
            ROW_NUMBER() OVER (
                PARTITION BY all_purch.FK_PASSENGER_USER_TID 
                ORDER BY COUNT(*) DESC, MAX(all_purch.purchase_date) DESC
            ) AS rn
        FROM (
            -- 保险
            SELECT FK_PASSENGER_USER_TID, '保险' AS category, FK_BKAUIS_DATE AS purchase_date 
            FROM DWQ_PROD.T_DWD_AUIS_SEG_FACT
            WHERE FK_BKAUIS_DATE IS NOT NULL
            
            UNION ALL 
            
            -- 选座
            SELECT FK_PASSENGER_USER_TID, '选座' AS category, FK_SEATTIK_DATE AS purchase_date 
            FROM DWQ_PROD.T_DWD_SEAT_TIK_FACT
            WHERE FK_SEATTIK_DATE IS NOT NULL
            
            UNION ALL 
            
            -- 行李
            SELECT FK_PASSENGER_USER_TID, '行李' AS category, FK_BAGGAGETIK_TIME AS purchase_date 
            FROM DWQ_PROD.T_DWD_BAGGAGE_TIK_FACT
            WHERE FK_BAGGAGETIK_TIME IS NOT NULL
            
            UNION ALL 
            
            -- 升舱（机上公务）
            SELECT FK_PASSENGER_USER_TID, '升舱' AS category, UPGRADE_DATE AS purchase_date 
            FROM DWQ_PROD.T_DWD_UPGRADE_INFLIGHT_BUSINESS_FACT
            WHERE UPGRADE_DATE IS NOT NULL
            
            UNION ALL 
            
            -- 升舱
            SELECT FK_PASSENGER_USER_TID, '升舱' AS category, FK_UPGRTIK_DATE AS purchase_date 
            FROM DWQ_PROD.T_DWD_UPGR_TIK_FACT
            WHERE FK_UPGRTIK_DATE IS NOT NULL
        ) all_purch
        GROUP BY all_purch.FK_PASSENGER_USER_TID, all_purch.category
    ) t
    WHERE rn = 1
),

-- CTE 4: 高价值会员
high_value_member AS (
    SELECT 
        FK_TID,
        MAX(IS_HIGH_VALUE_DEVELOPER) AS is_high_value
    FROM DWQ_PROD.T_DWD_MEMBER_REGISTER_FACT
    GROUP BY FK_TID
)

SELECT
    tdud.pk_id,
    -- 年龄计算
    YEAR(DATE(@ETL_DATE)) - YEAR(tdud.BIRTHDAY) - 
        CASE 
            WHEN DATE_FORMAT(DATE(@ETL_DATE), '%m%d') < DATE_FORMAT(tdud.BIRTHDAY, '%m%d') 
            THEN 1 
            ELSE 0 
        END AS AGE,
    
    -- 是否是常旅客高价值会员
    CASE 
        WHEN COALESCE(hvm.is_high_value, 0) >= 1 
        THEN 1 
        ELSE 0 
    END AS IS_HIGH_VALUE_FREQUENT_TRAVELER_MEMBER,
    
    -- 最近12个月平均提前出票时间
    bs.AVERAGE_ADVANCE_PURCHASE_TIME_LAST_12M,
    
    -- 座位偏好
    sp.seat_preference,
    
    -- 辅营产品购买偏好
    ap.ANCILLARY_PRODUCT_PREFERENCE_CATEGORY
FROM DWQ_TEST.T_DIM_USER_DIM_COPY tdud
LEFT JOIN booking_stats bs ON tdud.pk_id = bs.FK_PASSENGER_USER_TID
LEFT JOIN seat_pref sp ON tdud.pk_id = sp.FK_PASSENGER_USER_TID
LEFT JOIN ancillary_pref ap ON tdud.pk_id = ap.FK_PASSENGER_USER_TID
LEFT JOIN high_value_member hvm ON tdud.pk_id = hvm.FK_TID
WHERE tdud.IS_VALID_USER = 1;