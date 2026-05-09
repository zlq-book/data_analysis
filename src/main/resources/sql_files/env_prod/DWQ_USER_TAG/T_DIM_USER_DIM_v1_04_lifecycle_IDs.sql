SET enable_unique_key_partial_update = true;

-- v1 - 04 生命周期与ID风控
-- 目标表：DWQ_PROD.T_DIM_USER_DIM
-- 依赖表：DWQ_PROD.T_DIM_CUSTOM_DIM (直销用户维表), DWQ_PROD.T_DWD_BOOKING_SEG_FACT (机票预订)


INSERT INTO DWQ_PROD.T_DIM_USER_DIM (
    PK_ID,                                            -- 主键ID
    FLIGHT_BOOKING_COUNT_LAST_30_DAYS,                -- 过去30天机票预定次数
    CUSTOMER_LIFE_CYCLE_LABEL_DIRECT_SALES,           -- 直销用户生命周期标签
    HAS_MULTIPLE_FREQUENT_TRAVELER_CARD_NUMBERS,      -- 是否存在多个常客卡号
    HAS_MULTIPLE_CUSTOMER_IDS                         -- 是否存在多个customerID
)


-- CTE 1: 获取直销用户身份与注册时间 (数据源：直销用户维表)
WITH cte_direct_info AS (
    SELECT
        T_ID, -- 关联 PK_ID
        -- 获取注册日期，如果有重复记录取最早的
        MIN(DIRECT_REGISTER_DATE) as REG_DATE
    FROM DWQ_PROD.T_DIM_CUSTOM_DIM
    GROUP BY T_ID
),

-- CTE 2: 预订统计
cte_booking_stats AS (
    SELECT
        FK_BOOKING_USER_TID,
        -- 今年预订次数
        SUM(CASE WHEN YEAR(FK_BOOKING_DATE) = YEAR(@ETL_DATE) THEN 1 ELSE 0 END) as BOOK_CNT_THIS_YEAR,
        -- 去年预订次数
        SUM(CASE WHEN YEAR(FK_BOOKING_DATE) = YEAR(@ETL_DATE) - 1 THEN 1 ELSE 0 END) as BOOK_CNT_LAST_YEAR,
        -- 30天内预定次数
        SUM(CASE WHEN FK_BOOKING_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 30 DAY) THEN 1 ELSE 0 END) as BOOK_CNT_30D
    FROM DWQ_PROD.T_DWD_BOOKING_SEG_FACT
    GROUP BY FK_BOOKING_USER_TID
),

-- CTE 3: ID重复性检查
cte_dupe_check AS (
    SELECT
        T_ID,
        COUNT(DISTINCT FFRF) as FFP_COUNT,
        COUNT(DISTINCT customer_ID) as CUST_ID_COUNT
    FROM (
        SELECT T_ID, FFRF, NULL as customer_ID FROM DWQ_PROD.T_DIM_FFP_DIM
        UNION ALL
        SELECT T_ID, NULL, customer_ID FROM DWQ_PROD.T_DIM_CUSTOM_DIM
    ) t GROUP BY T_ID
)


SELECT
    base.PK_ID,
    -- 过去30天机票预定次数
    COALESCE(bk.BOOK_CNT_30D, 0),

    -- 直销生命周期标签 (NUS/GRO/ALV/SLP/CHN/RTN)
    -- 1. 判断是否直销用户：只要在 直销用户维表(cte_direct_info) 中存在，即为是
    CASE
        WHEN direct.T_ID IS NOT NULL THEN
            CASE
                -- === 分支 A: 今年注册的新用户 ===
                -- 使用维表中的 DIRECT_REGISTER_DATE 判断
                WHEN YEAR(direct.REG_DATE) = YEAR(@ETL_DATE) THEN
                    CASE
                        -- 1. 导入期 (NUS): 下单 < 1
                        WHEN COALESCE(bk.BOOK_CNT_THIS_YEAR, 0) < 1 THEN 'NUS'
                        -- 2. 成长期 (GRO): 下单 = 1
                        WHEN COALESCE(bk.BOOK_CNT_THIS_YEAR, 0) = 1 THEN 'GRO'
                        -- 3. 活跃期 (ALV): 下单 > 1
                        WHEN COALESCE(bk.BOOK_CNT_THIS_YEAR, 0) > 1 THEN 'ALV'
                        ELSE 'CHN'
                    END

                -- === 分支 B: 往年注册的老用户 ===
                ELSE
                    CASE
                        -- 3. 活跃期 (ALV): 连续两年下单 (去年>0 且 今年>0)
                        WHEN COALESCE(bk.BOOK_CNT_LAST_YEAR, 0) > 0 AND COALESCE(bk.BOOK_CNT_THIS_YEAR, 0) > 0
                            THEN 'ALV'
                        -- 4. 沉睡期 (SLP): 去年下单，今年未下单
                        WHEN COALESCE(bk.BOOK_CNT_LAST_YEAR, 0) > 0 AND COALESCE(bk.BOOK_CNT_THIS_YEAR, 0) = 0
                            THEN 'SLP'
                        -- 5. 流失期 (CHN): 两年均未下单
                        WHEN COALESCE(bk.BOOK_CNT_LAST_YEAR, 0) = 0 AND COALESCE(bk.BOOK_CNT_THIS_YEAR, 0) = 0
                            THEN 'CHN'
                        -- 6. 回流期 (RTN): 去年未下单，今年下单
                        WHEN COALESCE(bk.BOOK_CNT_LAST_YEAR, 0) = 0 AND COALESCE(bk.BOOK_CNT_THIS_YEAR, 0) > 0
                            THEN 'RTN'
                        ELSE 'CHN'
                    END
            END
        -- 非直销用户 (维表关联不上)
        ELSE NULL
    END,

    -- ID检查， 1为是，0为否
    IF(dupe.FFP_COUNT > 1, 1, 0),
    IF(dupe.CUST_ID_COUNT > 1, 1, 0)

FROM DWQ_PROD.T_DIM_USER_DIM base
-- 关联直销用户维表
LEFT JOIN cte_direct_info direct ON base.PK_ID = direct.T_ID
-- 关联预订统计表
LEFT JOIN cte_booking_stats bk ON base.PK_ID = bk.FK_BOOKING_USER_TID
-- 关联ID检查表
LEFT JOIN cte_dupe_check dupe ON base.PK_ID = dupe.T_ID

where base.IS_VALID_USER = 1;

