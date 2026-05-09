-- v1.1 - 04 生命周期与ID风控
-- 目标表：DIM_PROD.T_DIM_USER_DIM
-- 依赖表：DWD_PROD.T_DWD_BOOKING_SEG_FACT (机票预订), DIM_PROD.T_DIM_FFP_DIM, DIM_PROD.T_DIM_CUSTOM_DIM

INSERT INTO DIM_PROD.T_DIM_USER_DIM (
    PK_ID,                                     -- 主键ID
    FLIGHT_BOOKING_COUNT_LAST_30_DAYS,         -- 过去30天机票预定次数
    CUSTOMER_LIFE_CYCLE_LABEL_DIRECT_SALES,    -- 直销用户生命周期标签
    HAS_MULTIPLE_FREQUENT_TRAVELER_CARD_NUMBERS, -- 是否存在多个常客卡号
    HAS_MULTIPLE_CUSTOMER_IDS,                  -- 是否存在多个customerID
    UPDATE_TIME
)

-- CTE 1: 预订统计 (基于订单事实表)
WITH cte_booking_stats AS (
    SELECT
        FK_BOOKING_USER_TID,
        -- 今年预订次数
        SUM(CASE WHEN YEAR(FK_BOOKING_DATE) = YEAR(@ETL_DATE) THEN 1 ELSE 0 END) as BOOK_CNT_THIS_YEAR,
        -- 去年预订次数
        SUM(CASE WHEN YEAR(FK_BOOKING_DATE) = YEAR(@ETL_DATE) - 1 THEN 1 ELSE 0 END) as BOOK_CNT_LAST_YEAR,
        -- 30天内预定次数
        SUM(CASE WHEN FK_BOOKING_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 30 DAY) THEN 1 ELSE 0 END) as BOOK_CNT_30D
    FROM DWD_PROD.T_DWD_BOOKING_SEG_FACT
    GROUP BY FK_BOOKING_USER_TID
),

-- CTE 2: ID重复性检查 (风控逻辑)
cte_dupe_check AS (
    SELECT
        T_ID,
        COUNT(DISTINCT FFRF) as FFP_COUNT,
        COUNT(DISTINCT customer_ID) as CUST_ID_COUNT
    FROM (
        SELECT T_ID, FFRF, NULL as customer_ID FROM DIM_PROD.T_DIM_FFP_DIM
        UNION ALL
        SELECT T_ID, NULL, customer_ID FROM DIM_PROD.T_DIM_CUSTOM_DIM
    ) t GROUP BY T_ID
)

SELECT
    base.PK_ID,
    -- 过去30天机票预定次数
    COALESCE(bk.BOOK_CNT_30D, 0),

    -- 直销生命周期标签 (使用 base.IS_DIRECT_USER 判断)
    CASE
        WHEN base.IS_DIRECT_USER = 1 THEN
            CASE
                -- === 分支 A: 今年注册的新用户 (使用 base.DIRECT_REGISTER_DATE) ===
                WHEN YEAR(base.DIRECT_REGISTER_DATE) = YEAR(@ETL_DATE) THEN
                    CASE
                        WHEN COALESCE(bk.BOOK_CNT_THIS_YEAR, 0) < 1 THEN 'NUS' -- 导入期
                        WHEN COALESCE(bk.BOOK_CNT_THIS_YEAR, 0) = 1 THEN 'GRO' -- 成长期
                        WHEN COALESCE(bk.BOOK_CNT_THIS_YEAR, 0) > 1 THEN 'ALV' -- 活跃期
                        ELSE 'CHN'
                    END

                -- === 分支 B: 往年注册的老用户 ===
                ELSE
                    CASE
                        WHEN COALESCE(bk.BOOK_CNT_LAST_YEAR, 0) > 0 AND COALESCE(bk.BOOK_CNT_THIS_YEAR, 0) > 0 THEN 'ALV' -- 活跃期
                        WHEN COALESCE(bk.BOOK_CNT_LAST_YEAR, 0) > 0 AND COALESCE(bk.BOOK_CNT_THIS_YEAR, 0) = 0 THEN 'SLP' -- 沉睡期
                        WHEN COALESCE(bk.BOOK_CNT_LAST_YEAR, 0) = 0 AND COALESCE(bk.BOOK_CNT_THIS_YEAR, 0) = 0 THEN 'CHN' -- 流失期
                        WHEN COALESCE(bk.BOOK_CNT_LAST_YEAR, 0) = 0 AND COALESCE(bk.BOOK_CNT_THIS_YEAR, 0) > 0 THEN 'RTN' -- 回流期
                        ELSE 'CHN'
                    END
            END
        ELSE NULL -- 非直销用户标签置为空
    END,

    -- ID检查风险标识
    IF(dupe.FFP_COUNT > 1, 1, 0),
    IF(dupe.CUST_ID_COUNT > 1, 1, 0),
    NOW()
FROM DIM_PROD.T_DIM_USER_DIM base
LEFT JOIN cte_booking_stats bk ON base.PK_ID = bk.FK_BOOKING_USER_TID
LEFT JOIN cte_dupe_check dupe ON base.PK_ID = dupe.T_ID
WHERE base.IS_VALID_USER = 1;
