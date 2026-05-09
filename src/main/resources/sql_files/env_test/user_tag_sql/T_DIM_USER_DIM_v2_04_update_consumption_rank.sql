-- v2 - 04 消费额及排名 + 全期出票航段次数
-- 目标表：DIM_TEST.T_DIM_USER_DIM

INSERT INTO DIM_TEST.T_DIM_USER_DIM (
    PK_ID,                                              -- 用户T_ID
    TOTAL_CONSUMPTION_AMOUNT_ALL_TIME,                  -- 全期总消费额（乘机人）
    TOTAL_CONSUMPTION_RANK_ALL_TIME,                    -- 全期总消费额排名（乘机人）
    PASSENGER_TOTAL_CONSUME_PERCENTILE_ALL_TIME,        -- 全期总消费额所在百分位（乘机人）
    TOTAL_TICKET_ISSUANCE_AMOUNT_INTERNATIONAL_ALL_TIME, -- 全期出票金额（国际）
    TOTAL_TICKET_ISSUANCE_AMOUNT_DOMESTIC_ALL_TIME,     -- 全期出票金额（国内）
    TICKET_PURCHASE_AMOUNT_INTERNATIONAL_LAST_1_YEAR,   -- 过去12个月内出票金额（国际）
    TICKET_PURCHASE_AMOUNT_DOMESTIC_LAST_1_YEAR,        -- 过去12个月出票金额（国内）
    GROUP_TICKET_PURCHASE_ISSUANCE_COUNT_LAST_1_YEAR,   -- 过去12个月团队票出票航段数
    GROUP_TICKET_PURCHASE_ISSUANCE_COUNT_ALL_TIME,      -- 全期团队票出票次数
    TOTAL_TICKET_ISSUANCE_SEGMENTS_ALL_TIME,            -- 全期出票航段次数
    TICKET_PURCHASE_SEGMENTS_COUNT_LAST_1_YEAR,         -- 过去12个月内购票航段数量
    UPDATE_TIME                                         -- 更新时间
)
WITH
    -- CTE 1: 纵向合并所有消费明细
    cte_flat_consumption AS (
    SELECT
        FK_PASSENGER_USER_TID AS uid,
        SEG_TRANSPORT_TIK_PRICE AS amt
    FROM
        DWD_TEST.T_DWD_DEPART_SEG_FACT
    UNION ALL
    SELECT
        FK_PASSENGER_USER_TID,
        SEAT_AMOUNT_CNY
    FROM
        DWD_TEST.T_DWD_SEAT_TIK_FACT
    WHERE
        AK_EMD_STATUS = 'F'
    UNION ALL
    SELECT
        FK_PASSENGER_USER_TID,
        BAGGAGE_AMOUNT_CNY
    FROM
        DWD_TEST.T_DWD_BAGGAGE_TIK_FACT
    WHERE
        AK_EMD_STATUS = 'F'
    UNION ALL
    SELECT
        FK_PASSENGER_USER_TID,
        AK_EXCESSBAGGAGE_OLDAMOUNT_CNY
    FROM
        DWD_TEST.T_DWD_EXCESS_BAGGAGE_TIK_FACT
    WHERE
        AK_EMD_STATUS = 'F'
    UNION ALL
    SELECT
        FK_PASSENGER_USER_TID,
        UPGRDOLD_AMOUNT_CNY
    FROM
        DWD_TEST.T_DWD_UPGR_TIK_FACT
    WHERE
        AK_EMD_STATUS = 'F'
    UNION ALL
    SELECT
        FK_PASSENGER_USER_TID,
        INSURANCE_AMT
    FROM
        DWD_TEST.T_DWD_AUIS_SEG_FACT
    UNION ALL
    SELECT
        FK_PASSENGER_USER_TID,
        CAST(UPGRADE_PRICE AS DECIMAL(15, 2))
    FROM
        DWD_TEST.T_DWD_UPGRADE_INFLIGHT_BUSINESS_FACT
    WHERE
        UPGRADE_PRICE IS NOT NULL
        AND UPGRADE_PRICE != ''
    UNION ALL
    SELECT
        BUYER_TID,
        COUPON_VALUE
    FROM
        DWD_TEST.T_DWD_UPGRADE_COUPON_SALES_FACT
    WHERE
        COUPON_USE_DATE IS NOT NULL
    UNION ALL
    SELECT
        FK_BUYER_TID,
        VOUCHER_PAYMENT_AMOUNT
    FROM
        DWD_TEST.T_DWD_TRP_COUPON_SALE_FACT
),

    -- CTE 2: 全局聚合 & 全局排名计算
    cte_user_global_metrics AS (
    SELECT
        uid,
        SUM(amt) AS total_amt,
        RANK() OVER (ORDER BY SUM(amt) DESC) AS rank_val,
        PERCENT_RANK() OVER (ORDER BY SUM(amt) DESC) * 100 AS percentile_val
    FROM
        cte_flat_consumption
    WHERE
        uid IS NOT NULL
    GROUP BY
        uid
),

    -- CTE 3: 机票相关指标（包含全期出票航段次数）
    cte_ticket_metrics AS (
    SELECT
        ts.FK_BOOKING_USER_TID,
        -- 出票金额统计
        SUM(CASE WHEN ts.AK_DIMARK = 'I' THEN ts.AK_SALE_PRICE_ALL_CNY ELSE 0 END) AS total_intl,
        SUM(CASE WHEN ts.AK_DIMARK = 'D' THEN ts.AK_SALE_PRICE_ALL_CNY ELSE 0 END) AS total_dom,
        SUM(CASE WHEN ts.AK_DIMARK = 'I' AND (ts.IS_RESCHEDULED = 0 OR ts.IS_RESCHEDULED IS NULL) AND ts.FK_ISSUE_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 12 MONTH) THEN ts.AK_SALE_PRICE_ALL_CNY ELSE 0 END) AS last1y_intl,
        SUM(CASE WHEN ts.AK_DIMARK = 'D' AND (ts.IS_RESCHEDULED = 0 OR ts.IS_RESCHEDULED IS NULL) AND ts.FK_ISSUE_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 12 MONTH) THEN ts.AK_SALE_PRICE_ALL_CNY ELSE 0 END) AS last1y_dom,
        -- 团队票统计
        SUM(CASE WHEN ts.IS_TEAM_TICKET = 1 AND ts.FK_ISSUE_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 12 MONTH) THEN ts.TIK_SEGCOUNT ELSE 0 END) AS group_cnt_1y,
        SUM(CASE WHEN ts.IS_TEAM_TICKET = 1 THEN ts.TIK_SEGCOUNT ELSE 0 END) AS group_cnt_all,
        -- 全期出票航段次数（不区分是否换开，统计所有）
        SUM(ts.TIK_SEGCOUNT) AS total_segments_all_time,
        -- 过去12个月内购票航段数量
        SUM(CASE WHEN ts.FK_ISSUE_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 12 MONTH) THEN ts.TIK_SEGCOUNT ELSE 0 END) AS ticket_segments_count_last_1_year
    FROM
        DWD_TEST.T_DWD_TICKING_SEG_FACT ts
    GROUP BY
        ts.FK_BOOKING_USER_TID
)

SELECT
    target.PK_ID,
    -- 消费金额与排名
    ROUND(COALESCE(metrics.total_amt, 0), 2) AS TOTAL_CONSUMPTION_AMOUNT_ALL_TIME,
    COALESCE(metrics.rank_val, 0) AS TOTAL_CONSUMPTION_RANK_ALL_TIME,
    COALESCE(metrics.percentile_val, 0) AS PASSENGER_TOTAL_CONSUME_PERCENTILE_ALL_TIME,
    -- 机票指标
    COALESCE(tm.total_intl, 0) AS TOTAL_TICKET_ISSUANCE_AMOUNT_INTERNATIONAL_ALL_TIME,
    COALESCE(tm.total_dom, 0) AS TOTAL_TICKET_ISSUANCE_AMOUNT_DOMESTIC_ALL_TIME,
    COALESCE(tm.last1y_intl, 0) AS TICKET_PURCHASE_AMOUNT_INTERNATIONAL_LAST_1_YEAR,
    COALESCE(tm.last1y_dom, 0) AS TICKET_PURCHASE_AMOUNT_DOMESTIC_LAST_1_YEAR,
    COALESCE(tm.group_cnt_1y, 0) AS GROUP_TICKET_PURCHASE_ISSUANCE_COUNT_LAST_1_YEAR,
    COALESCE(tm.group_cnt_all, 0) AS GROUP_TICKET_PURCHASE_ISSUANCE_COUNT_ALL_TIME,
    -- 全期出票航段次数（使用CTE 3的统计）
    COALESCE(tm.total_segments_all_time, 0) AS TOTAL_TICKET_ISSUANCE_SEGMENTS_ALL_TIME,
    -- 过去12个月内购票航段数量（新增）
    COALESCE(tm.ticket_segments_count_last_1_year, 0) AS TICKET_PURCHASE_SEGMENTS_COUNT_LAST_1_YEAR,
    -- 更新时间
    NOW() AS UPDATE_TIME
FROM
    DIM_TEST.T_DIM_USER_DIM target
LEFT JOIN cte_user_global_metrics metrics ON
    target.PK_ID = metrics.uid
LEFT JOIN cte_ticket_metrics tm ON
    target.PK_ID = tm.FK_BOOKING_USER_TID
WHERE
    target.IS_VALID_USER = 1;