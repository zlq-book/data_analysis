SET enable_unique_key_partial_update = true;
-- v1 - 03 销售、附加服务与偏好

-- 目标表：DWQ_TEST.T_DIM_USER_DIM
INSERT INTO DWQ_TEST.T_DIM_USER_DIM (
    PK_ID,                                            -- 主键ID
    INSURANCE_PURCHASE_COUNT_LAST_12M,                -- 最近12个月购买保险次数
    PAID_SEAT_SELECTION_PURCHASE_COUNT_LAST_12M,      -- 最近12个月购买付费选座次数
    PREPAID_BAGGAGE_PURCHASE_COUNT_LAST_12M,          -- 最近12个月购买预付费行李次数
    AVERAGE_ADVANCE_PURCHASE_TIME_LAST_12M,           -- 最近12个月平均提前出票时间（Decimal, 客票级）
    PAYMENT_METHOD_PREFERENCE,                        -- 支付方式偏好
    CABIN_CLASS_PREFERENCE_LAST_1_YEAR                -- 过去12个月内购买舱位偏好
)

-- CTE 1: 附加服务聚合 (保险/选座/行李 合并计算)
WITH cte_ancillary AS (
    SELECT
        u_id,
        SUM(ins_cnt) as INS_COUNT,
        SUM(seat_cnt) as SEAT_COUNT,
        SUM(bag_cnt) as BAG_COUNT
    FROM (
        -- 保险
        SELECT FK_PASSENGER_USER_TID as u_id, INSURANCE_COUNT as ins_cnt, 0 as seat_cnt, 0 as bag_cnt
        FROM DWQ_PROD.T_DWD_AUIS_SEG_FACT WHERE FK_BKAUIS_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 1 YEAR)
        UNION ALL
        -- 选座
        SELECT FK_PASSENGER_USER_TID, 0, SEAT_COUNT, 0
        FROM DWQ_PROD.T_DWD_SEAT_TIK_FACT WHERE FK_SEATTIK_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 1 YEAR)
        UNION ALL
        -- 行李
        SELECT FK_PASSENGER_USER_TID, 0, 0, BAGGAGE_COUNT
        FROM DWQ_PROD.T_DWD_BAGGAGE_TIK_FACT WHERE FK_BAGGAGETIK_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 1 YEAR)
    ) t GROUP BY u_id
),

-- CTE 2: 提前购票天数聚合
cte_adv_ticket AS (
    SELECT
        FK_PASSENGER_USER_TID,
        -- 计算原始平均天数
        SUM(AK_ADVBOOK_DAY) / SUM(TICKET_COUNT) as RAW_AVG_DAYS
    FROM DWQ_PROD.T_DWD_TICKING_TIC_FACT
    WHERE FK_TICKETING_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 1 YEAR)
    GROUP BY FK_PASSENGER_USER_TID
),

-- CTE 3: 支付偏好排行 (Rank 1)
cte_pay_pref AS (
    SELECT FK_BOOKING_USER_TID,
	CASE
		WHEN TRIM(AK_CASH_PAY_PLATFORM) = '' THEN NULL
		ELSE AK_CASH_PAY_PLATFORM
	END AS FAVORITE_PAYMENT
    FROM (
        SELECT FK_BOOKING_USER_TID, AK_CASH_PAY_PLATFORM,
               ROW_NUMBER() OVER (PARTITION BY FK_BOOKING_USER_TID ORDER BY COUNT(*) DESC) as rn
        FROM DWQ_PROD.T_DWD_PAYDETAILS_ORD_FACT
        GROUP BY FK_BOOKING_USER_TID, AK_CASH_PAY_PLATFORM
    ) t WHERE rn = 1
),

-- CTE 4: 舱位偏好排行 (Rank 1)
cte_cabin_pref AS (
    SELECT FK_BOOKING_USER_TID, AK_SEGCABIN as FAVORITE_CABIN
    FROM (
        SELECT FK_BOOKING_USER_TID, AK_SEGCABIN,
               ROW_NUMBER() OVER (PARTITION BY FK_BOOKING_USER_TID ORDER BY COUNT(*) DESC) as rn
        FROM DWQ_PROD.T_DWD_TICKING_SEG_FACT
        WHERE FK_ISSUE_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 1 YEAR)
        GROUP BY FK_BOOKING_USER_TID, AK_SEGCABIN
    ) t WHERE rn = 1
)


SELECT
    base.PK_ID,
    COALESCE(anc.INS_COUNT, 0),
    COALESCE(anc.SEAT_COUNT, 0),
    COALESCE(anc.BAG_COUNT, 0),
    -- [逻辑适配 Decimal] 保留1位小数
    COALESCE(ROUND(tic.RAW_AVG_DAYS, 1), 0),
    pay.FAVORITE_PAYMENT,
    cab.FAVORITE_CABIN
FROM DWQ_TEST.T_DIM_USER_DIM base
LEFT JOIN cte_ancillary anc ON base.PK_ID = anc.u_id
LEFT JOIN cte_adv_ticket tic ON base.PK_ID = tic.FK_PASSENGER_USER_TID
LEFT JOIN cte_pay_pref pay ON base.PK_ID = pay.FK_BOOKING_USER_TID
LEFT JOIN cte_cabin_pref cab ON base.PK_ID = cab.FK_BOOKING_USER_TID

where base.IS_VALID_USER = 1;