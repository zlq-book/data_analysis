SET enable_unique_key_partial_update = true;

-- v2 - 02 机上升舱
-- 目标表：DWQ_TEST.T_DIM_USER_DIM

INSERT INTO DWQ_TEST.T_DIM_USER_DIM (
    PK_ID,
    -- 1. 升舱券相关
    UPGRADE_VOUCHER_USAGE_COUNT_LAST_12M,            -- 最近12个月使用升舱券的次数
    UPGRADE_VOUCHER_USAGE_AMOUNT_LAST_12M,           -- 最近12个月使用升舱券的金额
    -- 2. 机上升舱相关（次数）
    IN_FLIGHT_UPGRADE_COUNT_PREMIUM_ECONOMY_LAST_12M,-- 最近12个月机上升舱的次数（高经）
    IN_FLIGHT_UPGRADE_COUNT_BUSINESS_LAST_12M,       -- 最近12个月机上升舱的次数（公务）
    -- 3. 机上升舱相关（金额）
    IN_FLIGHT_UPGRADE_AMOUNT_PREMIUM_ECONOMY_LAST_12M,-- 最近12个月机上升舱的金额（高经）
    IN_FLIGHT_UPGRADE_AMOUNT_BUSINESS_LAST_12M,       -- 最近12个月机上升舱的金额（公务）
    -- 4. 登机口升舱相关（次数）
    BOARDING_GATE_UPGRADE_COUNT_PREMIUM_ECONOMY_LAST_12M, -- 最近12个月登机口升舱的次数（高经）
    BOARDING_GATE_UPGRADE_COUNT_BUSINESS_LAST_12M,        -- 最近12个月登机口升舱的次数（公务）
    -- 5. 登机口升舱相关（金额）
    BOARDING_GATE_UPGRADE_AMOUNT_PREMIUM_ECONOMY_LAST_12M,-- 最近12个月登机口升舱的金额（高经）
    BOARDING_GATE_UPGRADE_AMOUNT_BUSINESS_LAST_12M,       -- 最近12个月登机口升舱的金额（公务）
    -- 6. 偏好
    UPGRADE_CABIN_PREFERENCE_LAST_12M                -- 最近12个月升舱舱等偏好
)

WITH user_upgrade_pref AS (
    SELECT
        ranked.FK_PASSENGER_USER_TID,
        ranked.category AS pref
    FROM (
        SELECT
            agg.FK_PASSENGER_USER_TID,
            agg.category,
            -- 排名逻辑: 次数多优先 > 时间近优先
            ROW_NUMBER() OVER (
                PARTITION BY agg.FK_PASSENGER_USER_TID
                ORDER BY agg.cnt DESC, agg.last_date DESC
            ) AS rn
        FROM (
            -- 1. 聚合计算: 统计每个用户在每个舱位的总次数和最近时间
            SELECT
                raw_data.FK_PASSENGER_USER_TID,
                raw_data.category,
                COUNT(*) AS cnt,
                MAX(raw_data.event_date) AS last_date
            FROM (
                -- 来源A: 改升
                SELECT
                    FK_PASSENGER_USER_TID,
                    CASE WHEN AK_CABIN = 'BC' THEN '公务' WHEN AK_CABIN = 'PE' THEN '高经' ELSE AK_CABIN END AS category,
                    FK_EXCHANGE_DATE AS event_date
                FROM DWQ_PROD.T_DWD_DATECHANGE_SEG_FACT
                WHERE DATE_CHANGE_TYPE = 'UP' AND AK_CABIN IN ('BC', 'PE')
                  AND FK_EXCHANGE_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 12 MONTH)

                UNION ALL

                -- 来源B: 登机口 (EMD)
                SELECT
                    FK_PASSENGER_USER_TID,
                    CASE WHEN UPGRDNEW_CLASS = 'BC' THEN '公务' WHEN UPGRDNEW_CLASS = 'PE' THEN '高经' ELSE UPGRDNEW_CLASS END,
                    FK_UPGRTIK_DATE
                FROM DWQ_PROD.T_DWD_UPGR_TIK_FACT
                WHERE AK_EMD_STATUS = 'F' AND UPGRDNEW_CLASS IN ('BC', 'PE')
                  AND FK_UPGRTIK_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 12 MONTH)

                UNION ALL

                -- 来源C: 机上
                SELECT
                    FK_PASSENGER_USER_TID,
                    CASE WHEN AFTER_FARE = 'BC' THEN '公务' WHEN AFTER_FARE = 'PE' THEN '高经' ELSE AFTER_FARE END,
                    FLT_DATE
                FROM DWQ_PROD.T_DWD_UPGRADE_INFLIGHT_BUSINESS_FACT
                WHERE AFTER_FARE IN ('BC', 'PE')
                  AND FLT_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 12 MONTH)
            ) raw_data
            GROUP BY raw_data.FK_PASSENGER_USER_TID, raw_data.category
        ) agg
    ) ranked
    WHERE ranked.rn = 1 -- 只保留排名第一的记录
)

SELECT
    tdud.pk_id,

    -- 1. 升舱券
    COALESCE(sq_coupon.cnt, 0)      AS UPGRADE_VOUCHER_USAGE_COUNT_LAST_12M,            -- 最近12个月使用升舱券的次数
    COALESCE(sq_coupon.val, 0)      AS UPGRADE_VOUCHER_USAGE_AMOUNT_LAST_12M,           -- 最近12个月使用升舱券的金额

    -- 2. 机上升舱（次数）
    COALESCE(sq_inflight.pe_cnt, 0) AS IN_FLIGHT_UPGRADE_COUNT_PREMIUM_ECONOMY_LAST_12M,-- 最近12个月机上升舱的次数（高经）
    COALESCE(sq_inflight.bc_cnt, 0) AS IN_FLIGHT_UPGRADE_COUNT_BUSINESS_LAST_12M,       -- 最近12个月机上升舱的次数（公务）

    -- 3. 机上升舱（金额）
    COALESCE(sq_inflight.pe_amt, 0) AS IN_FLIGHT_UPGRADE_AMOUNT_PREMIUM_ECONOMY_LAST_12M,-- 最近12个月机上升舱的金额（高经）
    COALESCE(sq_inflight.bc_amt, 0) AS IN_FLIGHT_UPGRADE_AMOUNT_BUSINESS_LAST_12M,       -- 最近12个月机上升舱的金额（公务）

    -- 4. 登机口升舱（次数）
    COALESCE(sq_gate.pe_cnt, 0)     AS BOARDING_GATE_UPGRADE_COUNT_PREMIUM_ECONOMY_LAST_12M, -- 最近12个月登机口升舱的次数（高经）
    COALESCE(sq_gate.bc_cnt, 0)     AS BOARDING_GATE_UPGRADE_COUNT_BUSINESS_LAST_12M,        -- 最近12个月登机口升舱的次数（公务）

    -- 5. 登机口升舱（金额）
    COALESCE(sq_gate.pe_amt, 0)     AS BOARDING_GATE_UPGRADE_AMOUNT_PREMIUM_ECONOMY_LAST_12M,-- 最近12个月登机口升舱的金额（高经）
    COALESCE(sq_gate.bc_amt, 0)     AS BOARDING_GATE_UPGRADE_AMOUNT_BUSINESS_LAST_12M,       -- 最近12个月登机口升舱的金额（公务）

    -- 6. 偏好
    uup.pref            AS UPGRADE_CABIN_PREFERENCE_LAST_12M                -- 最近12个月升舱舱等偏好

FROM
    DWQ_TEST.T_DIM_USER_DIM tdud


-- A. 升舱券统计
LEFT JOIN (
    SELECT
       ucs.BUYER_TID,
       SUM(ucs.COUPON_COUNT) AS cnt,
       SUM(ucs.COUPON_VALUE) AS val
    FROM
       DWQ_PROD.T_DWD_UPGRADE_COUPON_SALES_FACT ucs
    WHERE
       ucs.COUPON_USE_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 12 MONTH)
    GROUP BY
       ucs.BUYER_TID
) sq_coupon ON tdud.pk_id = sq_coupon.BUYER_TID

-- B. 升舱偏好统计
LEFT JOIN  user_upgrade_pref uup
    ON tdud.pk_id = uup.FK_PASSENGER_USER_TID


-- C. 机上升舱统计 (次数 + 金额)
LEFT JOIN (
    SELECT
       uib.FK_PASSENGER_USER_TID,
       COUNT(CASE WHEN uib.AFTER_FARE = 'PE' THEN 1 END) AS pe_cnt,
       COUNT(CASE WHEN uib.AFTER_FARE = 'BC' THEN 1 END) AS bc_cnt,
       SUM(CASE WHEN uib.AFTER_FARE = 'PE' THEN CAST(uib.UPGRADE_PRICE AS DECIMAL(10,2)) ELSE 0 END) AS pe_amt,
       SUM(CASE WHEN uib.AFTER_FARE = 'BC' THEN CAST(uib.UPGRADE_PRICE AS DECIMAL(10,2))  ELSE 0 END) AS bc_amt
    FROM
       DWQ_PROD.T_DWD_UPGRADE_INFLIGHT_BUSINESS_FACT uib
    WHERE
    -- 现金支付方式， 1 2 待确认是什么意思
       uib.PAY_TYPE = 'CSH' -- 现金支付方式
       AND uib.AFTER_FARE IN ('PE', 'BC')
    GROUP BY
       uib.FK_PASSENGER_USER_TID
) sq_inflight ON tdud.pk_id = sq_inflight.FK_PASSENGER_USER_TID

-- D. 登机口升舱统计 (次数 + 金额)
LEFT JOIN (
    SELECT
       uit.FK_PASSENGER_USER_TID,
       COUNT(CASE WHEN uit.UPGRDNEW_CLASS = 'PE' THEN 1 END) AS pe_cnt,
       COUNT(CASE WHEN uit.UPGRDNEW_CLASS = 'BC' THEN 1 END) AS bc_cnt,
       SUM(CASE WHEN uit.UPGRDNEW_CLASS = 'PE' THEN uit.UPGRDOLD_AMOUNT_CNY ELSE 0 END) AS pe_amt,
       SUM(CASE WHEN uit.UPGRDNEW_CLASS = 'BC' THEN uit.UPGRDOLD_AMOUNT_CNY ELSE 0 END) AS bc_amt
    FROM
       DWQ_PROD.T_DWD_UPGR_TIK_FACT uit -- 附加服务升舱出票航段事实表
    WHERE
       uit.PAY_TYPE = 'CSH' -- 现金支付方式
       AND uit.AK_EMD_STATUS = 'F'
       AND uit.UPGRDNEW_CLASS IN ('PE', 'BC')
       AND uit.FK_UPGRTIK_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 12 MONTH)
    GROUP BY
       uit.FK_PASSENGER_USER_TID
) sq_gate ON tdud.pk_id = sq_gate.FK_PASSENGER_USER_TID

WHERE tdud.IS_VALID_USER = 1
