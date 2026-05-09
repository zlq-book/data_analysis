--  v1 - 01基础信息、常客报名、里程兑换

-- 目标表：DIM_TEST.T_DIM_USER_DIM
INSERT INTO DIM_TEST.T_DIM_USER_DIM (
    PK_ID,                                        -- 主键ID
    HAS_REGISTERED_FREQUENT_TRAVELER,             -- 是否报名过常客产品
    LAST_REGISTRATION_TIME_FREQUENT_TRAVELER,     -- 常客产品最近一次报名时间
    MILEAGE_EXCHANGE_COUNT_12M_AIR,               -- 最近12个月里程兑换次数-航空
    MILEAGE_EXCHANGE_AMOUNT_12M_AIR,              -- 最近12个月兑换里程数量-航空
    MILEAGE_EXCHANGE_COUNT_12M_NON_AIR,           -- 最近12个月里程兑换次数-非航
    MILEAGE_EXCHANGE_AMOUNT_12M_NON_AIR           -- 最近12个月兑换里程数量-非航
)

-- CTE 1: 常客报名聚合
WITH cte_signup AS (
    SELECT
        MEMBER_TID,
        MAX(FK_ACTION_DATE) as LAST_REG_TIME
    FROM DWD_TEST.T_DWD_ACT_SIGNUP_FACT
    GROUP BY MEMBER_TID
),

-- CTE 2: 里程兑换聚合 (近1年)
cte_redeem AS (
    SELECT
        CUMULATION_TID,
        SUM(IF(BIZ_TYPE = 'TKT', EXCH_COUNT, 0)) as AIR_COUNT,           -- 航空类次数
        SUM(IF(BIZ_TYPE = 'TKT', MILES_ACCUMULATED, 0)) as AIR_AMOUNT,   -- 航空类里程
        SUM(IF(BIZ_TYPE <> 'TKT', EXCH_COUNT, 0)) as NON_AIR_COUNT,      -- 非航类次数
        SUM(IF(BIZ_TYPE <> 'TKT', MILES_ACCUMULATED, 0)) as NON_AIR_AMOUNT -- 非航类里程
    FROM DWD_TEST.T_DWD_MILE_REDEMPTION_FACT
    WHERE EXCH_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 12 MONTH)
    GROUP BY CUMULATION_TID
)


SELECT
    base.PK_ID,
    IF(signup.MEMBER_TID IS NOT NULL, 1, 0), -- 只要关联上有记录即为是
    signup.LAST_REG_TIME,
    COALESCE(redeem.AIR_COUNT, 0),
    COALESCE(redeem.AIR_AMOUNT, 0),
    COALESCE(redeem.NON_AIR_COUNT, 0),
    COALESCE(redeem.NON_AIR_AMOUNT, 0)
FROM DIM_TEST.T_DIM_USER_DIM base
LEFT JOIN cte_signup signup ON base.PK_ID = signup.MEMBER_TID
LEFT JOIN cte_redeem redeem ON base.PK_ID = redeem.CUMULATION_TID

where base.IS_VALID_USER = 1;
