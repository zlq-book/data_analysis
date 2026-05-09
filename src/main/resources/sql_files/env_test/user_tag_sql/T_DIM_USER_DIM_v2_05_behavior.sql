-- v2 - 05 假期出行及优惠券使用
-- 目标表：DIM_TEST.T_DIM_USER_DIM
INSERT INTO DIM_TEST.T_DIM_USER_DIM (
    PK_ID,                                          -- 用户T_ID
    SUMMER_TRAVEL_COUNT_ALL_TIME,                   -- 全期暑期出行次数
    SPRING_FESTIVAL_TRAVEL_COUNT_ALL_TIME,          -- 全期春运出行次数
    HOLIDAY_TRAVEL_COUNT_ALL_TIME,                  -- 全期节假日出行次数
    TRAVEL_SEGMENTS_WITH_ELDERLY_ALL_TIME,          -- 全期与老人同行航段数
    TRAVEL_SEGMENTS_WITH_CHILDREN_ALL_TIME,         -- 全期与儿童同行航段数
    SELF_BOOKING_TRAVEL_SEGMENTS_ALL_TIME,          -- 全期为自己订票航段数
    COUPON_USAGE_AMOUNT_LAST_12M,                   -- 最近12个月使用优惠券金额
    COUPON_USAGE_COUNT_LAST_12M,                     -- 最近12个月使用机票优惠券张数
    UPDATE_TIME
)

WITH seg_passenger_stats AS (
    -- 1. 出行习惯统计
    SELECT
        ts.FK_PASSENGER_USER_TID,
        COUNT(CASE WHEN dd.HOLIDAYS = 'Summer' THEN ts.SEG_COUNT END) AS summer_cnt,
        COUNT(CASE WHEN dd.HOLIDAYS = 'SpringFestivalTravel' THEN ts.SEG_COUNT END) AS spring_cnt,
        COUNT(CASE WHEN dd.HOLIDAYS IN ('LabourDay', 'NationalDay') THEN ts.SEG_COUNT END) AS holiday_cnt,
        COUNT(CASE WHEN ts.PEER_SENIOR = 1 THEN ts.SEG_COUNT END) AS elder_cnt,
        COUNT(CASE WHEN ts.PEER_CHD = 1 THEN ts.SEG_COUNT END) AS child_cnt,
        COUNT(CASE WHEN ts.SELF_BOOKING = 1 THEN ts.SEG_COUNT END) AS self_cnt
    FROM
        DWD_TEST.T_DWD_DEPART_SEG_FACT ts	-- 251230 修改为成行-航段级
    JOIN DWQ_TEST.T_DIM_DATE_DIM dd
        ON ts.FK_DEPARTURES_DATE = dd.DATE_KEY
    GROUP BY
        ts.FK_PASSENGER_USER_TID
),
coupon_usage_stats AS (
    -- 2. 优惠券使用统计（最近12个月）
    SELECT
        po.FK_BOOKING_USER_TID,
        SUM(CASE WHEN po.COUPON_CATEGORY = 'AIR' THEN po.COUPON_DISCOUNT ELSE 0 END) AS coupon_amount,
        SUM(CASE WHEN po.COUPON_CATEGORY = 'AIR' THEN po.COUPON_COUNT ELSE 0 END) AS coupon_count
    FROM
        DWD_TEST.T_DWD_PAYDETAILS_ORD_FACT po
    WHERE
        po.FK_PAY_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 12 MONTH)
    GROUP BY
        po.FK_BOOKING_USER_TID
)
SELECT
    target.PK_ID,
    COALESCE(sps.summer_cnt, 0) AS SUMMER_TRAVEL_COUNT_ALL_TIME,
    COALESCE(sps.spring_cnt, 0) AS SPRING_FESTIVAL_TRAVEL_COUNT_ALL_TIME,
    COALESCE(sps.holiday_cnt, 0) AS HOLIDAY_TRAVEL_COUNT_ALL_TIME,
    COALESCE(sps.elder_cnt, 0) AS TRAVEL_SEGMENTS_WITH_ELDERLY_ALL_TIME,
    COALESCE(sps.child_cnt, 0) AS TRAVEL_SEGMENTS_WITH_CHILDREN_ALL_TIME,
    COALESCE(sps.self_cnt, 0) AS SELF_BOOKING_TRAVEL_SEGMENTS_ALL_TIME,
    COALESCE(cus.coupon_amount, 0) AS COUPON_USAGE_AMOUNT_LAST_12M,
    COALESCE(cus.coupon_count, 0) AS COUPON_USAGE_COUNT_LAST_12M,
    now()
FROM
    DIM_TEST.T_DIM_USER_DIM target
LEFT JOIN seg_passenger_stats sps
    ON target.PK_ID = sps.FK_PASSENGER_USER_TID
LEFT JOIN coupon_usage_stats cus
    ON target.PK_ID = cus.FK_BOOKING_USER_TID
WHERE
    target.IS_VALID_USER = 1;