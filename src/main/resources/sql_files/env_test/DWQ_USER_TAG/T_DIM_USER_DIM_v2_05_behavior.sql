SET enable_unique_key_partial_update = true;

-- v2 - 05 假期出行及优惠券使用
-- 目标表：DWQ_TEST.T_DIM_USER_DIM

INSERT INTO DWQ_TEST.T_DIM_USER_DIM (
    PK_ID,                                          -- 用户T_ID
    SUMMER_TRAVEL_COUNT_ALL_TIME,                   -- 全期暑期出行次数
    SPRING_FESTIVAL_TRAVEL_COUNT_ALL_TIME,          -- 全期春运出行次数
    HOLIDAY_TRAVEL_COUNT_ALL_TIME,                  -- 全期节假日出行次数
    TRAVEL_SEGMENTS_WITH_ELDERLY_ALL_TIME,          -- 全期与老人同行航段数
    TRAVEL_SEGMENTS_WITH_CHILDREN_ALL_TIME,         -- 全期与儿童同行航段数
    SELF_BOOKING_TRAVEL_SEGMENTS_ALL_TIME,          -- 全期为自己订票航段数
    COUPON_USAGE_AMOUNT_LAST_12M,                   -- 最近12个月使用优惠券金额
    COUPON_USAGE_COUNT_LAST_12M                     -- 最近12个月使用机票优惠券张数
)

SELECT
	target.PK_ID,
	sq_seg_pass.summer_cnt,
	sq_seg_pass.spring_cnt,
	sq_seg_pass.holiday_cnt,
	sq_seg_pass.elder_cnt,
	sq_seg_pass.child_cnt,
	sq_seg_pass.self_cnt,
	sq_pay.amt,
	sq_pay.cnt
    
FROM
    DWQ_TEST.T_DIM_USER_DIM target
	-- 1. 出行习惯
LEFT JOIN (
	SELECT
		ts.FK_PASSENGER_USER_TID,
		COUNT(CASE WHEN dd.HOLIDAYS = 'Summer' THEN ts.TIK_SEGCOUNT END) AS summer_cnt,
		COUNT(CASE WHEN dd.HOLIDAYS = 'SpringFestivalTravel' THEN ts.TIK_SEGCOUNT END) AS spring_cnt,
		COUNT(CASE WHEN dd.HOLIDAYS IN ('LabourDay', 'NationalDay') THEN ts.TIK_SEGCOUNT END) AS holiday_cnt,
		COUNT(CASE WHEN ts.PEER_SENIOR = 1 THEN ts.TIK_SEGCOUNT END) AS elder_cnt,
		COUNT(CASE WHEN ts.AK_PEER_CHD = 1 THEN ts.TIK_SEGCOUNT END) AS child_cnt,
		COUNT(CASE WHEN ts.SELF_BOOKING = 1 THEN ts.TIK_SEGCOUNT END) AS self_cnt
	FROM
		DWQ_PROD.T_DWD_TICKING_SEG_FACT ts
	JOIN DWQ_PROD.T_DIM_DATE_DIM dd ON
		ts.FK_SEG_DATE = dd.DATE_KEY
	GROUP BY
		ts.FK_PASSENGER_USER_TID
    ) AS sq_seg_pass ON
	target.PK_ID = sq_seg_pass.FK_PASSENGER_USER_TID
	-- 2. 优惠券
LEFT JOIN (
	SELECT
		po.FK_BOOKING_USER_TID,
		SUM(po.COUPON_DISCOUNT) AS amt,
		SUM(CASE WHEN po.COUPON_CATEGORY = 'air' THEN po.COUPON_COUNT ELSE 0 END) AS cnt
	FROM
		DWQ_PROD.T_DWD_PAYDETAILS_ORD_FACT po
	WHERE
		po.FK_PAY_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 12 MONTH)
	GROUP BY
		po.FK_BOOKING_USER_TID
    ) AS sq_pay ON
	target.PK_ID = sq_pay.FK_BOOKING_USER_TID

where target.IS_VALID_USER = 1;

-- WHERE target.CREATE_TIME BETWEEN CONCAT(@ETL_DATE, ' 00:00:00.000') AND CONCAT(@ETL_DATE, ' 23:59:59.999')
-- OR target.UPDATE_TIME BETWEEN CONCAT(@ETL_DATE, ' 00:00:00.000') AND CONCAT(@ETL_DATE, ' 23:59:59.999');
