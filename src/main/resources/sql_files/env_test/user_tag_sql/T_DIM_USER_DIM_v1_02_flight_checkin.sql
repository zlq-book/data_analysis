-- v1 - 02 值机习惯与实际出行

-- 目标表：DIM_TEST.T_DIM_USER_DIM
INSERT INTO DIM_TEST.T_DIM_USER_DIM (
    PK_ID,                                        -- 主键ID
    AVERAGE_ADVANCE_CHECK_IN_TIME_LAST_12M,       -- 最近12个月平均提前值机时间 (Decimal)
    FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS,           -- 近3年乘机航段数
    TOTAL_TRAVEL_SEGMENTS_ALL_TIME                -- 全期出行航段数
)


-- CTE 1: 提前值机数据聚合 (近1年)
WITH cte_checkin AS (
    SELECT
        FK_PASSENGER_USER_TID,
        -- 计算平均值：总提前时间 / 总次数
        -- DWD 中 EARLY_CHECKINTIME 单位是小时
        SUM(EARLY_CHECKINTIME) / SUM(CHECKIN_COUNT) as RAW_AVG_HOURS
    FROM DWD_TEST.T_DWD_CHECKIN_SEG_FACT
    WHERE FK_CHECKIN_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 1 YEAR)
    GROUP BY FK_PASSENGER_USER_TID
),

-- CTE 2: 近3年飞行统计
cte_travel_3y AS (
    SELECT
        FK_PASSENGER_USER_TID,
        COUNT(1) as SEG_3Y -- 统计行数即为航段数
    FROM DWD_TEST.T_DWD_DEPART_SEG_FACT
    WHERE FK_DEPARTURES_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 3 YEAR)
    GROUP BY FK_PASSENGER_USER_TID
),

-- CTE 3: 全期历史飞行统计
cte_travel_all AS (
    SELECT
        FK_PASSENGER_USER_TID,
        SUM(SEG_COUNT) as TOTAL_SEG
    FROM DWD_TEST.T_DWD_DEPART_SEG_FACT
    GROUP BY FK_PASSENGER_USER_TID
)


SELECT
    base.PK_ID,
    COALESCE(ROUND(checkin.RAW_AVG_HOURS,2), 0),	-- 251212 提前值机时间保存为两位小数格式，单位为小时
    COALESCE(travel_3y.SEG_3Y, 0),
    COALESCE(all_travel.TOTAL_SEG, 0)
FROM DIM_TEST.T_DIM_USER_DIM base
LEFT JOIN cte_checkin checkin ON base.PK_ID = checkin.FK_PASSENGER_USER_TID
LEFT JOIN cte_travel_3y travel_3y ON base.PK_ID = travel_3y.FK_PASSENGER_USER_TID
LEFT JOIN cte_travel_all all_travel ON base.PK_ID = all_travel.FK_PASSENGER_USER_TID

where base.IS_VALID_USER = 1;
