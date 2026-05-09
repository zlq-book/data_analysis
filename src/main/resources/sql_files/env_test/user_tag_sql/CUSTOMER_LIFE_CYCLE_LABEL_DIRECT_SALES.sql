-- 直销用户生命周期标签
INSERT INTO DIM_TEST.T_DIM_USER_DIM (PK_ID, CUSTOMER_LIFE_CYCLE_LABEL_DIRECT_SALES)
SELECT 
    user_data.PK_ID,
    CASE 
        -- 1. 导入期：今年注册，下单数 < 1
        WHEN user_data.REGISTER_YEAR = YEAR(CURRENT_DATE()) AND booking_count.TOTAL_BOOKINGS < 1 
            THEN 'NUS'
        
        -- 2. 成长期：今年注册，下单数 = 1
        WHEN user_data.REGISTER_YEAR = YEAR(CURRENT_DATE()) AND booking_count.TOTAL_BOOKINGS = 1 
            THEN 'GRO'
        
        -- 3. 活跃期：今年注册且下单超过一次 或 连续两年下单
        WHEN (user_data.REGISTER_YEAR = YEAR(CURRENT_DATE()) AND booking_count.TOTAL_BOOKINGS > 1) 
            OR (booking_count.THIS_YEAR_BOOKINGS > 0 AND booking_count.LAST_YEAR_BOOKINGS > 0)
            THEN 'ALV'
        
        -- 4. 沉睡期：去年下单，今年未下单
        WHEN booking_count.LAST_YEAR_BOOKINGS > 0 AND booking_count.THIS_YEAR_BOOKINGS = 0 
            THEN 'SLP'
        
        -- 5. 流失期：两年未下单
        WHEN booking_count.THIS_YEAR_BOOKINGS = 0 AND booking_count.LAST_YEAR_BOOKINGS = 0 
            THEN 'CHN'
        
        -- 6. 回流期：去年未下单，今年下单
        WHEN booking_count.LAST_YEAR_BOOKINGS = 0 AND booking_count.THIS_YEAR_BOOKINGS > 0 
            THEN 'RTN'
        
        -- 默认情况：没有预订记录的用户
        ELSE 'CHN'
    END as CUSTOMER_LIFE_CYCLE_LABEL_DIRECT_SALES

FROM (
    -- 获取直销用户基本信息
    SELECT 
        PK_ID,
        IS_DIRECT_USER,
        DIRECT_REGISTER_DATE,
        YEAR(DIRECT_REGISTER_DATE) as REGISTER_YEAR
    FROM DIM_TEST.T_DIM_USER_DIM
    WHERE IS_DIRECT_USER = 1
	AND CREATE_TIME BETWEEN CONCAT(@ETL_DATE,' 00:00:00.000') AND CONCAT(@ETL_DATE,' 23:59:59.999')
	OR UPDATE_TIME BETWEEN CONCAT(@ETL_DATE,' 00:00:00.000') AND CONCAT(@ETL_DATE,' 23:59:59.999')
) user_data

LEFT JOIN (
    -- 统计每个用户的预订情况
    SELECT 
        FK_BOOKING_USER_TID,
        -- 总下单数
        COUNT(*) as TOTAL_BOOKINGS,
        -- 今年下单数
        SUM(CASE WHEN YEAR(FK_BOOKING_DATE) = YEAR(CURRENT_DATE()) THEN 1 ELSE 0 END) as THIS_YEAR_BOOKINGS,
        -- 去年下单数
        SUM(CASE WHEN YEAR(FK_BOOKING_DATE) = YEAR(CURRENT_DATE()) - 1 THEN 1 ELSE 0 END) as LAST_YEAR_BOOKINGS
    FROM DWD_TEST.T_DWD_BOOKING_SEG_FACT
    WHERE FK_BOOKING_USER_TID IN (SELECT PK_ID FROM DIM_TEST.T_DIM_USER_DIM WHERE IS_DIRECT_USER = 1)
    GROUP BY FK_BOOKING_USER_TID
) booking_count ON user_data.PK_ID = booking_count.FK_BOOKING_USER_TID;

--事务提交
commit;