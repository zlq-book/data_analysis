INSERT INTO DIM_TEST.T_DIM_USER_DIM (
  PK_ID, 
  SUMMER_TRAVEL_COUNT_ALL_TIME,
  SPRING_FESTIVAL_TRAVEL_COUNT_ALL_TIME,
  HOLIDAY_TRAVEL_COUNT_ALL_TIME,
  TRAVEL_SEGMENTS_WITH_ELDERLY_ALL_TIME,
  TRAVEL_SEGMENTS_WITH_CHILDREN_ALL_TIME,
  SELF_BOOKING_TRAVEL_SEGMENTS_ALL_TIME,
  COUPON_USAGE_AMOUNT_LAST_12M,
  COUPON_USAGE_COUNT_LAST_12M
  )
  SELECT 
  tdud.pk_id,
  -- 全期总消费额所在百分位（乘机人）
  -- "根据“全期总消费额（乘机人）”从低到高消费额去重进行排名，展示当前消费额所在的百分位。
  -- 公式如下：
  -- 1、根据所有用户“全期总消费额（乘机人）”从低到高消费额去重进行进行排名，得到“全期消费额排名”去重的个数X。
  -- 2、找到该TID，对应的全期总消费额，找到这个数的排名Y。
  -- 3、该标签值= Y/X*100% "
  (SELECT COUNT(*)
 from DWD_TEST.T_DWD_TICKING_SEG_FACT ts
 JOIN DIM_TEST.T_DIM_DATE_DIM dd
 ON ts.FK_SEG_DATE = dd.DATEVALUE
     WHERE ts.FK_PASSENGER_USER_TID = tdud.PK_ID
     AND dd.HOLIDAYS IN ('Summer')),
  -- 客户贡献
  -- 由算法写入，本sql不涉及
  -- 全期暑期出行次数
  -- 历史至今该TID作为乘机人出行日期（机票出票-航段级航班：起飞日期）在（日期维表：节假日标识=）出行航段数count
  (SELECT COUNT(*)
 from DWD_TEST.T_DWD_TICKING_SEG_FACT ts
 JOIN DIM_TEST.T_DIM_DATE_DIM dd
 ON ts.FK_SEG_DATE = dd.DATEVALUE
     WHERE ts.FK_PASSENGER_USER_TID = tdud.PK_ID
     AND dd.HOLIDAYS IN ('Summer')),
  -- 全期春运出行次数
  -- 历史至今该TID作为乘机人出行日期的（机票出票-航段级航班：起飞日期）在（日期维表：节假日标识=春运）出行航段数count
  (SELECT COUNT(*)
 from DWD_TEST.T_DWD_TICKING_SEG_FACT ts
 JOIN DIM_TEST.T_DIM_DATE_DIM dd
 ON ts.FK_SEG_DATE = dd.DATEVALUE
     WHERE ts.FK_PASSENGER_USER_TID = tdud.PK_ID
     AND dd.HOLIDAYS IN ('SpringFestivalTravel')),
  -- 全期节假日出行次数
  -- 历史至今该TID作为乘机人出行日期的MMDD（机票出票-航段级：航班起飞日期）在（日期维表：节假日标识=五一前后 或者  十一前后）出行航段数count
  (SELECT COUNT(*)
 from DWD_TEST.T_DWD_TICKING_SEG_FACT ts
 JOIN DIM_TEST.T_DIM_DATE_DIM dd
 ON ts.FK_SEG_DATE = dd.DATEVALUE
     WHERE ts.FK_PASSENGER_USER_TID = tdud.PK_ID
     AND dd.HOLIDAYS IN ('LabourDay', 'NationalDay')),
  -- 全期与老人同行航段数
  -- 该TID作为乘机人，与老人同行(成行-航段级:是否与老人同行=是)count
  (SELECT SUM(ds.SEG_COUNT) 
 from DWD_TEST.T_DWD_DEPART_SEG_FACT ds
     WHERE ds.FK_PASSENGER_USER_TID = tdud.PK_ID
     AND ds.PEER_SENIOR = 1),
  -- 全期与儿童同行航段数
  -- 该TID作为乘机人，与儿童同行（成行-航段级:订单标签，是否与儿童同行=是）航段数
  (SELECT SUM(ds.SEG_COUNT) 
 from DWD_TEST.T_DWD_DEPART_SEG_FACT ds
     WHERE ds.FK_PASSENGER_USER_TID = tdud.PK_ID
     AND ds.PEER_CHD = 1),
  -- 全期为自己订票航段数
  -- 该TID作为乘机人，为自己订票（是否为自己订票=是）count 
  (SELECT SUM(bs.SEG_COUNT) 
 from DWD_TEST.T_DWD_BOOKING_SEG_FACT bs
     WHERE bs.FK_BOOKING_USER_TID = tdud.PK_ID
     AND bs.SELF_BOOKING = 1),
  -- 最近12个月使用优惠券金额
  -- 最近12个月（支付-流水号级事实表：支付日期）使用优惠券金额（支付-流水号级事实表：：优惠券优惠金额）求和
  (SELECT SUM(po.COUPON_DISCOUNT) 
 from DWD_TEST.T_DWD_PAYDETAILS_ORD_FACT po
     WHERE po.FK_BOOKING_USER_TID = tdud.PK_ID
     AND po.FK_PAY_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 1 YEAR)),
  -- 最近12个月使用机票优惠券张数
  -- 最近12个月（支付-流水号级事实表：支付日期）使用机票优惠券（支付-流水号级事实表：券品类=AIR）次数（支付-流水号级事实表：优惠券张数）求和
  (SELECT SUM(po.COUPON_COUNT)
 from DWD_TEST.T_DWD_PAYDETAILS_ORD_FACT po
     WHERE po.FK_BOOKING_USER_TID = tdud.PK_ID
     AND po.FK_PAY_DATE >= DATE_SUB(DATE(@ETL_DATE), INTERVAL 1 YEAR))
  FROM DIM_TEST.T_DIM_USER_DIM tdud 
  WHERE  CREATE_TIME BETWEEN CONCAT(@ETL_DATE,' 00:00:00.000') AND CONCAT(@ETL_DATE,' 23:59:59.999') 
  OR  UPDATE_TIME BETWEEN CONCAT(@ETL_DATE,' 00:00:00.000') AND CONCAT(@ETL_DATE,' 23:59:59.999');