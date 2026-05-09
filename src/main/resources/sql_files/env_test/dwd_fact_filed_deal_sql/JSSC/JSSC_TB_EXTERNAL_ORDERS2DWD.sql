-- 任务名 JSSC_TB_EXTERNAL_ORDERS2DWD
-- 升舱整合平台-机上升舱 T_ODS_JSSC_TB_EXTERNAL_ORDERS 写入事实表 主键：数据源代号+业务大订单编号+支付流水号 数据源代号：T_ODS_JSSC_TB_EXTERNAL_ORDERS- SCSJS
-- 机上升舱数据T+1入库后，TB_EXTERNAL_ORDERS升舱使用订单的UPGRADE_TIME是T到T-30天时间内的（每次更新1个月的数据），同时CHANNEL_TYPE=2，PAY_PRICE非0（PAY_PRICE是空或者大于0的），UPGRADE_STATUS=1的数据进入本表。
-- 现金情况：主键：数据源代号SCSJS+TB_EXTERNAL_ORDERS.ORDER_NO+TRADE_NO
-- 券情况：主键：数据源代号SCSJS+TB_EXTERNAL_ORDERS.ORDER_NO+UPGRADE_ORDER_ID
--1、写入T_DIM_TID_SET
--2、模拟IDMapping写入T_DIM_STRONGID表
--3、写入支付流水号表：现金/券
--SET batch_size = 4096;
--SET @ETL_DATE='2025-10-16'
BEGIN;
--1、不存在的TID写入T_DIM_TID_SET表
INSERT
    INTO
    DIM_TEST.T_DIM_TID_SET
    (TID,
    ETL_DATE,
    CREATE_TIME,
    TID_STATUS)
SELECT
    DISTINCT
    TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t1.PSG_CARDID), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY))) TID,         --预订人TID
    @ETL_DATE ETL_DATE,
    CURRENT_TIMESTAMP(3) CREATE_TIME,
    1 TID_STATUS
FROM
    ODS_TEST.T_ODS_JSSC_TB_EXTERNAL_ORDERS t1
LEFT JOIN DIM_TEST.T_DIM_STRONGID tds ON
    TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t1.PSG_CARDID), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))= tds.STRONGID
    AND tds.STRONGID_STATUS = 1
WHERE t1.ETL_DATE BETWEEN DATE_SUB(@ETL_DATE, INTERVAL 30 DAY) AND @ETL_DATE
AND t1.CHANNEL_TYPE=2
AND (t1.PAY_PRICE> 0 OR t1.PAY_PRICE is null)
AND t1.UPGRADE_STATUS=1
AND t1.PAY_STATUS IN(2,7)
AND tds.TID is null;
--模拟IDMapping写入T_DIM_STRONGID表
--2、写入证件类型+证件号强ID
INSERT
    INTO
    DIM_TEST.T_DIM_STRONGID
    (STRONGID,
    STRONGID_TYPE,
    TID,
    STRONGID_STATUS,
    CREATE_TIME,
    UPDATE_TIME,
    ETL_DATE)
SELECT
    DISTINCT
    TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t1.PSG_CARDID), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY))) STRONGID,
    CASE WHEN tds.TID IS NOT NULL THEN tds.STRONGID_TYPE ELSE 'OT' END AS STRONGID_TYPE,                                                                                                                                                                                 --证件号类型未知
    IFNULL(tds.TID,TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t1.PSG_CARDID), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))) TID,
    1 STRONGID_STATUS,
    CURRENT_TIMESTAMP(3) CREATE_TIME,
    CURRENT_TIMESTAMP(3) UPDATE_TIME,
    @ETL_DATE ETL_DATE
FROM
    ODS_TEST.T_ODS_JSSC_TB_EXTERNAL_ORDERS t1
LEFT JOIN DIM_TEST.T_DIM_STRONGID tds ON
    TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t1.PSG_CARDID), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))= tds.STRONGID
    AND tds.STRONGID_STATUS = 1
WHERE t1.ETL_DATE BETWEEN DATE_SUB(@ETL_DATE, INTERVAL 30 DAY) AND @ETL_DATE
AND t1.CHANNEL_TYPE=2
AND (t1.PAY_PRICE> 0 OR t1.PAY_PRICE is null)
AND t1.UPGRADE_STATUS=1
AND t1.PAY_STATUS IN(2,7)
    ;

COMMIT;

--写入支付流水号表
INSERT
    INTO
    DWD_TEST.T_DWD_PAYDETAILS_ORD_FACT(
    PK_ID
    ,BUSINESS_ORDER_ID
    ,PAY_NO
    ,FK_BOOKING_USER_TID
    ,FK_BOOKING_USER_ORIGIN_ID
    ,FK_BOOKING_DATE
    ,FK_BOOKING_TIME
    ,FK_PAY_DATE
    ,FK_PAY_TIME
    ,AK_PAY_CHANNEL
    ,AK_PAY_METHOD
    ,COUPON_CATEGORY
    ,AK_CASH_PAY_PLATFORM
    ,AK_PAY_STATUS
    ,AK_CASH_TYPE
    ,CASH_AMOUNT
    ,AK_COUPON
    ,COUPON_COUNT
    ,COUPON_NO
    ,COUPON_NAME
    ,SOURCE_LAST_UPDATETIME
    ,SYSTEM_CREATETIME
    ,SYSTEM_LAST_UPDATETIME
    ,ORD_COUNT
)
SELECT
    CASE WHEN t1.PAY_TYPE IN('1','2') THEN CONCAT('SCSJS',ORDER_NO,IFNULL(TRADE_NO, '')) WHEN t1.PAY_TYPE IN('3','4','5') THEN CONCAT('SCSJS',ORDER_NO,IFNULL(UPGRADE_ORDER_ID, '')) ELSE NULL END AS PK_ID           --PK_ID
    ,t1.ORDER_NO BUSINESS_ORDER_ID                                                                                                                                                                                    --业务大订单编
    ,CASE WHEN t1.PAY_TYPE IN('1','2') THEN t1.TRADE_NO WHEN t1.PAY_TYPE IN('3','4','5') THEN t1.UPGRADE_ORDER_ID END AS PAY_NO                                                                                       --支付流水号
    ,IFNULL(tds.TID,TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t1.PSG_CARDID), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))) FK_BOOKING_USER_TID                                                   --预订人TID
    ,t1.PSG_CARDID FK_BOOKING_USER_ORIGIN_ID                                                                                                                                                                          --预订人源ID
    ,DATE(t1.UPGRADE_TIME) FK_BOOKING_DATE                                                                                                                                                                            --预订日期
    ,DATE_FORMAT(t1.UPGRADE_TIME, '%H:%i:%S') FK_BOOKING_TIME                                                                                                                                                         --预订时间
    ,CASE WHEN t1.PAY_TYPE IN('1','2') THEN DATE(t1.PAY_TIME)  WHEN t1.PAY_TYPE IN('3','4','5') THEN DATE(t1.UPGRADE_TIME) END AS FK_PAY_DATE                                                                         --支付日期
    ,CASE WHEN t1.PAY_TYPE IN('1','2') THEN DATE_FORMAT(t1.PAY_TIME, '%H:%i:%S')  WHEN t1.PAY_TYPE IN('3','4','5') THEN DATE_FORMAT(t1.UPGRADE_TIME, '%H:%i:%S') END AS FK_PAY_TIME                                   --支付时间
    ,'SCSJS' AK_PAY_CHANNEL                                                                                                                                                                                           --支付渠道
    ,CASE WHEN t1.PAY_TYPE IN('1','2') THEN 'CSH' WHEN t1.PAY_TYPE IN('3','4','5') THEN 'VCH' END AS AK_PAY_METHOD                                                                                                    --支付方式
    ,CASE WHEN t1.PAY_TYPE IN('3','4','5') THEN 'UPG' ELSE NULL END AS COUPON_CATEGORY                                                                                                                                --券品类
    ,IFNULL(sid1.CFG_VALUE, CASE WHEN t1.PAY_TYPE IN('1','2') THEN t1.PAYCHANNEL ELSE NULL END) AK_CASH_PAY_PLATFORM                                                                                                  --现金支付平台（标准化后）
    ,IFNULL(sid2.CFG_VALUE, t1.PAY_STATUS) AK_PAY_STATUS                                                                                                                                                              --支付状态（标准化后）
    ,CASE WHEN t1.PAY_TYPE IN('1','2') THEN 'CNY' ELSE NULL END AS AK_CASH_TYPE                                                                                                                                       --现金支付币种
    ,CASE WHEN t1.PAY_TYPE IN('1','2') THEN t1.PAY_PRICE ELSE NULL END AS CASH_AMOUNT                                                                                                                                 --现金支付金额
    ,CASE WHEN t1.PAY_TYPE IN('3','4','5') THEN true ELSE NULL END AS AK_COUPON                                                                                                                                       --是否使用优惠券
    ,CASE WHEN t1.PAY_TYPE IN('3','4','5') THEN 1 ELSE NULL END AS COUPON_COUNT                                                                                                                                       --优惠券张数
    ,CASE WHEN t1.PAY_TYPE IN('3','4','5') THEN t1.COUON_NO ELSE NULL END AS COUPON_NO                                                                                                                                --优惠券券码
    ,CASE WHEN t1.PAY_TYPE IN('3','4','5') THEN '升舱券' ELSE NULL END AS COUPON_NAME                                                                                                                                 --优惠券名称
    ,t1.UPGRADE_TIME SOURCE_LAST_UPDATETIME                                                                                                                                                                           --源系统最后更新时间
    ,CURRENT_TIMESTAMP(3) SYSTEM_CREATETIME                                                                                                                                                                           ----本系统创建日期时间
    ,CURRENT_TIMESTAMP(3) SYSTEM_LAST_UPDATETIME                                                                                                                                                                      ----本系统最后更新日期时间
    ,1 AS ORD_COUNT                                                                                                                                                                                                   ----支付单计数
FROM ODS_TEST.T_ODS_JSSC_TB_EXTERNAL_ORDERS t1
    LEFT JOIN DIM_TEST.T_DIM_STRONGID tds 
    ON
        TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t1.PSG_CARDID), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))= tds.STRONGID
	    AND tds.STRONGID_STATUS = 1
    LEFT JOIN DWD_TEST.STANDARDIZE_FIELD sid1 
    ON
        CASE WHEN t1.PAY_TYPE IN('1','2') THEN t1.PAYCHANNEL ELSE NULL END = sid1.CFG_CODE
        AND sid1.CFG_TYPE = 'CASH_PAYMENT_PLATFORM'
        AND sid1.CFG_TYPE_CHANNEL = 'PRE_PURCHASED_UPGRADE'
    LEFT JOIN DWD_TEST.STANDARDIZE_FIELD sid2 
    ON
        t1.PAY_STATUS = sid2.CFG_CODE
        AND sid2.CFG_TYPE = 'PAYMENT_STATUS'
        AND sid2.CFG_TYPE_CHANNEL = 'PRE_PURCHASED_UPGRADE' 
    WHERE t1.ETL_DATE BETWEEN DATE_SUB(@ETL_DATE, INTERVAL 30 DAY) AND @ETL_DATE
    AND t1.CHANNEL_TYPE=2
    AND (t1.PAY_PRICE> 0 OR t1.PAY_PRICE is null)
    AND t1.UPGRADE_STATUS=1
    AND t1.PAY_STATUS IN(2,7)
;

COMMIT;