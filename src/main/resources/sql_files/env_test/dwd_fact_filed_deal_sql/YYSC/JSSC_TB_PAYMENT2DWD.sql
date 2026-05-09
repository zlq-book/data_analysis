-- 任务名 JSSC_TB_PAYMENT2DWD
-- 预约升舱（券）数据T+1入库后，TB_PAYMENT升舱券支付信息表的PAYDATE是T到T-120天内时间的（每次更新4个月的数），连表：TB_PAYMENT升舱券支付信息表的ORDER_ID=TB_ORDER 升舱券销售订单表(直销)的ID，TB_ORDER 升舱券销售订单表(直销)的CUSTOMER_ID=TB_CUSTOMER购买者基本信息表的ID，数据进入本表。
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
    TO_BASE64(SM4_ENCRYPT(TRIM(SM4_DECRYPT(FROM_BASE64(t3.EMAILPHONENUMBER), FROM_BASE64(@SM4_KEY))), FROM_BASE64(@SM4_KEY))) TID,         --预订人TID
    @ETL_DATE ETL_DATE,
    CURRENT_TIMESTAMP(3) CREATE_TIME,
    1 TID_STATUS
FROM ODS_TEST.T_ODS_JSSC_TB_PAYMENT t1 
    INNER JOIN ODS_TEST.T_ODS_JSSC_TB_ORDER t2 
    ON
        t1.ORDER_ID=t2.ID
    LEFT JOIN ODS_TEST.T_ODS_JSSC_TB_CUSTOMER t3
    ON 
        t2.CUSTOMER_ID=t3.ID
    LEFT JOIN DIM_TEST.T_DIM_STRONGID tds 
  ON
        TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t3.EMAILPHONENUMBER), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))= tds.STRONGID
	    AND tds.STRONGID_STATUS = 1
WHERE t1.ETL_DATE BETWEEN DATE_SUB(@ETL_DATE, INTERVAL 120 DAY) AND @ETL_DATE
    AND t1.PAYSTATUS IN (2,5,6,7)
    AND tds.TID is null
	AND t3.EMAILPHONENUMBER is not null;
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
    DISTINCT TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t3.EMAILPHONENUMBER), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY))) STRONGID,
    'PHONE' STRONGID_TYPE,
    IFNULL(tds.TID,TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t3.EMAILPHONENUMBER), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))) TID,
    1 STRONGID_STATUS,
    CURRENT_TIMESTAMP(3) CREATE_TIME,
    CURRENT_TIMESTAMP(3) UPDATE_TIME,
    @ETL_DATE ETL_DATE
FROM ODS_TEST.T_ODS_JSSC_TB_PAYMENT t1 
    INNER JOIN ODS_TEST.T_ODS_JSSC_TB_ORDER t2 
    ON
        t1.ORDER_ID=t2.ID
    LEFT JOIN ODS_TEST.T_ODS_JSSC_TB_CUSTOMER t3
    ON 
        t2.CUSTOMER_ID=t3.ID
    LEFT JOIN DIM_TEST.T_DIM_STRONGID tds 
  ON
        TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t3.EMAILPHONENUMBER), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))= tds.STRONGID
	    AND tds.STRONGID_STATUS = 1
WHERE t1.ETL_DATE BETWEEN DATE_SUB(@ETL_DATE, INTERVAL 120 DAY) AND @ETL_DATE
AND t1.PAYSTATUS IN (2,5,6,7)
AND t3.EMAILPHONENUMBER is not null;
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
    ,AK_CASH_PAY_PLATFORM
    ,AK_PAY_STATUS
    ,AK_CASH_TYPE
    ,CASH_AMOUNT
    ,SOURCE_LAST_UPDATETIME
    ,SYSTEM_CREATETIME
    ,SYSTEM_LAST_UPDATETIME
    ,ORD_COUNT
)
SELECT
    CONCAT('SCSYY',t2.ORDERNO,t1.REQNO) PK_ID                                                                                                                                                 --PK_ID
    ,t2.ORDERNO BUSINESS_ORDER_ID                                                                                                                                                             --业务大订单编号
    ,t1.REQNO PAY_NO                                                                                                                                                                          --支付流水号
    ,IFNULL(tds.TID,TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t3.EMAILPHONENUMBER), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))) FK_BOOKING_USER_TID                     --预订人TID
    ,TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t3.EMAILPHONENUMBER), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY))) FK_BOOKING_USER_ORIGIN_ID                               --预订人源ID
    ,DATE(t2.SELLDATE) FK_BOOKING_DATE                                                                                                                                                        --预订日期
    ,DATE_FORMAT(t2.SELLDATE, '%H:%i:%S') FK_BOOKING_TIME                                                                                                                                     --预订时间
    ,DATE(t1.PAYDATE) FK_PAY_DATE                                                                                                                                                             --支付日期
    ,DATE_FORMAT(t1.PAYDATE, '%H:%i:%S') FK_PAY_TIME                                                                                                                                          --支付时间
    ,'SCSYY' AK_PAY_CHANNEL                                                                                                                                                                   --支付渠道
    ,'CSH' AK_PAY_METHOD                                                                                                                                                                      --支付方式
    ,IFNULL(sid1.CFG_VALUE, t1.PAYTYPE) AK_CASH_PAY_PLATFORM                                                                                                                                  --现金支付平台（标准化后）
    ,IFNULL(sid2.CFG_VALUE, t1.PAYSTATUS) AK_PAY_STATUS                                                                                                                                       --支付状态（标准化后）
    ,'CNY' AK_CASH_TYPE                                                                                                                                                                       --现金支付币种
    ,t1.AMOUNT CASH_AMOUNT                                                                                                                                                                    --现金支付金额
    ,t1.PAYDATE SOURCE_LAST_UPDATETIME                                                                                                                                                        --源系统最后更新时间
    ,CURRENT_TIMESTAMP(3) SYSTEM_CREATETIME                                                                                                                                                   --本系统创建日期时间
    ,CURRENT_TIMESTAMP(3) SYSTEM_LAST_UPDATETIME                                                                                                                                              --本系统最后更新日期时间
    ,1 AS ORD_COUNT                                                                                                                                                                           --支付单计数
FROM ODS_TEST.T_ODS_JSSC_TB_PAYMENT t1 
    INNER JOIN ODS_TEST.T_ODS_JSSC_TB_ORDER t2 
    ON
        t1.ORDER_ID=t2.ID
    LEFT JOIN ODS_TEST.T_ODS_JSSC_TB_CUSTOMER t3
    ON 
        t2.CUSTOMER_ID=t3.ID
    LEFT JOIN DIM_TEST.T_DIM_STRONGID tds 
    ON
        TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t3.EMAILPHONENUMBER), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))= tds.STRONGID
	    AND tds.STRONGID_STATUS = 1
    LEFT JOIN DWD_TEST.STANDARDIZE_FIELD sid1 
    ON
        t1.PAYTYPE = sid1.CFG_CODE
        AND sid1.CFG_TYPE = 'CASH_PAYMENT_PLATFORM'
        AND sid1.CFG_TYPE_CHANNEL = 'PRE_PURCHASED_UPGRADE'
    LEFT JOIN DWD_TEST.STANDARDIZE_FIELD sid2 
    ON
        t1.PAYSTATUS = sid2.CFG_CODE
        AND sid2.CFG_TYPE = 'PAYMENT_STATUS'
        AND sid2.CFG_TYPE_CHANNEL = 'PRE_PURCHASED_UPGRADE'
    WHERE t1.ETL_DATE BETWEEN DATE_SUB(@ETL_DATE, INTERVAL 120 DAY) AND @ETL_DATE
	    AND t1.PAYSTATUS IN (2,5,6,7)
;

COMMIT;

