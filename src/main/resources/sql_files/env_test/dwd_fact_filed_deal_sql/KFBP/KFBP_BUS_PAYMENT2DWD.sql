-- 任务名 KFBP_BUS_PAYMENT2DWD
-- 呼叫白屏系统T_ODS_KFBP_BUS_PAYMENT (支付明细表)表，UPDATE_DATE是T的，PAYMENT_STATUS不等于0/1/2的数据进入此事实表。使用BUS_PAYMENT的UNION_ORDER_PKID和BUS_ORDER（基本订单表）的union_order_pkid连表。有1对多情况，取其中一条即可。
--1、写入T_DIM_TID_SET
--2、模拟IDMapping写入T_DIM_STRONGID表
--3、写入支付流水号表：现金/券
--SET batch_size = 4096;
--SET @ETL_DATE='2025-10-15'
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
    TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t2.CUR_TEL), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY))) TID,         --预订人TID
    @ETL_DATE ETL_DATE,
    CURRENT_TIMESTAMP(3) CREATE_TIME,
    1 TID_STATUS
FROM
    ODS_TEST.T_ODS_KFBP_BUS_PAYMENT t1
INNER JOIN ODS_TEST.T_ODS_KFBP_BUS_ORDER t2 
ON
    t1.UNION_ORDER_PKID = t2.UNION_ORDER_PKID
LEFT JOIN DIM_TEST.T_DIM_STRONGID tds 
    ON
        TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t2.CUR_TEL), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))= tds.STRONGID
        AND tds.STRONGID_STATUS = 1
WHERE
    t1.PAYMENT_STATUS IN (3, 4, 5)
    AND t1.ETL_DATE =  @ETL_DATE
    AND tds.TID is null;
--模拟IDMapping写入T_DIM_STRONGID表
--2、写入手机号强ID
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
    DISTINCT TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t2.CUR_TEL), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY))) STRONGID,
    'PHONE' STRONGID_TYPE,
    IFNULL(tds.TID,TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t2.CUR_TEL), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))) TID,
    1 STRONGID_STATUS,
    CURRENT_TIMESTAMP(3) CREATE_TIME,
    CURRENT_TIMESTAMP(3) UPDATE_TIME,
    @ETL_DATE ETL_DATE
FROM
    ODS_TEST.T_ODS_KFBP_BUS_PAYMENT t1
INNER JOIN ODS_TEST.T_ODS_KFBP_BUS_ORDER t2 
ON
    t1.UNION_ORDER_PKID = t2.UNION_ORDER_PKID
LEFT JOIN DIM_TEST.T_DIM_STRONGID tds 
    ON
        TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t2.CUR_TEL), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))= tds.STRONGID
        AND tds.STRONGID_STATUS = 1
WHERE
    t1.PAYMENT_STATUS IN (3, 4, 5)
    AND t1.ETL_DATE =  @ETL_DATE
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
    ,POINT_AMOUNT
    ,SOURCE_LAST_UPDATETIME
    ,SYSTEM_CREATETIME
    ,SYSTEM_LAST_UPDATETIME
    ,ORD_COUNT
)
SELECT
    CONCAT('SCWHP',t1.UNION_ORDER_PKID,t1.PAYMENT_SN) PK_ID,                                                                                                                        --PK_ID 
    t1.UNION_ORDER_PKID BUSINESS_ORDER_ID,                                                                                                                                          --业务大订单编
    t1.PAYMENT_SN PAY_NO,                                                                                                                                                           --支付流水号
    IFNULL(tds.TID,TO_BASE64(SM4_ENCRYPT(TRIM(SM4_DECRYPT(FROM_BASE64(t2.CUR_TEL), FROM_BASE64(@SM4_KEY))), FROM_BASE64(@SM4_KEY)))) FK_BOOKING_USER_TID,                           --预订人TID
    t2.CUR_TEL FK_BOOKING_USER_ORIGIN_ID,                                                                                                                                           --预订人源ID
    DATE(t2.CREATE_DATE) FK_BOOKING_DATE,                                                                                                                                           --预订日期
    DATE_FORMAT(t2.CREATE_DATE, '%H:%i:%S') FK_BOOKING_TIME,                                                                                                                        --预订时间
    DATE(t1.CREATE_DATE) FK_PAY_DATE,                                                                                                                                               --支付日期
    DATE_FORMAT(t1.CREATE_DATE, '%H:%i:%S') FK_PAY_TIME,                                                                                                                            --支付时间
    'SCWHP' AK_PAY_CHANNEL,                                                                                                                                                         --支付渠道
    CASE WHEN t1.PAYMENT_METHOD =5 THEN 'MLG' ELSE 'CSH' END AS AK_PAY_METHOD,                                                                                                      --支付方式
    IFNULL(sid1.CFG_VALUE, CASE WHEN t1.PAYMENT_METHOD =5 THEN null WHEN t1.PAYMENT_METHOD =23 THEN t1.PAYMENT_CHANNEL ELSE t1.PAYMENT_CHANNEL END)  AK_CASH_PAY_PLATFORM,          --现金支付平台（数据标准化后）
    IFNULL(sid2.CFG_VALUE, t1.PAYMENT_STATUS) AK_PAY_STATUS,                                                                                                                        --支付状态（数据标准化）
    t2.CASH_CURRENCY_CODE,                                                                                                                                                          --现金支付币种
    CASE WHEN t1.PAYMENT_METHOD =5 THEN NULL ELSE  t1.TOTAL_AMOUNT END AS CASH_AMOUNT,                                                                                              --现金支付金额
    CASE WHEN t1.PAYMENT_METHOD =5 THEN t1.TOTAL_AMOUNT ELSE  NULL END AS POINT_AMOUNT,                                                                                             --积分支付金额
    t1.UPDATE_DATE,                                                                                                                                                                 --源系统最后更新时间
    CURRENT_TIMESTAMP(3) SYSTEM_CREATETIME,                                                                                                                                         --本系统创建日期时间
    CURRENT_TIMESTAMP(3) SYSTEM_LAST_UPDATETIME,                                                                                                                                    --本系统最后更新日期时间
    1 AS ORD_COUNT                                                                                                                                                                  --支付单计数
FROM
    ODS_TEST.T_ODS_KFBP_BUS_PAYMENT t1
INNER JOIN ODS_TEST.T_ODS_KFBP_BUS_ORDER t2 
ON
    t1.UNION_ORDER_PKID = t2.UNION_ORDER_PKID
LEFT JOIN DIM_TEST.T_DIM_STRONGID tds 
    ON
        TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t2.CUR_TEL), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))= tds.STRONGID
        AND tds.STRONGID_STATUS = 1
LEFT JOIN DWD_TEST.STANDARDIZE_FIELD sid1 
  ON
        CASE WHEN t1.PAYMENT_METHOD =5 THEN null WHEN t1.PAYMENT_METHOD =23 THEN t1.PAYMENT_CHANNEL ELSE t1.PAYMENT_METHOD END = sid1.CFG_CODE
        AND sid1.CFG_TYPE = 'CASH_PAYMENT_PLATFORM'
        AND sid1.CFG_TYPE_CHANNEL = 'CALL_CENTER_TICKETING_SYSTEM'
LEFT JOIN DWD_TEST.STANDARDIZE_FIELD sid2 
    ON
        t1.PAYMENT_STATUS = sid2.CFG_CODE
        AND sid2.CFG_TYPE = 'PAYMENT_STATUS'
        AND sid2.CFG_TYPE_CHANNEL = 'PRE_PURCHASED_UPGRADE' 
WHERE
    t1.PAYMENT_STATUS IN (3, 4, 5)
    AND t1.ETL_DATE =  @ETL_DATE
;

COMMIT;