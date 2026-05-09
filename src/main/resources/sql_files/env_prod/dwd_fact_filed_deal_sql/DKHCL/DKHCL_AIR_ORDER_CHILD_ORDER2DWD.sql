-- 任务名DKHCL_AIR_ORDER_CHILD_ORDER2DWD
-- 大客户差旅系统T_ODS_TRP_SC_SALE_DETAIL 写入事实表 主键：数据源代号+业务大订单编号+支付流水号 数据源代号：大客户差旅系统-SCSME
-- 大客户差旅系统的数据T+1入库后，yee_quick_pay_info 支付记录表的TRADATE是T时间的。连表：AIR_ORDER_CHILD_ORDER 机票订单主表的ORDER_NO=yee_quick_pay_info 支付记录表的BUSINESNO，
--同时限制yee_quick_pay_info 支付记录表的PAYSTATUS=Y（支付成功）和TRADECODE=0000（交易成功）的，AIR_ORDER_CHILD_ORDER 机票订单主表CUS_BIG_CODE=99998181的，PNR非空的。这些数据进入本表。
--1、写入T_DIM_TID_SET
--2、模拟IDMapping写入T_DIM_STRONGID表
--3、写入支付流水号表：现金/券
--SET batch_size = 4096;
--SET @ETL_DATE='2025-10-26'
BEGIN;
--1、不存在的TID写入T_DIM_TID_SET表
INSERT
    INTO
    DIM_PROD.T_DIM_TID_SET
    (TID,
    ETL_DATE,
    CREATE_TIME,
    TID_STATUS)
SELECT
    DISTINCT
    TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t3.CERT_NO), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY))) TID,         --预订人TID
    @ETL_DATE ETL_DATE,
    CURRENT_TIMESTAMP(3) CREATE_TIME,
    1 TID_STATUS
FROM
    ODS_PROD.T_ODS_DKHCL_AIR_ORDER_CHILD_ORDER t1
INNER JOIN ODS_PROD.T_ODS_DKHCL_YEE_QUICK_PAY_INFO t2 on
    t1.ORDER_NO = t2.BUSINESNO
LEFT JOIN (
    SELECT 
        EMPLOYEE_ID,
        CERT_NO,
        CERT_TYPE,
        -- 为每个员工的证件分配优先级序号
        ROW_NUMBER() OVER(
            PARTITION BY EMPLOYEE_ID 
            ORDER BY 
                CASE WHEN CERT_TYPE = '身份证' THEN 1 ELSE 2 END
        ) as rn
    FROM 
        ODS_PROD.T_ODS_DKHCL_CRM_EMPLOYEE_CERT
) t3 on
    t1.CREATOR_ID=t3.EMPLOYEE_ID 
LEFT JOIN DIM_PROD.T_DIM_STRONGID tds ON
    TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t3.CERT_NO), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))= tds.STRONGID
    AND tds.STRONGID_STATUS = 1
WHERE
    t1.ETL_DATE=@ETL_DATE
    AND t2.PAYSTATUS = 'Y'
    AND t2.TRADECODE = '00000'
    AND t1.CUS_BIG_CODE = '99998181'
    AND t1.PNR is not null
    AND t3.rn=1
    AND tds.TID is null;
--模拟IDMapping写入T_DIM_STRONGID表
--2、写入证件类型+证件号强ID
INSERT
    INTO
    DIM_PROD.T_DIM_STRONGID
    (STRONGID,
    STRONGID_TYPE,
    TID,
    STRONGID_STATUS,
    CREATE_TIME,
    UPDATE_TIME,
    ETL_DATE)
SELECT
    DISTINCT TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t3.CERT_NO), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY))) STRONGID,
    IFNULL(sid.CFG_VALUE, t3.CERT_TYPE) STRONGID_TYPE,
    IFNULL(tds.TID,TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t3.CERT_NO), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))) TID,
    1 STRONGID_STATUS,
    CURRENT_TIMESTAMP(3) CREATE_TIME,
    CURRENT_TIMESTAMP(3) UPDATE_TIME,
    @ETL_DATE ETL_DATE
FROM
    ODS_PROD.T_ODS_DKHCL_AIR_ORDER_CHILD_ORDER t1
INNER JOIN ODS_PROD.T_ODS_DKHCL_YEE_QUICK_PAY_INFO t2 on
    t1.ORDER_NO = t2.BUSINESNO
LEFT JOIN (
    SELECT 
        EMPLOYEE_ID,
        CERT_NO,
        CERT_TYPE,
        -- 为每个员工的证件分配优先级序号
        ROW_NUMBER() OVER(
            PARTITION BY EMPLOYEE_ID 
            ORDER BY 
                CASE WHEN CERT_TYPE = '身份证' THEN 1 ELSE 2 END
        ) as rn
    FROM 
        ODS_PROD.T_ODS_DKHCL_CRM_EMPLOYEE_CERT
) t3 on
    t1.CREATOR_ID=t3.EMPLOYEE_ID 
LEFT JOIN DIM_PROD.T_DIM_STRONGID tds ON
    TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t3.CERT_NO), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))= tds.STRONGID
    AND tds.STRONGID_STATUS = 1
LEFT JOIN DWD_PROD.STANDARDIZE_FIELD sid
  ON
        t3.CERT_TYPE = sid.CFG_CODE
        AND sid.CFG_TYPE = 'DOCUMENT_TYPE'
        AND sid.CFG_TYPE_CHANNEL = 'KEY_ACCOUNT_TRAVEL'
WHERE
    t1.ETL_DATE=@ETL_DATE
    AND t2.PAYSTATUS = 'Y'
    AND t2.TRADECODE = '00000'
    AND t1.CUS_BIG_CODE = '99998181'
    AND t1.PNR is not null
    AND t3.rn=1
    AND t3.CERT_TYPE is not null
    ;

COMMIT;

--写入支付流水号表
INSERT
    INTO
    DWD_PROD.T_DWD_PAYDETAILS_ORD_FACT(
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
    CONCAT('SCSME',t1.ORDER_NO,CASE WHEN t2.PAYPLATFORM='WEICHAT_ONLINE_PAY' THEN t2.BANKORDERID ELSE t2.SERIALNUM END) PK_ID,                           --PK_ID
    t1.ORDER_NO,                                                                                                                                         --业务大订单编号
    CASE WHEN t2.PAYPLATFORM='WEICHAT_ONLINE_PAY' THEN t2.BANKORDERID ELSE t2.SERIALNUM END AS PAY_NO,                                                   --支付流水号
    IFNULL(tds.TID,TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t3.CERT_NO), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))) TID,         --预订人TID
    t1.CREATOR_ID,                                                                                                                                       --预订人源ID
    DATE(t1.CREATE_TIME) FK_BOOKING_DATE,                                                                                                                --预订日期
    DATE_FORMAT(t1.CREATE_TIME, '%H:%i:%S') FK_BOOKING_TIME,                                                                                             --预订时间
    DATE(t2.TRADATE) FK_PAY_DATE,                                                                                                                        --支付日期
    DATE_FORMAT(t2.TRADATE, '%H:%i:%S') FK_PAY_TIME,                                                                                                     --支付时间
    'SCSME' AS AK_PAY_CHANNEL,                                                                                                                           --支付渠道(数据标准化后)
    'CSH' AK_PAY_METHOD,                                                                                                                                 --支付方式
    IFNULL(sid.CFG_VALUE, t2.PAYPLATFORM) AK_CASH_PAY_PLATFORM,                                                                                          --现金支付平台(数据标准化后)
    IFNULL(sid2.CFG_VALUE, t2.PAYSTATUS) AK_PAY_STATUS,                                                                                                  --支付状态(数据标准化)
    'CNY' AK_CASH_TYPE,                                                                                                                                  --现金支付币种
    t2.AMOUNT CASH_AMOUNT,                                                                                                                               --现金支付金额
    t2.TRADATE SOURCE_LAST_UPDATETIME,                                                                                                                   --源系统最后更新时间
    CURRENT_TIMESTAMP(3) SYSTEM_CREATETIME,                                                                                                              --本系统创建日期时间
    CURRENT_TIMESTAMP(3) SYSTEM_LAST_UPDATETIME,                                                                                                         --本系统最后更新日期时间
    1 AS ORD_COUNT                                                                                                                                       --支付单计数
FROM
    ODS_PROD.T_ODS_DKHCL_AIR_ORDER_CHILD_ORDER t1
INNER JOIN ODS_PROD.T_ODS_DKHCL_YEE_QUICK_PAY_INFO t2 on
    t1.ORDER_NO = t2.BUSINESNO
LEFT JOIN (
    SELECT 
        EMPLOYEE_ID,
        CERT_NO,
        CERT_TYPE,
        -- 为每个员工的证件分配优先级序号
        ROW_NUMBER() OVER(
            PARTITION BY EMPLOYEE_ID 
            ORDER BY 
                CASE WHEN CERT_TYPE = '身份证' THEN 1 ELSE 2 END
        ) as rn
    FROM 
        ODS_PROD.T_ODS_DKHCL_CRM_EMPLOYEE_CERT
) t3 on
    t1.CREATOR_ID=t3.EMPLOYEE_ID 
LEFT JOIN DIM_PROD.T_DIM_STRONGID tds ON
    TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(t3.CERT_NO), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))= tds.STRONGID
    AND tds.STRONGID_STATUS = 1
LEFT JOIN DWD_PROD.STANDARDIZE_FIELD sid
  ON
        t2.PAYPLATFORM = sid.CFG_CODE
        AND sid.CFG_TYPE = 'CASH_PAYMENT_PLATFORM'
        AND sid.CFG_TYPE_CHANNEL = 'KEY_ACCOUNT_TRAVEL'
LEFT JOIN DWD_PROD.STANDARDIZE_FIELD sid2
    ON
        t2.PAYSTATUS = sid2.CFG_CODE
        AND sid2.CFG_TYPE = 'PAYMENT_STATUS'
        AND sid2.CFG_TYPE_CHANNEL = 'KEY_ACCOUNT_TRAVEL' 
WHERE
    t1.ETL_DATE=@ETL_DATE
    AND t2.PAYSTATUS = 'Y'
    AND t2.TRADECODE = '00000'
    AND t1.CUS_BIG_CODE = '99998181'
    AND t1.PNR is not null
    AND t3.rn=1;
COMMIT;