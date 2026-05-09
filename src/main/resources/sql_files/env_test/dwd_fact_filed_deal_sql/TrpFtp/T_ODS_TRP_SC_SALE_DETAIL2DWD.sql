-- 任务名 T_ODS_TRP_SC_SALE_DETAIL2DWD
-- TRPFTP报表sc_b2c_report_sale_YYYYMMDD（年月日）T_ODS_TRP_SC_SALE_DETAIL 写入事实表
-- T_ODS_TRP_SC_SALE_DETAIL的现金和券支付在源数据种是一条记录，需要拆分成2条记录。
--1、写入T_DIM_TID_SET
--2、模拟IDMapping写入T_DIM_STRONGID表
--3、写入支付流水号表：现金/券
--SET batch_size = 4096;
--SET @ETL_DATE='2025-10-28'

--写入T_DIM_TID_SET表和T_DIM_STRONGID开启事务
BEGIN;
--1、乘机人不存在的TID写入T_DIM_TID_SET表
INSERT
    INTO
    DIM_TEST.T_DIM_TID_SET
    (TID,
    ETL_DATE,
    CREATE_TIME,
    TID_STATUS)
SELECT
    DISTINCT 
    TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(ID_NUMBER), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY))) AS TID,
    @ETL_DATE ETL_DATE,
    CURRENT_TIMESTAMP(3) CREATE_TIME,
    1 TID_STATUS
FROM
    ODS_TEST.T_ODS_TRP_SC_SALE_DETAIL t1
LEFT JOIN DIM_TEST.T_DIM_STRONGID tds ON
    TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(ID_NUMBER), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))= tds.STRONGID
    AND tds.STRONGID_STATUS = 1
WHERE
    t1.BOOKING_STATUS = '已出票'
    AND t1.ETL_DATE = @ETL_DATE
    AND t1.ID_TYPE is not null
    AND t1.ID_NUMBER is not null
    AND tds.TID is null
    AND (t1.ORDER_CHANNEL NOT IN ('掌尚飞', 'TSDF', 'TSIF', 'SD', '全渠道')
        OR t1.VOUCHER_CODE is not null)
;
--模拟IDMapping写入T_DIM_STRONGID表
--2、乘机人写入证件类型+证件号强ID
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
    DISTINCT TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(ID_NUMBER), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY))) STRONGID,
    IFNULL(sid.CFG_VALUE, t1.ID_TYPE) STRONGID_TYPE,
    IFNULL(tds.TID,TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(ID_NUMBER), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))) TID,
    1 STRONGID_STATUS,
    CURRENT_TIMESTAMP(3) CREATE_TIME,
    CURRENT_TIMESTAMP(3) UPDATE_TIME,
    @ETL_DATE ETL_DATE
FROM
    ODS_TEST.T_ODS_TRP_SC_SALE_DETAIL t1
LEFT JOIN DIM_TEST.T_DIM_STRONGID tds ON
    TO_BASE64(SM4_ENCRYPT(UPPER(TRIM(SM4_DECRYPT(FROM_BASE64(ID_NUMBER), FROM_BASE64(@SM4_KEY)))), FROM_BASE64(@SM4_KEY)))= tds.STRONGID
    AND tds.STRONGID_STATUS = 1
LEFT JOIN DWD_TEST.STANDARDIZE_FIELD sid
  ON
        t1.ID_TYPE = sid.CFG_CODE
        AND sid.CFG_TYPE = 'DOCUMENT_TYPE'
        AND sid.CFG_TYPE_CHANNEL = 'TRP_FTP'
WHERE
    t1.BOOKING_STATUS = '已出票'
    AND t1.ETL_DATE = @ETL_DATE
    AND t1.ID_TYPE is not null
    AND t1.ID_NUMBER is not null
    AND (t1.ORDER_CHANNEL NOT IN ('掌尚飞', 'TSDF', 'TSIF', 'SD', '全渠道')
        OR t1.VOUCHER_CODE is not null)
;
--3、预定人不存在的TID写入T_DIM_TID_SET表
INSERT
    INTO
    DIM_TEST.T_DIM_TID_SET
    (TID,
    ETL_DATE,
    CREATE_TIME,
    TID_STATUS)
SELECT
    DISTINCT 
    t1.BOOKING_USERNAME AS TID,
    @ETL_DATE ETL_DATE,
    CURRENT_TIMESTAMP(3) CREATE_TIME,
    1 TID_STATUS
FROM
    ODS_TEST.T_ODS_TRP_SC_SALE_DETAIL t1
LEFT JOIN DIM_TEST.T_DIM_STRONGID tds ON
    t1.BOOKING_USERNAME= tds.STRONGID
    AND tds.STRONGID_STATUS = 1
WHERE
    t1.BOOKING_STATUS = '已出票'
    AND t1.ETL_DATE = @ETL_DATE
    AND tds.TID is null
    AND (t1.ORDER_CHANNEL NOT IN ('掌尚飞', 'TSDF', 'TSIF', 'SD', '全渠道')
        OR t1.VOUCHER_CODE is not null)
;
--4、预定人写入CUSTOMER_ID 强ID
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
    DISTINCT t1.BOOKING_USERNAME STRONGID,
    'CCID' STRONGID_TYPE,
    IFNULL(tds.TID, t1.BOOKING_USERNAME) TID,
    1 STRONGID_STATUS,
    CURRENT_TIMESTAMP(3) CREATE_TIME,
    CURRENT_TIMESTAMP(3) UPDATE_TIME,
    @ETL_DATE ETL_DATE
FROM
    ODS_TEST.T_ODS_TRP_SC_SALE_DETAIL t1
LEFT JOIN DIM_TEST.T_DIM_STRONGID tds ON
    t1.BOOKING_USERNAME= tds.STRONGID
    AND tds.STRONGID_STATUS = 1
WHERE
    t1.BOOKING_STATUS = '已出票'
    AND t1.ETL_DATE = @ETL_DATE
    AND (t1.ORDER_CHANNEL NOT IN ('掌尚飞', 'TSDF', 'TSIF', 'SD', '全渠道')
        OR t1.VOUCHER_CODE is not null)
;
-- 确认无误后提交事务
COMMIT;
--现金情况写入支付流水号表
INSERT
    INTO
    DWD_TEST.T_DWD_PAYDETAILS_ORD_FACT(
    PK_ID,                                                                                                      --PK_ID 
    BUSINESS_ORDER_ID,                                                                                          --业务大订单编号
    PAY_NO,                                                                                                     --支付流水号
    FK_BOOKING_USER_TID,                                                                                        --预订人TID
    FK_BOOKING_USER_ORIGIN_ID,                                                                                  --预订人源ID
    FK_BOOKING_DATE,                                                                                            --预订日期
    FK_BOOKING_TIME,                                                                                            --预订时间
    FK_PAY_DATE,                                                                                                --支付日期
    FK_PAY_TIME,                                                                                                --支付时间
    AK_PAY_CHANNEL,                                                                                             --支付渠道
    AK_PAY_METHOD,                                                                                              --支付方式
    AK_CASH_PAY_PLATFORM,
    AK_PAY_STATUS,
    AK_CASH_TYPE,
    CASH_AMOUNT,
    SOURCE_LAST_UPDATETIME,
    SYSTEM_CREATETIME,
    SYSTEM_LAST_UPDATETIME,
    ORD_COUNT
)
SELECT
    CONCAT('SCTRP', BUSINESS_ORDER_ID, IFNULL(PAY_NO, '')) PK_ID,                                               --PK_ID 
    BUSINESS_ORDER_ID,                                                                                          --业务大订单编
    PAY_NO,                                                                                                     --支付流水号
    TID FK_BOOKING_USER_TID,                                                                                    --预订人TID
    FK_BOOKING_USER_ORIGIN_ID,                                                                                  --预订人源ID
    DATE(BOOKING_DATE) FK_BOOKING_DATE,                                                                         --预订日期
    DATE_FORMAT(BOOKING_DATE, '%H:%i:%S') FK_BOOKING_TIME,                                                      --预订时间
    DATE(PAYMENT_TIME) FK_PAY_DATE,                                                                             --支付日期
    DATE_FORMAT(PAYMENT_TIME, '%H:%i:%S') FK_PAY_TIME,                                                          --支付时间
    AK_PAY_CHANNEL,                                                                                             --支付渠道
    'CSH' AK_PAY_METHOD,                                                                                        --支付方式
    AK_CASH_PAY_PLATFORM,                                                                                       --现金支付平台
    'PAD' AK_PAY_STATUS,                                                                                        --支付状态
    CURRENCY AK_CASH_TYPE,                                                                                      --现金支付币种
    AMOUNT_PAYABLE CASH_AMOUNT,                                                                                 --现金支付金额
    PAYMENT_TIME SOURCE_LAST_UPDATETIME,                                                                        --源系统最后更新时间
    CURRENT_TIMESTAMP(3) SYSTEM_CREATETIME,                                                                     --本系统创建日期时间
    CURRENT_TIMESTAMP(3) SYSTEM_LAST_UPDATETIME,                                                                --本系统最后更新日期时间
    1 AS ORD_COUNT                                                                                              --支付单计数
FROM
    (
    SELECT
        t1.ORDER_NUMBER BUSINESS_ORDER_ID,                                                                      --业务大订单编号
        t1.BANK_ORDER_NUMBER PAY_NO,                                                                            --支付流水号
        t1.BOOKING_DATE,                                                                                        --预订日期
        t1.BOOKING_USERNAME FK_BOOKING_USER_ORIGIN_ID,                                                          --预订人源ID
        t1.PAYMENT_TIME,                                                                                        --支付日期
        t1.BANK_NAME,                                                                                           --现金支付平台
        t1.CURRENCY,                                                                                            --现金支付币种
        t1.AMOUNT_PAYABLE,                                                                                      --现金支付金额
        IFNULL(tds.TID, t1.BOOKING_USERNAME) TID,                                                               --预订人TID
        IFNULL(sid1.CFG_VALUE, t1.ORDER_CHANNEL) AK_PAY_CHANNEL,                                                --支付渠道（标准化后）
        IFNULL(sid2.CFG_VALUE, t1.BANK_NAME) AK_CASH_PAY_PLATFORM                                               --现金支付平台（标准化后）
    FROM
        ODS_TEST.T_ODS_TRP_SC_SALE_DETAIL t1
    LEFT JOIN DIM_TEST.T_DIM_STRONGID tds 
  ON
        t1.BOOKING_USERNAME= tds.STRONGID
        AND tds.STRONGID_STATUS = 1
    LEFT JOIN DWD_TEST.STANDARDIZE_FIELD sid1 
  ON
        t1.ORDER_CHANNEL = sid1.CFG_CODE
        AND sid1.CFG_TYPE = 'PAYMENT_CHANNEL'
        AND sid1.CFG_TYPE_CHANNEL = 'TRP_FTP'
    LEFT JOIN DWD_TEST.STANDARDIZE_FIELD sid2 
  ON
        t1.BANK_NAME = sid2.CFG_CODE
        AND sid2.CFG_TYPE = 'CASH_PAYMENT_PLATFORM'
        AND sid2.CFG_TYPE_CHANNEL = 'TRP_FTP'
    WHERE
        t1.ETL_DATE = @ETL_DATE
        AND t1.BOOKING_STATUS = '已出票'
        AND t1.ORDER_CHANNEL NOT IN ('掌尚飞', 'TSDF', 'TSIF', 'SD', '全渠道')
    ) tt1
    ;
--券情况写入支付流水号表
INSERT
    INTO
    DWD_TEST.T_DWD_PAYDETAILS_ORD_FACT(
    PK_ID,
    BUSINESS_ORDER_ID,
    PAY_NO,
    FK_BOOKING_USER_TID,
    FK_BOOKING_USER_ORIGIN_ID,
    FK_BOOKING_DATE,
    FK_BOOKING_TIME,
    FK_PAY_DATE,
    FK_PAY_TIME,
    AK_PAY_CHANNEL,
    AK_PAY_METHOD,
    COUPON_CATEGORY,
    AK_PAY_STATUS,
    AK_COUPON,
    COUPON_COUNT,
    COUPON_DISCOUNT,
    COUPON_CODE,
    COUPON_NO,
    COUPON_NAME,
    SOURCE_LAST_UPDATETIME,
    SYSTEM_CREATETIME,
    SYSTEM_LAST_UPDATETIME,
    ORD_COUNT
    )
SELECT
    CONCAT('SCTRP', BUSINESS_ORDER_ID, IFNULL(PAY_NO, ''),VOUCHER_CODE) PK_ID,                             --PK_ID 
    BUSINESS_ORDER_ID,                                                                                     --业务大订单编号
    PAY_NO,                                                                                                --支付流水号
    TID FK_BOOKING_USER_TID,                                                                               --预订人TID
    FK_BOOKING_USER_ORIGIN_ID,                                                                             --预订人源ID
    DATE(BOOKING_DATE) FK_BOOKING_DATE,                                                                    --预订日期
    DATE_FORMAT(BOOKING_DATE, '%H:%i:%S') FK_BOOKING_TIME,                                                 --预订时间
    DATE(PAYMENT_TIME) FK_PAY_DATE,                                                                        --支付日期
    DATE_FORMAT(PAYMENT_TIME, '%H:%i:%S') FK_PAY_TIME,                                                     --支付时间
    AK_PAY_CHANNEL,                                                                                        --支付渠道
    'VCH' AK_PAY_METHOD,                                                                                   --支付方式
    'AIR' COUPON_CATEGORY,                                                                                 --券品类
    'PAD' AK_PAY_STATUS,                                                                                   --支付状态
    true AK_COUPON,                                                                                        --是否使用优惠券
    1 COUPON_COUNT,                                                                                        --优惠券张数
    COUPON_AMOUNT COUPON_DISCOUNT,                                                                         --优惠券优惠金额
    COUPON_CODE COUPON_CODE,                                                                               --优惠券产品编码
    VOUCHER_CODE COUPON_NO,                                                                                --优惠券券码
    COUPON_NAME,                                                                                           --优惠券名称
    PAYMENT_TIME SOURCE_LAST_UPDATETIME,                                                                    --源系统最后更新时间
    CURRENT_TIMESTAMP(3) SYSTEM_CREATETIME,                                                                --本系统创建日期时间
    CURRENT_TIMESTAMP(3) SYSTEM_LAST_UPDATETIME,                                                           --本系统最后更新日期时间
    1 AS ORD_COUNT                                                                                         --支付单计数
FROM
    (
    SELECT
        t1.ORDER_NUMBER BUSINESS_ORDER_ID,                                                                 --业务大订单编号
        t1.BANK_ORDER_NUMBER PAY_NO,                                                                       --支付流水号
        t1.BOOKING_DATE,                                                                                   --预订日期
        t1.BOOKING_USERNAME FK_BOOKING_USER_ORIGIN_ID,                                                     --预订人源ID
        t1.PAYMENT_TIME,                                                                                   --支付日期
        t1.COUPON_AMOUNT,                                                                                  --优惠券优惠金额
        t1.COUPON_CODE,                                                                                    --优惠券产品编码
        t1.VOUCHER_CODE,                                                                                   --优惠券券码
        t1.COUPON_NAME,                                                                                    --优惠券名称
        IFNULL(tds.TID, t1.BOOKING_USERNAME) TID,                                                          --预订人TID
        IFNULL(sid1.CFG_VALUE, t1.ORDER_CHANNEL) AK_PAY_CHANNEL                                            --支付渠道（标准化后）
    FROM
        ODS_TEST.T_ODS_TRP_SC_SALE_DETAIL t1
    LEFT JOIN DIM_TEST.T_DIM_STRONGID tds 
  ON
        t1.BOOKING_USERNAME= tds.STRONGID
        AND tds.STRONGID_STATUS = 1
    LEFT JOIN DWD_TEST.STANDARDIZE_FIELD sid1 
  ON
        t1.ORDER_CHANNEL = sid1.CFG_CODE
        AND sid1.CFG_TYPE = 'PAYMENT_CHANNEL'
        AND sid1.CFG_TYPE_CHANNEL = 'TRP_FTP'
    WHERE
        t1.ETL_DATE = @ETL_DATE
        AND t1.BOOKING_STATUS = '已出票'
        AND t1.COUPON_CODE is not null
    ) tt1
    ;
COMMIT;
