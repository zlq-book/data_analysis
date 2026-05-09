-- 1. 设置会话变量 (在执行 INSERT 语句前执行)
--SET batch_size = 4096;
--SET @ETL_DATE = '2025-01-01';

-- 附加服务_选座退票_航段事实表-写DWD
INSERT INTO DWD_TEST.T_DWD_SEAT_REFUND_FACT
(PK_ID, FK_REFUND_DATE, FK_REFUND_TIME, DEPAIRPORT, ARRIAIRPORT, FK_SEATSTART_DATE, AK_DIMARK,
 AK_TIKNUM, AK_EMDNUM, AK_EMD_STATUS, AK_SEGCABIN, AK_CURRENCY, SEAT_AMOUNT,
 SEAT_DETAIL, SEAT_ZONE, SEAT_NUMBER, SEAT_COUNT,
 DATA_ACTIVE, DATA_ACTIVE_TIME, SYSTEM_CREATETIME, SYSTEM_LAST_UPDATETIME,
 EN_FIRST_NAME, EN_LAST_NAME, CN_NAME, PASSENGER_TYPE, CERT_TYPE,
 CERT_NUMBER, PASSENGER_AGE, FK_PASSENGER_USER_TID, FK_SEAT_SEG, AK_CABIN)
SELECT CONCAT(temd.EMDTICKETNUMBER, temd.ORIG, temd.DEST)                                                            PK_ID,
       date_format(UPDATE_TIME, '%Y-%m-%d')                                                                       AS FK_REFUND_DATE,
       date_format(UPDATE_TIME, '%H:%i:%s')                                                                       AS FK_REFUND_TIME,
       ORIG                                                                                                       AS DEPAIRPORT,
       DEST                                                                                                       AS ARRIAIRPORT,
       FLT_DATE                                                                                                   AS FK_SEATSTART_DATE,
       EMDTICKETTYPE                                                                                              AS AK_DIMARK,
       ET_NUM                                                                                                     AS AK_TIKNUM,
       EMDTICKETNUMBER                                                                                            AS AK_EMDNUM,
       EMD_COUPONSTATUS                                                                                           AS AK_EMD_STATUS,
       seg.AK_SEGCABIN                                                                                            AS AK_SEGCABIN,
       PAYCURRENCYCODE                                                                                            AS AK_CURRENCY,
       CAST(PAYAMOUNT AS DOUBLE)                                                                                  AS SEAT_AMOUNT,
       ISSUANCE_DESCRIPTION                                                                                       AS SEAT_DETAIL,
       SUBSTRING(ISSUANCE_DESCRIPTION, 1, 1)                                                                      AS SEAT_ZONE,
       CASE
           WHEN INSTR(ISSUANCE_DESCRIPTION, ' ') > 0 THEN split_part(ISSUANCE_DESCRIPTION, ' ', 2)
           ELSE NULL END                                                                                          AS SEAT_NUMBER,
       1                                                                                                          AS SEAT_COUNT,
       1                                                                                                          AS DATA_ACTIVE,
       now()                                                                                                      AS DATA_ACTIVE_TIME,
       now()                                                                                                      AS SYSTEM_CREATETIME,
       now()                                                                                                      AS SYSTEM_LAST_UPDATETIME,
-- 直接获取航班事实表数据，无需再次标准化
       seg.EN_FIRST_NAME,
       seg.EN_LAST_NAME,
       seg.CN_NAME,
       seg.PASSENGER_TYPE,
       seg.CERT_TYPE,
       seg.CERT_NUMBER,
       seg.PASSENGER_AGE,
       seg.FK_PASSENGER_USER_TID,
       seg.FK_SEG_SEGMENT                                                                                         AS FK_SEAT_SEG,
       seg.AK_CABIN                                                                                               AS AK_CABIN
FROM ODS_TEST.T_ODS_LYGJ_TRACE_EMDINFO temd
         LEFT JOIN DWD_TEST.T_DWD_TICKING_SEG_FACT seg
                   ON CONCAT(temd.ET_NUM, temd.ORIG, REPLACE(temd.FLT_DATE, '-', '')) = seg.PK_ID
WHERE temd.ISSUANCE_CODE = 'A'
  AND temd.ISSUANCE_SUBCODE = '0B5'
  AND temd.EMD_COUPONSTATUS = 'R'
  AND temd.ETL_DATE >= @ETL_DATE;

-- 附加服务_选座退票_航段事实表-选座金额CNY
INSERT INTO DWD_TEST.T_DWD_SEAT_REFUND_FACT
    (PK_ID, SEAT_AMOUNT)
SELECT PK_ID,
       ROUND(
               CASE
                   WHEN rt.FIVE_DAY_RATE IS NULL THEN
                       CASE WHEN tdutf.AK_CURRENCY = 'CNY' THEN tdutf.SEAT_AMOUNT ELSE 0 END
                   ELSE tdutf.SEAT_AMOUNT / rt.FIVE_DAY_RATE
                   END,
               2
       ) AS SEAT_AMOUNT_CNY
FROM DWD_TEST.T_DWD_SEAT_REFUND_FACT AS tdutf
         LEFT JOIN
     (SELECT ee.CURRENCY_CODE,
             ee.FIVE_DAY_RATE
      FROM (SELECT CURRENCY_CODE,
                   MAX(EXCHANGE_MONTH) AS EXCHANGE_MONTH
            FROM ODS_TEST.T_ODS_SCCRM_EXCHANGE_RATE er
            GROUP BY CURRENCY_CODE) rate
               LEFT JOIN ODS_TEST.T_ODS_SCCRM_EXCHANGE_RATE ee
                         ON
                             rate.CURRENCY_CODE = ee.CURRENCY_CODE
                                 AND rate.EXCHANGE_MONTH = ee.EXCHANGE_MONTH) rt
     ON tdutf.AK_CURRENCY = rt.CURRENCY_CODE
WHERE tdutf.AK_CURRENCY IS NOT null
  AND (SYSTEM_CREATETIME >= CONCAT(@ETL_DATE, ' 00:00:00.000')
    OR SYSTEM_LAST_UPDATETIME >= CONCAT(@ETL_DATE, ' 00:00:00.000'));

-- 事务提交
commit;

