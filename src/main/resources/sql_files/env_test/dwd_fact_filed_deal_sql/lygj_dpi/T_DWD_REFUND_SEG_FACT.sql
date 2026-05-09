-- 1. 设置会话变量 (在执行 INSERT 语句前执行)
--SET batch_size = 4096;
--SET @ETL_DATE = '2025-01-01';
--SET @SM4_KEY = 'VFlLSFNUc200MjAyNX5+fg==';
--婴儿姓名标准化
--SELECT UPPER(TRIM(REGEXP_REPLACE('INF(01JAN)dfdfdCHD', '(?i)^INF\\((\\d{2})?(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\\)\\s*', '')));
--英文姓名标准化
--SELECT UPPER(TRIM(REGEXP_REPLACE('MS Lisa', '(?i)^\s*(CHD|INF|MR|MS|GM|JC|VIP|VVIP|CIP)\s*|\s*(CHD|INF|MR|MS|GM|JC|VIP|VVIP|CIP)\s*$', '')))
--机票退票-航段级事实表-写DWD
INSERT INTO DWD_TEST.T_DWD_REFUND_SEG_FACT
(PK_ID,AK_TIK_NUMBER,FK_DEPAIRPORT,FK_ARRIAIRPORT,TICKETING_DATE,TICKETING_TIME,FK_SEG_DATE,FK_SEG_SEGMENT,FK_SEG_TIME,FK_PASSENGER_USER_TID,EN_LAST_NAME,EN_FIRST_NAME,CN_NAME,CERT_TYPE,CERT_NUMBER,PASSENGER_AGE,AK_CABIN,AK_SEGCABIN,IS_KEY_ACCOUNT,FFRF,FF_LEVEL,FF_AIRLINE,AK_DIMARK,DATA_ACTIVE,DATA_ACTIVE_TIME,SOURCE_LAST_UPDATETIME,SYSTEM_CREATETIME,SYSTEM_LAST_UPDATETIME)
SELECT
    -- 主键：票号+起飞机场+起飞日期（复用改升表主键逻辑，确保唯一性）
    concat(ET_NUM,ORIG,date_format(FLT_DATE,'%Y%m%d')) AS PK_ID,
    -- 退票票号（复用值机表ET_NUM逻辑）
    ET_NUM AS AK_TIK_NUMBER,
    -- 起飞机场/到达机场（复用所有表ORIG/DEST逻辑）
    ORIG AS FK_DEPAIRPORT,
    DEST AS FK_ARRIAIRPORT,
    -- 出票日期时间（复用改升表PRINT_TICKET_TIME处理逻辑）
    date_format(PRINT_TICKET_TIME,'%Y-%m-%d') AS TICKETING_DATE,
    date_format(PRINT_TICKET_TIME,'%T') AS TICKETING_TIME,
    -- 航班起飞日期时间（复用值机表FLT_DATE/DCS_DPTM处理逻辑）
    date_format(FLT_DATE,'%Y-%m-%d') AS FK_SEG_DATE,
    -- 航程（复用预订表/值机表航程拼接逻辑：起飞日期+承运航班+市场航班+起飞机场+到达机场）
    concat(ifnull(date_format(FLT_DATE,'%Y%m%d'),null),FLT_NUM,MC_FLT,ORIG,DEST) AS FK_SEG_SEGMENT,
    concat(DCS_DPTM,':00') AS FK_SEG_TIME,
    -- 乘机人TID（复用所有表CERT_NO作为TID的逻辑）
    CERT_NO AS FK_PASSENGER_USER_TID,
    -- 英文姓名标准化（复用所有表姓名处理逻辑，拆分姓和名）
    SPLIT_BY_STRING(UPPER(TRIM(REGEXP_REPLACE(PSG_NAME_EN, '(?i)^\s*(CHD|INF|MR|MS|GM|JC|VIP|VVIP|CIP)\s*|\s*(CHD|INF|MR|MS|GM|JC|VIP|VVIP|CIP)\s*$', ''))),'/')[1] AS EN_LAST_NAME,
    SPLIT_BY_STRING(UPPER(TRIM(REGEXP_REPLACE(PSG_NAME_EN, '(?i)^\s*(CHD|INF|MR|MS|GM|JC|VIP|VVIP|CIP)\s*|\s*(CHD|INF|MR|MS|GM|JC|VIP|VVIP|CIP)\s*$', ''))),'/')[2] AS EN_FIRST_NAME,
    -- 中文姓名/证件类型/证件号（复用预订表逻辑，关联标准化配置表）
    PSG_NAME_CN AS CN_NAME,
    sid.CFG_VALUE AS CERT_TYPE,
    CERT_NO AS CERT_NUMBER,
    -- 乘机人年龄（复用所有表生日计算逻辑）
    (YEAR(CURDATE()) - YEAR(BIRTHDAY)) -
        CASE
           WHEN MONTH(CURDATE()) * 100 + DAY(CURDATE()) < MONTH(BIRTHDAY) * 100 + DAY(BIRTHDAY)
        THEN 1 ELSE 0 END
    AS PASSENGER_AGE,
    -- 舱等（复用所有表2023-10-29时间节点区分的舱等匹配逻辑）
    CASE
      WHEN to_date(FLT_DATE) < '2023-10-29' THEN  -- 2022.1.1-2023.10.28
        CASE
          WHEN locate(upper(substring(SELL_CLASS, 1, 1)), 'CDPI') > 0 THEN 'BC'
          WHEN locate(upper(substring(SELL_CLASS, 1, 1)), 'WR') > 0 THEN 'PE'
          WHEN locate(upper(substring(SELL_CLASS, 1, 1)), 'YBHLQGVUZMKTSJEAOXN') > 0 THEN 'EC'
          ELSE NULL  -- 处理未匹配舱位
        END
      ELSE  -- 2023.10.29及以后
        CASE
          WHEN locate(upper(substring(SELL_CLASS, 1, 1)), 'JCDRZI') > 0 THEN 'BC'
          WHEN locate(upper(substring(SELL_CLASS, 1, 1)), 'GE') > 0 THEN 'PE'
          WHEN locate(upper(substring(SELL_CLASS, 1, 1)), 'YBMUHQVWSLPNKTAOX') > 0 THEN 'EC'
          ELSE NULL  -- 处理未匹配舱位
        END
    END AS AK_CABIN,
    -- 舱位（复用所有表取SELL_CLASS首字符逻辑）
    SUBSTRING(SELL_CLASS,1,1) AS AK_SEGCABIN,
    -- 大客户标识（复用改升表正则匹配逻辑）
    CASE
        WHEN BIG_CUTNUM REGEXP '^[0-9][A-Za-z0-9]{7}$' THEN true  -- 匹配 CA_PATTERN
        WHEN BIG_CUTNUM REGEXP '^[A-Za-z0-9]{5}$' THEN true  -- 匹配 CA_GLOBAL_PATTERN
        WHEN BIG_CUTNUM REGEXP '^[0-9]{8}$' THEN true  -- 匹配 DIGITAL_PATTERN
        ELSE false
    END AS IS_KEY_ACCOUNT,
    -- 常客卡号加密（复用值机表SM4加密逻辑：截取第2位后加密）
    TO_BASE64(SM4_ENCRYPT(SUBSTRING(SM4_DECRYPT(FROM_BASE64(FFP), FROM_BASE64(@SM4_KEY)),3),FROM_BASE64(@SM4_KEY))) AS FFRF,
    -- 常客等级（复用所有表IS_FFP_JK逻辑）
    IS_FFP_JK AS FF_LEVEL,
    -- 常客航司（复用值机表截取FFP前2位逻辑）
    SUBSTRING(SM4_DECRYPT(FROM_BASE64(FFP), FROM_BASE64(@SM4_KEY)),1,2) AS FF_AIRLINE,
    -- 国内国际标识（复用改升表TICKET_TYPE逻辑）
    TICKET_TYPE AS AK_DIMARK,
    -- 数据启用状态（复用所有表true默认值）
    true AS DATA_ACTIVE,
    -- 数据启用时间/系统时间（复用所有表now()逻辑）
    now() AS DATA_ACTIVE_TIME,
    ETL_UPDATE_TIME AS SOURCE_LAST_UPDATETIME,
    now() AS SYSTEM_CREATETIME,
    now() AS SYSTEM_LAST_UPDATETIME
FROM ODS_TEST.T_ODS_LYGJ_DEPART_PASSENGER_INFO toldpi
-- 关联证件类型标准化配置表（复用所有表关联逻辑）
LEFT JOIN DWD_TEST.STANDARDIZE_FIELD sid ON ID_TYPE=sid.CFG_CODE
    AND sid.CFG_TYPE = 'DOCUMENT_TYPE' AND sid.CFG_TYPE_CHANNEL ='LUYAN_STEWARD'
-- 过滤条件：主键非空+退票状态（COUPON_STATUS='R'）+ETL时间范围（复用所有表过滤逻辑）
WHERE COUPON_STATUS = 'R' AND concat(ET_NUM,ORIG,date_format(FLT_DATE,'%Y%m%d')) IS NOT NULL
  AND (ETL_CREATE_TIME >= CONCAT(@ETL_DATE,' 00:00:00.000') OR ETL_UPDATE_TIME >= CONCAT(@ETL_DATE,' 00:00:00.000'));
--事务提交
--commit;