-- 以下三个参数在本地调试时需手动设置
-- SET batch_size = 4096;
-- SET @ETL_DATE='2017-04-12';

-- 1、成行-航段级事实表
-- 是否为返乡段
-- "以下情况为是，否则为否
-- 1、如果乘机人证件类型为=身份证，并且 乘机人证件号码对应的省份（通过《身份证号-省份+地级市》）与到达机场对应的省份（航程维表：省）相同。
-- 2、并且起飞机场（机场维表：省）和到达机场（机场维表：省）不在一个省份。"
INSERT INTO DWD_PROD.T_DWD_DEPART_SEG_FACT
    (PK_ID, HOMECOMING_SEG)
SELECT t.PK_ID,
       CASE
           WHEN t.CERT_TYPE  = 'NI' -- 身份证类型
               AND EXISTS(
                        SELECT 1
                        FROM DIM_PROD.T_DIM_PROVINCE_CITY_DIM pc
                        WHERE pc.ID = SUBSTRING(
                                SM4_DECRYPT(FROM_BASE64(t.CERT_NUMBER), FROM_BASE64(@SM4_KEY)), 1,
                                4) -- SM4解密后取前4位
                          AND pc.PROVINCE = addr_arr.PROVINCE -- 与到达机场省份相同
                    )
               AND addr_dep.PROVINCE != addr_arr.PROVINCE -- 起飞机场与到达机场省份不同
               THEN 1
           ELSE 0
           END AS HOMECOMING_SEG
FROM DWD_PROD.T_DWD_DEPART_SEG_FACT t
         LEFT JOIN DIM_PROD.T_DIM_ADDRESS_DIM addr_dep
                   ON t.FK_DEPAIRPORT = addr_dep.ADDRESS_KEY -- 起飞机场三字码关联
         LEFT JOIN DIM_PROD.T_DIM_ADDRESS_DIM addr_arr
                   ON t.FK_ARRIAIRPORT = addr_arr.ADDRESS_KEY -- 到达机场三字码关联
WHERE t.FK_DEPAIRPORT is not null
    and t.FK_ARRIAIRPORT is not null
    and t.CERT_NUMBER is not null
    and t.CERT_TYPE = 'NI'
    and t.SYSTEM_CREATETIME >= CONCAT(@ETL_DATE, ' 00:00:00.000')
   OR t.SYSTEM_LAST_UPDATETIME >= CONCAT(@ETL_DATE, ' 00:00:00.000');
-- 事务提交





-- 2、取本表的这一条数据的主键，到“出票-航段”事实表里面找对应主键
-- 常客卡号 飞行时是否使用常客卡 常客等级 常客卡航司 常客卡航司
BEGIN;
INSERT INTO DWD_PROD.T_DWD_DEPART_SEG_FACT
(PK_ID,FFRF,FF_LEVEL,FF_AIRLINE,FF_ALLIANCE_LEVEL,IS_USED_EFR)
SELECT t.PK_ID ,t.FFRF ,t.FF_LEVEL ,t.FF_AIRLINE ,t.FF_ALLIANCE_LEVEL ,
CASE
        WHEN t.FFRF IS NULL THEN FALSE
        ELSE TRUE
    END AS IS_USED_EFR
FROM DWD_PROD.T_DWD_TICKING_SEG_FACT t
LEFT JOIN DWD_PROD.T_DWD_DEPART_SEG_FACT m
ON t.PK_ID = m.PK_ID
WHERE m.SYSTEM_CREATETIME IS NOT NULL
and m.SYSTEM_CREATETIME >= CONCAT(@ETL_DATE, ' 00:00:00.000')
OR m.SYSTEM_LAST_UPDATETIME >= CONCAT(@ETL_DATE, ' 00:00:00.000');

-- 事务提交
commit;
