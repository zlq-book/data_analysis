-- 机票-出票-航段级事实表
-- 鲁雁管家-写入航段序号
INSERT INTO DWD_PROD.T_DWD_TICKING_SEG_FACT
(PK_ID, SEGMENT_SEQ)
SELECT PK_ID,
       ROW_NUMBER() OVER (
           PARTITION BY AK_TICKET_NUMBER
           ORDER BY FK_SEG_DATE ASC,
               FK_SEG_TIME ASC
           ) AS SEGMENT_SEQ
FROM (
         SELECT *,
                MAX(CASE WHEN FK_SEG_DATE IS NULL OR FK_SEG_TIME IS NULL THEN 1 ELSE 0 END)
                    OVER (PARTITION BY AK_TICKET_NUMBER) as has_null_in_group
         FROM DWD_PROD.T_DWD_TICKING_SEG_FACT
         WHERE AK_TICKET_NUMBER IS NOT NULL
           AND DATA_ACTIVE = 1
           AND FK_SEG_DATE IS NOT NULL
     ) t
WHERE has_null_in_group = 0;
-- 机票-退票-航段级事实表
-- 航段序号
-- 等鲁雁管家高频数据全部入事实表后，出票航段表也会计算航段序号，
-- 在出票航段表计算完成后，拿退票表里面的每一条数据的主键到出票航段表里面去查询对应主键下（两个表主键相同），
-- 在出票航段表里面“航段序号“字段里面的值写入本表本字段。
-- 只需运行一次
INSERT INTO DWD_PROD.T_DWD_REFUND_SEG_FACT
(PK_ID, SEG_NO)
SELECT t1.PK_ID,
       t2.SEGMENT_SEQ AS SEG_NO
FROM DWD_PROD.T_DWD_REFUND_SEG_FACT t1
         left join DWD_PROD.T_DWD_TICKING_SEG_FACT t2
                   on t1.PK_ID = t2.PK_ID
where t2.SEGMENT_SEQ IS NOT NULL
  and t1.DATA_ACTIVE = 1
  and t2.DATA_ACTIVE = 1;

-- 机票-改升换开出票-航段级事实表
-- 鲁雁管家一次性写入-换开前舱位、换开前舱等、换开前起飞日期、换开前起飞时间
-- 只需运行一次
INSERT INTO DWD_PROD.T_DWD_DATECHANGE_SEG_FACT
(PK_ID, OLD_AK_SEGCABIN,OLD_AK_CABIN, OLD_DEPARTURES_DATE, OLD_DEPARTURES_TIME, DATE_CHANGE_TYPE)
SELECT
    datechange.PK_ID,
    seg.AK_SEGCABIN AS OLD_AK_SEGCABIN,
    seg.AK_CABIN AS OLD_AK_CABIN,
    seg.FK_SEG_DATE AS OLD_DEPARTURES_DATE,
    seg.FK_SEG_TIME AS OLD_DEPARTURES_TIME,
    CASE WHEN seg.AK_SEGCABIN != datechange.AK_SEGCABIN THEN 'UP'
         ELSE 'EX'
        END AS DATE_CHANGE_TYPE
FROM DWD_PROD.T_DWD_DATECHANGE_SEG_FACT datechange
         LEFT JOIN DWD_PROD.T_DWD_TICKING_SEG_FACT seg ON (
            seg.AK_TICKET_NUMBER = datechange.ORI_TIK_NUM
        AND seg.FK_DEPAIRPORT = datechange.FK_DEPAIRPORT
        AND seg.FK_ARRIAIRPORT = datechange.FK_ARRIAIRPORT
        AND seg.DATA_ACTIVE = 1
    )
WHERE datechange.FK_EXCHANGE_DATE <= '2025-12-31'
  and seg.DATA_ACTIVE = 1
  and datechange.DATA_ACTIVE = 1
  AND seg.AK_SEGCABIN IS NOT NULL;


