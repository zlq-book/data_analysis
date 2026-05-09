-- 提前出票天数（客票级）
INSERT INTO DWD_PROD.T_DWD_TICKING_TIC_FACT (PK_ID,AK_ADVBOOK_DAY)
SELECT
    ticking.PK_ID as PK_ID,
    COALESCE(DATEDIFF(seg.FK_SEG_DATE, ticking.FK_TICKETING_DATE),0) as AK_ADVBOOK_DAY
FROM
    DWD_PROD.T_DWD_TICKING_TIC_FACT ticking
        INNER JOIN (
        SELECT
            AK_TICKET_NUMBER,
            FK_ISSUE_DATE,
            FK_SEG_DATE,
            ROW_NUMBER() OVER (
            PARTITION BY AK_TICKET_NUMBER
            ORDER BY FK_SEG_DATE ASC
        ) AS seg_rank
        FROM
            DWD_PROD.T_DWD_TICKING_SEG_FACT
    ) seg
         ON
             ticking.TICKET_NUMBER = seg.AK_TICKET_NUMBER
             AND ticking.FK_TICKETING_DATE = seg.FK_ISSUE_DATE
WHERE
    seg.seg_rank = 1
    AND  ticking.DATA_ACTIVE_TIME  BETWEEN CONCAT(@ETL_DATE,' 00:00:00.000') AND CONCAT(@ETL_DATE,' 23:59:59.999');
-- 提前出票天数（航段级）
-- 成行-航段级
INSERT INTO DWD_PROD.T_DWD_DEPART_SEG_FACT (PK_ID,AK_ADVBOOK_DAY)
SELECT
    depart_seg.PK_ID as PK_ID,
    ticking_seg.AK_ADVBOOK_DAY as AK_ADVBOOK_DAY
FROM
    DWD_PROD.T_DWD_DEPART_SEG_FACT depart_seg
        INNER JOIN
    DWD_PROD.T_DWD_TICKING_SEG_FACT ticking_seg
    ON
        depart_seg.PK_ID = ticking_seg.PK_ID
    WHERE depart_seg.DATA_ACTIVE_TIME  BETWEEN CONCAT(@ETL_DATE,' 00:00:00.000') AND CONCAT(@ETL_DATE,' 23:59:59.999');
-- 机票退票航段级事实表
INSERT INTO DWD_PROD.T_DWD_REFUND_SEG_FACT (PK_ID,AK_ADVBOOK_DAY)
SELECT
    refund_seg.PK_ID as PK_ID,
    ticking_seg.AK_ADVBOOK_DAY as AK_ADVBOOK_DAY
FROM
    DWD_PROD.T_DWD_REFUND_SEG_FACT refund_seg
        INNER JOIN
    DWD_PROD.T_DWD_TICKING_SEG_FACT ticking_seg
    ON
        ticking_seg.PK_ID = refund_seg.PK_ID
WHERE refund_seg.DATA_ACTIVE_TIME  BETWEEN CONCAT(@ETL_DATE,' 00:00:00.000') AND CONCAT(@ETL_DATE,' 23:59:59.999');
--事务提交
 commit;