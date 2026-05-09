-- 是否为自己订票
-- 成行-航段级
INSERT INTO DWD_TEST.T_DWD_DEPART_SEG_FACT (PK_ID,SELF_BOOKING)
SELECT
    depart_seg.PK_ID as PK_ID,
    ticking_seg.SELF_BOOKING as SELF_BOOKING
FROM
    DWD_TEST.T_DWD_DEPART_SEG_FACT depart_seg
        INNER JOIN
    DWD_TEST.T_DWD_TICKING_SEG_FACT ticking_seg
    ON
        depart_seg.PK_ID = ticking_seg.PK_ID
WHERE depart_seg.DATA_ACTIVE_TIME  BETWEEN CONCAT(@ETL_DATE,' 00:00:00.000') AND CONCAT(@ETL_DATE,' 23:59:59.999');
-- 是否为自己订票
-- 机票退票航段级事实表
INSERT INTO DWD_TEST.T_DWD_REFUND_SEG_FACT (PK_ID,SELF_BOOKING)
SELECT
    refund_seg.PK_ID as PK_ID,
    ticking_seg.SELF_BOOKING as SELF_BOOKING
FROM
    DWD_TEST.T_DWD_REFUND_SEG_FACT refund_seg
        INNER JOIN
    DWD_TEST.T_DWD_TICKING_SEG_FACT ticking_seg
    ON
        refund_seg.PK_ID = ticking_seg.PK_ID
WHERE refund_seg.DATA_ACTIVE_TIME  BETWEEN CONCAT(@ETL_DATE,' 00:00:00.000') AND CONCAT(@ETL_DATE,' 23:59:59.999');
--事务提交
commit;
