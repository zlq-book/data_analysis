-- 是否是团队票
INSERT INTO DWD_TEST.T_DWD_DEPART_SEG_FACT (PK_ID,AK_TEAMMARK)
SELECT
    depart_seg.PK_ID as PK_ID,
    ticking_seg.IS_TEAM_TICKET as AK_TEAMMARK
FROM
    DWD_TEST.T_DWD_DEPART_SEG_FACT depart_seg
        INNER JOIN
    DWD_TEST.T_DWD_TICKING_SEG_FACT ticking_seg
    ON
        depart_seg.PK_ID = ticking_seg.PK_ID
WHERE depart_seg.DATA_ACTIVE_TIME  BETWEEN CONCAT(@ETL_DATE,' 00:00:00.000') AND CONCAT(@ETL_DATE,' 23:59:59.999');
--事务提交
commit;
