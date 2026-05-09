-- 停留地三字码(机票出票航段级)
INSERT INTO DWD_TEST.T_DWD_TICKING_SEG_FACT (PK_ID,STOPOVER)
SELECT
    PK_ID,
    CASE
        WHEN total_segs > 1 AND seg_order < total_segs THEN FK_ARRIAIRPORT
        ELSE NULL
        END AS STOPOVER
FROM (
         SELECT
             PK_ID,
             FK_ARRIAIRPORT,
             SEGMENT_SEQ AS seg_order,
             MAX(SEGMENT_SEQ) OVER (PARTITION BY AK_TICKET_NUMBER) AS total_segs
         FROM
             DWD_TEST.T_DWD_TICKING_SEG_FACT
         WHERE DATA_ACTIVE_TIME  BETWEEN CONCAT(@ETL_DATE,' 00:00:00.000') AND CONCAT(@ETL_DATE,' 23:59:59.999')
           AND SEGMENT_SEQ IS NOT NULL

     ) AS sub_query;
-- （成行航段级）
INSERT INTO DWD_TEST.T_DWD_DEPART_SEG_FACT(PK_ID,STOPOVER)
SELECT
    flight_seg.PK_ID,
    ticket_seg.STOPOVER
FROM
    DWD_TEST.T_DWD_DEPART_SEG_FACT flight_seg
        INNER JOIN
    DWD_TEST.T_DWD_TICKING_SEG_FACT ticket_seg
    ON
        flight_seg.PK_ID = ticket_seg.PK_ID
WHERE ticket_seg.STOPOVER is not null
  and flight_seg.DATA_ACTIVE_TIME  BETWEEN CONCAT(@ETL_DATE,' 00:00:00.000') AND CONCAT(@ETL_DATE,' 23:59:59.999');
--事务提交
    commit;
