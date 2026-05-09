-- 停留天数
INSERT INTO DWD_TEST.T_DWD_TICKING_SEG_FACT (PK_ID,STOPOVER_DAY)
SELECT
    PK_ID,
    CASE
        WHEN seg_rank < total_seg THEN
            COALESCE(DATEDIFF(next_depart_date, ARRIVAL_DATE), NULL)
        ELSE NULL
        END AS STOPOVER_DAY
FROM (
         SELECT
             t.PK_ID,
             s.ARRIVAL_DATE ,
             ROW_NUMBER() OVER (PARTITION BY t.AK_TICKET_NUMBER ORDER BY t.SEGMENT_SEQ) AS seg_rank,
             MAX(t.SEGMENT_SEQ) OVER (PARTITION BY t.AK_TICKET_NUMBER) AS total_seg,
             LEAD(t.FK_SEG_DATE, 1, null) OVER (PARTITION BY t.AK_TICKET_NUMBER ORDER BY t.SEGMENT_SEQ) AS next_depart_date
         FROM
             DWD_TEST.T_DWD_TICKING_SEG_FACT t
                 INNER JOIN
             DIM_TEST.T_DIM_SEG_DIM s
             ON
                     t.FK_SEG_SEGMENT = s.SEGMENT_KEY
         WHERE t.DATA_ACTIVE_TIME  BETWEEN CONCAT(@ETL_DATE,' 00:00:00.000') AND CONCAT(@ETL_DATE,' 23:59:59.999')
           AND t.FK_SEG_SEGMENT IS NOT NULL
     ) temp;
-- （成行航段级）
INSERT INTO DWD_TEST.T_DWD_DEPART_SEG_FACT(PK_ID,STOPOVER_DAY)
SELECT
    flight_seg.PK_ID,
    ticket_seg.STOPOVER_DAY
FROM
    DWD_TEST.T_DWD_DEPART_SEG_FACT flight_seg
        INNER JOIN
    DWD_TEST.T_DWD_TICKING_SEG_FACT ticket_seg
    ON
        flight_seg.PK_ID = ticket_seg.PK_ID
WHERE flight_seg.DATA_ACTIVE_TIME  BETWEEN CONCAT(@ETL_DATE,' 00:00:00.000') AND CONCAT(@ETL_DATE,' 23:59:59.999');
--事务提交
commit;
