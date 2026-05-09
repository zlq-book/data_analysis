-- 座位偏好
INSERT INTO DWD_PROD.T_DWD_CHECKIN_SEG_FACT (PK_ID,SEAT_FEATURE)
SELECT
    air_type.PK_ID as PK_ID,
    CASE
        WHEN air_type.AIRTYPE = 'Boeing 737-85N' THEN
            CASE
                WHEN air_type.AK_SEAT_ASSIGN IN ('13B','13E','14B','14E') THEN '紧急出口座位'
                WHEN air_type.AK_SEAT_ASSIGN IN ('13A','13F','14A','14F') THEN '紧急出口座位-靠窗'
                WHEN air_type.AK_SEAT_ASSIGN IN ('13C','13D','14C','14D') THEN '紧急出口座位-靠过道'
                WHEN air_type.AK_SEAT_ASSIGN IN ('6B','6E','7B','7E','8B','8E','9B','9E','10B','10E') THEN '前排座位'
                WHEN air_type.AK_SEAT_ASSIGN IN ('6A','6F','7A','7F','8A','8F','9A','9F','10A','10F') THEN '前排座位-靠窗'
                WHEN air_type.AK_SEAT_ASSIGN IN ('6C','6D','7C','7D','8C','8D','9C','9D','10C','10D') THEN '前排座位-靠过道'
                ELSE NULL
                END
        WHEN air_type.AIRTYPE = 'Boeing 737 Max' THEN
            CASE
                WHEN air_type.AK_SEAT_ASSIGN IN ('13B','13E','14B','14E') THEN '紧急出口座位'
                WHEN air_type.AK_SEAT_ASSIGN IN ('13A','13F','14A','14F') THEN '紧急出口座位-靠窗'
                WHEN air_type.AK_SEAT_ASSIGN IN ('13C','13D','14C','14D') THEN '紧急出口座位-靠过道'
                WHEN air_type.AK_SEAT_ASSIGN IN ('6B','6E','7B','7E','8B','8E','9B','9E','10B','10E') THEN '前排座位'
                WHEN air_type.AK_SEAT_ASSIGN IN ('6A','6F','7A','7F','8A','8F','9A','9F','10A','10F') THEN '前排座位-靠窗'
                WHEN air_type.AK_SEAT_ASSIGN IN ('6C','6D','7C','7D','8C','8D','9C','9D','10C','10D') THEN '前排座位-靠过道'
                ELSE NULL
                END
        WHEN air_type.AIRTYPE = 'Boeing 737-8FH' THEN
            CASE
                WHEN air_type.AK_SEAT_ASSIGN IN ('13B','13E','14B','14E','15B','15E') THEN '紧急出口座位'
                WHEN air_type.AK_SEAT_ASSIGN IN ('13A','13F','15A','15F') THEN '紧急出口座位-靠窗'
                WHEN air_type.AK_SEAT_ASSIGN IN ('13C','13D','14C','14D','15C','15D') THEN '紧急出口座位-靠过道'
                WHEN air_type.AK_SEAT_ASSIGN IN ('4B','4E','5B','5E','6B','6E','7B','7E','8B','8E','9B','9E','10B','10E') THEN '前排座位'
                WHEN air_type.AK_SEAT_ASSIGN IN ('4A','4F','5A','5F','6A','6F','7A','7F','8A','8F','9A','9F','10A','10F') THEN '前排座位-靠窗'
                WHEN air_type.AK_SEAT_ASSIGN IN ('4C','4D','5C','5D','6C','6D','7C','7D','8C','8D','9C','9D','10C','10D') THEN '前排座位-靠过道'
                WHEN air_type.AK_SEAT_ASSIGN IN ('3B','3E') THEN '大空间座位'
                WHEN air_type.AK_SEAT_ASSIGN IN ('3A','3F') THEN '大空间座位-靠窗'
                WHEN air_type.AK_SEAT_ASSIGN IN ('3C','3D') THEN '大空间座位-靠过道'
                ELSE NULL
                END
        WHEN air_type.AIRTYPE = 'B737' THEN
            CASE
                WHEN air_type.AK_SEAT_ASSIGN IN ('12B','12E') THEN '紧急出口座位'
                WHEN air_type.AK_SEAT_ASSIGN IN ('12A','12F') THEN '紧急出口座位-靠窗'
                WHEN air_type.AK_SEAT_ASSIGN IN ('12C','12D') THEN '紧急出口座位-靠过道'
                WHEN air_type.AK_SEAT_ASSIGN IN ('1E','2D','2E','3D','3E','4B','4E','5B','5E','6B','6E','7B','7E','8B','8E','9B','9E','10B','10E') THEN '前排座位'
                WHEN air_type.AK_SEAT_ASSIGN IN ('2A','2F','3A','3F','4A','4F','5A','5F','6A','6F','7A','7F','8A','8F','9A','9F','10A','10F') THEN '前排座位-靠窗'
                WHEN air_type.AK_SEAT_ASSIGN IN ('1D','2C','2D','3C','3D','4C','4D','5C','5D','6C','6D','7C','7D','8C','8D','9C','9D','10C','10D') THEN '前排座位-靠过道'

                ELSE NULL
                END
        ELSE NULL
        END AS SEAT_FEATURE
FROM (
         SELECT
             f.PK_ID,
             d.AIRTYPE,
             f.AK_SEAT_ASSIGN
         FROM
             DIM_PROD.T_DIM_SEG_DIM d
                 INNER JOIN
             DWD_PROD.T_DWD_CHECKIN_SEG_FACT f
             ON
                 d.SEGMENT_KEY = f.FK_CHECKIN_SEG
         WHERE f.FK_SEG_DATE = @ETL_DATE
     ) AS air_type;
--事务提交
commit;