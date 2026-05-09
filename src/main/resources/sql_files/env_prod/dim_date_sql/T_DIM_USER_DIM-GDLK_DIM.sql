-- 鲁雁管家信息中的各个喜好字段
insert into DIM_PROD.T_DIM_USER_DIM (
    PK_ID,
    FOOD_PREFERENCE,
    SEAT_PREFERENCE,
    TEMP_FOOD_PREFERENCE,
    TEMP_SEAT_PREFERENCE,
    BEVERAGE_PREFERENCE,
    LONG_TERM_SEAT_PREFERENCE,
    FIRST_CLASS_LOUNGE_PREFERENCE,
    UPDATE_TIME  -- 新增更新时间字段
)
select
    T1.PK_ID,
    -- 保留所有字段的「拆分→去重→重组」业务逻辑
    ARRAY_JOIN(ARRAY_DISTINCT(SPLIT_BY_STRING(T2.FOOD_PREFERENCE, '/')), '/') as FOOD_PREFERENCE,
    ARRAY_JOIN(ARRAY_DISTINCT(SPLIT_BY_STRING(T2.SEAT_PREFERENCE, '/')), '/') as SEAT_PREFERENCE,
    ARRAY_JOIN(ARRAY_DISTINCT(SPLIT_BY_STRING(T2.TEMP_FOOD_PREFERENCE, '/')), '/') as TEMP_FOOD_PREFERENCE,
    ARRAY_JOIN(ARRAY_DISTINCT(SPLIT_BY_STRING(T2.TEMP_SEAT_PREFERENCE, '/')), '/') as TEMP_SEAT_PREFERENCE,
    ARRAY_JOIN(ARRAY_DISTINCT(SPLIT_BY_STRING(T2.BEVERAGE_PREFERENCE, '/')), '/') as BEVERAGE_PREFERENCE,
    ARRAY_JOIN(ARRAY_DISTINCT(SPLIT_BY_STRING(T2.LONG_TERM_SEAT_PREFERENCE, '/')), '/') as LONG_TERM_SEAT_PREFERENCE,
    ARRAY_JOIN(ARRAY_DISTINCT(SPLIT_BY_STRING(T2.FIRST_CLASS_LOUNGE_PREFERENCE, '/')), '/') as FIRST_CLASS_LOUNGE_PREFERENCE,
    current_timestamp() as UPDATE_TIME  -- Doris 标准时间函数
from
    (
    -- 子查询T1：仅查询自身表的PK_ID，不引用任何T2字段（保持独立，完成筛选逻辑即可）
    select
        PK_ID
    FROM DWQ_PROD.T_DIM_USER_DIM_VIEW where
         PK_ID IN ( select FK_USER_TID
         from DWQ_PROD.T_DIM_GDLK_DIM
         where (CREATE_TIME >= concat(@ETL_DATE, ' 00:00:00.000') AND CREATE_TIME < CONCAT(@ETL_DATE, ' 23:59:59.999'))
            OR (UPDATE_TIME >= CONCAT(@ETL_DATE, ' 00:00:00.000') AND UPDATE_TIME < CONCAT(@ETL_DATE, ' 23:59:59.999')))
    ) T1
    JOIN (
        select
            FK_USER_TID,
            -- 优化：IF(字段 is not null, 字段, null) 可简化，GROUP_CONCAT本身会忽略null值
            GROUP_CONCAT(FOOD_PREFERENCE, '/') as FOOD_PREFERENCE,
            GROUP_CONCAT(SEAT_PREFERENCE, '/') as SEAT_PREFERENCE,
            GROUP_CONCAT(TEMP_FOOD_PREFERENCE, '/') as TEMP_FOOD_PREFERENCE,
            GROUP_CONCAT(TEMP_SEAT_PREFERENCE, '/') as TEMP_SEAT_PREFERENCE,
            GROUP_CONCAT(BEVERAGE_PREFERENCE, '/') as BEVERAGE_PREFERENCE,
            GROUP_CONCAT(LONG_TERM_SEAT_PREFERENCE, '/') as LONG_TERM_SEAT_PREFERENCE,
            GROUP_CONCAT(FIRST_CLASS_LOUNGE_PREFERENCE, '/') as FIRST_CLASS_LOUNGE_PREFERENCE
        from DWQ_PROD.T_DIM_GDLK_DIM
        group by FK_USER_TID
    ) T2 ON T1.PK_ID = T2.FK_USER_TID;

-- 高端旅客标识/等级信息（新增UPDATE_TIME）
insert into DIM_PROD.T_DIM_USER_DIM (
    PK_ID,
    VIP_FLAG,
    CIP_FLAG,
    VVIP_FLAG,
    HIGH_TRAVELER_TIER,
    TIER_PRESTIGE_EXPIREDATE,
    TIER_HONOR_EXPIREDATE,
    UPDATE_TIME  -- 新增更新时间字段
)
select
  agg.PK_ID,
  agg.VIP_FLAG,
  agg.CIP_FLAG,
  agg.VVIP_FLAG,
  -- 将数字还原为原始分层名称
  case agg.min_tier_code
    when 0 then '至尊'
    when 1 then '荣耀'
    when 2 then '中端I'
    when 3 then '中端II'
    when 4 then '中端III'
    when 5 then '中端IV'
  end as HIGH_TRAVELER_TIER,
  agg.TIER_PRESTIGE_EXPIREDATE,
  agg.TIER_HONOR_EXPIREDATE,
  current_timestamp() as UPDATE_TIME  -- Doris 标准时间函数
from (
  -- 子查询1：先给分层打优先级编码，再聚合取最高优先级
  select
    t1.PK_ID,
    max(t2.VIP_FLAG) as VIP_FLAG,
    max(t2.CIP_FLAG) as CIP_FLAG,
    max(t2.VVIP_FLAG) as VVIP_FLAG,
    -- 核心：映射分层为数字，MIN取最小值=最高优先级
    min(case t2.HIGH_TRAVELER_TIER
        when '至尊' then 0
        when '荣耀' then 1
        when '中端I' then 2
        when '中端II' then 3
        when '中端III' then 4
        when '中端IV' then 5
        else 99  -- 兜底非标准分层
      end) as min_tier_code,
    -- 聚合所有至尊分层的有效期（去重取最大值，避免多记录）
    max(case when t2.HIGH_TRAVELER_TIER = '至尊' then t2.TIER_PRESTIGE_EXPIREDATE end) as TIER_PRESTIGE_EXPIREDATE,
    -- 聚合所有荣耀分层的有效期（去重取最大值）
    max(case when t2.HIGH_TRAVELER_TIER = '荣耀' then t2.TIER_HONOR_EXPIREDATE end) as TIER_HONOR_EXPIREDATE
from
  (
   select *
   from DWQ_PROD.T_DIM_USER_DIM_VIEW
    where
     PK_ID in ( select FK_USER_TID
     from DWQ_PROD.T_DIM_GDLK_DIM
     where (CREATE_TIME >= concat(@ETL_DATE, ' 00:00:00.000') AND CREATE_TIME < CONCAT(@ETL_DATE, ' 23:59:59.999'))
        OR (UPDATE_TIME >= CONCAT(@ETL_DATE, ' 00:00:00.000') AND UPDATE_TIME < CONCAT(@ETL_DATE, ' 23:59:59.999')))
  ) t1
INNER JOIN
  (select *
   from DWQ_PROD.T_DIM_GDLK_DIM
  ) t2
  ON t1.PK_ID = t2.FK_USER_TID
GROUP BY t1.PK_ID
) agg;


INSERT
	INTO
	DIM_PROD.T_DIM_USER_DIM (
    PK_ID,
	HIGH_TRAVELER_DS,
	UPDATE_TIME  -- 新增更新时间字段
)
select
	PK_ID,
	HIGH_TRAVELER_DS,
	current_timestamp() as UPDATE_TIME  -- Doris 标准时间函数
from
	(
	select
		t1.PK_ID,
		t2.HIGH_TRAVELER_DS,
		row_number() over (
            partition by t1.PK_ID
	order by
		t2.HIGH_TRAVELER_TIER asc
        ) as rn
	from
		(
		select PK_ID from DWQ_PROD.T_DIM_USER_DIM_VIEW
		WHERE
			PK_ID IN (
			SELECT
				FK_USER_TID
			FROM
				DWQ_PROD.T_DIM_GDLK_DIM
			WHERE
				(CREATE_TIME >= CONCAT(@ETL_DATE, ' 00:00:00.000') AND CREATE_TIME < CONCAT(@ETL_DATE, ' 23:59:59.999'))
				OR (UPDATE_TIME >= CONCAT(@ETL_DATE, ' 00:00:00.000') AND UPDATE_TIME < CONCAT(@ETL_DATE, ' 23:59:59.999')))
        ) t1
	INNER JOIN
        (
		select
			FK_USER_TID,
			HIGH_TRAVELER_DS,
			HIGH_TRAVELER_TIER
		from
			DWQ_PROD.T_DIM_GDLK_DIM
        ) t2
        on
		t1.PK_ID = t2.FK_USER_TID
) ranked_result
where
	rn = 1;