-- 国航系贵宾会员总量表 - ODS到DWD层数据转换脚本
-- 数据源：常旅客-国航日报汇总表-sheet1（国航系贵宾会员总量表）
-- 目标表：T_DWD_CLK_VIP_MEMBER_TOTAL_FACT

-- 1. 设置会话变量 (在执行 INSERT 语句前执行)
--SET batch_size = 4096;
--SET @ETL_DATE = '2025-01-01';

-- 2. 插入或更新数据到DWD层
-- 说明：根据日期进行全量更新，如已有相同日期的数据则执行更新操作
INSERT INTO DWD_PROD.T_DWD_CLK_VIP_MEMBER_TOTAL_FACT
(
    PK_ID,
    DATE,
    NAME,
    FINAL_WHITE,
    PLATINUM,
    GOLD,
    SILVER,
    TOTAL_BY_ATTRIBUTES,
    SYSTEM_CREATETIME,
    SYSTEM_LAST_UPDATETIME
)
SELECT
    CONCAT(CAST(ods.`DATE` AS STRING), '_', ods.NAME) AS PK_ID,                    -- 主键：由日期和名称拼接生成
    ods.`DATE` AS DATE,                                                              -- 日期
    ods.NAME AS NAME,                                                                -- 名称
    ods.FINAL_WHITE AS FINAL_WHITE,                                                  -- 终白
    ods.PLATINUM AS PLATINUM,                                                        -- 白金
    ods.GOLD AS GOLD,                                                                -- 金
    ods.SILVER AS SILVER,                                                            -- 银
    ods.TOTAL_BY_ATTRIBUTES AS TOTAL_BY_ATTRIBUTES,                                  -- 各属性总量
    COALESCE(dwd.SYSTEM_CREATETIME, NOW()) AS SYSTEM_CREATETIME,                     -- 首次创建时间保持不变，新数据使用当前时间
    NOW() AS SYSTEM_LAST_UPDATETIME                                                  -- 最后更新时间为当前时间
FROM ODS_PROD.T_ODS_CLK_VIP_MEMBER_TOTAL AS ods
LEFT JOIN DWD_PROD.T_DWD_CLK_VIP_MEMBER_TOTAL_FACT AS dwd
    ON CONCAT(CAST(ods.`DATE` AS STRING), '_', ods.NAME) = dwd.PK_ID
WHERE ods.ETL_DATE = @ETL_DATE                                                       -- 按ETL日期过滤增量数据
    OR (ods.ETL_CREATE_TIME >= CONCAT(@ETL_DATE, ' 00:00:00.000')                  -- 或按创建时间过滤
        OR ods.ETL_UPDATE_TIME >= CONCAT(@ETL_DATE, ' 00:00:00.000'));              -- 或按更新时间过滤

-- 事务提交
commit;
