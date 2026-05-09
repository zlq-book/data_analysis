-- 国航系里程累积明细表 - ODS到DWD层数据转换脚本
-- 数据源：常旅客-国航日报汇总表-sheet4（国航系里程累积明细表）
-- 目标表：T_DWD_CLK_MILEAGE_ACCUMULATION_FACT

-- 1. 设置会话变量 (在执行 INSERT 语句前执行)
--SET batch_size = 4096;
--SET @ETL_DATE = '2025-01-01';

-- 2. 插入或更新数据到DWD层
-- 说明：根据日期和航司进行全量更新，如已有相同日期和航司的数据则执行更新操作
INSERT INTO DWD_TEST.T_DWD_CLK_MILEAGE_ACCUMULATION_FACT
(
    PK_ID,
    DATE,
    AIRLINE_NAME,
    FLIGHT_MILEAGE,
    NON_FLIGHT_MILEAGE,
    PROMOTION_MILEAGE,
    EXTRA_MILEAGE,
    TOTAL_MILEAGE,
    SYSTEM_CREATETIME,
    SYSTEM_LAST_UPDATETIME
)
SELECT
    CONCAT(CAST(ods.`DATE` AS STRING), '_', ods.AIRLINE) AS PK_ID,                  -- 主键：由日期和航司拼接生成
    ods.`DATE` AS DATE,                                                              -- 日期
    ods.AIRLINE AS AIRLINE_NAME,                                                     -- 航司名称
    ods.FLIGHT AS FLIGHT_MILEAGE,                                                    -- 飞行里程
    ods.NON_FLIGHT AS NON_FLIGHT_MILEAGE,                                            -- 非航里程
    ods.PROMOTION AS PROMOTION_MILEAGE,                                              -- 促销里程
    ods.EXTRA AS EXTRA_MILEAGE,                                                      -- 额外里程
    ods.TOTAL AS TOTAL_MILEAGE,                                                      -- 总里程
    COALESCE(dwd.SYSTEM_CREATETIME, NOW()) AS SYSTEM_CREATETIME,                     -- 首次创建时间保持不变，新数据使用当前时间
    NOW() AS SYSTEM_LAST_UPDATETIME                                                  -- 最后更新时间为当前时间
FROM ODS_TEST.T_ODS_CLK_MILEAGE_ACCUMULATION AS ods
LEFT JOIN DWD_TEST.T_DWD_CLK_MILEAGE_ACCUMULATION_FACT AS dwd
    ON CONCAT(CAST(ods.`DATE` AS STRING), '_', ods.AIRLINE) = dwd.PK_ID
WHERE ods.ETL_DATE = @ETL_DATE                                                       -- 按ETL日期过滤增量数据
    OR (ods.ETL_CREATE_TIME >= CONCAT(@ETL_DATE, ' 00:00:00.000')                  -- 或按创建时间过滤
        OR ods.ETL_UPDATE_TIME >= CONCAT(@ETL_DATE, ' 00:00:00.000'));              -- 或按更新时间过滤

-- 事务提交
commit;
