-- 国航系会员发展量表 - ODS到DWD层数据转换脚本
-- 数据源：常旅客-国航日报汇总表-sheet3（国航系会员发展量表）
-- 目标表：T_DWD_CLK_MEMBER_DEVELOPMENT_FACT

-- 1. 设置会话变量 (在执行 INSERT 语句前执行)
--SET batch_size = 4096;
--SET @ETL_DATE = '2025-01-01';

-- 2. 插入或更新数据到DWD层
-- 说明：根据日期进行全量更新，如已有相同日期的数据则执行更新操作
INSERT INTO DWD_TEST.T_DWD_CLK_MEMBER_DEVELOPMENT_FACT
(
    PK_ID,
    DATE,
    AIR_CHINA,
    SHENZHEN_AIRLINES,
    SHANDONG_AIRLINES,
    AIR_MACAU,
    TOTAL_DEVELOPMENT,
    DAILY_MEMBER_TOTAL,
    SYSTEM_CREATETIME,
    SYSTEM_LAST_UPDATETIME
)
SELECT
    CAST(ods.`DATE` AS STRING) AS PK_ID,                                            -- 主键：日期
    ods.`DATE` AS DATE,                                                              -- 日期
    ods.CHINA AS AIR_CHINA,                                                          -- 国航
    ods.SHENZHEN AS SHENZHEN_AIRLINES,                                               -- 深航
    ods.SHANDONG AS SHANDONG_AIRLINES,                                               -- 山航
    ods.MACAO AS AIR_MACAU,                                                          -- 澳航
    ods.TOTAL_DEVELOPMENT AS TOTAL_DEVELOPMENT,                                      -- 总发展量
    ods.DAILY_MEMBER_TOTAL AS DAILY_MEMBER_TOTAL,                                    -- 当日会员总量
    COALESCE(dwd.SYSTEM_CREATETIME, NOW()) AS SYSTEM_CREATETIME,                     -- 首次创建时间保持不变，新数据使用当前时间
    NOW() AS SYSTEM_LAST_UPDATETIME                                                  -- 最后更新时间为当前时间
FROM ODS_TEST.T_ODS_CLK_MEMBER_DEVELOPMENT AS ods
LEFT JOIN DWD_TEST.T_DWD_CLK_MEMBER_DEVELOPMENT_FACT AS dwd
    ON CAST(ods.`DATE` AS STRING) = dwd.PK_ID
WHERE ods.ETL_DATE = @ETL_DATE                                                       -- 按ETL日期过滤增量数据
    OR (ods.ETL_CREATE_TIME >= CONCAT(@ETL_DATE, ' 00:00:00.000')                  -- 或按创建时间过滤
        OR ods.ETL_UPDATE_TIME >= CONCAT(@ETL_DATE, ' 00:00:00.000'));              -- 或按更新时间过滤

-- 事务提交
commit;
