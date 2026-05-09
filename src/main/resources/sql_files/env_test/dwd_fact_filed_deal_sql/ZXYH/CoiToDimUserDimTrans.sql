-- 密钥 (Base64格式)
-- SET @SM4_KEY = 'VFlLSFNUc200MjAyNX5+fg==';
-- =========================================================
-- 1. 写入主表 (T_DIM_USER_DIM)
-- =========================================================
INSERT INTO DIM_TEST.T_DIM_USER_DIM (
    PK_ID,          -- 主键
    WECHAT_ID,      -- 微信ID
    ALIPAY_ID,      -- 支付宝ID
    DOUYIN_ID,      -- 抖音ID
    CREATE_TIME,    -- 创建时间
    UPDATE_TIME     -- 更新时间
)
SELECT
    -- 【主键生成】
    -- 对应 Java 中的 TransUtils.zxyhPrepareHandle -> 计算 TID
    -- 数据来源是 b 表 (CUSTOMER) 的 CUSTOMER_ID
    TO_BASE64(SM4_ENCRYPT(b.CUSTOMER_ID, FROM_BASE64(@SM4_KEY))) AS PK_ID,
    -- 【微信ID】(Java逻辑: CHANNEL_TYPE = '1')
    CASE WHEN a.CHANNEL_TYPE = '1' THEN a.UNION_ID ELSE NULL END AS WECHAT_ID,
    -- 【支付宝ID】(Java逻辑: CHANNEL_TYPE = '4')
    CASE WHEN a.CHANNEL_TYPE = '4' THEN a.UNION_ID ELSE NULL END AS ALIPAY_ID,
    -- 【抖音ID】(Java逻辑: CHANNEL_TYPE = '10')
    CASE WHEN a.CHANNEL_TYPE = '10' THEN a.UNION_ID ELSE NULL END AS DOUYIN_ID,
    NOW() AS CREATE_TIME,
    NOW() AS UPDATE_TIME
FROM ODS_TEST.T_ODS_ZXYH_CRM_CUSTOMER_OPEN_INFO a
INNER JOIN ODS_TEST.T_ODS_ZXYH_CRM_CUSTOMER b
    ON a.CUSTOMER_ID = b.CUSTOMER_ID
WHERE a.ETL_DATE = @ETL_DATE AND a.CHANNEL_TYPE IN ('1','4','10');


-- =========================================================
-- 2. 写入 SUMMARY 概要表 (T_DIM_USER_DIM_SUMMARY)
-- =========================================================
-- 业务逻辑：记录对应渠道 ID 的来源为 'ZXYH'
INSERT INTO DIM_TEST.T_DIM_USER_DIM_SUMMARY (
    PK_ID,                  -- 主键
    WECHAT_ID_SOURCE,       -- 微信ID来源
    WECHAT_ID_UPDATETIME,   -- 微信ID更新时间
    ALIPAY_ID_SOURCE,       -- 支付宝ID来源
    ALIPAY_ID_UPDATETIME,   -- 支付宝ID更新时间
    DOUYIN_ID_SOURCE,       -- 抖音ID来源
    DOUYIN_ID_UPDATETIME    -- 抖音ID更新时间
)
SELECT
    -- 【主键生成】
    TO_BASE64(SM4_ENCRYPT(b.CUSTOMER_ID, FROM_BASE64(@SM4_KEY))) AS PK_ID,
    -- 【微信来源记录】
    CASE WHEN a.CHANNEL_TYPE = '1' THEN 'ZXYH' ELSE NULL END,
    CASE WHEN a.CHANNEL_TYPE = '1' THEN NOW() ELSE NULL END,
    -- 【支付宝来源记录】
    CASE WHEN a.CHANNEL_TYPE = '4' THEN 'ZXYH' ELSE NULL END,
    CASE WHEN a.CHANNEL_TYPE = '4' THEN NOW() ELSE NULL END,
    -- 【抖音来源记录】
    CASE WHEN a.CHANNEL_TYPE = '10' THEN 'ZXYH' ELSE NULL END,
    CASE WHEN a.CHANNEL_TYPE = '10' THEN NOW() ELSE NULL END
FROM ODS_TEST.T_ODS_ZXYH_CRM_CUSTOMER_OPEN_INFO a
INNER JOIN ODS_TEST.T_ODS_ZXYH_CRM_CUSTOMER b
    ON a.CUSTOMER_ID = b.CUSTOMER_ID
WHERE a.ETL_DATE = @ETL_DATE AND a.CHANNEL_TYPE IN ('1','4','10');