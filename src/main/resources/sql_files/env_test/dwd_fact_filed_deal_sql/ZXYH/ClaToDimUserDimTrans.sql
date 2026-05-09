-- 密钥 (Base64格式)
-- SET @SM4_KEY = 'VFlLSFNUc200MjAyNX5+fg==';
-- 写入主表 (T_DIM_USER_DIM)

INSERT INTO DIM_TEST.T_DIM_USER_DIM (
    PK_ID,          -- 主键
    EMAIL,          -- 邮箱
    CREATE_TIME,    -- 创建时间
    UPDATE_TIME     -- 更新时间
)
SELECT
    -- 【主键生成】
    -- 对应 Java 中的 TransUtils.zxyhPrepareHandle -> 计算 TID
    TO_BASE64(SM4_ENCRYPT(CUSTOMER_ID, FROM_BASE64(@SM4_KEY))) AS PK_ID,
    -- 【邮箱】
    -- Java逻辑: if ("5".equals(ACCOUNT_TYPE)) setEmail(...)
    ACCOUNT_NAME AS EMAIL,
    NOW() AS CREATE_TIME,
    NOW() AS UPDATE_TIME
FROM ODS_TEST.T_ODS_ZXYH_CRM_CUSTOMER_LOGIN_ACCOUNT a
WHERE a.ETL_DATE = @ETL_DATE AND ACCOUNT_TYPE = '5'; -- 只处理类型为5(邮箱)的数据，避免无效更新


-- =========================================================
-- 2. 写入 SUMMARY 概要表 (T_DIM_USER_DIM_SUMMARY)
-- =========================================================
-- 业务逻辑：记录邮箱字段的来源为 'ZXYH'
INSERT INTO DIM_TEST.T_DIM_USER_DIM_SUMMARY (
    PK_ID,              -- 主键
    EMAIL_SOURCE,       -- 邮箱来源 (已核对表结构存在)
    EMAIL_UPDATETIME    -- 邮箱更新时间 (已核对表结构存在)
)
SELECT
    -- 【主键生成】
    TO_BASE64(SM4_ENCRYPT(CUSTOMER_ID, FROM_BASE64(@SM4_KEY))) AS PK_ID,
    -- 【来源记录】
    'ZXYH' AS EMAIL_SOURCE,
    NOW() AS EMAIL_UPDATETIME
FROM ODS_TEST.T_ODS_ZXYH_CRM_CUSTOMER_LOGIN_ACCOUNT a
WHERE a.ETL_DATE = @ETL_DATE AND ACCOUNT_TYPE = '5'; -- 保持一致的过滤条件