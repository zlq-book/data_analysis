-- 密钥 (Base64格式)
-- SET @SM4_KEY = 'VFlLSFNUc200MjAyNX5+fg==';
-- 写入主表 (T_DIM_USER_DIM)
INSERT INTO DIM_PROD.T_DIM_USER_DIM (
    PK_ID,                              -- 主键
    IS_OFFICIAL_WEBSITE_NON_REGISTERED, -- 是否官网非注册用户
    CREATE_TIME,                        -- 创建时间
    UPDATE_TIME                         -- 更新时间
)
SELECT
    -- 【主键生成】
    -- 逻辑：解密(DECRYPT) -> 转字符串 -> 清洗非数字(REGEXP) -> 加密(ENCRYPT) -> Base64
    -- 对应 Java 中的 NormalizationUtils + IdMapping
	IFNULL(tds.TID,
    TO_BASE64(
        SM4_ENCRYPT(
            REGEXP_REPLACE(
                CAST(SM4_DECRYPT(FROM_BASE64(a.MOBILEPHONE), FROM_BASE64(@SM4_KEY)) AS STRING),
                '\\D', ''  -- 移除所有非数字字符
            ),
            FROM_BASE64(@SM4_KEY)
        )
    )) AS PK_ID,
    -- 【业务标签】
    1 AS IS_OFFICIAL_WEBSITE_NON_REGISTERED,
    NOW() AS CREATE_TIME,
    NOW() AS UPDATE_TIME
FROM ODS_PROD.T_ODS_ZXYH_CRM_WEB_NO_REGISTERED a
left join DIM_PROD.T_DIM_STRONGID tds
ON SM4_ENCRYPT(
            REGEXP_REPLACE(
                CAST(SM4_DECRYPT(FROM_BASE64(a.MOBILEPHONE), FROM_BASE64(@SM4_KEY)) AS STRING),
                '\\D', ''  -- 移除所有非数字字符
            ),
            FROM_BASE64(@SM4_KEY)
        ) = tds.STRONGID
AND tds.STRONGID_STATUS = 1
WHERE a.MOBILEPHONE IS NOT NULL
AND a.ETL_DATE = @ETL_DATE;


-- =========================================================
-- 2. 写入 SUMMARY 概要表 (T_DIM_USER_DIM_SUMMARY)
-- =========================================================
-- 业务逻辑：记录该标签的来源为 'ZXYH'
INSERT INTO DIM_PROD.T_DIM_USER_DIM_SUMMARY (
    PK_ID,                                          -- 主键
    IS_OFFICIAL_WEBSITE_NON_REGISTERED_SOURCE,      -- 来源字段
    IS_OFFICIAL_WEBSITE_NON_REGISTERED_UPDATETIME   -- 更新时间字段
)
SELECT
    -- 【主键生成】
	IFNULL(tds.TID,
    TO_BASE64(
        SM4_ENCRYPT(
            REGEXP_REPLACE(
                CAST(SM4_DECRYPT(FROM_BASE64(a.MOBILEPHONE), FROM_BASE64(@SM4_KEY)) AS STRING),
                '\\D', ''
            ),
            FROM_BASE64(@SM4_KEY)
        )
    )) AS PK_ID,
    -- 【来源记录】
    'ZXYH' AS IS_OFFICIAL_WEBSITE_NON_REGISTERED_SOURCE,
    NOW() AS IS_OFFICIAL_WEBSITE_NON_REGISTERED_UPDATETIME
FROM ODS_PROD.T_ODS_ZXYH_CRM_WEB_NO_REGISTERED a
left join DIM_PROD.T_DIM_STRONGID tds
ON SM4_ENCRYPT(
            REGEXP_REPLACE(
                CAST(SM4_DECRYPT(FROM_BASE64(a.MOBILEPHONE), FROM_BASE64(@SM4_KEY)) AS STRING),
                '\\D', ''  -- 移除所有非数字字符
            ),
            FROM_BASE64(@SM4_KEY)
        ) = tds.STRONGID
AND tds.STRONGID_STATUS = 1
WHERE a.MOBILEPHONE IS NOT NULL
AND a.ETL_DATE >= @ETL_DATE;