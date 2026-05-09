package com.travelsky.dataplatform.constans;

import java.util.UUID;

public class CreateTableSql {

    public static String ZXYH_CRM_CUSTOMER_CONTACT_CERT = "CREATE TABLE T_ODS_ZXYH_CRM_CUSTOMER_CONTACT_CERT_VIEW (\n" +
            "    ETL_DATE TIMESTAMP(6),\n" +
            "    TID VARCHAR(1500),\n" +
            "    CUSTOMER_ID VARCHAR(500),\n" +
            "    CN_NAME VARCHAR(64),\n" +
            "    FIRST_CN_NAME VARCHAR(64),\n" +
            "    LAST_CN_NAME VARCHAR(64),\n" +
            "    ENG_NAME VARCHAR(64),\n" +
            "    FIRST_ENG_NAME VARCHAR(64),\n" +
            "    LAST_ENG_NAME VARCHAR(64),\n" +
            "    BIRTHDAY VARCHAR(2000),\n" +
            "    SEX CHAR(1),\n" +
            "    PROVINCE VARCHAR(100),\n" +
            "    `NATIONAL` VARCHAR(100),\n" +
            "    ETHNIC_GROUP VARCHAR(100),\n" +
            "    REGISTER_IP VARCHAR(300),\n" +
            "    REAL_NAME_FLAG CHAR(1),\n" +
            "    REAL_NAME_TIME VARCHAR(100),\n" +
            "    CONTACT_NUMBER VARCHAR(256),\n" +
            "    REAL_NAME_TYPE VARCHAR(2),\n" +
            "    REGIST_CHANNEL VARCHAR(2),\n" +
            "    CUSTOMER_STATUS CHAR(1),\n" +
            "    LAST_LOGIN_TIME TIMESTAMP(6),\n" +
            "    REGIST_DATE TIMESTAMP(6),\n" +
            "    ENABLE CHAR(1),\n" +
            "    CREATE_TIME TIMESTAMP(6),\n" +
            "    UPDATE_TIME TIMESTAMP(6),\n" +
            "    WEB_IS_LIMITED CHAR(1),\n" +
            "    REAL_NAME_FLAG_CONTACT CHAR(1),\n" +
            "    CERTIFICATION_TYPE VARCHAR(2),\n" +
            "    CERTIFICATION_STATUS VARCHAR(2),\n" +
            "    WEB_AUTHORIZE CHAR(1)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_ZXYH_CRM_CUSTOMER_CONTACT_CERT_VIEW', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String ZXYH_CRM_CUSTOMER = "CREATE TABLE T_ODS_ZXYH_CRM_CUSTOMER (\n" +
            "    ETL_DATE TIMESTAMP(6),\n" +
            "    ID VARCHAR(32),\n" +
            "    CUSTOMER_ID VARCHAR(500),\n" +
            "    CN_NAME VARCHAR(64),\n" +
            "    FIRST_CN_NAME VARCHAR(64),\n" +
            "    LAST_CN_NAME VARCHAR(64),\n" +
            "    ENG_NAME VARCHAR(64),\n" +
            "    FIRST_ENG_NAME VARCHAR(64),\n" +
            "    LAST_ENG_NAME VARCHAR(64),\n" +
            "    BIRTHDAY VARCHAR(2000),\n" +
            "    SEX CHAR(1),\n" +
            "    PROVINCE VARCHAR(100),\n" +
            "    `NATIONAL` VARCHAR(100),\n" +
            "    ETHNIC_GROUP VARCHAR(100),\n" +
            "    PRIMARY_PHONE_ID VARCHAR(32),\n" +
            "    REAL_NAME_FLAG CHAR(1),\n" +
            "    REAL_NAME_TIME VARCHAR(100),\n" +
            "    REAL_NAME_TYPE VARCHAR(2),\n" +
            "    REGIST_CHANNEL VARCHAR(2),\n" +
            "    POTENTIAL_FLAG CHAR(1),\n" +
            "    AGENT_FLAG CHAR(1),\n" +
            "    HISTORY_FLAG CHAR(1),\n" +
            "    CUSTOMER_STATUS CHAR(1),\n" +
            "    LAST_LOGIN_TIME TIMESTAMP(6),\n" +
            "    CUSTOMER_DESC VARCHAR(500),\n" +
            "    COMMENTS VARCHAR(500),\n" +
            "    CUSTOMER_SERVICE VARCHAR(4000),\n" +
            "    REGIST_DATE TIMESTAMP(6),\n" +
            "    TRUSTWORTHY CHAR(1),\n" +
            "    REGIST_IP VARCHAR(100),\n" +
            "    REFERRER VARCHAR(32),\n" +
            "    CUSTOMER_LEVEL CHAR(1),\n" +
            "    ENABLE CHAR(1),\n" +
            "    CREATE_TIME TIMESTAMP(6),\n" +
            "    UPDATE_TIME TIMESTAMP(6),\n" +
            "    CREATE_USER VARCHAR(32),\n" +
            "    UPDATE_USER VARCHAR(32),\n" +
            "    FILTER1 VARCHAR(200),\n" +
            "    FILTER2 VARCHAR(200),\n" +
            "    FILTER3 VARCHAR(200),\n" +
            "    FILTER4 VARCHAR(200),\n" +
            "    FILTER5 VARCHAR(200),\n" +
            "    FILTER6 VARCHAR(200),\n" +
            "    FILTER7 VARCHAR(200),\n" +
            "    FILTER8 VARCHAR(200),\n" +
            "    FILTER9 VARCHAR(200),\n" +
            "    VERIFY_CLV_FLAG CHAR(1),\n" +
            "    FOOD_PREF VARCHAR(200),\n" +
            "    SEAT_PREF VARCHAR(200),\n" +
            "    DEVICE_NO VARCHAR(200),\n" +
            "    FIRST_LOGIN_DATE TIMESTAMP(6),\n" +
            "    RETENTION_FLAG CHAR(1),\n" +
            "    WEB_IS_LIMITED CHAR(1),\n" +
            "    WEB_USER_STATUS CHAR(1),\n" +
            "    WEB_AUTHORIZE CHAR(1)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_ZXYH_CRM_CUSTOMER', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"',\n" +
            "'lookup.cache.max-rows' = '10000',\n" +
            "    'lookup.cache.ttl' = '10min',\n" +
            "    'lookup.max-retries' = '3'" +
            ")";

    public static String ZXYH_CRM_CUSTOMER_OPEN_INFO = "CREATE TABLE T_ODS_ZXYH_CRM_CUSTOMER_OPEN_INFO (\n" +
            "    ETL_DATE DATE,\n" +
            "ID VARCHAR(32) ,\n" +
            "CUSTOMER_ID VARCHAR(200) ,\n" +
            "SYS_ID VARCHAR(200) ,\n" +
            "OPEN_ID VARCHAR(120) ,\n" +
            "UNION_ID VARCHAR(120) ,\n" +
            "CHANNEL_TYPE VARCHAR(50) ,\n" +
            "PLATFORM_TYPE VARCHAR(50) ,\n" +
            "COMMENTS VARCHAR(500) ,\n" +
            "ENABLE CHAR(1) ,\n" +
            "CREATE_TIME TIMESTAMP(6) ,\n" +
            "UPDATE_TIME TIMESTAMP(6) ,\n" +
            "CREATE_USER VARCHAR(32) ,\n" +
            "UPDATE_USER VARCHAR(32)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_ZXYH_CRM_CUSTOMER_OPEN_INFO', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String ZXYH_CRM_BLOCKLIST = "CREATE TABLE T_ODS_ZXYH_CRM_BLOCKLIST (\n" +
            "    ETL_DATE DATE,\n" +
            "    ID VARCHAR(32),\n" +
            "    TYPE VARCHAR(2),\n" +
            "    CONTEXT VARCHAR(4000),\n" +
            "    REASON VARCHAR(4000),\n" +
            "    EFFECTIVE_STATUS CHAR(1),\n" +
            "    CREATE_TIME TIMESTAMP(6),\n" +
            "    CREATE_USER VARCHAR(32),\n" +
            "    UPDATE_TIME TIMESTAMP(6),\n" +
            "    UPDATE_USER VARCHAR(32),\n" +
            "    ENABLE CHAR(1),\n" +
            "    BLOCK_RULE_ID VARCHAR(100)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_ZXYH_CRM_BLOCKLIST', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String CRM_WEB_NO_REGISTERED ="CREATE TABLE T_ODS_ZXYH_CRM_WEB_NO_REGISTERED (\n" +
            "    ETL_DATE DATE,\n" +
            "ID VARCHAR(60) ,\n" +
            "USERID VARCHAR(150) ,\n" +
            "SEX VARCHAR(3) ,\n" +
            "USERNAME VARCHAR(150) ,\n" +
            "USERPWD VARCHAR(768) ,\n" +
            "REQUESTION VARCHAR(30) ,\n" +
            "REANSWER VARCHAR(300) ,\n" +
            "BORNDATE VARCHAR(30) ,\n" +
            "NATIONALITY VARCHAR(30) ,\n" +
            "FOLK VARCHAR(30) ,\n" +
            "IDETIFYTYPE VARCHAR(6) ,\n" +
            "IDENTIFYNO VARCHAR(768) ,\n" +
            "NATION VARCHAR(60) ,\n" +
            "PROVINCE VARCHAR(300) ,\n" +
            "CITY VARCHAR(300) ,\n" +
            "POSTCODE VARCHAR(60) ,\n" +
            "MOBILEPHONE VARCHAR(768) ,\n" +
            "EMAIL VARCHAR(768) ,\n" +
            "PHONE VARCHAR(768) ,\n" +
            "ADDRESTRING VARCHAR(300) ,\n" +
            "ADDRESTRING2 VARCHAR(300) ,\n" +
            "ADDRESTRING3 VARCHAR(300) ,\n" +
            "COMPANYNAME VARCHAR(150) ,\n" +
            "`POSITION` VARCHAR(60) ,\n" +
            "COMPANYTELEPHONE VARCHAR(60) ,\n" +
            "COMPANYNATION VARCHAR(60) ,\n" +
            "COMPANYPROVINCE VARCHAR(300) ,\n" +
            "COMPANYCITY VARCHAR(300) ,\n" +
            "COMPANYPOSTCODE VARCHAR(60) ,\n" +
            "COMPANYADDRESS VARCHAR(60) ,\n" +
            "COMPANYADDRESS2 VARCHAR(60) ,\n" +
            "COMPANYADDRESS3 VARCHAR(60) ,\n" +
            "CARDTYPE VARCHAR(60) ,\n" +
            "REGDEPARTMENT VARCHAR(60) ,\n" +
            "EDUDG VARCHAR(60) ,\n" +
            "WORK VARCHAR(60) ,\n" +
            "AIRPORT VARCHAR(60) ,\n" +
            "FFPFLAG VARCHAR(60) ,\n" +
            "AUTHORIZE VARCHAR(60) ,\n" +
            "DISABLEREASON VARCHAR(300) ,\n" +
            "ABLEREASON VARCHAR(300) ,\n" +
            "REGTIME VARCHAR(60) ,\n" +
            "UPDATETIME VARCHAR(60) ,\n" +
            "FAMILY_NAME VARCHAR(60) ,\n" +
            "GIVEN_NAME VARCHAR(60) ,\n" +
            "CONTACTTYPE VARCHAR(60) ,\n" +
            "USERSTATUS VARCHAR(60) ,\n" +
            "BINDABLEREASON VARCHAR(300) ,\n" +
            "FFPID VARCHAR(60) ,\n" +
            "NICKNAME VARCHAR(60) ,\n" +
            "UPDATESUCC VARCHAR(60) ,\n" +
            "ACCOUNTYPE VARCHAR(60) ,\n" +
            "BINDEMAIL VARCHAR(150) ,\n" +
            "BINDMOBILE VARCHAR(60) ,\n" +
            "USERTYPE VARCHAR(60) ,\n" +
            "USERFROM VARCHAR(60) ,\n" +
            "USERCATEGORY VARCHAR(60) ,\n" +
            "IS_MANJIAN VARCHAR(60) ,\n" +
            "IS_LIMITED VARCHAR(60) ,\n" +
            "IS_REALNAME VARCHAR(60) ,\n" +
            "DISABLE_TIME VARCHAR(60) ,\n" +
            "DISABLE_OPERATOR VARCHAR(60) ,\n" +
            "IS_WHITELIST VARCHAR(60) ,\n" +
            "CRMID VARCHAR(60) ,\n" +
            "PHONELAND VARCHAR(60) ,\n" +
            "LAST_LOGIN_TIME VARCHAR(60) ,\n" +
            "PINYIN_SURNAME VARCHAR(60) ,\n" +
            "PINYIN_FIRSTNAME VARCHAR(60) ,\n" +
            "BUSINESS_DEPARTMENT VARCHAR(60) ,\n" +
            "TICKET_COUNT VARCHAR(60) ,\n" +
            "SUCCESSFUL_ORDERS_COUNT VARCHAR(60) ,\n" +
            "CONSUMPTION_AMOUNT VARCHAR(60) ,\n" +
            "CREDIT_STATUS VARCHAR(60) ,\n" +
            "CREDIT_SCORE VARCHAR(60) ,\n" +
            "VIP_TYPE VARCHAR(60) ,\n" +
            "CONTACT_NAME VARCHAR(60) ,\n" +
            "CONTACT_MOBILE_PHONE VARCHAR(60) ,\n" +
            "CONTACT_EMAIL VARCHAR(150) ,\n" +
            "CONTACT_PHONE VARCHAR(60) ,\n" +
            "BOOKING_TIME VARCHAR(60) ,\n" +
            "REGISTER_IP VARCHAR(60) ,\n" +
            "LOGIN_COUNT VARCHAR(60) ,\n" +
            "VERIFYMOBILE_TIME VARCHAR(60) ,\n" +
            "DELETE_TIME VARCHAR(60) ,\n" +
            "IS_DELETE CHAR(1) ,\n" +
            "IS_PROFILEREALNAME VARCHAR(3072) ,\n" +
            "REFRESH_FLAG VARCHAR(6) ,\n" +
            "ENABLE CHAR(1) ,\n" +
            "CREATE_TIME TIMESTAMP(6) ,\n" +
            "UPDATE_TIME TIMESTAMP(6) ,\n" +
            "CREATE_USER VARCHAR(96) ,\n" +
            "UPDATE_USER VARCHAR(96),\n" +
            "WEB_IS_LIMITED VARCHAR(30)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_ZXYH_CRM_WEB_NO_REGISTERED', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String ZXYH_CRM_CUSTOMER_CERT = "CREATE TABLE T_ODS_ZXYH_CRM_CUSTOMER_CERT (\n" +
            "    ETL_DATE DATE,\n" +
            "ID VARCHAR(32) ,\n" +
            "CUSTOMER_ID VARCHAR(200) ,\n" +
            "REAL_NAME_FLAG CHAR(1) ,\n" +
            "CERT_TYPE VARCHAR(2) ,\n" +
            "CERT_NUMBER VARCHAR(256) ,\n" +
            "CERT_DESC VARCHAR(500) ,\n" +
            "ISSUE_DATE TIMESTAMP(6) ,\n" +
            "EXPIRATION_DATE TIMESTAMP(6) ,\n" +
            "SIGNING_AUTHORITY VARCHAR(200) ,\n" +
            "ISSUE_AGENCY VARCHAR(200) ,\n" +
            "COMMENTS VARCHAR(500) ,\n" +
            "ENABLE CHAR(1) ,\n" +
            "CREATE_TIME TIMESTAMP(6) ,\n" +
            "UPDATE_TIME TIMESTAMP(6) ,\n" +
            "CREATE_USER VARCHAR(32) ,\n" +
            "UPDATE_USER VARCHAR(32) \n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_ZXYH_CRM_CUSTOMER_CERT', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String ZXYH_CRM_CUSTOMER_LOGIN_ACCOUNT = "CREATE TABLE T_ODS_ZXYH_CRM_CUSTOMER_LOGIN_ACCOUNT (\n" +
            "    ETL_DATE DATE,\n" +
            "ID VARCHAR(32) ,\n" +
            "CUSTOMER_ID VARCHAR(200) ,\n" +
            "USERNAME VARCHAR(200) ,\n" +
            "ACCOUNT_TYPE VARCHAR(64) ,\n" +
            "ACCOUNT_NAME VARCHAR(200) ,\n" +
            "PWD VARCHAR(200) ,\n" +
            "PWD_TYPE VARCHAR(64) ,\n" +
            "CHANNEL VARCHAR(2) ,\n" +
            "COMMENTS VARCHAR(500) ,\n" +
            "VERSION VARCHAR(2) ,\n" +
            "DEFAULT_PASSWORD_FLAG VARCHAR(1) ,\n" +
            "ENABLE CHAR(1) ,\n" +
            "CREATE_TIME TIMESTAMP(6) ,\n" +
            "UPDATE_TIME TIMESTAMP(6) ,\n" +
            "CREATE_USER VARCHAR(32) ,\n" +
            "UPDATE_USER VARCHAR(32) \n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_ZXYH_CRM_CUSTOMER_LOGIN_ACCOUNT', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String ZXYH_CRM_CUSTOMER_VOCATION_INFO = "CREATE TABLE T_ODS_ZXYH_CRM_CUSTOMER_VOCATION_INFO (\n" +
            "    ETL_DATE DATE,\n" +
            "ID VARCHAR(32) ,\n" +
            "CUSTOMER_ID VARCHAR(200) ,\n" +
            "CHANNEL VARCHAR(2) ,\n" +
            "VOCATION_TYPE VARCHAR(20) ,\n" +
            "VOCATION_NUMBER VARCHAR(20) ,\n" +
            "PROTOCOL_NUMBER VARCHAR(20) ,\n" +
            "VOCATION_LEVEL VARCHAR(50) ,\n" +
            "COMMENTS VARCHAR(500) ,\n" +
            "ENABLE CHAR(1) ,\n" +
            "CREATE_TIME TIMESTAMP(6) ,\n" +
            "UPDATE_TIME TIMESTAMP(6) ,\n" +
            "CREATE_USER VARCHAR(32) ,\n" +
            "UPDATE_USER VARCHAR(32)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_WXAP_CHECKIN_TRP_ORDER', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String ZXYH_CRM_CUSTOMER_ADDRESS = "CREATE TABLE T_ODS_ZXYH_CRM_CUSTOMER_ADDRESS (\n" +
            "    ETL_DATE DATE,\n" +
            "ID VARCHAR(32),\n" +
            "CUSTOMER_ID VARCHAR(200),\n" +
            "ZIP_CODE VARCHAR(20),\n" +
            "COUNTRY_CODE VARCHAR(50),\n" +
            "COUNTRY_NAME VARCHAR(500),\n" +
            "PROVINCE_CODE VARCHAR(50),\n" +
            "PROVINCE_NAME VARCHAR(500),\n" +
            "CITY_CODE VARCHAR(50),\n" +
            "CITY_NAME VARCHAR(500),\n" +
            "DETAIL_ADDRESS VARCHAR(500),\n" +
            "FULL_ADDRESS VARCHAR(500),\n" +
            "COMMENTS VARCHAR(500),\n" +
            "ENABLE CHAR(1),\n" +
            "CREATE_TIME TIMESTAMP(6),\n" +
            "UPDATE_TIME TIMESTAMP(6),\n" +
            "CREATE_USER VARCHAR(32),\n" +
            "UPDATE_USER VARCHAR(32),\n" +
            "CONTACT_MOBILE VARCHAR(256),\n" +
            "RECEIVE_NAME VARCHAR(100)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_ZXYH_CRM_CUSTOMER_ADDRESS', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String ZXYH_CRM_CUSTOMER_INVOICE_TITLE = "CREATE TABLE T_ODS_ZXYH_CRM_CUSTOMER_INVOICE_TITLE (\n" +
            "    ETL_DATE DATE,\n" +
            "ID VARCHAR(128) ,\n" +
            "CUSTOMER_ID VARCHAR(128) ,\n" +
            "TAXPAYER_TYPE CHAR(1) ,\n" +
            "TAXPAYER_NAME VARCHAR(300) ,\n" +
            "TAXPAYER_NO VARCHAR(20) ,\n" +
            "TAXPAYER_ADDRESS VARCHAR(300) ,\n" +
            "TAXPAYER_PHONE VARCHAR(256) ,\n" +
            "TAXPAYER_BANK_NAME VARCHAR(300) ,\n" +
            "TAXPAYER_BANK_ACCOUNT VARCHAR(30) ,\n" +
            "CONTACT_EMAIL VARCHAR(300) ,\n" +
            "ENABLE CHAR(1) ,\n" +
            "CREATE_USER VARCHAR(128) ,\n" +
            "CREATE_TIME TIMESTAMP(6) ,\n" +
            "UPDATE_USER VARCHAR(128) ,\n" +
            "UPDATE_TIME TIMESTAMP(6) ,\n" +
            "DEFAULT_USE_STATUS CHAR(1) \n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_ZXYH_CRM_CUSTOMER_INVOICE_TITLE', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String ZXYH_CRM_CUSTOMER_CONTACT = "CREATE TABLE T_ODS_ZXYH_CRM_CUSTOMER_CONTACT (\n" +
            "    ETL_DATE DATE,\n" +
            "ID VARCHAR(32) PRIMARY KEY,\n" +
            "CUSTOMER_ID VARCHAR(200) ,\n" +
            "REAL_NAME_FLAG CHAR(1) ,\n" +
            "CONTACT_TYPE VARCHAR(2) ,\n" +
            "CONTACT_NUMBER VARCHAR(256) ,\n" +
            "CONTACT_DESC VARCHAR(500) ,\n" +
            "COMMENTS VARCHAR(500) ,\n" +
            "ENABLE CHAR(1) ,\n" +
            "CREATE_TIME TIMESTAMP(6) ,\n" +
            "UPDATE_TIME TIMESTAMP(6) ,\n" +
            "CREATE_USER VARCHAR(32) ,\n" +
            "UPDATE_USER VARCHAR(32) \n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_ZXYH_CRM_CUSTOMER_CONTACT', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"',\n" +
            "'lookup.cache.max-rows' = '10000',\n" +
            "    'lookup.cache.ttl' = '10min',\n" +
            "    'lookup.max-retries' = '3'" +
            ")";

    public static String ZXYH_CRM_CUS_CERTIFICATION_RECORD = "CREATE TABLE T_ODS_ZXYH_CRM_CUS_CERTIFICATION_RECORD (\n" +
            "    ETL_DATE DATE,\n" +
            "ID VARCHAR(32) ,\n" +
            "CUSTOMER_ID VARCHAR(32) PRIMARY KEY,\n" +
            "CN_NAME VARCHAR(64) ,\n" +
            "ENG_NAME VARCHAR(64) ,\n" +
            "CERT_TYPE VARCHAR(2) ,\n" +
            "CERT_NUMBER VARCHAR(256) ,\n" +
            "MOBILE VARCHAR(256) ,\n" +
            "AUDIT_STATUS VARCHAR(2) ,\n" +
            "ENABLE CHAR(1) ,\n" +
            "CREATE_TIME TIMESTAMP(6) ,\n" +
            "UPDATE_TIME TIMESTAMP(6) ,\n" +
            "CREATE_USER VARCHAR(32) ,\n" +
            "UPDATE_USER VARCHAR(32) ,\n" +
            "CERTIFICATION_TYPE VARCHAR(2) ,\n" +
            "CERTIFICATION_STATUS VARCHAR(2) ,\n" +
            "UNIONCARD_NUMBER VARCHAR(20)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_ZXYH_CRM_CUS_CERTIFICATION_RECORD', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"',\n" +
            "'lookup.cache.max-rows' = '10000',\n" +
            "    'lookup.cache.ttl' = '10min',\n" +
            "    'lookup.max-retries' = '3'" +
            ")";

    // 鲁雁行建表
    public static String LYX_ORDER_BASE = "CREATE TABLE T_ODS_LYX_ORDER_BASE (\n" +
            "    ETL_DATE DATE,\n" +
            "ID BIGINT ,\n" +
            "ORDER_NO VARCHAR(100) ,\n" +
            "ORDER_STATUS VARCHAR(4) ,\n" +
            "ORDER_TYPE VARCHAR(10) ,\n" +
            "PRODUCT_NAME VARCHAR(50) ,\n" +
            "ORDER_TIME TIMESTAMP(6) ,\n" +
            "ORDER_ORIGIN VARCHAR(20) ,\n" +
            "CURRENT_DEALER VARCHAR(20) ,\n" +
            "DEAL_TIME TIMESTAMP(6) ,\n" +
            "PSG_NAME VARCHAR(50) ,\n" +
            "VIP_IF VARCHAR(2) ,\n" +
            "VIP_LEVEL VARCHAR(4) ,\n" +
            "VIP_TYPE VARCHAR(20) ,\n" +
            "CERTIFY_TYPE VARCHAR(6) ,\n" +
            "CERTIFY_NUM VARCHAR(100) ,\n" +
            "PHONE_NUM VARCHAR(100) ,\n" +
            "TICKET_NUM VARCHAR(20) ,\n" +
            "TICKET_STATUS VARCHAR(20) ,\n" +
            "ISSUE_OFFICE VARCHAR(50) ,\n" +
            "BOOK_OFFICE VARCHAR(50) ,\n" +
            "FLT_NUM VARCHAR(8) ,\n" +
            "FLT_DATE DATE ,\n" +
            "ORIG VARCHAR(6) ,\n" +
            "DEST VARCHAR(6) ,\n" +
            "LAUNCH_TIME TIMESTAMP(6) ,\n" +
            "ARRIVE_TIME TIMESTAMP(6) ,\n" +
            "FLT_STATUS VARCHAR(4) ,\n" +
            "TICKET_PRICE VARCHAR(10) ,\n" +
            "CABIN_CLASS VARCHAR(2) ,\n" +
            "DISCUSS_STATUS VARCHAR(4) ,\n" +
            "DISCUSS_CONTENT VARCHAR(500) ,\n" +
            "DISCUSS_LEVEL VARCHAR(2) ,\n" +
            "DISCUSS_TIME TIMESTAMP(6) ,\n" +
            "CREATOR VARCHAR(20) ,\n" +
            "CREATE_TIME TIMESTAMP(6) ,\n" +
            "UPDATOR VARCHAR(20) ,\n" +
            "UPDATE_TIME TIMESTAMP(6) ,\n" +
            "TICKET_FLAG VARCHAR(20) ,\n" +
            "ORDER_OLD_NO VARCHAR(100) ,\n" +
            "PRODUCT_CODE VARCHAR(50) ,\n" +
            "PRODUCT_ID BIGINT ,\n" +
            "ORIG_NAME VARCHAR(20) ,\n" +
            "DEST_NAME VARCHAR(20) ,\n" +
            "PSG_ID BIGINT ,\n" +
            "ORIG_TICKET_NUM VARCHAR(20) ,\n" +
            "ORIG_TICKET_PRICE VARCHAR(10) ,\n" +
            "ORIG_FLT_NUM VARCHAR(8) ,\n" +
            "ORIG_FLT_DATE DATE ,\n" +
            "ORIG_ORIG VARCHAR(6) ,\n" +
            "ORIG_DEST VARCHAR(6) ,\n" +
            "ORIG_LAUNCH_TIME TIMESTAMP(6) ,\n" +
            "ORIG_ARRIVE_TIME TIMESTAMP(6) ,\n" +
            "ORIG_CABIN_CLASS VARCHAR(2) ,\n" +
            "GROUP_ID BIGINT ,\n" +
            "PSG_CARDNO VARCHAR(20) ,\n" +
            "OUT_CLASS VARCHAR(4) ,\n" +
            "OUT_PRICE VARCHAR(20) ,\n" +
            "ADDPRICE_INCOME VARCHAR(20) ,\n" +
            "LINK_ID VARCHAR(20) ,\n" +
            "LINK_SOURCE VARCHAR(20) ,\n" +
            "PNR_ICS VARCHAR(20) ,\n" +
            "PNR_CRS VARCHAR(20) ,\n" +
            "LINK_FLAG VARCHAR(4) ,\n" +
            "IS_REVALIDATION VARCHAR(3) ,\n" +
            "PAY_OPERATE_REASON VARCHAR(100) ,\n" +
            "PAY_OPERATE_TYPE VARCHAR(100) ,\n" +
            "REMIND_FLAG VARCHAR(5) ,\n" +
            "IATA_NO VARCHAR(10) ,\n" +
            "AGENT_NAME VARCHAR(50) ,\n" +
            "SERVICE_COST VARCHAR(10) ,\n" +
            "PAY_STATUS VARCHAR(4) ,\n" +
            "SALES_DEPT VARCHAR(100) ,\n" +
            "B2B_OFFICE VARCHAR(100) ,\n" +
            "BI_DEPTNAME VARCHAR(200) ,\n" +
            "BI_SERVICE_COST VARCHAR(10) ,\n" +
            "BI_PROMOTION_COST VARCHAR(10) ,\n" +
            "SERVICE_LEVEL VARCHAR(10) ,\n" +
            "ISSUE_TIME TIMESTAMP(6) ,\n" +
            "BI_MARKETING_INCENTIVES_PRICE VARCHAR(500)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_LYX_ORDER_BASE', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String LYX_ORDER_CHANGE_REFUND_INFO = "CREATE TABLE T_ODS_LYX_ORDER_CHANGE_REFUND_INFO (\n" +
            "    ETL_DATE DATE,\n" +
            "ID BIGINT ,\n" +
            "ORDER_NO VARCHAR(100) ,\n" +
            "TOTAL_REFUND_STATUS VARCHAR(2) ,\n" +
            "ORDER_TYPE VARCHAR(2) ,\n" +
            "OLD_ORDER_NO VARCHAR(100) ,\n" +
            "ORIGINAL_ORDER_NO VARCHAR(100) ,\n" +
            "ORDER_SOURCE VARCHAR(2) ,\n" +
            "ORDER_STATUS VARCHAR(2) ,\n" +
            "LY_NAME VARCHAR(50) ,\n" +
            "LY_CARD VARCHAR(100) ,\n" +
            "LY_LEVEL VARCHAR(10) ,\n" +
            "LY_MOBILE VARCHAR(200) ,\n" +
            "ORDER_PERSON VARCHAR(50) ,\n" +
            "CREATE_TIME TIMESTAMP(6) ,\n" +
            "UPDATOR VARCHAR(50) ,\n" +
            "UPDATE_TIME TIMESTAMP(6) ,\n" +
            "CHANGE_FEE_TOTAL VARCHAR(20) ,\n" +
            "COMPENSATION_FEE_TOTAL VARCHAR(20) ,\n" +
            "LY_SUM VARCHAR(20) ,\n" +
            "CASH_SUM VARCHAR(20) ,\n" +
            "CHANGE_PERPSON VARCHAR(50) ,\n" +
            "CHANGE_TIME TIMESTAMP(6) ,\n" +
            "REFUND_PERPSON VARCHAR(50) ,\n" +
            "REFUND_TIME TIMESTAMP(6) ,\n" +
            "REFUND_LY_VALUE VARCHAR(20) ,\n" +
            "REFUND_LY_STATUS VARCHAR(2) ,\n" +
            "LY_EXPIRES_TIME DATE ,\n" +
            "REFUND_CASH VARCHAR(20) ,\n" +
            "REFUND_CASH_STATUS VARCHAR(2) ,\n" +
            "AIRPORT_TAX_TOTAL VARCHAR(20) ,\n" +
            "FUEL_TAX_TOTAL VARCHAR(20) ,\n" +
            "INSURE_FEE_TOTAL VARCHAR(20) ,\n" +
            "CHANGE_SUM_FEE VARCHAR(20) ,\n" +
            "LAST_CHANGE_FEE VARCHAR(20) ,\n" +
            "REFUND_TYPE VARCHAR(2) ,\n" +
            "NOVOLUNTEER_REASON VARCHAR(100) ,\n" +
            "REFUND_EXPLAIN VARCHAR(100) ,\n" +
            "REFUND_EVIDENCE VARCHAR(50) ,\n" +
            "REFUND_EVIDENCE_IMAGE VARCHAR(500) ,\n" +
            "APPLY_PERSON VARCHAR(150) ,\n" +
            "APPLY_TIME TIMESTAMP(6) ,\n" +
            "AUDIT_PERSON VARCHAR(50) ,\n" +
            "AUDIT_TIME TIMESTAMP(6) ,\n" +
            "PAY_ID BIGINT ,\n" +
            "USED_LEFT_FLAG VARCHAR(2) ,\n" +
            "IS_ABNORMAL VARCHAR(2) ,\n" +
            "ERROR_STATUS VARCHAR(2) ,\n" +
            "AUDIT_STATUS VARCHAR(2) ,\n" +
            "COMPLETE_STATUS VARCHAR(2) ,\n" +
            "AUDIT_REMARK VARCHAR(255) ,\n" +
            "AUDIT_RESULT VARCHAR(255) ,\n" +
            "AUDIT_SECOND_PERSON VARCHAR(255) ,\n" +
            "AUDIT_SECOND_TIME TIMESTAMP(6) ,\n" +
            "AUDIT_SECOND_RESULT VARCHAR(255) ,\n" +
            "AUDIT_SECOND_REMARK VARCHAR(255) ,\n" +
            "CHANGE_TYPE VARCHAR(2) ,\n" +
            "CHANGE_EXPLAIN VARCHAR(100) ,\n" +
            "CHANGE_NOVOLUNTEER_REASON VARCHAR(255) ,\n" +
            "CHANGE_EVIDENCE VARCHAR(50) ,\n" +
            "CHANGE_EVIDENCE_IMAGE VARCHAR(300) ,\n" +
            "REFUND_LY_VALUE_METHOD VARCHAR(2) ,\n" +
            "EFFECTIVE_DATE DATE ,\n" +
            "ERROR_SOLUTION VARCHAR(2)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_LYX_ORDER_CHANGE_REFUND_INFO', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String LYX_ORDER_TICKET_INFO = "CREATE TABLE T_ODS_LYX_ORDER_TICKET_INFO (\n" +
            "    ETL_DATE DATE,\n" +
            "ID BIGINT ,\n" +
            "ORDER_NO VARCHAR(100) ,\n" +
            "ORDER_SOURCE VARCHAR(2) ,\n" +
            "ORDER_STATUS VARCHAR(2) ,\n" +
            "LY_NAME VARCHAR(50) ,\n" +
            "LY_CARD VARCHAR(100) ,\n" +
            "LY_LEVEL VARCHAR(10) ,\n" +
            "LY_MOBILE VARCHAR(200) ,\n" +
            "ORDER_PERSON VARCHAR(50) ,\n" +
            "CREATE_TIME TIMESTAMP(6) ,\n" +
            "UPDATOR VARCHAR(50) ,\n" +
            "UPDATE_TIME TIMESTAMP(6) ,\n" +
            "TOTAL_PRICE VARCHAR(20) ,\n" +
            "TOTAL_TAX VARCHAR(20) ,\n" +
            "TOTAL_EXTRA VARCHAR(20) ,\n" +
            "PAY_ID BIGINT ,\n" +
            "USED_LEFT_FLAG VARCHAR(2) ,\n" +
            "ERROR_STATUS VARCHAR(2) ,\n" +
            "ERROR_SOLUTION VARCHAR(2)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_LYX_ORDER_TICKET_INFO', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String LYX_ORDER_PAY_INFO = "CREATE TABLE T_ODS_LYX_ORDER_PAY_INFO (\n" +
            "    ETL_DATE DATE,\n" +
            "ID BIGINT ,\n" +
            "LY_VALUE_STATUS VARCHAR(2) ,\n" +
            "LY_SUM VARCHAR(20) ,\n" +
            "LY_PAY_TIME TIMESTAMP(6) ,\n" +
            "CASH_PAY_STATUS VARCHAR(2) ,\n" +
            "PAY_WAY VARCHAR(2) ,\n" +
            "CASH_SUM VARCHAR(20) ,\n" +
            "CASH_PAY_NO VARCHAR(50) ,\n" +
            "CASH_PAY_MOBILE VARCHAR(200) ,\n" +
            "CASH_PAY_TIME TIMESTAMP(6) ,\n" +
            "CREATE_TIME TIMESTAMP(6) ,\n" +
            "UPDATE_TIME TIMESTAMP(6) ,\n" +
            "PAY_SOURCE VARCHAR(2) ,\n" +
            "CASH_FAIL_REASON VARCHAR(1000) ,\n" +
            "LY_FAIL_REASON VARCHAR(1000) ,\n" +
            "ORDER_TYPE VARCHAR(2) ,\n" +
            "CASHPAY_STARTTIME TIMESTAMP(6) ,\n" +
            "ORIGIN_TYPE VARCHAR(20)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_LYX_ORDER_PAY_INFO', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String LYX_ORDER_FLIGHT_INFOS = "CREATE TABLE T_ODS_LYX_ORDER_FLIGHT_INFOS (\n" +
            "    ETL_DATE DATE,\n" +
            "ID BIGINT ,\n" +
            "OLD_FLT_ID BIGINT ,\n" +
            "ORDER_NO VARCHAR(100) ,\n" +
            "FLIGHT_NUM VARCHAR(10) ,\n" +
            "FLIGHT_DATE DATE ,\n" +
            "ORIG VARCHAR(10) ,\n" +
            "DEST VARCHAR(10) ,\n" +
            "DEPATURE_TIME TIMESTAMP(6) ,\n" +
            "ARRIVE_TIME TIMESTAMP(6) ,\n" +
            "IS_GO_BACK VARCHAR(2) ,\n" +
            "CABIN VARCHAR(6) ,\n" +
            "FLT_TYPE VARCHAR(2) ,\n" +
            "STOP_CTIY VARCHAR(6) ,\n" +
            "DEPART_TEMINAL VARCHAR(30) ,\n" +
            "ARRIVAL_TEMINAL VARCHAR(30)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_LYX_ORDER_FLIGHT_INFOS', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String LYX_ORDER_PASSENGER_INFO = "CREATE TABLE T_ODS_LYX_ORDER_PASSENGER_INFO (\n" +
            "    ETL_DATE DATE,\n" +
            "ID BIGINT ,\n" +
            "PSG_NAME_CN VARCHAR(100) ,\n" +
            "PSG_NAME_EN VARCHAR(100) ,\n" +
            "CERT_TYPE VARCHAR(6) ,\n" +
            "CERT_NUM VARCHAR(200) ,\n" +
            "PASSENGER_TYPE VARCHAR(6) ,\n" +
            "CONTACT_NAME VARCHAR(50) ,\n" +
            "CONTACT_NO VARCHAR(200) ,\n" +
            "FLIGHT_ID BIGINT ,\n" +
            "TICKET_NUM VARCHAR(20) ,\n" +
            "PNR_NO VARCHAR(20) ,\n" +
            "TICKET_PRICE VARCHAR(20) ,\n" +
            "TICKET_STATUS VARCHAR(20) ,\n" +
            "TICKET_TIME TIMESTAMP(6) ,\n" +
            "AIRPORT_TAX VARCHAR(20) ,\n" +
            "FUEL_TAX VARCHAR(20) ,\n" +
            "INSURE_TAX VARCHAR(20) ,\n" +
            "INSURE_STATUS VARCHAR(2) ,\n" +
            "LY_VALUE VARCHAR(20) ,\n" +
            "LY_STATUS VARCHAR(2) ,\n" +
            "ACCOMPANY_NAME VARCHAR(100) ,\n" +
            "ACCOMPANY_CERT_NO VARCHAR(200) ,\n" +
            "ACCOMPANY_TICKET_NUM VARCHAR(20) ,\n" +
            "IS_ACCOMPANY VARCHAR(2) ,\n" +
            "OLD_PSG_ID BIGINT ,\n" +
            "AGE INT ,\n" +
            "CHANGE_FEE VARCHAR(20) ,\n" +
            "COMPENSATION_FEE VARCHAR(20) ,\n" +
            "LAST_CHANGE_FEE VARCHAR(20) ,\n" +
            "REFUND_NUM VARCHAR(30) ,\n" +
            "REFUND_NUM_STATUS VARCHAR(2) ,\n" +
            "REFUND_AMOUNT_STATUS VARCHAR(2) ,\n" +
            "INSURE_NO VARCHAR(100) ,\n" +
            "INSURE_SERIAL_NUMBER VARCHAR(100) ,\n" +
            "OPT_STATUS VARCHAR(2) ,\n" +
            "CASH VARCHAR(20) ,\n" +
            "TICKET_FAIL_REASON VARCHAR(1000) ,\n" +
            "INSURANCE_FAIL_REASON VARCHAR(1000) ,\n" +
            "STOP_AIRPORT VARCHAR(50) ,\n" +
            "STOP_TERMINAL VARCHAR(50) ,\n" +
            "CABIN_TYPE_NAME CHAR(6) ,\n" +
            "MEAL VARCHAR(20)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_LYX_ORDER_PASSENGER_INFO', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String LYX_LY_MTMEMBER = "CREATE TABLE T_ODS_LYX_LY_MTMEMBER (\n" +
            "    ETL_DATE DATE,\n" +
            "ID BIGINT ,\n" +
            "MTM_CARD_NUM VARCHAR(20) ,\n" +
            "MTM_STATUS VARCHAR(4) ,\n" +
            "MTM_PASS VARCHAR(100) ,\n" +
            "MTM_SURNAME_CN VARCHAR(50) ,\n" +
            "MTM_NAME_CN VARCHAR(50) ,\n" +
            "MTM_SURNAME_EN VARCHAR(50) ,\n" +
            "MTM_NAME_EN VARCHAR(50) ,\n" +
            "MTM_HIPPOCRATES VARCHAR(20) ,\n" +
            "MTM_NATIONALITY VARCHAR(20) ,\n" +
            "MTM_BIRTHDAY TIMESTAMP(6) ,\n" +
            "MTM_REGISTER_TIME TIMESTAMP(6) ,\n" +
            "PASS_ACTIVE_STATE VARCHAR(4) ,\n" +
            "REALNAME_ATTESTATION VARCHAR(4) ,\n" +
            "ATTESTATION_MODE VARCHAR(4) ,\n" +
            "ATTESTATION_TIME TIMESTAMP(6) ,\n" +
            "MTM_MOBILE VARCHAR(100) ,\n" +
            "MAIN_ID_TYPE VARCHAR(20) ,\n" +
            "MAIN_ID_NUM VARCHAR(100) ,\n" +
            "VERSION BIGINT ,\n" +
            "DEL_FLG VARCHAR(20) ,\n" +
            "CREATE_ID VARCHAR(20) ,\n" +
            "CREATE_TIME TIMESTAMP(6) ,\n" +
            "UPDATE_ID VARCHAR(20) ,\n" +
            "UPDATE_TIME TIMESTAMP(6) ,\n" +
            "LINK_ID VARCHAR(20) ,\n" +
            "LINK_SOURCE VARCHAR(20) ,\n" +
            "GESTURE_STATUS VARCHAR(20) ,\n" +
            "GESTURE_PASS VARCHAR(100) ,\n" +
            "GESTURE_TIP VARCHAR(20) ,\n" +
            "LIP_ATTESTATION VARCHAR(2) ,\n" +
            "TIMING_TASK_STATUS VARCHAR(2) ,\n" +
            "ORI_LEVEL VARCHAR(100) ,\n" +
            "LEVEL_UPDATE_TIME TIMESTAMP(6) ,\n" +
            "REGISTER_STATUS VARCHAR(5) ,\n" +
            "PRIVATE_POLICY_STATUS VARCHAR(5) ,\n" +
            "PRIVATE_POLICY_SEND_TIME TIMESTAMP(6) ,\n" +
            "PRIVATE_POLICY_CONFIRM_TIME TIMESTAMP(6) ,\n" +
            "UPPWD_DATE TIMESTAMP(6) ,\n" +
            "DIRECT_SELL_USER_ID VARCHAR(100)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_LYX_LY_MTMEMBER', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String LYX_LYMTM_LEVEL_UNIT  = "CREATE TABLE T_ODS_LYX_LYMTM_LEVEL_UNIT (\n" +
            "    ETL_DATE DATE,\n" +
            "ID BIGINT,\n" +
            "MTM_CARD_NUM VARCHAR(10),\n" +
            "LEVEL_CODE VARCHAR(8),\n" +
            "SPE_IDENTIFICATION VARCHAR(80),\n" +
            "GUEST_SCODE VARCHAR(30),\n" +
            "MTM_POST VARCHAR(300),\n" +
            "GUEST_SNAME VARCHAR(200),\n" +
            "GUEST_SADDR VARCHAR(80),\n" +
            "EFFECTIVE_TIME TIMESTAMP(6),\n" +
            "EFFECTIVE_OPERATOR VARCHAR(20),\n" +
            "EXPIRES_TIME TIMESTAMP(6),\n" +
            "EXPIRES_OPERATOR VARCHAR(20),\n" +
            "UPD_REMARK VARCHAR(100),\n" +
            "VERSION BIGINT,\n" +
            "DEL_FLG VARCHAR(2),\n" +
            "ORIG_LEVEL_CODE VARCHAR(4),\n" +
            "CREATE_TIME TIMESTAMP(6),\n" +
            "UPDATE_TIME TIMESTAMP(6) \n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_LYX_LYMTM_LEVEL_UNIT', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String LYX_MTM_LYVAL_SUMMARY = "CREATE TABLE T_ODS_LYX_MTM_LYVAL_SUMMARY (\n" +
            "    ETL_DATE DATE,\n" +
            "ID BIGINT ,\n" +
            "SETTLE_CYCLE VARCHAR(8) ,\n" +
            "CYCLE_LYVAL DECIMAL(12,2) ,\n" +
            "USABLE_LYVAL DECIMAL(12,2) ,\n" +
            "PRE_LOSE_LYVAL DECIMAL(12,2) ,\n" +
            "EXPIRED_LYVAL DECIMAL(12,2) ,\n" +
            "CONSUMED_LYVAL DECIMAL(12,2) ,\n" +
            "ACCOUNT_FLG VARCHAR(2) ,\n" +
            "MPUPI VARCHAR(2) ,\n" +
            "DEL_FLG VARCHAR(2) ,\n" +
            "VERSION BIGINT ,\n" +
            "CREATE_ID VARCHAR(20) ,\n" +
            "CREATE_TIME TIMESTAMP(6) ,\n" +
            "UPDATE_ID VARCHAR(20) ,\n" +
            "UPDATE_TIME TIMESTAMP(6) ,\n" +
            "MTM_CARD_NUM VARCHAR(20)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_LYX_MTM_LYVAL_SUMMARY', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String LYX_LYMTM_CERTIFICATES = "CREATE TABLE T_ODS_LYX_LYMTM_CERTIFICATES (\n" +
            "    ETL_DATE DATE,\n" +
            "ID BIGINT ,\n" +
            "MTM_CARD_NUM VARCHAR(100) ,\n" +
            "ID_TYPE VARCHAR(4) ,\n" +
            "ID_NUM VARCHAR(100) ,\n" +
            "MTM_ID_FLG VARCHAR(2) ,\n" +
            "ID_STATUS VARCHAR(2) ,\n" +
            "EFFECTIVE_TIME TIMESTAMP(6) ,\n" +
            "EXPIRES_TIME TIMESTAMP(6) ,\n" +
            "DOC_PHOTO1 VARCHAR(500) ,\n" +
            "DOC_PHOTO2 VARCHAR(500) ,\n" +
            "DOC_PHOTO3 VARCHAR(500) ,\n" +
            "DOC_PHOTO4 VARCHAR(500) ,\n" +
            "DOC_PHOTO5 VARCHAR(500) ,\n" +
            "EXPIRES_OPERATOR VARCHAR(20) ,\n" +
            "VERSION BIGINT ,\n" +
            "DEL_FLG VARCHAR(2) ,\n" +
            "CREATE_ID VARCHAR(20) ,\n" +
            "CREATE_TIME TIMESTAMP(6) ,\n" +
            "UPDATE_ID VARCHAR(20) ,\n" +
            "UPDATE_TIME TIMESTAMP(6) ,\n" +
            "ID_NUM_LYGJ VARCHAR(256)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_LYX_LYMTM_CERTIFICATES', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    //常客建表
    public static String CLK_FFP_REGISTER_INFO = "CREATE TABLE T_ODS_CLK_FFP_REGISTER_INFO (\n" +
            "    ETL_DATE DATE,\n" +
            "ID VARCHAR(64) ,\n" +
            "FID VARCHAR(255) ,\n" +
            "FFLTDATE VARCHAR(255) ,\n" +
            "FNAMECN VARCHAR(255) ,\n" +
            "F_FIRTST_NAME_CN VARCHAR(255) ,\n" +
            "F_SECOND_NAME_CN VARCHAR(255) ,\n" +
            "FNAMEEN VARCHAR(255) ,\n" +
            "F_FIRST_NAME_EN VARCHAR(255) ,\n" +
            "F_SECOND_NAME_EN VARCHAR(255) ,\n" +
            "FSEX VARCHAR(10) ,\n" +
            "FBIRTHDAY VARCHAR(255) ,\n" +
            "FCARDTYPE VARCHAR(50) ,\n" +
            "FIDCARD VARCHAR(256) ,\n" +
            "FPHONE VARCHAR(256) ,\n" +
            "FADDRESS VARCHAR(255) ,\n" +
            "FFLTNUM VARCHAR(50) ,\n" +
            "FSEATNUM VARCHAR(20) ,\n" +
            "FCARD VARCHAR(50) ,\n" +
            "FDEPT VARCHAR(50) ,\n" +
            "FUSER VARCHAR(50) ,\n" +
            "FUPDATE VARCHAR(50) ,\n" +
            "FDOWN VARCHAR(10) ,\n" +
            "FOPERATOR VARCHAR(100) ,\n" +
            "FDEMO VARCHAR(255) ,\n" +
            "BL1 VARCHAR(500) ,\n" +
            "EMAIL VARCHAR(50) ,\n" +
            "CITY VARCHAR(200) ,\n" +
            "PROVINCE VARCHAR(200) ,\n" +
            "CARRIER_AIRLINE VARCHAR(10) ,\n" +
            "PWD VARCHAR(50) ,\n" +
            "SEND_STATUS VARCHAR(2) ,\n" +
            "SEND_TIME DATE ,\n" +
            "PARENT_FFP_NUM VARCHAR(50) ,\n" +
            "OPERATE_DEPT VARCHAR(40) ,\n" +
            "ORG VARCHAR(10) ,\n" +
            "DEST VARCHAR(10) ,\n" +
            "OP_ACCOUNT VARCHAR(50) ,\n" +
            "OP_NAME VARCHAR(50) ,\n" +
            "CREW VARCHAR(300) ,\n" +
            "BOOKING_CLASS VARCHAR(10) ,\n" +
            "TKT_NUM VARCHAR(20) ,\n" +
            "BOARDING_NUMBER VARCHAR(20) ,\n" +
            "RETRO_FLAG VARCHAR(5) ,\n" +
            "WS_FEED_MSG VARCHAR(500) ,\n" +
            "RETRO_FEED_MSG VARCHAR(500) ,\n" +
            "DEV_ACCOUNT VARCHAR(50) ,\n" +
            "DATA_ORIGIN VARCHAR(10) ,\n" +
            "CREW_BASE VARCHAR(100) ,\n" +
            "INTEGRAL_FLAG VARCHAR(10) ,\n" +
            "SEND_FLAG VARCHAR(10) ,\n" +
            "RETRO_DATE DATE ,\n" +
            "REMARKS VARCHAR(255) ,\n" +
            "DEL_FLAG CHAR(1) ,\n" +
            "CREATE_BY VARCHAR(64) ,\n" +
            "CREATE_DATE TIMESTAMP(6) ,\n" +
            "UPDATE_BY VARCHAR(64) ,\n" +
            "UPDATE_DATE TIMESTAMP(6) ,\n" +
            "STATUS_FLG VARCHAR(10) ,\n" +
            "ETL_INSERT_DT DATE ,\n" +
            "ETL_UPD_DT DATE ,\n" +
            "HF_FLAG VARCHAR(1) ,\n" +
            "COUNTRY_CODE VARCHAR(15) ,\n" +
            "ETL_HANDLE_FLAG VARCHAR(10)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_CLK_FFP_REGISTER_INFO', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String CLK_FFP_REGISTER_SCAN = "CREATE TABLE T_ODS_CLK_FFP_REGISTER_SCAN (\n" +
            "    ID VARCHAR(64) NOT NULL,\n" +
            "    TRADEID VARCHAR(30),\n" +
            "    CREATE_TIME DATE,\n" +
            "    CNNAME VARCHAR(50),\n" +
            "    FLTDATE VARCHAR(30),\n" +
            "    FLTNUM VARCHAR(30),\n" +
            "    RESERVED_SEAT VARCHAR(30),\n" +
            "    FPHONE VARCHAR(256),\n" +
            "    FCARD_TYPE VARCHAR(30),\n" +
            "    MATCH_FLAG CHAR(1),\n" +
            "    FFP_REGISTER_INFO_ID VARCHAR(64),\n" +
            "    REMARKS VARCHAR(255),\n" +
            "    ETL_CREATE_TIME TIMESTAMP(3),\n" +
            "    ETL_UPDATE_TIME TIMESTAMP(3),\n" +
            "    ETL_DATE DATE\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_CLK_FFP_REGISTER_SCAN',\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String FFP_FZ_USER_SCORE = "CREATE TABLE `T_ODS_CLK_FFP_FZ_USER_SCORE` (\n" +
            "  `ID` VARCHAR(320) NOT NULL,\n" +
            "  `USERCODE` VARCHAR(250),\n" +
            "  `INTEGRAL` INT,\n" +
            "  `ADD_DATE` DATE,\n" +
            "  `DEPT` VARCHAR(500),\n" +
            "  `FFP_SDMEMBER_ATTRS_ID` VARCHAR(320),\n" +
            "  `FLAG` TINYINT,\n" +
            "  `REST_INTEGRAL` DECIMAL(7, 2),\n" +
            "  `REMARKS` VARCHAR(1275),\n" +
            "  `CREATE_BY` VARCHAR(320),\n" +
            "  `CREATE_DATE` TIMESTAMP,\n" +
            "  `UPDATE_BY` VARCHAR(320),\n" +
            "  `UPDATE_DATE` TIMESTAMP,\n" +
            "  `DEL_FLAG` VARCHAR(1),\n" +
            "  `EXP_DATE` DATE,\n" +
            "  `ETL_CREATE_TIME` TIMESTAMP(6),\n" +
            "  `ETL_UPDATE_TIME` TIMESTAMP(6),\n" +
            "  `ETL_DATE` DATE" +

            ") WITH (\n" +
            " 'connector' = 'doris',\n" +
            "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
            "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
            " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_CLK_FFP_FZ_USER_SCORE',\n" +
            "'sink.label-prefix' = '" + System.currentTimeMillis() +  UUID.randomUUID() + "'," +
            " 'sink.properties.read_json_by_line' = 'true'," +
            " 'sink.properties.format' = 'json'," +
            "    'username' = '" + Constants.ODS_USER + "',\n" +
            "    'password' = '" + Constants.ODS_PWD + "'\n" +
            ")";

    public static String CLK_T_DW_D_FFP = "CREATE TABLE T_ODS_CLK_T_DW_D_FFP (\n" +
            "    ETL_DATE DATE,\n" +
            "FFP_ID VARCHAR(15) ,\n" +
            "AGE_ID DECIMAL ,\n" +
            "GUEST_ID DECIMAL ,\n" +
            "B2C_MEM_ID VARCHAR(20) ,\n" +
            "BRAND_ID VARCHAR(20) ,\n" +
            "PR_MOBILE_ID DECIMAL ,\n" +
            "LST_MOBILE_ID DECIMAL ,\n" +
            "HIGH_MOBILE_ID DECIMAL ,\n" +
            "REGION_ID VARCHAR(20) ,\n" +
            "SC_TIER_ID VARCHAR(30) ,\n" +
            "FFP_TIER_ID VARCHAR(30) ,\n" +
            "CHANNEL_DEV_ID DECIMAL ,\n" +
            "GENDER_ID VARCHAR(10) ,\n" +
            "FFP_CREATED TIMESTAMP(6),\n" +
            "FFP_LAST_UPD TIMESTAMP(6),\n" +
            "MEM_NUM VARCHAR(30),\n" +
            "MEM_TYPE_CD VARCHAR(30),\n" +
            "CONTACT_ID VARCHAR(15),\n" +
            "EXPIRY_DT TIMESTAMP(6),\n" +
            "LAST_ACCR_TXN_DT TIMESTAMP(6),\n" +
            "LAST_RDM_TXN_DT TIMESTAMP(6),\n" +
            "LAST_TXN_PROCED_DT TIMESTAMP(6),\n" +
            "LFTM_PT_TYPE_A_VAL DECIMAL(22,7),\n" +
            "LFTM_PT_TYPE_B_VAL DECIMAL(22,7),\n" +
            "LFTM_PT_TYPE_D_VAL DECIMAL(22,7),\n" +
            "MAX_POINT_LOAN DECIMAL(22,7),\n" +
            "POINT_TYPE_A_VAL DECIMAL(22,7),\n" +
            "POINT_TYPE_B_VAL DECIMAL(22,7),\n" +
            "POINT_TYPE_C_VAL DECIMAL(22,7),\n" +
            "START_DT TIMESTAMP(6),\n" +
            "SUBMIT_DT TIMESTAMP(6),\n" +
            "NAME VARCHAR(100),\n" +
            "ORG_GROUP_ID VARCHAR(15),\n" +
            "PAR_MEM_ID VARCHAR(15),\n" +
            "STATUS_CD VARCHAR(30),\n" +
            "VAL_SCORE VARCHAR(30),\n" +
            "MEM_VAL VARCHAR(30),\n" +
            "SUBMIT_PERSON VARCHAR(30),\n" +
            "TEST_MEM_FLAG VARCHAR(1),\n" +
            "PR_MEM_CARD_ID VARCHAR(15),\n" +
            "ABBRE_NAME VARCHAR(30),\n" +
            "BLOCK_LIST VARCHAR(20),\n" +
            "CARD_NAME VARCHAR(50),\n" +
            "FLIGHT_FRQ_FLAG VARCHAR(1),\n" +
            "FLIGHT_TIMES DECIMAL(10,0),\n" +
            "FORCE_CREATE_FLG VARCHAR(1),\n" +
            "FROZ_POINT DECIMAL(10,0),\n" +
            "NONAIR_FRQ_FLAG VARCHAR(1),\n" +
            "TRANSFER VARCHAR(30),\n" +
            "TRANSFER_END_DT TIMESTAMP(6),\n" +
            "TRANSFER_START_DT TIMESTAMP(6),\n" +
            "CARD_FLG VARCHAR(5),\n" +
            "QF_POINT DECIMAL(22,0),\n" +
            "QF_SEGMENT DECIMAL(22,2),\n" +
            "QF_UPDATE_DT TIMESTAMP(6),\n" +
            "LAST_AIR_TXN_DT TIMESTAMP(6),\n" +
            "SLEEP_AIR_TXN_DT DECIMAL,\n" +
            "LAST_ALL_TXN_DT TIMESTAMP(6),\n" +
            "SLEEP_ALL_TXN_DT DECIMAL,\n" +
            "LAST_SCAIR_TXN_DT TIMESTAMP(6),\n" +
            "SLEEP_SCAIR_TXN_DT DECIMAL,\n" +
            "POINT_A DECIMAL(22,7),\n" +
            "TXN_CREATED TIMESTAMP(6),\n" +
            "TXN_LAST_UPD TIMESTAMP(6),\n" +
            "SC_VALUE DECIMAL(22,7),\n" +
            "IS_BIG_GUEST VARCHAR(1),\n" +
            "IS_BUS_MEM VARCHAR(1),\n" +
            "PIN_STATUS VARCHAR(50),\n" +
            "PR_ADDR VARCHAR(500),\n" +
            "PR_COMPANY VARCHAR(100),\n" +
            "PR_DEPART VARCHAR(100),\n" +
            "PR_ADDR_TYPE VARCHAR(50),\n" +
            "PR_ADDR_CITY VARCHAR(50),\n" +
            "PR_ADDR_STATE VARCHAR(50),\n" +
            "PR_ADDR_ZIPCODE VARCHAR(50),\n" +
            "PR_EMAIL VARCHAR(100),\n" +
            "PR_EMAIL_TYPE VARCHAR(30),\n" +
            "PR_MOBILE VARCHAR(50),\n" +
            "LST_MOBILE VARCHAR(50),\n" +
            "PR_ID VARCHAR(100),\n" +
            "PR_CATEGORY_CD VARCHAR(30),\n" +
            "PR_EXPIRATION_TS TIMESTAMP(6),\n" +
            "PR_ISSUE_TS TIMESTAMP(6),\n" +
            "PR_ISSUING_COUNTRY VARCHAR(30),\n" +
            "HIGH_SCORE_MOBILE VARCHAR(50),\n" +
            "PR_CHECKIN VARCHAR(100),\n" +
            "PR_PORT_CITY VARCHAR(200),\n" +
            "PR_BUY_CHANNEL VARCHAR(100),\n" +
            "PR_EX_AIRLINE VARCHAR(100),\n" +
            "PR_EX_SC_SEGMENT VARCHAR(100),\n" +
            "PR_EX_SC_CABIN VARCHAR(100),\n" +
            "PR_EX_SC_SEAT VARCHAR(100),\n" +
            "P_MOBILE VARCHAR(100),\n" +
            "P_PRODUCT VARCHAR(200),\n" +
            "P_ACTIVITY VARCHAR(200),\n" +
            "P_BUY_TICKETS VARCHAR(100),\n" +
            "P_SEAT VARCHAR(100),\n" +
            "P_CABIN VARCHAR(100),\n" +
            "P_MEALS VARCHAR(100),\n" +
            "FFP_TIER_START_DT TIMESTAMP(6),\n" +
            "FFP_TIER_END_DT TIMESTAMP(6),\n" +
            "FFP_PTIER VARCHAR(50),\n" +
            "CON_LAST_UPD TIMESTAMP(6),\n" +
            "EN_FST_NAME VARCHAR(50),\n" +
            "EN_LAST_NAME VARCHAR(50),\n" +
            "CN_FST_NAME VARCHAR(50),\n" +
            "CN_LAST_NAME VARCHAR(50),\n" +
            "SP_FST_NAME VARCHAR(50),\n" +
            "SP_LST_NAME VARCHAR(50),\n" +
            "TS_FST_NAME VARCHAR(50),\n" +
            "TS_LST_NAME VARCHAR(50),\n" +
            "BIRTH_DT TIMESTAMP(6),\n" +
            "JOB_TITLE VARCHAR(75),\n" +
            "NATIONALITY VARCHAR(30),\n" +
            "PER_TITLE VARCHAR(15),\n" +
            "MARRIAGE_STATUS VARCHAR(20),\n" +
            "DATA_ACTIVE_FLG VARCHAR(1),\n" +
            "SOURCE_ID DECIMAL,\t\n" +
            "ETL_INSERT_TIME TIMESTAMP(6) ,\n" +
            "ETL_UPDATE_TIME TIMESTAMP(6) ,\n" +
            "SC_TYPE_RFM VARCHAR(200)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_CLK_T_DW_D_FFP', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String CLK_T_DW_F_SCFFP_CHANNEL = "CREATE TABLE T_ODS_CLK_T_DW_F_SCFFP_CHANNEL (\n" +
            "    ETL_DATE DATE,\n" +
            "ROW_ID BIGINT,\n" +
            "CHANNEL_CODE VARCHAR(50),\n" +
            "CHANNEL_NAME VARCHAR(100),\n" +
            "DEP_CODE VARCHAR(100),\n" +
            "DEP_NAME VARCHAR(100),\n" +
            "UNINT_CODE VARCHAR(100),\n" +
            "UNINT_NAME VARCHAR(100),\n" +
            "USER_CODE VARCHAR(100),\n" +
            "USER_NAME VARCHAR(100),\n" +
            "START_DATE TIMESTAMP(6),\n" +
            "MEM_NUM VARCHAR(50),\n" +
            "DATA_SOURCE VARCHAR(10)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_CLK_T_DW_F_SCFFP_CHANNEL', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String CLK_ACCU = "CREATE TABLE T_ODS_CLK_ACCU (\n" +
            " ETL_DATE DATE ,\n" +
            "ID BIGINT ,\n" +
            "DOCNAME VARCHAR(50) ,\n" +
            "TOTALRECORD BIGINT ,\n" +
            "TOTALAMOUNT BIGINT ,\n" +
            "TOTALVALUE DECIMAL(18,5) ,\n" +
            "BILLINGMONTH VARCHAR(6) ,\n" +
            "BILLINGSTARTDT VARCHAR(8) ,\n" +
            "BILLINGENDDT VARCHAR(8) ,\n" +
            "MEMBERNO VARCHAR(35) ,\n" +
            "MEMBERTIERCODE VARCHAR(30) ,\n" +
            "MEMBERBRAND VARCHAR(3) ,\n" +
            "BIZTYPECODE VARCHAR(10) ,\n" +
            "BIZSUBTYPECODE VARCHAR(10) ,\n" +
            "CHANNELCODE VARCHAR(30) ,\n" +
            "PARTNERCODE VARCHAR(20) ,\n" +
            "EVENTNO VARCHAR(20) ,\n" +
            "ACTIVITYID VARCHAR(20) ,\n" +
            "TRANID VARCHAR(50) ,\n" +
            "ORDERNO VARCHAR(50) ,\n" +
            "TKTNO VARCHAR(20) ,\n" +
            "COUPONNO VARCHAR(2) ,\n" +
            "BILLCOMPANY VARCHAR(5) ,\n" +
            "BILLEDCOMPANY VARCHAR(5) ,\n" +
            "MILES BIGINT ,\n" +
            "CURRENCY VARCHAR(3) ,\n" +
            "SALESPRICE DECIMAL(10,5) ,\n" +
            "COSTSPRICE DECIMAL(10,5) ,\n" +
            "`VALUE` DECIMAL(18,5) ,\n" +
            "EXCHPOINT DECIMAL(6,5) ,\n" +
            "ACTIVITYDATE VARCHAR(8) ,\n" +
            "FLIGHTDATE VARCHAR(8) ,\n" +
            "OC VARCHAR(3) ,\n" +
            "OCFLIGHTNO VARCHAR(4) ,\n" +
            "OCCABIN VARCHAR(2) ,\n" +
            "OCSUBCLASS VARCHAR(2) ,\n" +
            "UPLSTN VARCHAR(3) ,\n" +
            "DESSTN VARCHAR(3) ,\n" +
            "ASS VARCHAR(1) ,\n" +
            "IRN VARCHAR(16) ,\n" +
            "OAN VARCHAR(16) \n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_CLK_ACCU', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";


    public static String CLK_EXCH = "CREATE TABLE T_ODS_CLK_EXCH (\n" +
            " ETL_DATE DATE ,\n" +
            "ID BIGINT ,\n" +
            "DOCNAME VARCHAR(50) ,\n" +
            "TOTALRECORD BIGINT ,\n" +
            "TOTALAMOUNT BIGINT ,\n" +
            "TOTALVALUE DECIMAL(18,5) ,\n" +
            "BILLINGMONTH VARCHAR(6) ,\n" +
            "BILLINGSTARTDT VARCHAR(8) ,\n" +
            "BILLINGENDDT VARCHAR(8) ,\n" +
            "MEMBERNO VARCHAR(35) ,\n" +
            "MEMBERTIERCODE VARCHAR(30) ,\n" +
            "MEMBERBRAND VARCHAR(3) ,\n" +
            "BIZTYPECODE VARCHAR(10) ,\n" +
            "BIZSUBTYPECODE VARCHAR(10) ,\n" +
            "EXCHCHANNEL VARCHAR(30) ,\n" +
            "PARTNERCODE VARCHAR(20) ,\n" +
            "EXCHNO VARCHAR(20) ,\n" +
            "ACTIVITYID VARCHAR(20) ,\n" +
            "TRANID VARCHAR(50) ,\n" +
            "ORDERNO VARCHAR(50) ,\n" +
            "TKTNO VARCHAR(20) ,\n" +
            "COUPONNO VARCHAR(2) ,\n" +
            "BILLCOMPANY VARCHAR(5) ,\n" +
            "BILLEDCOMPANY VARCHAR(5) ,\n" +
            "MILES BIGINT ,\n" +
            "`VALUE` DECIMAL(18,5) ,\n" +
            "TPM VARCHAR(50) ,\n" +
            "CURRENCY VARCHAR(3) ,\n" +
            "SALESPRICE DECIMAL(18,5) ,\n" +
            "COSTSPRICE DECIMAL(18,5) ,\n" +
            "PNR VARCHAR(10) ,\n" +
            "EXCHDATE VARCHAR(8) ,\n" +
            "FLIGHTDATE VARCHAR(8) ,\n" +
            "OC VARCHAR(3) ,\n" +
            "OCFLIGHTNO VARCHAR(4) ,\n" +
            "OCCABIN VARCHAR(2) ,\n" +
            "OCSUBCLASS VARCHAR(2) ,\n" +
            "BUSUBCLASS VARCHAR(2) ,\n" +
            "UPLSTN VARCHAR(3) ,\n" +
            "DESSTN VARCHAR(3) ,\n" +
            "COMMODITYNO VARCHAR(20) ,\n" +
            "PRODCUINFO VARCHAR(50) ,\n" +
            "ASS VARCHAR(1)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_CLK_EXCH', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String CLK_EXCHINT = "CREATE TABLE T_ODS_CLK_EXCHINT (\n" +
            " ETL_DATE DATE ,\n" +
            "ID BIGINT ,\n" +
            "DOCNAME VARCHAR(50) ,\n" +
            "TOTALRECORD BIGINT ,\n" +
            "TOTALAMOUNT BIGINT ,\n" +
            "TOTALVALUE DECIMAL(18,5) ,\n" +
            "BILLINGMONTH VARCHAR(6) ,\n" +
            "BILLINGSTARTDT VARCHAR(8) ,\n" +
            "BILLINGENDDT VARCHAR(8) ,\n" +
            "MEMBERNO VARCHAR(35) ,\n" +
            "MEMBERTIERCODE VARCHAR(30) ,\n" +
            "MEMBERBRAND VARCHAR(3) ,\n" +
            "BIZTYPECODE VARCHAR(10) ,\n" +
            "BIZSUBTYPECODE VARCHAR(10) ,\n" +
            "EXCHCHANNEL VARCHAR(30) ,\n" +
            "PARTNERCODE VARCHAR(20) ,\n" +
            "EXCHNO VARCHAR(20) ,\n" +
            "ACTIVITYID VARCHAR(20) ,\n" +
            "TRANID VARCHAR(50) ,\n" +
            "ORDERNO VARCHAR(50) ,\n" +
            "TKTNO VARCHAR(20) ,\n" +
            "COUPONNO VARCHAR(2) ,\n" +
            "BILLCOMPANY VARCHAR(5) ,\n" +
            "BILLEDCOMPANY VARCHAR(5) ,\n" +
            "MILES BIGINT ,\n" +
            "`VALUE` DECIMAL(18,5) ,\n" +
            "TPM VARCHAR(50) ,\n" +
            "CURRENCY VARCHAR(3) ,\n" +
            "SALESPRICE DECIMAL(18,5) ,\n" +
            "COSTSPRICE DECIMAL(18,5) ,\n" +
            "PNR VARCHAR(10) ,\n" +
            "EXCHDATE VARCHAR(8) ,\n" +
            "FLIGHTDATE VARCHAR(8) ,\n" +
            "OC VARCHAR(3) ,\n" +
            "OCFLIGHTNO VARCHAR(4) ,\n" +
            "OCCABIN VARCHAR(2) ,\n" +
            "OCSUBCLASS VARCHAR(2) ,\n" +
            "BUSUBCLASS VARCHAR(2) ,\n" +
            "UPLSTN VARCHAR(3) ,\n" +
            "DESSTN VARCHAR(3) ,\n" +
            "COMMODITYNO VARCHAR(20) ,\n" +
            "PRODCUINFO VARCHAR(50) ,\n" +
            "ASS VARCHAR(1)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_CLK_EXCHINT', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    public static String ZSF_ODR_ORDER_CUSTOMER_RELATION = "CREATE TABLE T_ODS_ZSF_ODR_ORDER_CUSTOMER_RELATION (\n" +
            "    ETL_DATE DATE NOT NULL,\n" +
            "    ID VARCHAR(32) NOT NULL,\n" +
            "    MAIN_ORDER_ID VARCHAR(32),\n" +
            "    MAIN_ORDER_NO VARCHAR(32),\n" +
            "    CUSTOMER_ID VARCHAR(32),\n" +
            "    CUSTOMER_NAME VARCHAR(128),\n" +
            "    CUSTOMER_MOBILE VARCHAR(32),\n" +
            "    IS_REAL_NAME CHAR(1),\n" +
            "    CONTACTS_MOBILE VARCHAR(512),\n" +
            "    CONTACTS_NAME VARCHAR(128),\n" +
            "    CONTACTS_EMAIL VARCHAR(512),\n" +
            "    OPEN_ID VARCHAR(64),\n" +
            "    MEMBER_ID VARCHAR(32),\n" +
            "    MEMBER_LEVEL VARCHAR(32),\n" +
            "    MEMBER_TYPE VARCHAR(32),\n" +
            "    LIMIT_TYPE VARCHAR(8),\n" +
            "    LIMIT_ID VARCHAR(16),\n" +
            "    ENABLE CHAR(1) NOT NULL,\n" +
            "    CREATE_TIME TIMESTAMP(6) NOT NULL,\n" +
            "    CREATE_USER VARCHAR(128) NOT NULL,\n" +
            "    UPDATE_TIME TIMESTAMP(6) NOT NULL,\n" +
            "    UPDATE_USER VARCHAR(128) NOT NULL" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
            "    'table-name' = 'T_ODS_ZSF_ODR_ORDER_CUSTOMER_RELATION', -- 替换为实际的表名\n" +
            "    'username' = '" + Constants.ODS_USER + "',\n" +
            "    'password' = '" + Constants.ODS_PWD + "'\n" +
            ")";

    public static String ZSF_ODR_ORDER_CUSTOMER_RELATION_DOUYIN = "CREATE TABLE T_ODS_ZSF_ODR_ORDER_CUSTOMER_RELATION_DOUYIN (\n" +
            "    ETL_DATE DATE NOT NULL,\n" +
            "    ID VARCHAR(32) NOT NULL,\n" +
            "    MAIN_ORDER_ID VARCHAR(32),\n" +
            "    MAIN_ORDER_NO VARCHAR(32),\n" +
            "    CUSTOMER_ID VARCHAR(32),\n" +
            "    CUSTOMER_NAME VARCHAR(128),\n" +
            "    CUSTOMER_MOBILE VARCHAR(128),\n" +
            "    IS_REAL_NAME CHAR(1),\n" +
            "    CONTACTS_MOBILE VARCHAR(128),\n" +
            "    CONTACTS_NAME VARCHAR(128),\n" +
            "    CONTACTS_EMAIL VARCHAR(128),\n" +
            "    OPEN_ID VARCHAR(64),\n" +
            "    MEMBER_ID VARCHAR(32),\n" +
            "    MEMBER_LEVEL VARCHAR(32),\n" +
            "    MEMBER_TYPE VARCHAR(32),\n" +
            "    LIMIT_TYPE VARCHAR(8),\n" +
            "    LIMIT_ID VARCHAR(32),\n" +
            "    ENABLE CHAR(1),\n" +
            "    CREATE_TIME TIMESTAMP(6),\n" +
            "    CREATE_USER VARCHAR(128),\n" +
            "    UPDATE_TIME TIMESTAMP(6),\n" +
            "    UPDATE_USER VARCHAR(128)" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
            "    'table-name' = 'T_ODS_ZSF_ODR_ORDER_CUSTOMER_RELATION_DOUYIN', -- 替换为实际的表名\n" +
            "    'username' = '" + Constants.ODS_USER + "',\n" +
            "    'password' = '" + Constants.ODS_PWD + "'\n" +
            ")";

    public static String ZSF_ODR_ORDER_DETAIL_INSURANCE = "CREATE TABLE T_ODS_ZSF_ODR_ORDER_DETAIL_INSURANCE (\n" +
            "    ETL_DATE DATE NOT NULL,\n" +
            "    ID VARCHAR(32) NOT NULL,\n" +
            "    ORDER_DETAIL_ID VARCHAR(32) NOT NULL,\n" +
            "    ORDER_NO VARCHAR(32) NOT NULL,\n" +
            "    MAIN_ORDER_NO VARCHAR(32),\n" +
            "    MAIN_ORDER_ID VARCHAR(32),\n" +
            "    SERIAL_NUMBER VARCHAR(32),\n" +
            "    OPERATE_SERIAL_NUMBER VARCHAR(32),\n" +
            "    INSTANCE_ID VARCHAR(32),\n" +
            "    INSTANCE_NO VARCHAR(32),\n" +
            "    INS_TRADE_NO VARCHAR(32),\n" +
            "    INS_PRICE DECIMAL(10,2),\n" +
            "    INS_AMOUNT DECIMAL(10,2),\n" +
            "    INS_TYPE VARCHAR(20),\n" +
            "    INS_STATUS VARCHAR(2),\n" +
            "    INS_COMPANY_CODE VARCHAR(30),\n" +
            "    INS_COMPANY STRING,\n" +
            "    INS_CREATE_TIME TIMESTAMP(6),\n" +
            "    INS_COMPLETE_TIME TIMESTAMP(6),\n" +
            "    INS_RETURN_TIME TIMESTAMP(6),\n" +
            "    HOLDER_NAME VARCHAR(50),\n" +
            "    HOLDER_TYPE VARCHAR(10),\n" +
            "    HOLDER_ID VARCHAR(400),\n" +
            "    HOLDER_PHONE VARCHAR(400),\n" +
            "    HOLDER_EMAIL VARCHAR(128),\n" +
            "    HOLDER_ADDRESS VARCHAR(256),\n" +
            "    BENEFICIARY_TYPE VARCHAR(2),\n" +
            "    BENEFICIARY_NAME VARCHAR(50),\n" +
            "    BENEFICIARY_ID VARCHAR(400),\n" +
            "    BENEFICIARY_ID_TYPE VARCHAR(10),\n" +
            "    PSGR_NAME VARCHAR(100),\n" +
            "    CERT_TYPE VARCHAR(100),\n" +
            "    CERT_NO VARCHAR(400),\n" +
            "    PSGR_PHONE VARCHAR(400),\n" +
            "    PSGR_MAIL VARCHAR(512),\n" +
            "    FLIGHT_DATE VARCHAR(10),\n" +
            "    FLIGHT_TIME VARCHAR(10),\n" +
            "    ORG_CITY VARCHAR(3),\n" +
            "    DST_CITY VARCHAR(3),\n" +
            "    FLIGHT_NO VARCHAR(10),\n" +
            "    CARRIER VARCHAR(5),\n" +
            "    TICKET_NO VARCHAR(20),\n" +
            "    TICKET_PRICE DECIMAL(10,2),\n" +
            "    INTERNATIONAL_FLAG VARCHAR(20),\n" +
            "    ENABLE CHAR(1) NOT NULL,\n" +
            "    CREATE_TIME TIMESTAMP(6) NOT NULL,\n" +
            "    CREATE_USER VARCHAR(128) NOT NULL,\n" +
            "    UPDATE_TIME TIMESTAMP(6) NOT NULL,\n" +
            "    UPDATE_USER VARCHAR(128) NOT NULL,\n" +
            "    RELATION_INSURANT VARCHAR(10),\n" +
            "    ASSIGN_BENEFICIARY CHAR(1),\n" +
            "    INS_ORDER_NO VARCHAR(32),\n" +
            "    BIRTH_DATE VARCHAR(50),\n" +
            "    CUSTOMER_BIRTHDAY VARCHAR(50)" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
            "    'table-name' = 'T_ODS_ZSF_ODR_ORDER_DETAIL_INSURANCE', -- 替换为实际的表名\n" +
            "    'username' = '" + Constants.ODS_USER + "',\n" +
            "    'password' = '" + Constants.ODS_PWD + "'\n" +
            ")";

    public static String ZSF_ODR_ORDER_DETAIL_SEAT = "CREATE TABLE T_ODS_ZSF_ODR_ORDER_DETAIL_SEAT (\n" +
            "    ETL_DATE DATE NOT NULL,\n" +
            "    ID VARCHAR(32) NOT NULL,\n" +
            "    ORDER_DETAIL_ID VARCHAR(32) NOT NULL,\n" +
            "    ORDER_NO VARCHAR(32) NOT NULL,\n" +
            "    MAIN_ORDER_NO VARCHAR(32),\n" +
            "    MAIN_ORDER_ID VARCHAR(32),\n" +
            "    INSTANCE_ID VARCHAR(32),\n" +
            "    INSTANCE_NO VARCHAR(32),\n" +
            "    MAIN_FLAG VARCHAR(20) NOT NULL,\n" +
            "    TICKET_NO VARCHAR(20) NOT NULL,\n" +
            "    ADT_TICKET_NO VARCHAR(20),\n" +
            "    PNR VARCHAR(8),\n" +
            "    CARRIER VARCHAR(255),\n" +
            "    FLIGHT_NO VARCHAR(32),\n" +
            "    DEP_DATE_TIME TIMESTAMP(6),\n" +
            "    DEP_DATE VARCHAR(10),\n" +
            "    ARR_DATE VARCHAR(10),\n" +
            "    DEP_TIME VARCHAR(10),\n" +
            "    ARR_TIME VARCHAR(10),\n" +
            "    DEP_TERMINAL VARCHAR(10),\n" +
            "    ARR_TERMINAL VARCHAR(10),\n" +
            "    FLIGHT_DURATION VARCHAR(30),\n" +
            "    ORG_CITY VARCHAR(3),\n" +
            "    STOP_CITY VARCHAR(3),\n" +
            "    DST_CITY VARCHAR(3),\n" +
            "    CABIN VARCHAR(2),\n" +
            "    PSGR_NAME VARCHAR(100),\n" +
            "    CERT_TYPE VARCHAR(100),\n" +
            "    CERT_NO VARCHAR(400),\n" +
            "    OLD_SEAT_NO VARCHAR(100),\n" +
            "    SEAT_NO VARCHAR(100),\n" +
            "    MOBILE VARCHAR(400),\n" +
            "    SEAT_STATUS VARCHAR(6),\n" +
            "    UPDATE_TIMES INT NOT NULL,\n" +
            "    SEAT_TIME TIMESTAMP(6),\n" +
            "    SEAT_SOURCE VARCHAR(255),\n" +
            "    SEAT_PRICE DECIMAL(10,2) NOT NULL,\n" +
            "    PSGR_TYPE VARCHAR(8),\n" +
            "    PSGR_NAME_EN VARCHAR(64),\n" +
            "    CHECKIN_STATUS VARCHAR(5),\n" +
            "    IS_DIFF VARCHAR(2),\n" +
            "    SEAT_TYPE VARCHAR(20),\n" +
            "    CHECK_IN_TYPE VARCHAR(2),\n" +
            "    APP_CHECK_IN_TYPE VARCHAR(2),\n" +
            "    SEG_INDEX INT,\n" +
            "    ENABLE CHAR(1) NOT NULL,\n" +
            "    CREATE_TIME TIMESTAMP(6) NOT NULL,\n" +
            "    CREATE_USER VARCHAR(128) NOT NULL,\n" +
            "    UPDATE_TIME TIMESTAMP(6) NOT NULL,\n" +
            "    UPDATE_USER VARCHAR(128) NOT NULL,\n" +
            "    EMD_TICKET_NO VARCHAR(20),\n" +
            "    IS_PRICE_SEAT VARCHAR(2)" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
            "    'table-name' = 'T_ODS_ZSF_ODR_ORDER_DETAIL_SEAT', -- 替换为实际的表名\n" +
            "    'username' = '" + Constants.ODS_USER + "',\n" +
            "    'password' = '" + Constants.ODS_PWD + "'\n" +
            ")";

    public static String ZSF_ODR_ORDER_DETAIL_SECONDARYCARD = "CREATE TABLE T_ODS_ZSF_ODR_ORDER_DETAIL_SECONDARYCARD (\n" +
            "    ETL_DATE DATE NOT NULL,\n" +
            "    ID VARCHAR(32) NOT NULL,\n" +
            "    ORDER_PACKAGE_ID VARCHAR(32),\n" +
            "    ORDER_DETAIL_ID VARCHAR(32),\n" +
            "    ORDER_NO VARCHAR(32),\n" +
            "    MAIN_ORDER_NO VARCHAR(32),\n" +
            "    MAIN_ORDER_ID VARCHAR(32),\n" +
            "    INSTANCE_ID VARCHAR(32),\n" +
            "    INSTANCE_NO VARCHAR(32),\n" +
            "    ACTIVITY_NO VARCHAR(32),\n" +
            "    PRODUCT_NO VARCHAR(32),\n" +
            "    PACKAGE_INDEX INT,\n" +
            "    SALE_PRICE DECIMAL(10,2),\n" +
            "    STATE VARCHAR(8),\n" +
            "    CUSTOMER_ID VARCHAR(32),\n" +
            "    MEMBER_ID VARCHAR(32),\n" +
            "    CUSTOMER_NAME VARCHAR(16),\n" +
            "    ACTIVE_TIME TIMESTAMP(6),\n" +
            "    REMARK VARCHAR(255),\n" +
            "    INTERNATIONAL_FLAG VARCHAR(20),\n" +
            "    CERT_TYPE VARCHAR(5),\n" +
            "    CERT_NO VARCHAR(32),\n" +
            "    CN_FIRST_NAME VARCHAR(255),\n" +
            "    CN_LAST_NAME VARCHAR(255),\n" +
            "    EN_FIRST_NAME VARCHAR(255),\n" +
            "    EN_LAST_NAME VARCHAR(255),\n" +
            "    EXPIRATION_DATE VARCHAR(32),\n" +
            "    SEX CHAR(1),\n" +
            "    BIRTHDAY VARCHAR(64),\n" +
            "    PASSPORT_ISSUE_NATION VARCHAR(64),\n" +
            "    PASSPORT_NATION VARCHAR(64),\n" +
            "    PHONE_NUM VARCHAR(32),\n" +
            "    ENABLE CHAR(1) NOT NULL,\n" +
            "    CREATE_TIME TIMESTAMP(6) NOT NULL,\n" +
            "    CREATE_USER VARCHAR(128) NOT NULL,\n" +
            "    UPDATE_USER VARCHAR(128) NOT NULL,\n" +
            "    UPDATE_TIME TIMESTAMP(6) NOT NULL,\n" +
            "    SEND_MSG_FLAG CHAR(1),\n" +
            "    EXCHANGE_NUM VARCHAR(10),\n" +
            "    EXPIRY_DATE TIMESTAMP(6),\n" +
            "    TRAVEL_START_DATE TIMESTAMP(6),\n" +
            "    TRAVEL_END_DATE TIMESTAMP(6)" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
            "    'table-name' = 'T_ODS_ZSF_ODR_ORDER_DETAIL_SECONDARYCARD', -- 替换为实际的表名\n" +
            "    'username' = '" + Constants.ODS_USER + "',\n" +
            "    'password' = '" + Constants.ODS_PWD + "'\n" +
            ")";

    public static String ZSF_ODR_ORDER_DETAIL_SECONDARY_DOUYIN = "CREATE TABLE T_ODS_ZSF_ODR_ORDER_DETAIL_SECONDARY_DOUYIN (\n" +
            "    ETL_DATE DATE NOT NULL,\n" +
            "    ID VARCHAR(32) NOT NULL,\n" +
            "    ORDER_PACKAGE_ID VARCHAR(32),\n" +
            "    ORDER_DETAIL_ID VARCHAR(32),\n" +
            "    ORDER_NO VARCHAR(32),\n" +
            "    MAIN_ORDER_NO VARCHAR(32),\n" +
            "    MAIN_ORDER_ID VARCHAR(32),\n" +
            "    INSTANCE_ID VARCHAR(32),\n" +
            "    INSTANCE_NO VARCHAR(32),\n" +
            "    PRODUCT_NO VARCHAR(32),\n" +
            "    PACKAGE_INDEX INT,\n" +
            "    SALE_PRICE DECIMAL(10,2),\n" +
            "    STATE CHAR(2),\n" +
            "    CUSTOMER_ID VARCHAR(32),\n" +
            "    MEMBER_ID VARCHAR(32),\n" +
            "    CUSTOMER_NAME VARCHAR(128),\n" +
            "    ACTIVE_TIME TIMESTAMP(6),\n" +
            "    REMARK VARCHAR(255),\n" +
            "    INTERNATIONAL_FLAG VARCHAR(32),\n" +
            "    CERT_TYPE VARCHAR(5),\n" +
            "    CERT_NO VARCHAR(32),\n" +
            "    CN_FIRST_NAME VARCHAR(128),\n" +
            "    CN_LAST_NAME VARCHAR(128),\n" +
            "    EN_FIRST_NAME VARCHAR(128),\n" +
            "    EN_LAST_NAME VARCHAR(128),\n" +
            "    EXPIRATION_DATE VARCHAR(32),\n" +
            "    SEX CHAR(2),\n" +
            "    BIRTHDAY VARCHAR(64),\n" +
            "    PASSPORT_ISSUE_NATION VARCHAR(64),\n" +
            "    PASSPORT_NATION VARCHAR(64),\n" +
            "    PHONE_NUM VARCHAR(32),\n" +
            "    SEND_MSG_FLAG CHAR(2),\n" +
            "    EXCHANGE_NUM CHAR(10),\n" +
            "    EXPIRY_DATE TIMESTAMP(6),\n" +
            "    TRAVEL_START_DATE TIMESTAMP(6),\n" +
            "    TRAVEL_END_DATE TIMESTAMP(6),\n" +
            "    ENABLE CHAR(1),\n" +
            "    CREATE_TIME TIMESTAMP(6),\n" +
            "    CREATE_USER VARCHAR(128),\n" +
            "    UPDATE_TIME TIMESTAMP(6),\n" +
            "    UPDATE_USER VARCHAR(128)" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
            "    'table-name' = 'T_ODS_ZSF_ODR_ORDER_DETAIL_SECONDARY_DOUYIN', -- 替换为实际的表名\n" +
            "    'username' = '" + Constants.ODS_USER + "',\n" +
            "    'password' = '" + Constants.ODS_PWD + "'\n" +
            ")";


    public static String T_ODS_DKHCL_AIR_ORDER_CHILD_ORDER = "CREATE TABLE T_ODS_DKHCL_AIR_ORDER_CHILD_ORDER (\n" +
            " ID BIGINT,\n" +
            " PNR VARCHAR(60),\n" +
            " PNRXML VARCHAR(11400),\n" +
            " CARD_TYPE VARCHAR(60),\n" +
            " CHANGE_OR_UP INT,\n" +
            " CREATE_ORDER_TYPE INT,\n" +
            " CREATE_TIME TIMESTAMP(6),\n" +
            " CREATOR_ID BIGINT,\n" +
            " CREATOR_NAME VARCHAR(90),\n" +
            " CUR_OPERATOR_ID BIGINT,\n" +
            " CUR_OPERATOR_NAME VARCHAR(90),\n" +
            " CUR_OPERATOR_TIME TIMESTAMP(6),\n" +
            " DELIVERY_TYPE VARCHAR(90),\n" +
            " EMPLOYEE_NO VARCHAR(90),\n" +
            " FN VARCHAR(30),\n" +
            " HAVING_NOSHOW INT,\n" +
            " LAST_TICKET_LIMIT TIMESTAMP(6),\n" +
            " MARK_CODE VARCHAR(300),\n" +
            " ORDER_NO VARCHAR(60),\n" +
            " ORDER_REMARK VARCHAR(600),\n" +
            " ORDER_SOURCE INT,\n" +
            " ORDER_STATUS INT,\n" +
            " ORDER_TYPE VARCHAR(60),\n" +
            " ORI_ORDER_NO VARCHAR(60),\n" +
            " PAY_BANK VARCHAR(150),\n" +
            " PAY_BANK_CODE VARCHAR(90),\n" +
            " PAY_BANK_NAME_CODE VARCHAR(90),\n" +
            " PAY_MENT VARCHAR(60),\n" +
            " PAY_NAME VARCHAR(150),\n" +
            " PAY_STATUS INT,\n" +
            " PRINT_NO VARCHAR(60),\n" +
            " REMARKS VARCHAR(1500),\n" +
            " TICKET_MAN_ID BIGINT,\n" +
            " TICKET_MAN_NAME VARCHAR(90),\n" +
            " TICKET_OUT_DATE TIMESTAMP(6),\n" +
            " TICKET_TYPE INT,\n" +
            " TRAVEL_MATTERS VARCHAR(1500),\n" +
            " VERSION INT,\n" +
            " APP_REMARKS VARCHAR(1500),\n" +
            " APPROVE_DEPT_ID BIGINT,\n" +
            " APPROVE_DEPT_NAME VARCHAR(90),\n" +
            " APPROVE_STATUS INT,\n" +
            " BOOK_WAY VARCHAR(60),\n" +
            " CHECK_NOTE VARCHAR(600),\n" +
            " CHECK_STATUS INT,\n" +
            " COMPANY_ID BIGINT,\n" +
            " COMPANY_NAME VARCHAR(240),\n" +
            " CONTACT_TEL VARCHAR(192),\n" +
            " CUS_BIG_CODE VARCHAR(30),\n" +
            " CUST_ORDER_NO VARCHAR(90),\n" +
            " DELI_ADDRESS VARCHAR(150),\n" +
            " DELI_DATE VARCHAR(60),\n" +
            " DELIVARY_STATUS INT,\n" +
            " DELIVERY_NO VARCHAR(900),\n" +
            " DELIVERY_ORG VARCHAR(90),\n" +
            " FACE_MARK_CODE VARCHAR(300),\n" +
            " IS_URGENT_ORDER INT,\n" +
            " LOWEST_FARES VARCHAR(900),\n" +
            " NEED_CHECK INT,\n" +
            " PAY_INFO_COLLET TINYINT,\n" +
            " REVIEW_DATE TIMESTAMP(6),\n" +
            " REVIEW_ID BIGINT,\n" +
            " REVIEW_NAME VARCHAR(300),\n" +
            " REVIEW_NO VARCHAR(90),\n" +
            " SIGN_MAN VARCHAR(90),\n" +
            " YPOSITION VARCHAR(900),\n" +
            " ZIP_CODE VARCHAR(60),\n" +
            " BIG_ORDER_NO BIGINT,\n" +
            " DELI_WAY VARCHAR(60),\n" +
            " IS_HAVINSUR TINYINT,\n" +
            " DELIVERY_ROUTE_NO VARCHAR(900),\n" +
            " IS_REVOCATION TINYINT,\n" +
            " FULL_PRICE_TICKET TINYINT,\n" +
            " MANUA_STATUS TINYINT,\n" +
            " GUESTBOOK BIGINT,\n" +
            " PROVIDER TINYINT,\n" +
            " OFFICE_USER_NAME VARCHAR(60),\n" +
            " SEND_MESSAGE VARCHAR(60),\n" +
            " IS_FAIL TINYINT,\n" +
            " FAIL_MESSAGE VARCHAR(150),\n" +
            " IS_TRP VARCHAR(6),\n" +
            " IS_CANCEL_PNR INT,\n" +
            " ETL_CREATE_TIME TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
            " ETL_UPDATE_TIME TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
            " ETL_DATE date  NULL COMMENT '数据ETL日期'" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
            "    'table-name' = 'T_ODS_DKHCL_AIR_ORDER_CHILD_ORDER', -- 替换为实际的表名\n" +
            "    'username' = '" + Constants.ODS_USER + "',\n" +
            "    'password' = '" + Constants.ODS_PWD + "'\n" +
            ")";

    public static String T_ODS_DKHCL_CRM_EMPLOYEE_CERT = "CREATE TABLE T_ODS_DKHCL_CRM_EMPLOYEE_CERT (\n" +
            " ID BIGINT,\n" +
            " CREATE_TIME TIMESTAMP(6),\n" +
            " DEL_STATUS INT,\n" +
            " MARK_CODE VARCHAR(100),\n" +
            " BIRTHDAY VARCHAR(30),\n" +
            " CERT_NAME VARCHAR(30),\n" +
            " CERT_NO VARCHAR(600),\n" +
            " CERT_TYPE VARCHAR(90),\n" +
            " EFFECTIVE_DATE TIMESTAMP(6),\n" +
            " NATIONALITY VARCHAR(50),\n" +
            " SEX INT,\n" +
            " CREATOR_ID BIGINT,\n" +
            " EMPLOYEE_ID BIGINT,\n" +
            " SIGN_COUNTRY VARCHAR(30),\n" +
            " NATIONALITY_CN VARCHAR(20),\n" +
            " SIGN_COUNTRY_CN VARCHAR(20),\n" +
            " EFFECTIVE_DATE_STR VARCHAR(100),\n" +
            " CN_NAME_FIRST VARCHAR(50),\n" +
            " CN_NAME_SECOND VARCHAR(50),\n" +
            " ZN_NAME_FIRST VARCHAR(100),\n" +
            " ZN_NAME_SECOND VARCHAR(100),\n" +
            " ETL_CREATE_TIME TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
            " ETL_UPDATE_TIME TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
            " ETL_DATE date  NULL COMMENT '数据ETL日期'" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
            "    'table-name' = 'T_ODS_DKHCL_CRM_EMPLOYEE_CERT', -- 替换为实际的表名\n" +
            "    'username' = '" + Constants.ODS_USER + "',\n" +
            "    'password' = '" + Constants.ODS_PWD + "'\n" +
            ")";


    public static String T_ODS_DKHCL_AIR_ORDER_PASSENGER =
            "CREATE TABLE T_ODS_DKHCL_AIR_ORDER_PASSENGER (\n" +
                    "    ID BIGINT,\n" +
                    "    BIRTH_DAY TIMESTAMP(6),\n" +
                    "    BOOKING_SMS INT,\n" +
                    "    CARRIERCARDNO VARCHAR(600),\n" +
                    "    CERT_NO VARCHAR(192),\n" +
                    "    CERT_TYPE VARCHAR(60),\n" +
                    "    EMAIL VARCHAR(90),\n" +
                    "    EMP_NUMBER VARCHAR(300),\n" +
                    "    EMPLOYEE_ID BIGINT,\n" +
                    "    NAME VARCHAR(90),\n" +
                    "    PASS_TYPE VARCHAR(30),\n" +
                    "    PSGID VARCHAR(30),\n" +
                    "    SEX INT,\n" +
                    "    STATUS INT,\n" +
                    "    TELEPHONE VARCHAR(192),\n" +
                    "    TICKET_SMS INT,\n" +
                    "    COST_CENTER_ID BIGINT,\n" +
                    "    COST_CENTER_NAME VARCHAR(90),\n" +
                    "    DEPT_ID BIGINT,\n" +
                    "    DEPT_MARK_CODE VARCHAR(150),\n" +
                    "    DEPT_NAME VARCHAR(90),\n" +
                    "    PROJECT_CODE VARCHAR(90),\n" +
                    "    AIR_ORDER BIGINT,\n" +
                    "    CUSTOMER_LEVEL_CODE VARCHAR(90),\n" +
                    "    IS_EMPLOYEE TINYINT,\n" +
                    "    PNR VARCHAR(30),\n" +
                    "    EFFECTIVE_DATE VARCHAR(30),\n" +
                    "    NATIONALITY VARCHAR(30),\n" +
                    "    NATIONALITY_VALUE VARCHAR(150),\n" +
                    "    SIGN_COUNTRY VARCHAR(30),\n" +
                    "    SIGN_COUNTRY_VALUE VARCHAR(150),\n" +
                    "    PSGNUM VARCHAR(30),\n" +
                    "    CN_NAME_FIRST VARCHAR(150),\n" +
                    "    CN_NAME_SECOND VARCHAR(150),\n" +
                    "    ZN_NAME_FIRST VARCHAR(300),\n" +
                    "    ZN_NAME_SECOND VARCHAR(300),\n" +
                    " ETL_CREATE_TIME TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                    " ETL_UPDATE_TIME TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                    " ETL_DATE date  NULL COMMENT '数据ETL日期'" +
                    ") WITH (\n" +
                    "    'connector' = 'jdbc',\n" +
                    "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
                    "    'table-name' = 'T_ODS_DKHCL_AIR_ORDER_PASSENGER',\n" +
                    "    'username' = '" + Constants.ODS_USER + "',\n" +
                    "    'password' = '" + Constants.ODS_PWD + "'\n" +
                    ")";

    public static String T_ODS_DKHCL_AIR_ORDER_PASSENGER_SEGMENT =
            "CREATE TABLE T_ODS_DKHCL_AIR_ORDER_PASSENGER_SEGMENT (\n" +
                    "    ID BIGINT,\n" +
                    "    CHANGE_PRICE DOUBLE,\n" +
                    "    CREATE_TIME TIMESTAMP(6),\n" +
                    "    DEL_STATUS INT,\n" +
                    "    FCNY DOUBLE,\n" +
                    "    FUEL_TAX DOUBLE,\n" +
                    "    INSURANCE DOUBLE,\n" +
                    "    IS_OPEN INT,\n" +
                    "    LAPSE DOUBLE,\n" +
                    "    MARK_CODE VARCHAR(100),\n" +
                    "    OTHER_TAX DOUBLE,\n" +
                    "    SALE_PRICE DOUBLE,\n" +
                    "    SAVE_PRICE DOUBLE,\n" +
                    "    SEAT_NO VARCHAR(255),\n" +
                    "    STATUS INT,\n" +
                    "    TICKET_NUM VARCHAR(255),\n" +
                    "    TKT_TAX DOUBLE,\n" +
                    "    UP_CABIN_PRICE DOUBLE,\n" +
                    "    YUAN_TICKET_NUM VARCHAR(255),\n" +
                    "    AIR_ORDER BIGINT,\n" +
                    "    PASSENGER BIGINT,\n" +
                    "    SEGMENT BIGINT,\n" +
                    "    IS_SEAT_NO BIGINT,\n" +
                    "    SEAT_PHONE VARCHAR(100),\n" +
                    "    SEAT_NO_STATUS VARCHAR(10),\n" +
                    "    SEQUENCE TINYINT,\n" +
                    "    SYS_PRICE DOUBLE,\n" +
                    "    CORRELATION_ORDER_ID VARCHAR(100),\n" +
                    " ETL_CREATE_TIME TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                    " ETL_UPDATE_TIME TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                    " ETL_DATE date  NULL COMMENT '数据ETL日期'" +
                    ") WITH (\n" +
                    "    'connector' = 'jdbc',\n" +
                    "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
                    "    'table-name' = 'T_ODS_DKHCL_AIR_ORDER_PASSENGER_SEGMENT',\n" +
                    "    'username' = '" + Constants.ODS_USER + "',\n" +
                    "    'password' = '" + Constants.ODS_PWD + "'\n" +
                    ")";

    public static String T_ODS_DKHCL_AIR_ORDER_SEGMENT =
            "CREATE TABLE T_ODS_DKHCL_AIR_ORDER_SEGMENT (\n" +
                    "    ID BIGINT,\n" +
                    "    AIR_CRAFT VARCHAR(255),\n" +
                    "    AIRPORT_TAX DOUBLE,\n" +
                    "    ARRIVAL_DATE TIMESTAMP(6),\n" +
                    "    ARRIVAL_TIME VARCHAR(50),\n" +
                    "    ASR VARCHAR(50),\n" +
                    "    BOARD_AIRPORT_NAME VARCHAR(70),\n" +
                    "    BOARD_POINT VARCHAR(30),\n" +
                    "    BOARD_POINTAT VARCHAR(80),\n" +
                    "    BOARD_POINT_NAME VARCHAR(80),\n" +
                    "    CARRIER VARCHAR(90),\n" +
                    "    CARRIER_NAME VARCHAR(90),\n" +
                    "    DEPARTURE_DATE TIMESTAMP(6),\n" +
                    "    DEPARTURE_TIME VARCHAR(60),\n" +
                    "    E_TICKET VARCHAR(100),\n" +
                    "    ELEMENT_NO INT,\n" +
                    "    FLIGHTID INT,\n" +
                    "    FLIGHT_NO VARCHAR(20),\n" +
                    "    FLIGHT_TYPE VARCHAR(20),\n" +
                    "    FUEL_SUR_TAX DOUBLE,\n" +
                    "    MEAL VARCHAR(50),\n" +
                    "    OFF_AIRPORT_NAME VARCHAR(255),\n" +
                    "    OFF_POINT VARCHAR(255),\n" +
                    "    OFF_POINTAT VARCHAR(255),\n" +
                    "    OFF_POINT_NAME VARCHAR(255),\n" +
                    "    `SEQUENCE` INT,\n" +
                    "    SHARE_CARRIER VARCHAR(255),\n" +
                    "    SHARE_CARRIER_NAME VARCHAR(255),\n" +
                    "    SHARE_FLIGHT VARCHAR(255),\n" +
                    "    TPM INT,\n" +
                    "    VIA_POINT VARCHAR(255),\n" +
                    "    Y_CLASS_PRICE DOUBLE,\n" +
                    "    CHANGE_RULE VARCHAR(3800),\n" +
                    "    CLASS_CODE VARCHAR(255),\n" +
                    "    DISCOUNT DOUBLE,\n" +
                    "    EXT_CODE VARCHAR(255),\n" +
                    "    ORIGINAL_PRICE DOUBLE,\n" +
                    "    PRICE DOUBLE,\n" +
                    "    RE_FUND_RULE VARCHAR(3800),\n" +
                    "    SAVE_PRICE DOUBLE,\n" +
                    "    SEAT_NUM VARCHAR(255),\n" +
                    "    TICKET_FBC VARCHAR(255),\n" +
                    "    `ALLOW` VARCHAR(100),\n" +
                    "    PAT_PARAMS VARCHAR(20),\n" +
                    "    PNR VARCHAR(10),\n" +
                    "    PNRX_XML VARCHAR(3500),\n" +
                    "    STATUS INT,\n" +
                    "    ZHONG_ZHUAN TINYINT,\n" +
                    "    AIR_ORDER BIGINT,\n" +
                    "    CHILD_ID BIGINT,\n" +
                    "    PRICE_TYPE VARCHAR(6),\n" +
                    "    SOURCE_PRICE VARCHAR(10),\n" +
                    "    PNR_XML VARCHAR(10000),\n" +
                    "    SELLCODE VARCHAR(50),\n" +
                    "    FLIGHT_RULE VARCHAR(10000),\n" +
                    "    FLIGHT_RULE_JSON VARCHAR(10000),\n" +
                    "    CLASS_NAME VARCHAR(20),\n" +
                    "    LOWPRICE DOUBLE,\n" +
                    "    STOP_AIRPORT VARCHAR(100),\n" +
                    "    BAGGAGE VARCHAR(500),\n" +
                    " ETL_CREATE_TIME TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                    " ETL_UPDATE_TIME TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                    " ETL_DATE date  NULL COMMENT '数据ETL日期'" +
                    ") WITH (\n" +
                    "    'connector' = 'jdbc',\n" +
                    "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
                    "    'table-name' = 'T_ODS_DKHCL_AIR_ORDER_SEGMENT',\n" +
                    "    'username' = '" + Constants.ODS_USER + "',\n" +
                    "    'password' = '" + Constants.ODS_PWD + "'\n" +
                    ")";

    public static String T_ODS_DKHCL_CRM_COMPANY =
            "CREATE TABLE T_ODS_DKHCL_CRM_COMPANY (\n" +
                    "    ID BIGINT,\n" +
                    "    CREATE_TIME TIMESTAMP(6),\n" +
                    "    DEL_STATUS INT,\n" +
                    "    MARK_CODE VARCHAR(300),\n" +
                    "    ADDRESS VARCHAR(150),\n" +
                    "    ADMIN_USER_NAME VARCHAR(90),\n" +
                    "    BELONG_CITY VARCHAR(60),\n" +
                    "    BELONG_PROVINCE VARCHAR(60),\n" +
                    "    CA_TCIKET_NUM DOUBLE,\n" +
                    "    CONTACT_NAME VARCHAR(90),\n" +
                    "    CONTACT_TEL VARCHAR(192),\n" +
                    "    EMAIL VARCHAR(600),\n" +
                    "    FAX VARCHAR(90),\n" +
                    "    HOPE_BUY_CHANNEL VARCHAR(150),\n" +
                    "    INDUSTRY VARCHAR(150),\n" +
                    "    NAME VARCHAR(240),\n" +
                    "    NAME_EN VARCHAR(240),\n" +
                    "    `SCOPE` VARCHAR(90),\n" +
                    "    SOURCE VARCHAR(90),\n" +
                    "    TELEPHONE VARCHAR(180),\n" +
                    "    TRAVEL_FEES DOUBLE,\n" +
                    "    WEB_SITE VARCHAR(240),\n" +
                    "    ZIP_CODE VARCHAR(30),\n" +
                    "    BEL_CID BIGINT,\n" +
                    "    CATEGORY VARCHAR(90),\n" +
                    "    COMPANY_CODE VARCHAR(90),\n" +
                    "    CONTRACT_BEGIN TIMESTAMP(6),\n" +
                    "    CONTRACT_END TIMESTAMP(6),\n" +
                    "    CONTRACT_PATH VARCHAR(450),\n" +
                    "    CUS_BIG_CODE VARCHAR(150),\n" +
                    "    CUS_SIGN_CONTACT VARCHAR(192),\n" +
                    "    CUS_SIGN_MAN VARCHAR(90),\n" +
                    "    DELI_TIME VARCHAR(90),\n" +
                    "    DELI_TYPE VARCHAR(90),\n" +
                    "    DELIVERY_ORG VARCHAR(90),\n" +
                    "    OPER_DEPARTMENT BIGINT,\n" +
                    "    BEN_ADDRESS VARCHAR(300),\n" +
                    "    BEN_CONTACT_MAN VARCHAR(90),\n" +
                    "    BEN_CONTACT_TEL VARCHAR(96),\n" +
                    "    BEN_FUEL_CARD_NO VARCHAR(90),\n" +
                    "    BEN_MILE_AGE_NO VARCHAR(90),\n" +
                    "    BEN_MOBILE VARCHAR(90),\n" +
                    "    BEN_ZIP_CODE VARCHAR(30),\n" +
                    "    POLICY_BENEFIT_TYPE VARCHAR(90),\n" +
                    "    POLICY_STATUS INT,\n" +
                    "    RE_CUS_BIG_CODE VARCHAR(60),\n" +
                    "    RE_CUS_COMPANY_NAME VARCHAR(765),\n" +
                    "    RE_CUS_MAN_NAME VARCHAR(765),\n" +
                    "    RE_CUS_MAN_TELPHONE VARCHAR(192),\n" +
                    "    REG_ID BIGINT,\n" +
                    "    SERVICE_GRADE VARCHAR(90),\n" +
                    "    SIGNING_DATE TIMESTAMP(6),\n" +
                    "    UATP_CARD_NO VARCHAR(300),\n" +
                    "    VALIDITY_DATE TIMESTAMP(6),\n" +
                    "    CREATOR_ID BIGINT,\n" +
                    "    SALES_DEPT_ID BIGINT,\n" +
                    "    CA_SIGN_MAN BIGINT,\n" +
                    "    MANAGER_TELEPHONE VARCHAR(300),\n" +
                    "    MANAGER_MOBILE VARCHAR(300),\n" +
                    "    DELI_WAY VARCHAR(60),\n" +
                    "    EFF_DATE_TYPE TINYINT,\n" +
                    "    MONGODB_NAME VARCHAR(1500),\n" +
                    "    AGREE_TIME TIMESTAMP(6),\n" +
                    "    ATTESTATION_TIME TIMESTAMP(6),\n" +
                    "    ATTESTATION_PATH VARCHAR(600),\n" +
                    "    ATTESTATION_NAME VARCHAR(1500),\n" +
                    "    AGREE_NAME VARCHAR(90),\n" +
                    "    EMP_ID BIGINT,\n" +
                    "    IS_AUDIT_PASS TINYINT,\n" +
                    "    IS_AGREEMENT TINYINT,\n" +
                    " ETL_CREATE_TIME TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                    " ETL_UPDATE_TIME TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                    " ETL_DATE date  NULL COMMENT '数据ETL日期'" +
                    ") WITH (\n" +
                    "    'connector' = 'jdbc',\n" +
                    "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
                    "    'table-name' = 'T_ODS_DKHCL_CRM_COMPANY',\n" +
                    "    'username' = '" + Constants.ODS_USER + "',\n" +
                    "    'password' = '" + Constants.ODS_PWD + "'\n" +
                    ")";

    public static String T_ODS_DKHCL_CRM_EMPLOYEE =
            "CREATE TABLE T_ODS_DKHCL_CRM_EMPLOYEE (\n" +
                    "    ID BIGINT,\n" +
                    "    CREATE_TIME TIMESTAMP(6),\n" +
                    "    DEL_STATUS INT,\n" +
                    "    MARK_CODE VARCHAR(300),\n" +
                    "    BIRTHDAY TIMESTAMP(6),\n" +
                    "    CONTACT_TEL VARCHAR(192),\n" +
                    "    EMAIL VARCHAR(150),\n" +
                    "    EMP_NUMBER VARCHAR(90),\n" +
                    "    GENDER INT,\n" +
                    "    IS_NORMAL_EXIT INT,\n" +
                    "    LAST_LOGIN_IP VARCHAR(90),\n" +
                    "    LAST_LOGIN_TIME TIMESTAMP(6),\n" +
                    "    LOGIN_NAME VARCHAR(90),\n" +
                    "    MOBILE VARCHAR(384),\n" +
                    "    NAME VARCHAR(90),\n" +
                    "    PASS_WORD VARCHAR(900),\n" +
                    "    `POSITION` VARCHAR(90),\n" +
                    "    EMP_TYPE VARCHAR(30),\n" +
                    "    FAMILY_NAME VARCHAR(90),\n" +
                    "    FIRST_NAME VARCHAR(90),\n" +
                    "    HISTORY_PASS_ONE VARCHAR(900),\n" +
                    "    HISTORY_PASS_THREE VARCHAR(900),\n" +
                    "    HISTORY_PASS_TWO VARCHAR(900),\n" +
                    "    IF_APP INT,\n" +
                    "    IS_ALLOW_LOGIN TINYINT,\n" +
                    "    LAST_CREATE_PASS_TIEM TIMESTAMP(6),\n" +
                    "    MIDDEL_NAME VARCHAR(90),\n" +
                    "    BEN_ADDRESS VARCHAR(300),\n" +
                    "    BEN_CONTACT_MAN VARCHAR(90),\n" +
                    "    BEN_CONTACT_TEL VARCHAR(90),\n" +
                    "    BEN_FUEL_CARD_NO VARCHAR(90),\n" +
                    "    BEN_MILE_AGE_NO VARCHAR(90),\n" +
                    "    BEN_MOBILE VARCHAR(90),\n" +
                    "    BEN_ZIP_CODE VARCHAR(30),\n" +
                    "    REMARK VARCHAR(150),\n" +
                    "    SOFT_PHONE INT,\n" +
                    "    TELL_TYPE INT,\n" +
                    "    WP_PASS_WORD VARCHAR(30),\n" +
                    "    WP_USER_NAME VARCHAR(30),\n" +
                    "    CREATOR_ID BIGINT,\n" +
                    "    DEPT_ID BIGINT,\n" +
                    "    COMPANY_ID BIGINT,\n" +
                    "    COST_CENTER_ID BIGINT,\n" +
                    "    OFFICE_NO_ID BIGINT,\n" +
                    "    OWNED_SALES BIGINT,\n" +
                    "    CUSTOMER_LEVEL_CODE VARCHAR(90),\n" +
                    "    ADD_OP_NAME VARCHAR(90),\n" +
                    "    IS_CONTACT TINYINT,\n" +
                    "    VIP_STATUS TINYINT,\n" +
                    "    IS_PRIVACY TINYINT,\n" +
                    "    PW_ERR_COUNT TINYINT,\n" +
                    "    PW_ERR_TIME TIMESTAMP(6),\n" +
                    "    COM_ID BIGINT,\n" +
                    "    UPDATE_PASSWORD_TIME TIMESTAMP(6),\n" +
                    "    UPDATE_PASS_WORD VARCHAR(300),\n" +
                    " ETL_CREATE_TIME TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                    " ETL_UPDATE_TIME TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                    " ETL_DATE date  NULL COMMENT '数据ETL日期'" +
                    ") WITH (\n" +
                    "    'connector' = 'jdbc',\n" +
                    "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
                    "    'table-name' = 'T_ODS_DKHCL_CRM_EMPLOYEE',\n" +
                    "    'username' = '" + Constants.ODS_USER + "',\n" +
                    "    'password' = '" + Constants.ODS_PWD + "'\n" +
                    ")";

    public static String T_ODS_DKHCL_YEE_QUICK_PAY_INFO =
            "CREATE TABLE T_ODS_DKHCL_YEE_QUICK_PAY_INFO (\n" +
                    "    ID BIGINT,\n" +
                    "    BUSINESNO VARCHAR(90),\n" +
                    "    BUNISSTYPE VARCHAR(60),\n" +
                    "    ORDERNO VARCHAR(90),\n" +
                    "    ORGORDERNO VARCHAR(90),\n" +
                    "    CURTYPE VARCHAR(30),\n" +
                    "    BANKCODE VARCHAR(60),\n" +
                    "    BANKBRAN VARCHAR(60),\n" +
                    "    TRANTYPE VARCHAR(60),\n" +
                    "    ORDAMOUNT DOUBLE,\n" +
                    "    AMOUNT DOUBLE,\n" +
                    "    TRADEDATE VARCHAR(60),\n" +
                    "    TRADETIME VARCHAR(60),\n" +
                    "    TRADATE VARCHAR(90),\n" +
                    "    PAYPLATFORM VARCHAR(60),\n" +
                    "    BANKORDERID VARCHAR(150),\n" +
                    "    PAYSTATUS VARCHAR(60),\n" +
                    "    REFUNDSTATUS VARCHAR(60),\n" +
                    "    SERIALNUM VARCHAR(150),\n" +
                    "    TRADECODE VARCHAR(60),\n" +
                    "    TRADEMSG VARCHAR(300),\n" +
                    "    PRODUCTNAME VARCHAR(60),\n" +
                    "    TRANNUMBER BIGINT,\n" +
                    "    CID BIGINT,\n" +
                    "    ACCOUNTID BIGINT,\n" +
                    "    PLATFORM VARCHAR(60),\n" +
                    "    SCENARIO VARCHAR(60),\n" +
                    "    PAYPLATFORM_NAME VARCHAR(300),\n" +
                    "    BANKCODE_NAME VARCHAR(300),\n" +
                    " ETL_CREATE_TIME TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                    " ETL_UPDATE_TIME TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                    " ETL_DATE date  NULL COMMENT '数据ETL日期'" +
                    ") WITH (\n" +
                    "    'connector' = 'jdbc',\n" +
                    "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
                    "    'table-name' = 'T_ODS_DKHCL_YEE_QUICK_PAY_INFO',\n" +
                    "    'username' = '" + Constants.ODS_USER + "',\n" +
                    "    'password' = '" + Constants.ODS_PWD + "'\n" +
                    ")";


    public static String T_ODS_QWSY_CUSTOMER_INFO = "CREATE TABLE T_ODS_QWSY_CUSTOMER_INFO (\n" +
            " ID BIGINT,\n" +
            " CORP_ID VARCHAR(765),\n" +
            " EXTERNAL_USER_ID VARCHAR(765),\n" +
            " EXTERNAL_USER_NAME VARCHAR(765),\n" +
            " EXTERNAL_TYPE TINYINT,\n" +
            " FOLLOW_USER_ID VARCHAR(765),\n" +
            " FOLLOW_USER_NAME VARCHAR(765),\n" +
            " FOLLOW_REMARK_MOBILES VARCHAR(765),\n" +
            " FOLLOW_DESCRIPTION VARCHAR(765),\n" +
            " FOLLOW_ADD_TIME BIGINT,\n" +
            " FOLLOW_TAGS VARCHAR(3072),\n" +
            " IS_DEL TINYINT,\n" +
            " IS_LOSS TINYINT,\n" +
            " CREATE_TIME TIMESTAMP(6),\n" +
            " UPDATE_TIME TIMESTAMP(6),\n" +
            " ETL_CREATE_TIME TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
            " ETL_UPDATE_TIME TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
            " ETL_DATE date  NULL COMMENT '数据ETL日期'" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
            "    'table-name' = 'T_ODS_QWSY_CUSTOMER_INFO', -- 替换为实际的表名\n" +
            "    'username' = '" + Constants.ODS_USER + "',\n" +
            "    'password' = '" + Constants.ODS_PWD + "'\n" +
            ")";

    public static String T_ODS_QWSY_CUSTOMER_TAG_INFO = "CREATE TABLE T_ODS_QWSY_CUSTOMER_TAG_INFO (\n" +
            " ID BIGINT,\n" +
            " CORP_ID VARCHAR(765),\n" +
            " EXTERNAL_USER_ID VARCHAR(765),\n" +
            " FOLLOW_USER_ID VARCHAR(765),\n" +
            " GROUP_NAME VARCHAR(765),\n" +
            " TAG_ID VARCHAR(765),\n" +
            " CREATE_TIME TIMESTAMP(3),\n" +
            " UPDATE_TIME TIMESTAMP(3),\n" +
            " TAG_NAME VARCHAR(765),\n" +
            " ETL_CREATE_TIME TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
            " ETL_UPDATE_TIME TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
            " ETL_DATE date  NULL COMMENT '数据ETL日期'" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
            "    'table-name' = 'T_ODS_QWSY_CUSTOMER_TAG_INFO', -- 替换为实际的表名\n" +
            "    'username' = '" + Constants.ODS_USER + "',\n" +
            "    'password' = '" + Constants.ODS_PWD + "'\n" +
            ")";


    public static String ZSF_ODR_ORDER_DOUYIN = "CREATE TABLE T_ODS_ZSF_ODR_ORDER_DOUYIN (\n" +
            "    ETL_DATE DATE NOT NULL,\n" +
            "    ID VARCHAR(32) NOT NULL,\n" +
            "    COUPON_ID VARCHAR(32),\n" +
            "    CARD_CODE VARCHAR(32),\n" +
            "    ORDER_NO VARCHAR(32),\n" +
            "    OLD_ORDER_NO VARCHAR(32),\n" +
            "    ORDER_PRICE DECIMAL(10, 2),\n" +
            "    ORDER_SOURCE VARCHAR(20),\n" +
            "    ORDER_TYPE VARCHAR(32),\n" +
            "    ORDER_STATUS VARCHAR(2),\n" +
            "    PARENT_ID VARCHAR(32),\n" +
            "    MAIN_ORDER_NO VARCHAR(32),\n" +
            "    ORDER_TIME TIMESTAMP(6),\n" +
            "    VERSION INT,\n" +
            "    REMARK VARCHAR(2000),\n" +
            "    ENABLE CHAR(1),\n" +
            "    CREATE_TIME TIMESTAMP(6),\n" +
            "    CREATE_USER VARCHAR(128),\n" +
            "    UPDATE_TIME TIMESTAMP(6),\n" +
            "    UPDATE_USER VARCHAR(128)" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
            "    'table-name' = 'T_ODS_ZSF_ODR_ORDER_DOUYIN', -- 替换为实际的表名\n" +
            "    'username' = '" + Constants.ODS_USER + "',\n" +
            "    'password' = '" + Constants.ODS_PWD + "'\n" +
            ")";

    public static String ZSF_ODR_ORDER_PAY = "CREATE TABLE T_ODS_ZSF_ODR_ORDER_PAY (\n" +
            "    ETL_DATE DATE NOT NULL,\n" +
            "    ID VARCHAR(32) NOT NULL,\n" +
            "    ORDER_ID VARCHAR(32) NOT NULL,\n" +
            "    ORDER_NO VARCHAR(32),\n" +
            "    PAY_TYPE VARCHAR(20),\n" +
            "    PAY_PRICE DECIMAL(10,2),\n" +
            "    PAID_PRICE DECIMAL(10,2),\n" +
            "    PAY_CURRENCY VARCHAR(64),\n" +
            "    ENABLE CHAR(1) NOT NULL,\n" +
            "    CREATE_TIME TIMESTAMP(6) NOT NULL,\n" +
            "    CREATE_USER VARCHAR(128) NOT NULL,\n" +
            "    UPDATE_TIME TIMESTAMP(6) NOT NULL,\n" +
            "    UPDATE_USER VARCHAR(128) NOT NULL,\n" +
            "    IS_EQUITY_UPGRADE CHAR(1)" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
            "    'table-name' = 'T_ODS_ZSF_ODR_ORDER_PAY', -- 替换为实际的表名\n" +
            "    'username' = '" + Constants.ODS_USER + "',\n" +
            "    'password' = '" + Constants.ODS_PWD + "'\n" +
            ")";

    public static String ZSF_ODR_ORDER_PAY_ONLINE = "CREATE TABLE T_ODS_ZSF_ODR_ORDER_PAY_ONLINE (\n" +
            "    ETL_DATE DATE NOT NULL,\n" +
            "    ID VARCHAR(32) NOT NULL,\n" +
            "    ORDER_PAY_ID VARCHAR(32) NOT NULL,\n" +
            "    PAY_NO VARCHAR(32) NOT NULL,\n" +
            "    PAY_AMOUNT DECIMAL(20,2),\n" +
            "    PAY_TIME TIMESTAMP(6),\n" +
            "    PAY_REASON VARCHAR(500),\n" +
            "    PAY_RETURN_MSG VARCHAR(1000),\n" +
            "    PAY_RETURN_TIME TIMESTAMP(6),\n" +
            "    BANK_ORDER_NO VARCHAR(64),\n" +
            "    BANK_TRADE_NO VARCHAR(64),\n" +
            "    PAY_STATUS VARCHAR(2),\n" +
            "    PAY_SOURCE VARCHAR(20),\n" +
            "    PAY_BANK_CODE VARCHAR(50),\n" +
            "    PAY_BANK_NAME VARCHAR(100),\n" +
            "    BUS_PARTER_NO VARCHAR(32),\n" +
            "    PAY_USER_NAME VARCHAR(64),\n" +
            "    EXTENDS1 VARCHAR(255),\n" +
            "    EXTENDS2 VARCHAR(255),\n" +
            "    EXTENDS3 VARCHAR(255),\n" +
            "    EXTENDS4 VARCHAR(255),\n" +
            "    IS_BINDING CHAR(1),\n" +
            "    CUSTOMER_ID VARCHAR(100),\n" +
            "    ENABLE CHAR(1) NOT NULL,\n" +
            "    CREATE_TIME TIMESTAMP(6) NOT NULL,\n" +
            "    CREATE_USER VARCHAR(128) NOT NULL,\n" +
            "    UPDATE_TIME TIMESTAMP(6) NOT NULL,\n" +
            "    UPDATE_USER VARCHAR(128) NOT NULL,\n" +
            "    CARD_NO VARCHAR(128),\n" +
            "    PAY_TYPE VARCHAR(36),\n" +
            "    PAY_ACTUAL_AMOUNT DECIMAL(20,0),\n" +
            "    DEDUCTION_AMOUNT DECIMAL(20,0),\n" +
            "    DEDUCTION_TYPE VARCHAR(2),\n" +
            "    TRP_PAYMENT_ID VARCHAR(32)" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
            "    'table-name' = 'T_ODS_ZSF_ODR_ORDER_PAY_ONLINE', -- 替换为实际的表名\n" +
            "    'username' = '" + Constants.ODS_USER + "',\n" +
            "    'password' = '" + Constants.ODS_PWD + "'\n" +
            ")";

    public static String ZSF_ODR_ORDER_PAY_SECONDARYCARD = "CREATE TABLE T_ODS_ZSF_ODR_ORDER_PAY_SECONDARYCARD (\n" +
            "    ETL_DATE DATE NOT NULL,\n" +
            "    ID VARCHAR(32) NOT NULL,\n" +
            "    ORDER_PAY_ID VARCHAR(32) NOT NULL,\n" +
            "    RESOURCE_ID VARCHAR(32),\n" +
            "    RESOURCE_NO VARCHAR(32),\n" +
            "    INSTANCE_ID VARCHAR(32),\n" +
            "    INSTANCE_NO VARCHAR(32),\n" +
            "    PRODUCT_ID VARCHAR(32),\n" +
            "    PRODUCT_NO VARCHAR(32),\n" +
            "    SALE_ORDER_NO VARCHAR(32),\n" +
            "    PSGR_NAME VARCHAR(100),\n" +
            "    CERT_NO VARCHAR(400),\n" +
            "    ORG_CITY VARCHAR(3),\n" +
            "    DST_CITY VARCHAR(3),\n" +
            "    CABIN VARCHAR(2),\n" +
            "    DEP_DATE VARCHAR(10),\n" +
            "    DEP_TIME VARCHAR(10),\n" +
            "    ENABLE CHAR(1) NOT NULL,\n" +
            "    CREATE_TIME TIMESTAMP(6) NOT NULL,\n" +
            "    CREATE_USER VARCHAR(128) NOT NULL,\n" +
            "    UPDATE_TIME TIMESTAMP(6) NOT NULL,\n" +
            "    UPDATE_USER VARCHAR(128) NOT NULL,\n" +
            "    CARD_NO VARCHAR(32),\n" +
            "    CARD_VALUE DECIMAL(10,2),\n" +
            "    CARD_DESCRIBE VARCHAR(4000),\n" +
            "    CARD_PAY_STATUS VARCHAR(255)" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
            "    'table-name' = 'T_ODS_ZSF_ODR_ORDER_PAY_SECONDARYCARD', -- 替换为实际的表名\n" +
            "    'username' = '" + Constants.ODS_USER + "',\n" +
            "    'password' = '" + Constants.ODS_PWD + "'\n" +
            ")";

    public static String ZSF_PRD_PRODUCT_COUPON_DOUYIN = "CREATE TABLE T_ODS_ZSF_PRD_PRODUCT_COUPON_DOUYIN (\n" +
            "    ETL_DATE DATE NOT NULL,\n" +
            "    ID VARCHAR(36) NOT NULL,\n" +
            "    ORDER_ID VARCHAR(36),\n" +
            "    `COUNT` INT,\n" +
            "    START_TIME INT,\n" +
            "    EXPIRE_TIME INT,\n" +
            "    SKU_NAME VARCHAR(32),\n" +
            "    SKU_ID VARCHAR(32),\n" +
            "    CARD_CODE VARCHAR(32),\n" +
            "    COUPON_CODE VARCHAR(32),\n" +
            "    PHONE VARCHAR(32),\n" +
            "    STATUS CHAR(1),\n" +
            "    VERIFICATION_TIME TIMESTAMP(6),\n" +
            "    REFUND_TYPE INT,\n" +
            "    AFTER_SALE_ID VARCHAR(32),\n" +
            "    CERTIFICATE_ID VARCHAR(32),\n" +
            "    REFUND_APPLY_TIME INT,\n" +
            "    REFUND_AUDIT_TIME INT,\n" +
            "    REFUND_AUDIT_RESULT INT,\n" +
            "    ENABLE CHAR(1),\n" +
            "    CREATE_TIME TIMESTAMP(6),\n" +
            "    CREATE_USER VARCHAR(128),\n" +
            "    UPDATE_TIME TIMESTAMP(6),\n" +
            "    UPDATE_USER VARCHAR(128)" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
            "    'table-name' = 'T_ODS_ZSF_PRD_PRODUCT_COUPON_DOUYIN', -- 替换为实际的表名\n" +
            "    'username' = '" + Constants.ODS_USER + "',\n" +
            "    'password' = '" + Constants.ODS_PWD + "'\n" +
            ")";

    public static String CLK_ACCUINT = "CREATE TABLE T_ODS_CLK_ACCUINT (\n" +
            "ETL_DATE DATE ,\n" +
            "ID BIGINT ,\n" +
            "DOCNAME VARCHAR(50) ,\n" +
            "TOTALRECORD BIGINT ,\n" +
            "TOTALAMOUNT BIGINT ,\n" +
            "TOTALVALUE DECIMAL(18,5) ,\n" +
            "BILLINGMONTH VARCHAR(6) ,\n" +
            "BILLINGSTARTDT VARCHAR(8) ,\n" +
            "BILLINGENDDT VARCHAR(8) ,\n" +
            "MEMBERNO VARCHAR(35) ,\n" +
            "MEMBERTIERCODE VARCHAR(30) ,\n" +
            "MEMBERBRAND VARCHAR(3) ,\n" +
            "BIZTYPECODE VARCHAR(10) ,\n" +
            "BIZSUBTYPECODE VARCHAR(10) ,\n" +
            "CHANNELCODE VARCHAR(30) ,\n" +
            "PARTNERCODE VARCHAR(20) ,\n" +
            "EVENTNO VARCHAR(20) ,\n" +
            "ACTIVITYID VARCHAR(20) ,\n" +
            "TRANID VARCHAR(50) ,\n" +
            "ORDERNO VARCHAR(50) ,\n" +
            "TKTNO VARCHAR(20) ,\n" +
            "COUPONNO VARCHAR(2) ,\n" +
            "BILLCOMPANY VARCHAR(5) ,\n" +
            "BILLEDCOMPANY VARCHAR(5) ,\n" +
            "MILES BIGINT ,\n" +
            "CURRENCY VARCHAR(3) ,\n" +
            "SALESPRICE DECIMAL(10,5) ,\n" +
            "COSTSPRICE DECIMAL(10,5) ,\n" +
            "`VALUE` DECIMAL(18,5) ,\n" +
            "EXCHPOINT DECIMAL(6,5) ,\n" +
            "ACTIVITYDATE VARCHAR(8) ,\n" +
            "FLIGHTDATE VARCHAR(8) ,\n" +
            "OC VARCHAR(3) ,\n" +
            "OCFLIGHTNO VARCHAR(4) ,\n" +
            "OCCABIN VARCHAR(2) ,\n" +
            "OCSUBCLASS VARCHAR(2) ,\n" +
            "UPLSTN VARCHAR(3) ,\n" +
            "DESSTN VARCHAR(3) ,\n" +
            "ASS VARCHAR(1) ,\n" +
            "IRN VARCHAR(16) ,\n" +
            "OAN VARCHAR(16) \n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
            "    'table-name' = 'T_ODS_CLK_ACCUINT', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.ODS_USER+"',\n" +
            "    'password' = '"+Constants.ODS_PWD+"'\n" +
            ")";

    // DWD 建表sql
    public static String DWD_REGISTER_UDO = "CREATE TABLE T_DWD_REGISTER_UDO_FACT (\n" +
            "    PK_ID varchar(500) ,\n" +
            "FK_REGISTER_USER varchar(36) ,\n" +
            "FK_REGISTER_DATE Date ,\n" +
            "REGISTER_TIME varchar(30) ,\n" +
            "AK_REGISER_CHANNEL varchar(20) ,\n" +
            "REGISTER_IP varchar(20) ,\n" +
            "REGISTER_COUNT int ,\n" +
            "SOURCE_LAST_UPDATETIME varchar(30) ,\n" +
            "SYSTEM_CREATETIME TIMESTAMP(6) ,\n" +
            "SYSTEM_LAST_UPDATETIME TIMESTAMP(6)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.DWD_DB+"',\n" +
            "    'table-name' = 'T_DWD_REGISTER_UDO_FACT', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.DWD_USER+"',\n" +
            "    'password' = '"+Constants.DWD_PWD+"'\n" +
            ")";

    public static String DWD_BOOKING_SEG_FACT = "CREATE TABLE T_DWD_BOOKING_SEG_FACT (\n" +
            "    PK_ID VARCHAR(128),  \n" +
            "    PNR_NUMBER VARCHAR(32),  \n" +
            "    FK_BOOKING_DATE DATE,  \n" +
            "    FK_BOOKING_TIME VARCHAR(30),  \n" +
            "    FK_DEPAIRPORT VARCHAR(10),  \n" +
            "    FK_ARRIAIRPORT VARCHAR(10),  \n" +
            "    FK_BOOKING_SEG VARCHAR(128),  \n" +
            "    FK_SEG_DATE DATE,  \n" +
            "    FK_SEG_TIME VARCHAR(30),  \n" +
            "    AIRLINE_CODE VARCHAR(10),  \n" +
            "    FLIGHT_NUMBER VARCHAR(20),  \n" +
            "    PASSENGER_TYPE VARCHAR(30),  \n" +
            "    EN_LAST_NAME VARCHAR(50),  \n" +
            "    EN_FIRST_NAME VARCHAR(50),  \n" +
            "    CN_NAME VARCHAR(100),  \n" +
            "    CERTI_TYPE VARCHAR(20),  \n" +
            "    CERTI_NUMBER VARCHAR(128),  \n" +
            "    PASSENGER_AGE INT,  \n" +
            "    FK_PASSENGER_USER_TID VARCHAR(256),  \n" +
            "    VVIP VARCHAR(64),  \n" +
            "    AK_BOOKING_OFFICE_NUMBER VARCHAR(32),  \n" +
            "    AK_TEAMMARK VARCHAR(10),  \n" +
            "    AK_SEG_STATUS VARCHAR(10),  \n" +
            "    AK_CABIN VARCHAR(30),  \n" +
            "    AK_SEGCABIN VARCHAR(10),  \n" +
            "    PRICE_TYPE VARCHAR(20),  \n" +
            "    FFRF VARCHAR(128),  \n" +
            "    FF_LEVEL VARCHAR(10),  \n" +
            "    FF_AIRLINE VARCHAR(10),  \n" +
            "    FF_ALLIANCE_LEVEL VARCHAR(20),  \n" +
            "    AK_ADVBOOK_DAY INT,  \n" +
            "    AK_KEY_ACCOUNT_CODE VARCHAR(32),  \n" +
            "    IS_KEY_ACCOUNT BOOLEAN,  \n" +
            "    AK_PEER_NUMBER INT,  \n" +
            "    AK_PEER_CHD BOOLEAN,  \n" +
            "    PEER_SENIOR BOOLEAN,  \n" +
            //"    PEER_INFANT BOOLEAN,  \n" +
            "    SELF_BOOKING BOOLEAN,  \n" +
            "    HOMECOMING_SEG BOOLEAN,  \n" +
            "    SINGLE_TRAVEL BOOLEAN,  \n" +
            "    SEG_COUNT INT,  \n" +
            "    CHANNEL_ORDER_ID VARCHAR(32),  \n" +
            "    AK_CHANNEL VARCHAR(20),  \n" +
            "    CONTACT_MOBILE_NUMBER VARCHAR(128),  \n" +
            "    CONTACT_NAME VARCHAR(100),  \n" +
            "    CONTACT_INTER_MOBILE_CODE VARCHAR(10),  \n" +
            "    CONTACT_LANDLINE_MOBILE_NUMBER VARCHAR(128),  \n" +
            "    CONTACT_EMAIL VARCHAR(256),  \n" +
            "    FK_BOOKING_USER_TID VARCHAR(256),  \n" +
            "    FK_BOOKING_USER_ORIGIN_ID VARCHAR(64),  \n" +
            "    AK_BOOKING_BRAND VARCHAR(30),  \n" +
            "    AK_BOOKER_IS_PASSENGER BOOLEAN,  \n" +
            "    AK_ISSECKILL BOOLEAN,  \n" +
            "    DATA_ACTIVE BOOLEAN,  \n" +
            "    DATA_ACTIVE_TIME TIMESTAMP(6),  \n" +
            "    SOURCE_LAST_UPDATETIME VARCHAR(30),  \n" +
            "    SYSTEM_CREATETIME TIMESTAMP(6),  \n" +
            "    SYSTEM_LAST_UPDATETIME TIMESTAMP(6),\n" +
            "    PRIMARY KEY (PK_ID) NOT ENFORCED"+
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.DWD_DB+"',\n" +
            "    'table-name' = 'T_DWD_BOOKING_SEG_FACT', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.DWD_USER+"',\n" +
            "    'password' = '"+Constants.DWD_PWD+"'\n" +
            ")";


    public static String DWD_TICKING_TIC_FACT = "CREATE TABLE T_DWD_TICKING_TIC_FACT (\n" +
            " PK_ID varchar(64) ,\n" +
            "AK_TICKET_TYPE varchar(20) ,\n" +
            "PNR_NUMBER varchar(32) ,\n" +
            "TICKET_NUMBER varchar(32) ,\n" +
            "AK_BOOKING_OFFICE_NUMBER varchar(32) ,\n" +
            "FK_TICKETING_DATE date ,\n" +
            "FK_TICKETING_TIME varchar(30) ,\n" +
            "PASSENGER_TYPE varchar(20) ,\n" +
            "EN_LAST_NAME varchar(30) ,\n" +
            "EN_FIRST_NAME varchar(30) ,\n" +
            "CN_NAME varchar(30) ,\n" +
            "CERT_TYPE varchar(20) ,\n" +
            "CERT_NUMBER varchar(128) ,\n" +
            "PASSENGER_AGE int ,\n" +
            "FK_PASSENGER_USER_TID varchar(256) ,\n" +
            "FFRF varchar(128) ,\n" +
            "FF_LEVEL varchar(10) ,\n" +
            "FF_AIRLINE varchar(10) ,\n" +
            "FF_ALLIANCE_LEVEL varchar(20) ,\n" +
            "AK_DIMARK varchar(8) ,\n" +
            "AK_CONJUCTION boolean ,\n" +
            "AK_FAREBASIS varchar(50) ,\n" +
            "UNACCOMPANIED_MINOR boolean ,\n" +
            "IS_GOVERNMENT_PURCHASE boolean ,\n" +
            "AK_KEY_ACCOUNT_CODE varchar(32) ,\n" +
            "IS_KEY_ACCOUNT boolean ,\n" +
            "AK_PEER_NUMBER int ,\n" +
            "SINGLE_TRAVELER boolean ,\n" +
            "AK_PEER_CHD boolean ,\n" +
            "PEER_SENIOR boolean ,\n" +
            //"PEER_INFANT boolean ,\n" +
            "SELF_BOOKING boolean ,\n" +
            "AK_FIRST_ISSUE boolean ,\n" +
            "AK_BOOKING_PASSENGER boolean ,\n" +
            "AK_ADVBOOK_DAY int ,\n" +
            "AK_CURRENCY varchar(10) ,\n" +
            "FARE_CURAMOUNT decimal(16,6) ,\n" +
            "TAX_CURAMOUNT decimal(16,6) ,\n" +
            "INFRASTRUCTURE_TAX_CURRENCY varchar(10) ,\n" +
            "FUEL_TAX_CURRENCY varchar(10) ,\n" +
            "OTHER_TAX_CURRENCY varchar(10) ,\n" +
            "CUR_AMOUNT decimal(16,6) ,\n" +
            "FARE_AMOUNT decimal(16,6) ,\n" +
            "TAX_AMOUNT decimal(16,6) ,\n" +
            "INFRASTRUCTURE_TAX_CNY decimal(16,6) ,\n" +
            "FUEL_TAX_CNY decimal(16,6) ,\n" +
            "OTHER_TAX_CNY decimal(16,6) ,\n" +
            "AMOUNT_CNY decimal(16,6) ,\n" +
            "TICKET_COUNT int ,\n" +
            "AK_ISSECKILL boolean ,\n" +
            "AK_COUPON boolean ,\n" +
            "COUPON_AMOUNT decimal(16,6) ,\n" +
            "COUPON_COUNT int ,\n" +
            "AK_ORDERNUM varchar(64) ,\n" +
            "AK_CHANNEL varchar(20) ,\n" +
            "FK_BOOKING_USER_TID varchar(256) ,\n" +
            "FK_BOOKING_USER_ORIGIN_ID varchar(64) ,\n" +
            "CONTACT_MOBILE_NUMBER varchar(128) ,\n" +
            "CONTACT_NAME varchar(100) ,\n" +
            "CONTACT_INTER_MOBILE_CODE varchar(10) ,\n" +
            "CONTACT_LANDLINE_MOBILE_NUMBER varchar(128) ,\n" +
            "CONTACT_EMAIL varchar(256) ,\n" +
            "DATA_ACTIVE boolean ,\n" +
            "DATA_ACTIVE_TIME TIMESTAMP(6) ,\n" +
            "SOURCE_LAST_UPDATETIME TIMESTAMP(6) ,\n" +
            "SYSTEM_CREATETIME TIMESTAMP(6) ,\n" +
            "SYSTEM_LAST_UPDATETIME TIMESTAMP(6) "+
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.DWD_DB+"',\n" +
            "    'table-name' = 'T_DWD_TICKING_TIC_FACT', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.DWD_USER+"',\n" +
            "    'password' = '"+Constants.DWD_PWD+"'\n" +
            ")";

    public static String DWD_TICKING_SEG_FACT = "CREATE TABLE T_DWD_TICKING_SEG_FACT (\n" +
            " PK_ID varchar(128) ,\n" +
            "AK_PNR_NUMBER varchar(32) ,\n" +
            "AK_TICKET_TYPE varchar(20) ,\n" +
            "AK_TICKET_NUMBER varchar(32) ,\n" +
            "FK_ISSUE_DATE date ,\n" +
            "FK_ISSUE_TIME varchar(30) ,\n" +
            "AK_BOOKING_OFFICE_NUMBER varchar(32) ,\n" +
            "PASSENGER_TYPE varchar(30) ,\n" +
            "EN_LAST_NAME varchar(50) ,\n" +
            "EN_FIRST_NAME varchar(50) ,\n" +
            "CN_NAME varchar(100) ,\n" +
            "CERT_TYPE varchar(20) ,\n" +
            "CERT_NUMBER varchar(128) ,\n" +
            "PASSENGER_AGE int ,\n" +
            "FK_PASSENGER_USER_TID varchar(256) ,\n" +
            "FFRF varchar(128) ,\n" +
            "FF_LEVEL varchar(20) ,\n" +
            "FF_AIRLINE varchar(10) ,\n" +
            "FF_ALLIANCE_LEVEL varchar(20) ,\n" +
            "AK_DIMARK varchar(8) ,\n" +
            "AK_CONJUCTION boolean ,\n" +
            "PRIOR_TICKET varchar(32) ,\n" +
            "NEXT_TICKET varchar(32) ,\n" +
            "AK_FAREBASIS varchar(50) ,\n" +
            "UNACCOMPANIED_MINOR boolean ,\n" +
            "IS_GOVERNMENT_PURCHASE boolean ,\n" +
            "AK_KEY_ACCOUNT_CODE varchar(32) ,\n" +
            "IS_KEY_ACCOUNT boolean ,\n" +
            "IS_TEAM_TICKET boolean ,\n" +
            "AK_PEER_NUMBER int ,\n" +
            "SINGLE_TRAVELER boolean ,\n" +
            "AK_PEER_CHD boolean ,\n" +
            "PEER_SENIOR boolean ,\n" +
            //"PEER_INFANT boolean ,\n" +
            "SELF_BOOKING boolean ,\n" +
            "AK_FIRST_ISSUE boolean ,\n" +
            "AK_BOOKING_PASSENGER boolean ,\n" +
            "FK_DEPAIRPORT varchar(10) ,\n" +
            "FK_ARRIAIRPORT varchar(10) ,\n" +
            "FK_SEG_SEGMENT varchar(128) ,\n" +
            "FK_SEG_DATE date ,\n" +
            "FK_SEG_TIME varchar(30) ,\n" +
            "AK_CABIN varchar(30) ,\n" +
            "AK_SEGCABIN varchar(10) ,\n" +
            "SEGMENT_STATUS varchar(30) ,\n" +
            "AK_IRRFLAG varchar(10) ,\n" +
            "AK_SPCA_ATT varchar(50) ,\n" +
            "TIK_SEGSTARTDATE date ,\n" +
            "TIK_SEGENDDATE date ,\n" +
            "IS_RESCHEDULED boolean ,\n" +
            "AK_ADVBOOK_DAY int ,\n" +
            "SEAT_NUMBER varchar(20) ,\n" +
            "SEGMENT_SEQ int ,\n" +
            "HOMECOMING_SEG boolean ,\n" +
            "IS_DESTINATION_STOP boolean ,\n" +
            "STOPOVER_DAY int ,\n" +
            "TIK_SEGCOUNT int ,\n" +
            "SEAT_CHANNEL varchar(30) ,\n" +
            "PRICE_TYPE varchar(20) ,\n" +
            "AK_BOOKING_BRAND varchar(30) ,\n" +
            "CHANNEL_ORDERNO varchar(32) ,\n" +
            "AK_CHANNEL varchar(50) ,\n" +
            "FK_BOOKING_USER_TID varchar(256) ,\n" +
            "FK_BOOKING_USER_ORIGIN_ID varchar(64) ,\n" +
            "AK_COUPON boolean ,\n" +
            "AK_COUPONCODE varchar(50) ,\n" +
            "AK_COUPONTYPE varchar(20) ,\n" +
            "COUPON_AMOUNT decimal(16,6) ,\n" +
            "COUPON_COUNT int ,\n" +
            "COUPONNO varchar(50) ,\n" +
            "COUPON_NAME varchar(50) ,\n" +
            "CONTACT_MOBILE_NUMBER varchar(128) ,\n" +
            "CONTACT_NAME varchar(100) ,\n" +
            "CONTACT_INTER_MOBILE_CODE varchar(10) ,\n" +
            "CONTACT_LANDLINE_MOBILE_NUMBER varchar(128) ,\n" +
            "CONTACT_EMAIL varchar(256) ,\n" +
            "DATA_ACTIVE boolean ,\n" +
            "DATA_ACTIVE_TIME TIMESTAMP(6) ,\n" +
            "SOURCE_LAST_UPDATETIME TIMESTAMP(6) ,\n" +
            "SYSTEM_CREATETIME TIMESTAMP(6) ,\n" +
            "SYSTEM_LAST_UPDATETIME TIMESTAMP(6)  "+
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.DWD_DB+"',\n" +
            "    'table-name' = 'T_DWD_TICKING_SEG_FACT', -- 替换为实际的表名\n" +
            "    'username' = '"+Constants.DWD_USER+"',\n" +
            "    'password' = '"+Constants.DWD_PWD+"'\n" +
            ")";


    public static String DIM_SEG_DIM = "CREATE TABLE IF NOT EXISTS T_DIM_SEG_DIM (\n" +
            "    SEGMENT_KEY STRING NOT NULL,\n" +
            "    AIRLINE STRING,\n" +
            "    OPERAT_AIRLINE STRING,\n" +
            "    OPERAT_FLIGHT_NUM STRING,\n" +
            "    DEPARTURE_DATE DATE,\n" +
            "    DEPARTURE_TIME STRING,\n" +
            "    ARRIVAL_DATE DATE,\n" +
            "    ARRIVAL_TIME STRING,\n" +
            "    DURATION STRING,\n" +
            "    AIRTYPE STRING,\n" +
            "    MARKET_AIRLINE STRING,\n" +
            "    MARKET_FLIGHT_NUM STRING,\n" +
            "    DISTANCE_TPM STRING,\n" +
            "    IS_DELAYED STRING,\n" +
            "    IS_CANCELLED STRING,\n" +
            "    IS_CODE_SHARE STRING,\n" +
            "    CREATE_TIME TIMESTAMP(3),\n" +
            "    UPDATE_TIME TIMESTAMP(3),\n" +
            "    PRIMARY KEY (SEGMENT_KEY) NOT ENFORCED\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.DIM_DB + "',\n" +
            "    'table-name' = 'T_DIM_SEG_DIM',\n" +
            "    'username' = '" + Constants.DIM_USER + "',\n" +
            "    'password' = '" + Constants.DIM_PWD + "'\n" +
            ");";

    public static String SC_TB_TICKET_PRICE_ORDER = "CREATE TABLE SC_TB_TICKET_PRICE_ORDER (\n" +
            "    ETL_DATE DATE,\n" +
            "    ID BIGINT,\n" +
            "    AIR_CODE VARCHAR(30),\n" +
            "    EX_DATE VARCHAR(10),\n" +
            "    UP_LOCATION VARCHAR(10),\n" +
            "    DIS_LOCATION VARCHAR(10),\n" +
            "    PRICE_ONEWAY DECIMAL(8,2),\n" +
            "    PRICE_TOWWAY DECIMAL(8,2),\n" +
            "    START_DATE VARCHAR(10),\n" +
            "    END_DATE VARCHAR(10),\n" +
            "    SEAT_TYPE VARCHAR(1),\n" +
            "    OUT_LINE VARCHAR(240),\n" +
            "    CMD VARCHAR(12),\n" +
            "    DIST BIGINT,\n" +
            "    ETL_CREATE_DATE TIMESTAMP(3),\n" +
            "    ETL_UPDATE_DATE TIMESTAMP(3)\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
            "    'table-name' = 'T_ODS_SC_TB_TICKET_PRICE_ORDER',\n" +
            "    'username' = '" + Constants.ODS_USER + "',\n" +
            "    'password' = '" + Constants.ODS_PWD + "'\n" +
            ")";


}
