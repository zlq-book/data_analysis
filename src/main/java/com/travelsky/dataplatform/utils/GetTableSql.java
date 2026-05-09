package com.travelsky.dataplatform.utils;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.trp.usercenter.data.analysis.transform.hsd.NoshowFactTrans;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kuangaihua
 * @date 2025/7/31 10:30
 */
public class GetTableSql {
    static final Logger logger = LoggerFactory.getLogger(GetTableSql.class);

    /* public static String getDorisConnector(String Database, String TableName, String user, String pwd) {
         String connStr = "  'connector' = 'doris',\n"
                 + "  'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "',\n"
                 + "  'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "',\n"
                 + "'source.use-flight-sql' = 'true',\n"
                 + "'source.flight-sql-port' = '" + Constants.DORIS_FLIGHT_PORT + "',"
                 + "'doris.batch.size' = '" + Constants.DORIS_BATCH_SIZE + "',"
                 + "'doris.request.read.timeout' = '" + Constants.DORIS_READ_TIMEOUT + "',"
                 + "  'table.identifier' = '" + Database + "." + TableName + "',\n"
                 + "  'username' = '" + user + "',\n"
                 + "  'password' = '" + pwd + "'\n";

         return connStr;
     }*/
    public static String getDorisConnector(String Database, String TableName, String user, String pwd) {
        String connStr = "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Database + "',\n" +
                "    'table-name' = '" + TableName + "', -- 替换为实际的表名\n" +
                "    'username' = '" + user + "',\n" +
                "    'password' = '" + pwd + "'\n";
        return connStr;
    }

    //注册TRP数仓T_ODS_UPLOAD_B2C_ORDERINFO查询表 sql
    public static String getQueryTOdsUploadB2cOrderinfo() {
        String sql = "CREATE TABLE T_ODS_UPLOAD_B2C_ORDERINFO (\n" +
                "ID BIGINT ,\n" +
                "ORDER_NO VARCHAR(54) ,\n" +
                "BANK_ORDER_NO VARCHAR(210) ,\n" +
                "VOUCHER_NO VARCHAR(30) ,\n" +
                "PNR VARCHAR(18) ,\n" +
                "TICKET_NO VARCHAR(42) ,\n" +
                "FLIGHT_NO VARCHAR(30) ,\n" +
                "UP_LOCATION VARCHAR(60) ,\n" +
                "DEST_LOCATION VARCHAR(60) ,\n" +
                "CABIN VARCHAR(15) ,\n" +
                "TAKEOFF_TIME TIMESTAMP(6) ,\n" +
                "PASS_NAME VARCHAR(108) ,\n" +
                "ID_NO VARCHAR(256) ,\n" +
                "ORIGINAL_FARE DECIMAL(10,2) ,\n" +
                "DISCOUNT_FARE DECIMAL(10,2) ,\n" +
                "CTAX DECIMAL(10,2) ,\n" +
                "FTAX DECIMAL(10,2) ,\n" +
                "FARE_OTHER DECIMAL(10,2) ,\n" +
                "INSURANCE DECIMAL(10,2) ,\n" +
                "FARE_TOTAL DECIMAL(10,2) ,\n" +
                "PRODUCT_NAME VARCHAR(60) ,\n" +
                "ORDER_DATE TIMESTAMP(6) ,\n" +
                "ORDER_STATUS VARCHAR(60) ,\n" +
                "ORDER_SOURCE DECIMAL(1,0) ,\n" +
                "ORDER_USER VARCHAR(150) ,\n" +
                "REGISTER_USER VARCHAR(150) ,\n" +
                "ORDER_MOBILE VARCHAR(60) ,\n" +
                "ORDER_EMAIL VARCHAR(256) ,\n" +
                "BANK_NAME VARCHAR(90) ,\n" +
                "R_CARRIER VARCHAR(12) ,\n" +
                "GUEST_SCODE VARCHAR(300) ,\n" +
                "CONTACT_NAME VARCHAR(90) ,\n" +
                "MEM_NUM VARCHAR(256) ,\n" +
                "FILE_DATE VARCHAR(900) ,\n" +
                "ETL_INSERT_TIME TIMESTAMP(6) ,\n" +
                "ETL_UPDATE_TIME TIMESTAMP(6) ,\n" +
                "FLIGHT_SEGMENT VARCHAR(300) ,\n" +
                "FLIGHT_SEGMENT_CODE VARCHAR(120) ,\n" +
                "TK_TIME TIMESTAMP(6) ,\n" +
                "ORDER_CHANNEL VARCHAR(120) ,\n" +
                "PAY_TIME TIMESTAMP(6) ,\n" +
                "TK_PROCEDURE VARCHAR(90) ,\n" +
                "PASSENGER_TYPE VARCHAR(30) ,\n" +
                "ID_TYPE VARCHAR(90) ,\n" +
                "TRIP_TYPE VARCHAR(30) ,\n" +
                "D_OR_I VARCHAR(30) ,\n" +
                "REDUCTION VARCHAR(30) ,\n" +
                "COUPON_NO VARCHAR(90) ,\n" +
                "CONN_MOBILE VARCHAR(256) ,\n" +
                "SITE VARCHAR(30) ,\n" +
                "`LANGUAGE` VARCHAR(30) ,\n" +
                "CURRENCY_TYPE VARCHAR(15) ,\n" +
                "TEL_PHONE VARCHAR(60) ,\n" +
                "COUPON_NAME VARCHAR(150) ,\n" +
                "COUPON_FARE VARCHAR(30) ,\n" +
                "COUPON_NUM VARCHAR(120) ,\n" +
                "FARE_ZHIJIAN VARCHAR(30) ,\n" +
                "FARE_JTCXZJ VARCHAR(30) ,\n" +
                "IS_JTCXZJ VARCHAR(60) ,\n" +
                "FAREBASIS VARCHAR(120) ,\n" +
                "IS_ZSBX VARCHAR(12) ,\n" +
                "NAME_ZSBX VARCHAR(300) ,\n" +
                "IS_ZSKQ VARCHAR(12) ,\n" +
                "ETL_DATE DATE  \n" +
                ") WITH (\n" +
                GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_UPLOAD_B2C_ORDERINFO", Constants.ODS_USER, Constants.ODS_PWD)
                + ")";
        logger.info("TRP数仓B2C订单信息：" + sql.replace(Constants.ODS_PWD, "******"));
        return sql;
    }

    public static String getQueryTOdsHytdTManageOthercard() {
        String sql = "CREATE TABLE T_ODS_HYTD_T_MANAGE_OTHERCARD (\n" +
                "  `ETL_DATE` DATE,\n" +
                "    `ID` STRING COMMENT '唯一标识',\n" +
                "    `CRM_CARDNO` STRING COMMENT 'CRM系统卡号',\n" +
                "    `CARD_NO` STRING COMMENT '物理卡号',\n" +
                "    `NAME` STRING COMMENT '姓名(中文)',\n" +
                "    `NAME_EN` STRING COMMENT '姓名(英文)',\n" +
                "    `CERT_TYPE` STRING COMMENT '证件类型',\n" +
                "    `CERT_NO` STRING COMMENT '证件号码',\n" +
                "    `END_TIME` TIMESTAMP(6) COMMENT '有效期截止时间'\n" +
                ") WITH (\n" +
                GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_HYTD_T_MANAGE_OTHERCARD", Constants.ODS_USER, Constants.ODS_PWD)
                + ")";
        logger.info("注册表：" + sql.replace(Constants.ODS_PWD, "******"));
        return sql;
    }

    public static String getQueryTOdsHytdTManageProductNamelist() {
        String sql = "CREATE TABLE T_ODS_HYTD_T_MANAGE_PRODUCT_NAMELIST (\n" +
                "  `ETL_DATE` DATE,\n" +
                "    ID VARCHAR(150)  COMMENT '主键',\n" +
                "    PRODUCT_ID VARCHAR(150) COMMENT '产品ID (产品名称，中英文均可能，对内)',\n" +
                "    MEMBER_ID VARCHAR(150) COMMENT '会员ID (国航的数据，非卡号)',\n" +
                "    CREATED_TIME TIMESTAMP(6) COMMENT '报名时间',\n" +
                "    MODIFIED_TIME TIMESTAMP(6) COMMENT '修改时间（同报名时间）',\n" +
                "    MEMBER_CARD VARCHAR(150) COMMENT '会员卡号',\n" +
                "    CRED_CODE VARCHAR(300) COMMENT '证件号',\n" +
                "    MOBILE_NUMBER VARCHAR(60) COMMENT '手机号',\n" +
                "    ATTRIBUTE VARCHAR(300) COMMENT '属性（不关心）',\n" +
                "    LEVEL_NAME VARCHAR(300) COMMENT '会员级别',\n" +
                "    CN_LAST_NAME VARCHAR(600) COMMENT '中文姓',\n" +
                "    CN_FIRST_NAME VARCHAR(600) COMMENT '中文名',\n" +
                "    LAST_NAME VARCHAR(600) COMMENT '英文姓',\n" +
                "    FIRST_NAME VARCHAR(600) COMMENT '英文名',\n" +
                "    DATEOF_BIRTH TIMESTAMP(6) COMMENT '生日',\n" +
                "    PROMO_CODE VARCHAR(300) COMMENT '促销代码（只有部分产品有，产品里程上传相关）',\n" +
                "    CHANNEL_ID VARCHAR(300) COMMENT '渠道ID（从哪里报名的，APP、网站、小程序等）--需要渠道号及枚举值，以及与会员发展渠道的关系',\n" +
                "    CHANNEL_NAME VARCHAR(300) COMMENT '渠道名称'" +
                ") WITH (\n" +
                GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_HYTD_T_MANAGE_PRODUCT_NAMELIST", Constants.ODS_USER, Constants.ODS_PWD)
                + ")";
        logger.info("注册表：" + sql.replace(Constants.ODS_PWD, "******"));
        return sql;
    }

    public static String getQueryTOdsHytdSysRegister() {
        String sql = "CREATE TABLE T_ODS_HYTD_SYS_REGISTER (\n" +
                "  `ETL_DATE` DATE,\n" +
                "  `ID` BIGINT,\n" +
                "  `CRM_MEMBER_ID` VARCHAR(32),\n" +
                "  `MEMBER_NUMBER` VARCHAR(32),\n" +
                "  `CN_LAST_NAME` VARCHAR(100),\n" +
                "  `CN_FIRST_NAME` VARCHAR(100),\n" +
                "  `LAST_NAME` VARCHAR(100),\n" +
                "  `FIRST_NAME` VARCHAR(100),\n" +
                "  `GENDER` VARCHAR(30),\n" +
                "  `BIRTHDAY` VARCHAR(12),\n" +
                "  `CREDENTIAL_TYPE` VARCHAR(32),\n" +
                "  `CREDENTIAL_NUM` VARCHAR(100),\n" +
                "  `NATIONALITY` VARCHAR(60),\n" +
                "  `LANGUAGE` VARCHAR(100),\n" +
                "  `PIN_QUESTION` VARCHAR(100),\n" +
                "  `PIN_ANSWER` VARCHAR(100),\n" +
                "  `EMAIL_TYPE` VARCHAR(60),\n" +
                "  `EMAIL_ADDR` VARCHAR(100),\n" +
                "  `COMPANY` VARCHAR(100),\n" +
                "  `DEPARTMENT` VARCHAR(100),\n" +
                "  `CARD_STATUS` VARCHAR(60),\n" +
                "  `PIN_STATUS` VARCHAR(12),\n" +
                "  `ADDRESS_TYPE` VARCHAR(60),\n" +
                "  `COUNTRY` VARCHAR(60),\n" +
                "  `STATE` VARCHAR(32),\n" +
                "  `CITY` VARCHAR(100),\n" +
                "  `STREET` VARCHAR(512),\n" +
                "  `POSTAL_CODE` VARCHAR(30),\n" +
                "  `PHONE_TYPE` VARCHAR(30),\n" +
                "  `COUNTRY_CODE` VARCHAR(15),\n" +
                "  `PHONE_NUM` VARCHAR(100),\n" +
                "  `PARENT_MEMBER_NUM` VARCHAR(30),\n" +
                "  `FAST_CREATE_FLAG` VARCHAR(60),\n" +
                "  `FORCE_CREATE_FLAG` VARCHAR(60),\n" +
                "  `NO_PRO_SMS_FLAG` VARCHAR(12),\n" +
                "  `NO_PRO_EMAIL_FLAG` VARCHAR(12),\n" +
                "  `SUBMIT_PERSON` VARCHAR(32),\n" +
                "  `CHANNEL_ID` VARCHAR(32),\n" +
                "  `CHANNEL_NAME` VARCHAR(32),\n" +
                "  `REGISTER_STATUS` VARCHAR(6),\n" +
                "  `CRM_CODE` VARCHAR(32),\n" +
                "  `CRMR_MSG` VARCHAR(1024),\n" +
                "  `CREATOR` BIGINT,\n" +
                "  `CREATE_DATE` TIMESTAMP(6),\n" +
                "  `REGISTER_CHANNEL` VARCHAR(32)\n" +
                ") WITH (\n" +
                GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_HYTD_SYS_REGISTER", Constants.ODS_USER, Constants.ODS_PWD)
                + ")";
        logger.info("注册表：" + sql.replace(Constants.ODS_PWD, "******"));
        return sql;
    }

    public static String getQueryTDimStrongid() {
        String sql = "CREATE TABLE T_DIM_STRONGID (\n" +
                "    STRONGID VARCHAR(256) COMMENT '强ID',\n" +
                "    STRONGID_TYPE VARCHAR(20) COMMENT '强ID类型',\n" +
                "    TID VARCHAR(256)  COMMENT '强ID挂载的TID',\n" +
                "    STRONGID_STATUS INT COMMENT '挂载的TID有效性',\n" +
                "    CREATE_TIME TIMESTAMP(6)  COMMENT '创建时间',\n" +
                "    UPDATE_TIME TIMESTAMP(6)  COMMENT '更新时间',\n" +
                "    ETL_DATE DATE COMMENT '数据ETL日期'" +
                ") WITH (\n" +
                GetTableSql.getDorisConnector(Constants.DIM_DB, "T_DIM_STRONGID", Constants.DIM_USER, Constants.DIM_PWD)
                + ")";
        logger.info("注册表：" + sql.replace(Constants.DIM_PWD, "******"));
        return sql;
    }

    public static String getQueryTDimIdmappingStrongid() {
        String sql = "CREATE TABLE T_DIM_IDMAPPING_STRONGID (\n" +
                "    STRONGID VARCHAR(256) COMMENT '强ID',\n" +
                "    CREATE_TIME TIMESTAMP(6)  COMMENT '创建时间',\n" +
                "    ETL_DATE DATE COMMENT '数据ETL日期'" +
                ") WITH (\n" +
                GetTableSql.getDorisConnector(Constants.DIM_DB, "T_DIM_IDMAPPING_STRONGID", Constants.DIM_USER, Constants.DIM_PWD)
                + ")";
        logger.info("注册表：" + sql.replace(Constants.DIM_PWD, "******"));
        return sql;
    }

    public static String getQueryTOdsWxapWxCheckinInsure() {
        String sql = "CREATE TABLE T_ODS_WXAP_WX_CHECKIN_INSURE (\n" +
                "ETL_DATE DATE, " +
                "ID VARCHAR(96), " +
                "FID VARCHAR(96), " +
                "UNIONID VARCHAR(96), " +
                "INSID VARCHAR(96), " +
                "FLIGHTNO VARCHAR(90), " +
                "DEPDATE TIMESTAMP(6), " +
                "ARRDATE TIMESTAMP(6), " +
                "DEP VARCHAR(60), " +
                "ARR VARCHAR(60), " +
                "DEPNAME VARCHAR(150), " +
                "ARRNAME VARCHAR(150), " +
                "CHECKID VARCHAR(96), " +
                "CERTNAME VARCHAR(150), " +
                "CERTTYPE VARCHAR(30), " +
                "CERTNO VARCHAR(300), " +
                "MOBILE VARCHAR(300), " +
                "PRODUCTNAME VARCHAR(150), " +
                "PRICE BIGINT, " +
                "TOTAL BIGINT, " +
                "PAYID VARCHAR(150), " +
                "PAYTIME BIGINT, " +
                "OSTATUS TINYINT, " +
                "CREATE_TIME TIMESTAMP(6), " +
                "DELETE_FLAG TINYINT, " +
                "FIDDISPLAY SMALLINT, " +
                "PAYLOGID VARCHAR(96), " +
                "OPENID VARCHAR(96), " +
                "INVOICE STRING, " +
                "ETN VARCHAR(60), " +
                "PAYSTATUS TINYINT, " +
                "INSURE_TYPE TINYINT, " +
                "UPDATE_TIME TIMESTAMP(6), " +
                "CERTEMAIL VARCHAR(150), " +
                "CERTBIRTHDAY VARCHAR(60), " +
                "POLICYID VARCHAR(96), " +
                "ORDER_NO VARCHAR(150), " +
                "CABIN VARCHAR(12), " +
                "POLICY_RESULT STRING, " +
                "POLICY_TYPE TINYINT, " +
                "INSURE_MOLD TINYINT, " +
                "FLIGHT_INDEX TINYINT, " +
                "CHANNEL_TYPE TINYINT " +
                ") WITH (\n" +
                GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_WXAP_WX_CHECKIN_INSURE", Constants.ODS_USER, Constants.ODS_PWD)
                + ")";
        logger.info("注册表：" + sql.replace(Constants.ODS_PWD, "******"));
        return sql;
    }

    public static String getQueryTOdsWxapCheckinItinerary() {
        String sql = "CREATE TABLE T_ODS_WXAP_CHECKIN_ITINERARY (\n" +
                "ETL_DATE DATE," +
                "ID BIGINT, " +
                "DEP VARCHAR(5), " +
                "ARR VARCHAR(5), " +
                "FLIGHT_NO VARCHAR(16), " +
                "CARR_FLIGHT_NO VARCHAR(16), " +
                "FLIGHT_DATE VARCHAR(10), " +
                "SCH_DEPT_TIME VARCHAR(8), " +
                "SCH_ARR_TIME VARCHAR(8), " +
                "EXP_DEPT_TIME VARCHAR(8), " +
                "BOARDING_TIME VARCHAR(8), " +
                "BOARDING_GATE_NUMBER VARCHAR(32), " +
                "FROM_CITY_STATUS VARCHAR(2), " +
                "TO_CITY_STATUS VARCHAR(2), " +
                "DEPT_TERMINAL_NAME VARCHAR(32), " +
                "ARRIVE_TERMINAL_NAME VARCHAR(32), " +
                "ISSUE_AIRLINE VARCHAR(32), " +
                "EXTRA_GATES VARCHAR(32), " +
                "PLANE_TYPE VARCHAR(32), " +
                "PLANE_CLASS VARCHAR(32), " +
                "STOP_OVER_INFO VARCHAR(4000), " +
                "CREATE_TIME TIMESTAMP(6), " +
                "UPDATE_TIME TIMESTAMP(6), " +
                "CREATE_BY VARCHAR(64), " +
                "UPDATE_BY VARCHAR(64), " +
                "ARR_DATE VARCHAR(10)" +
                ") WITH (\n" +
                GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_WXAP_CHECKIN_ITINERARY", Constants.ODS_USER, Constants.ODS_PWD)
                + ")";
        logger.info("注册表：" + sql.replace(Constants.ODS_PWD, "******"));
        return sql;
    }

    public static String getQueryTOdsWxapCheckinSypr() {
        String sql = "CREATE TABLE T_ODS_WXAP_CHECKIN_SYPR (\n" +
                "    ETL_DATE DATE,\n" +
                "  ID BIGINT,\n" +
                "  CI_ID BIGINT,\n" +
                "  CHECKIN_NO STRING,\n" +
                "  TKT_NUMBER STRING,\n" +
                "  PNR STRING,\n" +
                "  PSR_NAME STRING,\n" +
                "  PSR_EN_NAME STRING,\n" +
                "  CERTIFICATE_TYPE STRING,\n" +
                "  CERTIFICATE_NUMBER STRING,\n" +
                "  PHONE STRING,\n" +
                "  ASR_SEAT STRING,\n" +
                "  CABIN_TYPE STRING,\n" +
                "  SPEICIAL_SVC STRING,\n" +
                "  ASR_STATUS STRING,\n" +
                "  HOST_NUM STRING,\n" +
                "  TOUR_INDEX STRING,\n" +
                "  CHD_FLAG STRING,\n" +
                "  CHD STRING,\n" +
                "  SEAT_NO STRING,\n" +
                "  BOARD_STREAM STRING,\n" +
                "  BOARDING_NUMBER STRING,\n" +
                "  CHECK_CODE STRING,\n" +
                "  CHECKIN_NUM TINYINT,\n" +
                "  CHANGE_SEAT_NO_NUM TINYINT,\n" +
                "  CHECKIN_STATUS TINYINT,\n" +
                "  CHECKIN_TYPE TINYINT,\n" +
                "  FARE_BASIS STRING,\n" +
                "  DIFF_CHECKIN TINYINT,\n" +
                "  SECURITY_CHECK_TIME TIMESTAMP(6),\n" +
                "  CHECKIN_TIME TIMESTAMP(6),\n" +
                "  WRAP_COUNT INT,\n" +
                "  FAIL_REASON STRING,\n" +
                "  HANDLE_TYPE TINYINT,\n" +
                "  CREATE_TIME TIMESTAMP(6),\n" +
                "  UPDATE_TIME TIMESTAMP(6),\n" +
                "  CREATE_BY STRING,\n" +
                "  UPDATE_BY STRING,\n" +
                "  AUTO_CHECKIN_TIME TIMESTAMP(6),\n" +
                "  LAST_CHECKIN_CHANNEL TINYINT\n" +
                ") WITH (\n" +
                GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_WXAP_CHECKIN_SYPR", Constants.ODS_USER, Constants.ODS_PWD)
                + ")";
        logger.info("注册表：" + sql.replace(Constants.ODS_PWD, "******"));
        return sql;
    }

    public static String getQueryTOdsWxapCheckinChannelUser() {
        String sql = "CREATE TABLE T_ODS_WXAP_CHECKIN_CHANNEL_USER (\n" +
                "    ETL_DATE DATE,\n" +
                "    ID BIGINT NOT NULL,\n" +
                "    SYPR_ID BIGINT NOT NULL,\n" +
                "    USER_ID VARCHAR(32),\n" +
                "    OPENID VARCHAR(32),\n" +
                "    UNIONID VARCHAR(32),\n" +
                "    CHECKIN_CHANNEL TINYINT NOT NULL,\n" +
                "    DEL_STATUS TINYINT,\n" +
                "    CREATE_TIME TIMESTAMP(6),\n" +
                "    UPDATE_TIME TIMESTAMP(6),\n" +
                "    CREATE_BY VARCHAR(64),\n" +
                "    UPDATE_BY VARCHAR(64),\n" +
                "    MAIN_OPERATOR TINYINT" +
                ") WITH (\n" +
                GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_WXAP_CHECKIN_CHANNEL_USER", Constants.ODS_USER, Constants.ODS_PWD)
                + ")";
        logger.info("注册表：" + sql.replace(Constants.ODS_PWD, "******"));
        return sql;
    }

    public static String getQueryTOdsAlpUserStudentAuthen() {
        String sql = "CREATE TABLE T_ODS_ALP_USER_STUDENT_AUTHEN (\n" +
                "ETL_DATE DATE, " +
                "ID INT, " +
                "OPEN_ID STRING, " +
                "UNION_ID STRING, " +
                "CHANNEL_TYPE TINYINT, " +
                "CERT_NAME STRING, " +
                "CERT_NO STRING, " +
                "LINK_PHONE STRING, " +
                "SCHOOL_ENROLL_DATE DATE, " +
                "SCHOOL_EXPIRE_DATE DATE, " +
                "SCHOOL_NAME STRING, " +
                "DEGREE TINYINT, " +
                "IS_EXAMINE TINYINT, " +
                "IS_STUDENT TINYINT, " +
                "DELETED TINYINT, " +
                "CREATE_TIME TIMESTAMP(6), " +
                "UPDATE_TIME TIMESTAMP(6), " +
                "AUTH_TYPE TINYINT, " +
                "AUTHEN_TYPE TINYINT, " +
                "CUSTOMER_ID STRING " +
                ") WITH (\n" +

                GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_ALP_USER_STUDENT_AUTHEN", Constants.ODS_USER, Constants.ODS_PWD)

                + ")";
        logger.info("注册表：" + sql.replace(Constants.ODS_PWD, "******"));
        return sql;
    }

    public static String getQueryTOdsWxapCheckinTrpOrder() {
        String sql = "CREATE TABLE T_ODS_WXAP_CHECKIN_TRP_ORDER (\n" +
                "    ETL_DATE DATE,\n" +
                "    ID BIGINT,\n" +
                "    ORDER_NO VARCHAR(96),\n" +
                "    ORDER_AMOUNT BIGINT,\n" +
                "    SEAT_TYPE TINYINT,\n" +
                "    ORDER_STATUS TINYINT,\n" +
                "    ORDER_TIME TIMESTAMP(6),\n" +
                "    CONTACT_PERSON VARCHAR(96),\n" +
                "    CONTACT_PHONE VARCHAR(192),\n" +
                "    CREATE_TIME TIMESTAMP(6),\n" +
                "    UPDATE_TIME TIMESTAMP(6),\n" +
                "    PAY_TYPE TINYINT,\n" +
                "    CUSTOMER_ID VARCHAR(96),\n" +
                "    OPEN_ID VARCHAR(96),\n" +
                "    CHANNEL TINYINT,\n" +
                "    PAY_NO VARCHAR(96),\n" +
                "    THIRD_PAY_NO VARCHAR(96),\n" +
                "    TRP_PAY_NO VARCHAR(96),\n" +
                "    PAY_STATUS TINYINT,\n" +
                "    TRP_PAY_STATUS TINYINT\n" +
                ") WITH (\n" +
                GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_WXAP_CHECKIN_TRP_ORDER", Constants.ODS_USER, Constants.ODS_PWD)
                + ")";
        logger.info("注册表：" + sql.replace(Constants.ODS_PWD, "******"));
        return sql;
    }

    public static String getQueryTOdsWxapCheckinTrpOrderPassenger() {
        String sql = "CREATE TABLE T_ODS_WXAP_CHECKIN_TRP_ORDER_PASSENGER (\n" +
                "ETL_DATE DATE, " +
                "ID BIGINT, " +
                "ORDER_NO VARCHAR(96), " +
                "FLIGHT_NO VARCHAR(36), " +
                "FLIGHT_DATE VARCHAR(30), " +
                "DEP VARCHAR(15), " +
                "ARR VARCHAR(15), " +
                "TICKET_NO VARCHAR(96), " +
                "SEAT_NO VARCHAR(24), " +
                "PSR_NAME VARCHAR(192), " +
                "SEAT_TYPE TINYINT, " +
                "SEAT_AMOUNT BIGINT, " +
                "DEP_TIME VARCHAR(24), " +
                "ARR_TIME VARCHAR(24), " +
                "EMD_NO VARCHAR(96), " +
                "SEAT_STATUS TINYINT, " +
                "PNR VARCHAR(36), " +
                "TRP_ORDER_NO VARCHAR(96), " +
                "SHARE_FLIGHT_NO VARCHAR(36), " +
                "ASR_SEAT_NO VARCHAR(24), " +
                "CREATE_TIME TIMESTAMP(6), " +
                "UPDATE_TIME TIMESTAMP(6), " +
                "PSR_TYPE VARCHAR(48), " +
                "ARR_DATE VARCHAR(30), " +
                "DURATION BIGINT" +
                ") WITH (\n" +
                GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_WXAP_CHECKIN_TRP_ORDER_PASSENGER", Constants.ODS_USER, Constants.ODS_PWD)
                + ")";
        logger.info("注册表：" + sql.replace(Constants.ODS_PWD, "******"));
        return sql;
    }

    public static String getFieldStr(String str) {
        return str == null ? "NULL ,\n" : ("'" + str + "',\n");
    }

    public static String getFieldStr(Boolean bool) {
        return bool == null ? "NULL ,\n" : (bool + ",\n");
    }

    public static String getFieldStr(Integer num) {
        return num == null ? "NULL ,\n" : (num + ",\n");
    }

    public static String getFieldStrEnd(String str) {
        return str == null ? "NULL \n" : ("'" + str + "'\n");
    }

    public static String getFieldStrEnd(Boolean bool) {
        return bool == null ? "NULL \n" : (bool + "\n");
    }

    public static String getFieldStrEnd(Integer num) {
        return num == null ? "NULL \n" : (num + "\n");
    }

    public static String getQueryTOdsHytdTManageProduct() {
        String sql = "CREATE TABLE T_ODS_HYTD_T_MANAGE_PRODUCT (\n" +
                "  `ID` STRING NOT NULL COMMENT '主键',\n" +
                "  `NAME` STRING COMMENT '产品名称',\n" +
                "  `START_TIME` TIMESTAMP(6) COMMENT '生效时间',\n" +
                "  `END_TIME` TIMESTAMP(6) COMMENT '失效时间',\n" +
                "  `STATUS` STRING COMMENT '生效状态',\n" +
                "  `IMG_URL` STRING COMMENT '产品封面',\n" +
                "  `CONTENT` STRING COMMENT '产品描述',\n" +
                "  `CREATED_TIME` TIMESTAMP(6) COMMENT '创建时间',\n" +
                "  `MODIFIED_TIME` TIMESTAMP(6) COMMENT '修改时间',\n" +
                "  `PROMO_CODE` STRING COMMENT '国航促销活动Code',\n" +
                "  `PAGE_TYPE` STRING COMMENT '宣传页类型',\n" +
                "  `PAGE_URL` STRING COMMENT 'H5定制页面地址',\n" +
                "  `PAGE_URL_PC` STRING COMMENT 'PC端定制页面地址',\n" +
                "  `IMG_URL_HEIGHT` STRING COMMENT '封面图片',\n" +
                "  `IMG_URL_LIST` STRING COMMENT '封面图片',\n" +
                "  `IMG_URL_PC` STRING COMMENT '封面图片',\n" +
                "  `AIR_CHINA_END_TIME` TIMESTAMP(6) COMMENT '国航平台系统设置结束时间',\n" +
                "  `CLICK_VOLUME` INT COMMENT '点击次数',\n" +
                "  `CONTENT_PC` STRING COMMENT 'PC端内容',\n" +
                "  `NEED_SIGN` STRING,\n" +
                "  `ETL_CREATE_TIME` TIMESTAMP(6) COMMENT '数据入仓时间',\n" +
                "  `ETL_UPDATE_TIME` TIMESTAMP(6) COMMENT '数据在数仓更新时间时间',\n" +
                "  `ETL_DATE` DATE COMMENT '数据ETL日期'\n" +
                ") WITH (\n" +
                GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_HYTD_T_MANAGE_PRODUCT", Constants.ODS_USER, Constants.ODS_PWD)
                + ")";
        logger.info("注册表：" + sql.replace(Constants.ODS_PWD, "******"));
        return sql;
    }
}
