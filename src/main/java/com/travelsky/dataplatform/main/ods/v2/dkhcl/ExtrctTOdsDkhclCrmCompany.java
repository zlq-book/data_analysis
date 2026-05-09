package com.travelsky.dataplatform.main.ods.v2.dkhcl;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.DorisUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.UUID;

public class ExtrctTOdsDkhclCrmCompany {
    private static final Logger log = LoggerFactory.getLogger(ExtrctTOdsDkhclCrmCompany.class);

    public static void main(String[] args) throws Exception {
        log.info("ExtrctTOdsDkhclCrmCompany data transfer start");

        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String startDate = etlDate;
        String endDate = etlDate;
        if (args.length > 2) {
            startDate = args[1];
            endDate = args[2];
        }
        System.out.println(" ExtrctTOdsDkhclCrmCompany etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);

        // === 查询 Doris 目标表当前最大 ID ===
        long lastMaxId = DorisUtils.queryMaxIdFromDoris("T_ODS_DKHCL_CRM_COMPANY");
        log.info("Last Max ID in Doris: " + lastMaxId);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtrctTOdsDkhclCrmCompany");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();

        // 创建上游ORACLE数据源表
        log.info("ExtrctTOdsDkhclCrmCompany executeSql create table for source start");
        tEnv.executeSql("CREATE TABLE CRM_COMPANY (\n" +
                " ID BIGINT,\n" +
                " CREATE_TIME TIMESTAMP(6),\n" +
                " DEL_STATUS INT,\n" +
                " MARK_CODE VARCHAR(100),\n" +
                " ADDRESS VARCHAR(50),\n" +
                " ADMIN_USER_NAME VARCHAR(30),\n" +
                " BELONG_CITY VARCHAR(20),\n" +
                " BELONG_PROVINCE VARCHAR(20),\n" +
                " CA_TCIKET_NUM DOUBLE,\n" +
                " CONTACT_NAME VARCHAR(30),\n" +
                " CONTACT_TEL VARCHAR(64),\n" +
                " EMAIL VARCHAR(200),\n" +
                " FAX VARCHAR(30),\n" +
                " HOPE_BUY_CHANNEL VARCHAR(50),\n" +
                " INDUSTRY VARCHAR(50),\n" +
                " NAME VARCHAR(80),\n" +
                " NAME_EN VARCHAR(80),\n" +
                " `SCOPE` VARCHAR(30),\n" +
                " SOURCE VARCHAR(30),\n" +
                " TELEPHONE VARCHAR(60),\n" +
                " TRAVEL_FEES DOUBLE,\n" +
                " WEB_SITE VARCHAR(80),\n" +
                " ZIP_CODE VARCHAR(10),\n" +
                " BEL_CID BIGINT,\n" +
                " CATEGORY VARCHAR(30),\n" +
                " COMPANY_CODE VARCHAR(30),\n" +
                " CONTRACT_BEGIN TIMESTAMP(6),\n" +
                " CONTRACT_END TIMESTAMP(6),\n" +
                " CONTRACT_PATH VARCHAR(150),\n" +
                " CUS_BIG_CODE VARCHAR(50),\n" +
                " CUS_SIGN_CONTACT VARCHAR(64),\n" +
                " CUS_SIGN_MAN VARCHAR(30),\n" +
                " DELI_TIME VARCHAR(30),\n" +
                " DELI_TYPE VARCHAR(30),\n" +
                " DELIVERY_ORG VARCHAR(30),\n" +
                " OPER_DEPARTMENT BIGINT,\n" +
                " BEN_ADDRESS VARCHAR(100),\n" +
                " BEN_CONTACT_MAN VARCHAR(30),\n" +
                " BEN_CONTACT_TEL VARCHAR(32),\n" +
                " BEN_FUEL_CARD_NO VARCHAR(30),\n" +
                " BEN_MILE_AGE_NO VARCHAR(30),\n" +
                " BEN_MOBILE VARCHAR(30),\n" +
                " BEN_ZIP_CODE VARCHAR(10),\n" +
                " POLICY_BENEFIT_TYPE VARCHAR(30),\n" +
                " POLICY_STATUS INT,\n" +
                " RE_CUS_BIG_CODE VARCHAR(20),\n" +
                " RE_CUS_COMPANY_NAME VARCHAR(255),\n" +
                " RE_CUS_MAN_NAME VARCHAR(255),\n" +
                " RE_CUS_MAN_TELPHONE VARCHAR(64),\n" +
                " REG_ID BIGINT,\n" +
                " SERVICE_GRADE VARCHAR(30),\n" +
                " SIGNING_DATE TIMESTAMP(6),\n" +
                " UATP_CARD_NO VARCHAR(100),\n" +
                " VALIDITY_DATE TIMESTAMP(6),\n" +
                " CREATOR_ID BIGINT,\n" +
                " SALES_DEPT_ID BIGINT,\n" +
                " CA_SIGN_MAN BIGINT,\n" +
                " MANAGER_TELEPHONE VARCHAR(100),\n" +
                " MANAGER_MOBILE VARCHAR(100),\n" +
                " DELI_WAY VARCHAR(20),\n" +
                " EFF_DATE_TYPE TINYINT,\n" +
                " MONGODB_NAME VARCHAR(500),\n" +
                " AGREE_TIME TIMESTAMP(6),\n" +
                " ATTESTATION_TIME TIMESTAMP(6),\n" +
                " ATTESTATION_PATH VARCHAR(200),\n" +
                " ATTESTATION_NAME VARCHAR(500),\n" +
                " AGREE_NAME VARCHAR(30),\n" +
                " EMP_ID BIGINT,\n" +
                " IS_AUDIT_PASS TINYINT,\n" +
                " IS_AGREEMENT TINYINT" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.DKHCL_IP + ":" + Constants.DKHCL_PORT + "/" + Constants.DKHCL_DB + "',\n" +
                "    'table-name' = '" + Constants.DKHCL_SCHEMA + ".CRM_COMPANY', \n" +
                "    'username' = '" + Constants.DKHCL_USER + "',\n" +
                "    'password' = '" + Constants.DKHCL_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        log.info("ExtrctTOdsDkhclCrmCompany executeSql create table for source end");

        // 创建Doris目标表
        log.info("ExtrctTOdsDkhclCrmCompany executeSql create table for doris start");
        tEnv.executeSql("CREATE TABLE T_ODS_DKHCL_CRM_COMPANY (\n" +
                " ID BIGINT,\n" +
                " CREATE_TIME TIMESTAMP(6),\n" +
                " DEL_STATUS INT,\n" +
                " MARK_CODE VARCHAR(300),\n" +
                " ADDRESS VARCHAR(150),\n" +
                " ADMIN_USER_NAME VARCHAR(90),\n" +
                " BELONG_CITY VARCHAR(60),\n" +
                " BELONG_PROVINCE VARCHAR(60),\n" +
                " CA_TCIKET_NUM DOUBLE,\n" +
                " CONTACT_NAME VARCHAR(90),\n" +
                " CONTACT_TEL VARCHAR(192),\n" +
                " EMAIL VARCHAR(600),\n" +
                " FAX VARCHAR(90),\n" +
                " HOPE_BUY_CHANNEL VARCHAR(150),\n" +
                " INDUSTRY VARCHAR(150),\n" +
                " NAME VARCHAR(240),\n" +
                " NAME_EN VARCHAR(240),\n" +
                " `SCOPE` VARCHAR(90),\n" +
                " SOURCE VARCHAR(90),\n" +
                " TELEPHONE VARCHAR(180),\n" +
                " TRAVEL_FEES DOUBLE,\n" +
                " WEB_SITE VARCHAR(240),\n" +
                " ZIP_CODE VARCHAR(30),\n" +
                " BEL_CID BIGINT,\n" +
                " CATEGORY VARCHAR(90),\n" +
                " COMPANY_CODE VARCHAR(90),\n" +
                " CONTRACT_BEGIN TIMESTAMP(6),\n" +
                " CONTRACT_END TIMESTAMP(6),\n" +
                " CONTRACT_PATH VARCHAR(450),\n" +
                " CUS_BIG_CODE VARCHAR(150),\n" +
                " CUS_SIGN_CONTACT VARCHAR(192),\n" +
                " CUS_SIGN_MAN VARCHAR(90),\n" +
                " DELI_TIME VARCHAR(90),\n" +
                " DELI_TYPE VARCHAR(90),\n" +
                " DELIVERY_ORG VARCHAR(90),\n" +
                " OPER_DEPARTMENT BIGINT,\n" +
                " BEN_ADDRESS VARCHAR(300),\n" +
                " BEN_CONTACT_MAN VARCHAR(90),\n" +
                " BEN_CONTACT_TEL VARCHAR(96),\n" +
                " BEN_FUEL_CARD_NO VARCHAR(90),\n" +
                " BEN_MILE_AGE_NO VARCHAR(90),\n" +
                " BEN_MOBILE VARCHAR(90),\n" +
                " BEN_ZIP_CODE VARCHAR(30),\n" +
                " POLICY_BENEFIT_TYPE VARCHAR(90),\n" +
                " POLICY_STATUS INT,\n" +
                " RE_CUS_BIG_CODE VARCHAR(60),\n" +
                " RE_CUS_COMPANY_NAME VARCHAR(765),\n" +
                " RE_CUS_MAN_NAME VARCHAR(765),\n" +
                " RE_CUS_MAN_TELPHONE VARCHAR(192),\n" +
                " REG_ID BIGINT,\n" +
                " SERVICE_GRADE VARCHAR(90),\n" +
                " SIGNING_DATE TIMESTAMP(6),\n" +
                " UATP_CARD_NO VARCHAR(300),\n" +
                " VALIDITY_DATE TIMESTAMP(6),\n" +
                " CREATOR_ID BIGINT,\n" +
                " SALES_DEPT_ID BIGINT,\n" +
                " CA_SIGN_MAN BIGINT,\n" +
                " MANAGER_TELEPHONE VARCHAR(300),\n" +
                " MANAGER_MOBILE VARCHAR(300),\n" +
                " DELI_WAY VARCHAR(60),\n" +
                " EFF_DATE_TYPE TINYINT,\n" +
                " MONGODB_NAME VARCHAR(1500),\n" +
                " AGREE_TIME TIMESTAMP(6),\n" +
                " ATTESTATION_TIME TIMESTAMP(6),\n" +
                " ATTESTATION_PATH VARCHAR(600),\n" +
                " ATTESTATION_NAME VARCHAR(1500),\n" +
                " AGREE_NAME VARCHAR(90),\n" +
                " EMP_ID BIGINT,\n" +
                " IS_AUDIT_PASS TINYINT,\n" +
                " IS_AGREEMENT TINYINT,\n" +
                " ETL_CREATE_TIME TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                " ETL_UPDATE_TIME TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                " ETL_DATE date  NULL COMMENT '数据ETL日期'" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "',\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "',\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_DKHCL_CRM_COMPANY',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "',\n" +
                " 'sink.properties.read_json_by_line' = 'true',\n" +
                " 'sink.properties.format' = 'json',\n" +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        log.info("ExtrctTOdsDkhclCrmCompany executeSql create table for doris end");

        // 去除敏感字段
        String extractSql = "INSERT INTO T_ODS_DKHCL_CRM_COMPANY(\n" +
                " ID,\n" +
                " CREATE_TIME,\n" +
                " DEL_STATUS,\n" +
                " MARK_CODE,\n" +
                " ADDRESS,\n" +
                " ADMIN_USER_NAME,\n" +
                " BELONG_CITY,\n" +
                " BELONG_PROVINCE,\n" +
                " CA_TCIKET_NUM,\n" +
                " CONTACT_NAME,\n" +
                " EMAIL,\n" +
                " HOPE_BUY_CHANNEL,\n" +
                " INDUSTRY,\n" +
                " NAME,\n" +
                " NAME_EN,\n" +
                " `SCOPE`,\n" +
                " SOURCE,\n" +
                " TRAVEL_FEES,\n" +
                " WEB_SITE,\n" +
                " ZIP_CODE,\n" +
                " BEL_CID,\n" +
                " CATEGORY,\n" +
                " COMPANY_CODE,\n" +
                " CONTRACT_BEGIN,\n" +
                " CONTRACT_END,\n" +
                " CONTRACT_PATH,\n" +
                " CUS_BIG_CODE,\n" +
                " CUS_SIGN_MAN,\n" +
                " DELI_TIME,\n" +
                " DELI_TYPE,\n" +
                " DELIVERY_ORG,\n" +
                " OPER_DEPARTMENT,\n" +
                " BEN_ADDRESS,\n" +
                " BEN_CONTACT_MAN,\n" +
                " BEN_CONTACT_TEL,\n" +
                " BEN_FUEL_CARD_NO,\n" +
                " BEN_MILE_AGE_NO,\n" +
                " BEN_MOBILE,\n" +
                " BEN_ZIP_CODE,\n" +
                " POLICY_BENEFIT_TYPE,\n" +
                " POLICY_STATUS,\n" +
                " RE_CUS_BIG_CODE,\n" +
                " RE_CUS_COMPANY_NAME,\n" +
                " RE_CUS_MAN_NAME,\n" +
                " RE_CUS_MAN_TELPHONE,\n" +
                " REG_ID,\n" +
                " SERVICE_GRADE,\n" +
                " SIGNING_DATE,\n" +
                " UATP_CARD_NO,\n" +
                " VALIDITY_DATE,\n" +
                " CREATOR_ID,\n" +
                " SALES_DEPT_ID,\n" +
                " CA_SIGN_MAN,\n" +
                " MANAGER_TELEPHONE,\n" +
                " MANAGER_MOBILE,\n" +
                " DELI_WAY,\n" +
                " EFF_DATE_TYPE,\n" +
                " MONGODB_NAME,\n" +
                " AGREE_TIME,\n" +
                " ATTESTATION_TIME,\n" +
                " ATTESTATION_PATH,\n" +
                " ATTESTATION_NAME,\n" +
                " AGREE_NAME,\n" +
                " EMP_ID,\n" +
                " IS_AUDIT_PASS,\n" +
                " IS_AGREEMENT, \n" +
                " ETL_CREATE_TIME,\n" +
                " ETL_UPDATE_TIME,\n" +
                " ETL_DATE)" +
                " SELECT\n" +
                " ID,\n" +
                " CREATE_TIME,\n" +
                " DEL_STATUS,\n" +
                " MARK_CODE,\n" +
                " ADDRESS,\n" +
                " ADMIN_USER_NAME,\n" +
                " BELONG_CITY,\n" +
                " BELONG_PROVINCE,\n" +
                " CA_TCIKET_NUM,\n" +
                " CONTACT_NAME,\n" +
                " EMAIL,\n" +
                " HOPE_BUY_CHANNEL,\n" +
                " INDUSTRY,\n" +
                " NAME,\n" +
                " NAME_EN,\n" +
                " `SCOPE`,\n" +
                " SOURCE,\n" +
                " TRAVEL_FEES,\n" +
                " WEB_SITE,\n" +
                " ZIP_CODE,\n" +
                " BEL_CID,\n" +
                " CATEGORY,\n" +
                " COMPANY_CODE,\n" +
                " CONTRACT_BEGIN,\n" +
                " CONTRACT_END,\n" +
                " CONTRACT_PATH,\n" +
                " CUS_BIG_CODE,\n" +
                " CUS_SIGN_MAN,\n" +
                " DELI_TIME,\n" +
                " DELI_TYPE,\n" +
                " DELIVERY_ORG,\n" +
                " OPER_DEPARTMENT,\n" +
                " BEN_ADDRESS,\n" +
                " BEN_CONTACT_MAN,\n" +
                " BEN_CONTACT_TEL,\n" +
                " BEN_FUEL_CARD_NO,\n" +
                " BEN_MILE_AGE_NO,\n" +
                " BEN_MOBILE,\n" +
                " BEN_ZIP_CODE,\n" +
                " POLICY_BENEFIT_TYPE,\n" +
                " POLICY_STATUS,\n" +
                " RE_CUS_BIG_CODE,\n" +
                " RE_CUS_COMPANY_NAME,\n" +
                " RE_CUS_MAN_NAME,\n" +
                " RE_CUS_MAN_TELPHONE,\n" +
                " REG_ID,\n" +
                " SERVICE_GRADE,\n" +
                " SIGNING_DATE,\n" +
                " UATP_CARD_NO,\n" +
                " VALIDITY_DATE,\n" +
                " CREATOR_ID,\n" +
                " SALES_DEPT_ID,\n" +
                " CA_SIGN_MAN,\n" +
                " MANAGER_TELEPHONE,\n" +
                " MANAGER_MOBILE,\n" +
                " DELI_WAY,\n" +
                " EFF_DATE_TYPE,\n" +
                " MONGODB_NAME,\n" +
                " AGREE_TIME,\n" +
                " ATTESTATION_TIME,\n" +
                " ATTESTATION_PATH,\n" +
                " ATTESTATION_NAME,\n" +
                " AGREE_NAME,\n" +
                " EMP_ID,\n" +
                " IS_AUDIT_PASS,\n" +
                " IS_AGREEMENT, \n" +
                " CURRENT_TIMESTAMP AS ETL_CREATE_TIME,\n" +
                " CURRENT_TIMESTAMP AS ETL_UPDATE_TIME,\n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                " FROM CRM_COMPANY " +
                "WHERE ID > " + lastMaxId;

        log.info("ExtrctTOdsDkhclCrmCompany executeSql extract start");
        TableResult result = tEnv.executeSql(extractSql);
        log.info("ExtrctTOdsDkhclCrmCompany executeSql extract end");

        result.print();
        log.info("ExtrctTOdsDkhclCrmCompany data transfer end");
    }
}