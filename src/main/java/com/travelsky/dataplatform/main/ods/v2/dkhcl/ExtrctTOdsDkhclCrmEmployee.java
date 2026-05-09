package com.travelsky.dataplatform.main.ods.v2.dkhcl;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.ApplicationDecryptor;
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

public class ExtrctTOdsDkhclCrmEmployee {
    private static final Logger log = LoggerFactory.getLogger(ExtrctTOdsDkhclCrmEmployee.class);

    public static void main(String[] args) throws Exception {
        log.info("ExtrctTOdsDkhclCrmEmployee data transfer start");

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
        System.out.println(" ExtrctTOdsDkhclCrmEmployee etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);

        // === 查询 Doris 目标表当前最大 ID ===
        long lastMaxId = DorisUtils.queryMaxIdFromDoris("T_ODS_DKHCL_CRM_EMPLOYEE");
        log.info("Last Max ID in Doris: " + lastMaxId);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtrctTOdsDkhclCrmEmployee");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册解密UDF
        tEnv.createTemporarySystemFunction("application_decrptor", ApplicationDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();

        // 创建上游ORACLE数据源表
        log.info("ExtrctTOdsDkhclCrmEmployee executeSql create table for source start");
        tEnv.executeSql("CREATE TABLE CRM_EMPLOYEE (\n" +
                " ID BIGINT,\n" +
                " CREATE_TIME TIMESTAMP(6),\n" +
                " DEL_STATUS INT,\n" +
                " MARK_CODE VARCHAR(100),\n" +
                " BIRTHDAY TIMESTAMP(6),\n" +
                " CONTACT_TEL VARCHAR(64),\n" +
                " EMAIL VARCHAR(50),\n" +
                " EMP_NUMBER VARCHAR(30),\n" +
                " GENDER INT,\n" +
                " IS_NORMAL_EXIT INT,\n" +
                " LAST_LOGIN_IP VARCHAR(30),\n" +
                " LAST_LOGIN_TIME TIMESTAMP(6),\n" +
                " LOGIN_NAME VARCHAR(30),\n" +
                " MOBILE VARCHAR(128),\n" +
                " NAME VARCHAR(30),\n" +
                " PASS_WORD VARCHAR(300),\n" +
                " `POSITION` VARCHAR(30),\n" +
                " EMP_TYPE VARCHAR(10),\n" +
                " FAMILY_NAME VARCHAR(30),\n" +
                " FIRST_NAME VARCHAR(30),\n" +
                " HISTORY_PASS_ONE VARCHAR(300),\n" +
                " HISTORY_PASS_THREE VARCHAR(300),\n" +
                " HISTORY_PASS_TWO VARCHAR(300),\n" +
                " IF_APP INT,\n" +
                " IS_ALLOW_LOGIN TINYINT,\n" +
                " LAST_CREATE_PASS_TIEM TIMESTAMP(6),\n" +
                " MIDDEL_NAME VARCHAR(30),\n" +
                " BEN_ADDRESS VARCHAR(100),\n" +
                " BEN_CONTACT_MAN VARCHAR(30),\n" +
                " BEN_CONTACT_TEL VARCHAR(30),\n" +
                " BEN_FUEL_CARD_NO VARCHAR(30),\n" +
                " BEN_MILE_AGE_NO VARCHAR(30),\n" +
                " BEN_MOBILE VARCHAR(30),\n" +
                " BEN_ZIP_CODE VARCHAR(10),\n" +
                " REMARK VARCHAR(50),\n" +
                " SOFT_PHONE INT,\n" +
                " TELL_TYPE INT,\n" +
                " WP_PASS_WORD VARCHAR(10),\n" +
                " WP_USER_NAME VARCHAR(10),\n" +
                " CREATOR_ID BIGINT,\n" +
                " DEPT_ID BIGINT,\n" +
                " COMPANY_ID BIGINT,\n" +
                " COST_CENTER_ID BIGINT,\n" +
                " OFFICE_NO_ID BIGINT,\n" +
                " OWNED_SALES BIGINT,\n" +
                " CUSTOMER_LEVEL_CODE VARCHAR(30),\n" +
                " ADD_OP_NAME VARCHAR(30),\n" +
                " IS_CONTACT TINYINT,\n" +
                " VIP_STATUS TINYINT,\n" +
                " IS_PRIVACY TINYINT,\n" +
                " PW_ERR_COUNT TINYINT,\n" +
                " PW_ERR_TIME TIMESTAMP(6),\n" +
                " COM_ID BIGINT,\n" +
                " UPDATE_PASSWORD_TIME TIMESTAMP(6),\n" +
                " UPDATE_PASS_WORD VARCHAR(100)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.DKHCL_IP + ":" + Constants.DKHCL_PORT + "/" + Constants.DKHCL_DB + "',\n" +
                "    'table-name' = '" + Constants.DKHCL_SCHEMA + ".CRM_EMPLOYEE', \n" +
                "    'username' = '" + Constants.DKHCL_USER + "',\n" +
                "    'password' = '" + Constants.DKHCL_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        log.info("ExtrctTOdsDkhclCrmEmployee executeSql create table for source end");

        // 创建Doris目标表
        log.info("ExtrctTOdsDkhclCrmEmployee executeSql create table for doris start");
        tEnv.executeSql("CREATE TABLE T_ODS_DKHCL_CRM_EMPLOYEE (\n" +
                " ID BIGINT,\n" +
                " CREATE_TIME TIMESTAMP(6),\n" +
                " DEL_STATUS INT,\n" +
                " MARK_CODE VARCHAR(300),\n" +
                " BIRTHDAY TIMESTAMP(6),\n" +
                " CONTACT_TEL VARCHAR(192),\n" +
                " EMAIL VARCHAR(150),\n" +
                " EMP_NUMBER VARCHAR(90),\n" +
                " GENDER INT,\n" +
                " IS_NORMAL_EXIT INT,\n" +
                " LAST_LOGIN_IP VARCHAR(90),\n" +
                " LAST_LOGIN_TIME TIMESTAMP(6),\n" +
                " LOGIN_NAME VARCHAR(90),\n" +
                " MOBILE VARCHAR(384),\n" +
                " NAME VARCHAR(90),\n" +
                " PASS_WORD VARCHAR(900),\n" +
                " `POSITION` VARCHAR(90),\n" +
                " EMP_TYPE VARCHAR(30),\n" +
                " FAMILY_NAME VARCHAR(90),\n" +
                " FIRST_NAME VARCHAR(90),\n" +
                " HISTORY_PASS_ONE VARCHAR(900),\n" +
                " HISTORY_PASS_THREE VARCHAR(900),\n" +
                " HISTORY_PASS_TWO VARCHAR(900),\n" +
                " IF_APP INT,\n" +
                " IS_ALLOW_LOGIN TINYINT,\n" +
                " LAST_CREATE_PASS_TIEM TIMESTAMP(6),\n" +
                " MIDDEL_NAME VARCHAR(90),\n" +
                " BEN_ADDRESS VARCHAR(300),\n" +
                " BEN_CONTACT_MAN VARCHAR(90),\n" +
                " BEN_CONTACT_TEL VARCHAR(90),\n" +
                " BEN_FUEL_CARD_NO VARCHAR(90),\n" +
                " BEN_MILE_AGE_NO VARCHAR(90),\n" +
                " BEN_MOBILE VARCHAR(90),\n" +
                " BEN_ZIP_CODE VARCHAR(30),\n" +
                " REMARK VARCHAR(150),\n" +
                " SOFT_PHONE INT,\n" +
                " TELL_TYPE INT,\n" +
                " WP_PASS_WORD VARCHAR(30),\n" +
                " WP_USER_NAME VARCHAR(30),\n" +
                " CREATOR_ID BIGINT,\n" +
                " DEPT_ID BIGINT,\n" +
                " COMPANY_ID BIGINT,\n" +
                " COST_CENTER_ID BIGINT,\n" +
                " OFFICE_NO_ID BIGINT,\n" +
                " OWNED_SALES BIGINT,\n" +
                " CUSTOMER_LEVEL_CODE VARCHAR(90),\n" +
                " ADD_OP_NAME VARCHAR(90),\n" +
                " IS_CONTACT TINYINT,\n" +
                " VIP_STATUS TINYINT,\n" +
                " IS_PRIVACY TINYINT,\n" +
                " PW_ERR_COUNT TINYINT,\n" +
                " PW_ERR_TIME TIMESTAMP(6),\n" +
                " COM_ID BIGINT,\n" +
                " UPDATE_PASSWORD_TIME TIMESTAMP(6),\n" +
                " UPDATE_PASS_WORD VARCHAR(300),\n" +
                " ETL_CREATE_TIME TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                " ETL_UPDATE_TIME TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                " ETL_DATE date  NULL COMMENT '数据ETL日期'" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "',\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "',\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_DKHCL_CRM_EMPLOYEE',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "',\n" +
                " 'sink.properties.read_json_by_line' = 'true',\n" +
                " 'sink.properties.format' = 'json',\n" +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        log.info("ExtrctTOdsDkhclCrmEmployee executeSql create table for doris end");

        // 构建抽取SQL（删除ETL_DATE，增加ID增量过滤）
        String extractSql = "INSERT INTO T_ODS_DKHCL_CRM_EMPLOYEE(\n" +
                " ID,\n" +
                " CREATE_TIME,\n" +
                " DEL_STATUS,\n" +
                " MARK_CODE,\n" +
                " BIRTHDAY,\n" +
                " CONTACT_TEL,\n" +
                " EMAIL,\n" +
                " EMP_NUMBER,\n" +
                " GENDER,\n" +
                " IS_NORMAL_EXIT,\n" +
                " LAST_LOGIN_IP,\n" +
                " LAST_LOGIN_TIME,\n" +
                " LOGIN_NAME,\n" +
                " MOBILE,\n" +
                " NAME,\n" +
                " PASS_WORD,\n" +
                " `POSITION`,\n" +
                " EMP_TYPE,\n" +
                " FAMILY_NAME,\n" +
                " FIRST_NAME,\n" +
                " HISTORY_PASS_ONE,\n" +
                " HISTORY_PASS_THREE,\n" +
                " HISTORY_PASS_TWO,\n" +
                " IF_APP,\n" +
                " IS_ALLOW_LOGIN,\n" +
                " LAST_CREATE_PASS_TIEM,\n" +
                " MIDDEL_NAME,\n" +
                " BEN_ADDRESS,\n" +
                " BEN_CONTACT_MAN,\n" +
                " BEN_CONTACT_TEL,\n" +
                " BEN_FUEL_CARD_NO,\n" +
                " BEN_MILE_AGE_NO,\n" +
                " BEN_MOBILE,\n" +
                " BEN_ZIP_CODE,\n" +
                " REMARK,\n" +
                " SOFT_PHONE,\n" +
                " TELL_TYPE,\n" +
                " WP_PASS_WORD,\n" +
                " WP_USER_NAME,\n" +
                " CREATOR_ID,\n" +
                " DEPT_ID,\n" +
                " COMPANY_ID,\n" +
                " COST_CENTER_ID,\n" +
                " OFFICE_NO_ID,\n" +
                " OWNED_SALES,\n" +
                " CUSTOMER_LEVEL_CODE,\n" +
                " ADD_OP_NAME,\n" +
                " IS_CONTACT,\n" +
                " VIP_STATUS,\n" +
                " IS_PRIVACY,\n" +
                " PW_ERR_COUNT,\n" +
                " PW_ERR_TIME,\n" +
                " COM_ID,\n" +
                " UPDATE_PASSWORD_TIME,\n" +
                " UPDATE_PASS_WORD, \n" +
                " ETL_CREATE_TIME,\n" +
                " ETL_UPDATE_TIME,\n" +
                " ETL_DATE)" +
                " SELECT\n" +
                " ID,\n" +
                " CREATE_TIME,\n" +
                " DEL_STATUS,\n" +
                " MARK_CODE,\n" +
                " BIRTHDAY,\n" +
                " CASE WHEN CONTACT_TEL IS NOT NULL THEN sm4_encrypt(application_decrptor(CONTACT_TEL, '" + Constants.DKHCL_AES_KEY_CRM_EMPLOYEE + "'), '" + Constants.SM4_KEY + "') END CONTACT_TEL,\n" +
                " CASE WHEN EMAIL IS NOT NULL THEN sm4_encrypt(EMAIL, '" + Constants.SM4_KEY + "') END EMAIL, \n" +
                " EMP_NUMBER,\n" +
                " GENDER,\n" +
                " IS_NORMAL_EXIT,\n" +
                " LAST_LOGIN_IP,\n" +
                " LAST_LOGIN_TIME,\n" +
                " LOGIN_NAME,\n" +
                " CASE WHEN MOBILE IS NOT NULL THEN sm4_encrypt(MOBILE, '" + Constants.SM4_KEY + "') END MOBILE, \n" +
                " NAME,\n" +
                " PASS_WORD,\n" +
                " `POSITION`,\n" +
                " EMP_TYPE,\n" +
                " FAMILY_NAME,\n" +
                " FIRST_NAME,\n" +
                " HISTORY_PASS_ONE,\n" +
                " HISTORY_PASS_THREE,\n" +
                " HISTORY_PASS_TWO,\n" +
                " IF_APP,\n" +
                " IS_ALLOW_LOGIN,\n" +
                " LAST_CREATE_PASS_TIEM,\n" +
                " MIDDEL_NAME,\n" +
                " BEN_ADDRESS,\n" +
                " BEN_CONTACT_MAN,\n" +
                " BEN_CONTACT_TEL,\n" +
                " BEN_FUEL_CARD_NO,\n" +
                " BEN_MILE_AGE_NO,\n" +
                " BEN_MOBILE,\n" +
                " BEN_ZIP_CODE,\n" +
                " REMARK,\n" +
                " SOFT_PHONE,\n" +
                " TELL_TYPE,\n" +
                " WP_PASS_WORD,\n" +
                " WP_USER_NAME,\n" +
                " CREATOR_ID,\n" +
                " DEPT_ID,\n" +
                " COMPANY_ID,\n" +
                " COST_CENTER_ID,\n" +
                " OFFICE_NO_ID,\n" +
                " OWNED_SALES,\n" +
                " CUSTOMER_LEVEL_CODE,\n" +
                " ADD_OP_NAME,\n" +
                " IS_CONTACT,\n" +
                " VIP_STATUS,\n" +
                " IS_PRIVACY,\n" +
                " PW_ERR_COUNT,\n" +
                " PW_ERR_TIME,\n" +
                " COM_ID,\n" +
                " UPDATE_PASSWORD_TIME,\n" +
                " UPDATE_PASS_WORD, \n" +
                " CURRENT_TIMESTAMP AS ETL_CREATE_TIME,\n" +
                " CURRENT_TIMESTAMP AS ETL_UPDATE_TIME,\n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                " FROM CRM_EMPLOYEE " +
                "WHERE ID > " +  lastMaxId;

        log.info("ExtrctTOdsDkhclCrmEmployee executeSql extract start");
        TableResult result = tEnv.executeSql(extractSql);
        log.info("ExtrctTOdsDkhclCrmEmployee executeSql extract end");

        result.print();
        log.info("ExtrctTOdsDkhclCrmEmployee data transfer end");
    }

}