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

import java.util.UUID;

public class ExtrctTOdsDkhclCrmEmployeeCert {
    private static final Logger log = LoggerFactory.getLogger(ExtrctTOdsDkhclCrmEmployeeCert.class);

    public static void main(String[] args) throws Exception {
        log.info("ExtrctTOdsDkhclCrmEmployeeCert data transfer start");

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
        System.out.println(" ExtrctTOdsDkhclCrmEmployeeCert etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);

        // === 查询 Doris 目标表当前最大 ID ===
        long lastMaxId = DorisUtils.queryMaxIdFromDoris("T_ODS_DKHCL_CRM_EMPLOYEE_CERT");
        log.info("Last Max ID in Doris: " + lastMaxId);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtrctTOdsDkhclCrmEmployeeCert");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册解密UDF
        tEnv.createTemporarySystemFunction("application_decrptor", ApplicationDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();

        // 创建上游ORACLE数据源表
        log.info("ExtrctTOdsDkhclCrmEmployeeCert executeSql create table for source start");
        tEnv.executeSql("CREATE TABLE CRM_EMPLOYEE_CERT (\n" +
                " ID BIGINT,\n" +
                " CREATE_TIME TIMESTAMP(6),\n" +
                " DEL_STATUS INT,\n" +
                " MARK_CODE VARCHAR(100),\n" +
                " BIRTHDAY VARCHAR(30),\n" +
                " CERT_NAME VARCHAR(30),\n" +
                " CERT_NO VARCHAR(200),\n" +
                " CERT_TYPE VARCHAR(30),\n" +
                " EFFECTIVE_DATE TIMESTAMP(6),\n" +
                " NATIONALITY VARCHAR(50),\n" +
                " SEX INT,\n" +
                " CREATOR_ID BIGINT,\n" +
                " EMPLOYEE_ID BIGINT,\n" +
                " SIGN_COUNTRY VARCHAR(30),\n" +
                " NATIONALITY_CN VARCHAR(20),\n" +
                " SIGN_COUNTRY_CN VARCHAR(20),\n" +
//                " EFFECTIVE_DATE_STR VARCHAR(100),\n" +
                " CN_NAME_FIRST VARCHAR(50),\n" +
                " CN_NAME_SECOND VARCHAR(50),\n" +
                " ZN_NAME_FIRST VARCHAR(100),\n" +
                " ZN_NAME_SECOND VARCHAR(100)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.DKHCL_IP + ":" + Constants.DKHCL_PORT + "/" + Constants.DKHCL_DB + "',\n" +
                "    'table-name' = '" + Constants.DKHCL_SCHEMA + ".CRM_EMPLOYEE_CERT', \n" +
                "    'username' = '" + Constants.DKHCL_USER + "',\n" +
                "    'password' = '" + Constants.DKHCL_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        log.info("ExtrctTOdsDkhclCrmEmployeeCert executeSql create table for source end");

        // 创建Doris目标表
        log.info("ExtrctTOdsDkhclCrmEmployeeCert executeSql create table for doris start");
        tEnv.executeSql("CREATE TABLE T_ODS_DKHCL_CRM_EMPLOYEE_CERT (\n" +
                " ID BIGINT,\n" +
                " CREATE_TIME TIMESTAMP(6),\n" +
                " DEL_STATUS INT,\n" +
                " MARK_CODE VARCHAR(300),\n" +
                " BIRTHDAY VARCHAR(90),\n" +
                " CERT_NAME VARCHAR(90),\n" +
                " CERT_NO VARCHAR(600),\n" +
                " CERT_TYPE VARCHAR(90),\n" +
                " EFFECTIVE_DATE TIMESTAMP(6),\n" +
                " NATIONALITY VARCHAR(150),\n" +
                " SEX INT,\n" +
                " CREATOR_ID BIGINT,\n" +
                " EMPLOYEE_ID BIGINT,\n" +
                " SIGN_COUNTRY VARCHAR(90),\n" +
                " NATIONALITY_CN VARCHAR(60),\n" +
                " SIGN_COUNTRY_CN VARCHAR(60),\n" +
                " EFFECTIVE_DATE_STR VARCHAR(300),\n" +
                " CN_NAME_FIRST VARCHAR(150),\n" +
                " CN_NAME_SECOND VARCHAR(150),\n" +
                " ZN_NAME_FIRST VARCHAR(300),\n" +
                " ZN_NAME_SECOND VARCHAR(300),\n" +
                " ETL_CREATE_TIME TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                " ETL_UPDATE_TIME TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                " ETL_DATE date  NULL COMMENT '数据ETL日期'" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "',\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "',\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_DKHCL_CRM_EMPLOYEE_CERT',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "',\n" +
                " 'sink.properties.read_json_by_line' = 'true',\n" +
                " 'sink.properties.format' = 'json',\n" +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        log.info("ExtrctTOdsDkhclCrmEmployeeCert executeSql create table for doris end");

        // 构建抽取SQL（删除ETL_DATE，增加ID增量过滤）
        String extractSql = "INSERT INTO T_ODS_DKHCL_CRM_EMPLOYEE_CERT(\n" +
                " ID,\n" +
                " CREATE_TIME,\n" +
                " DEL_STATUS,\n" +
                " MARK_CODE,\n" +
                " BIRTHDAY,\n" +
                " CERT_NAME,\n" +
                " CERT_NO,\n" +
                " CERT_TYPE,\n" +
                " EFFECTIVE_DATE,\n" +
                " NATIONALITY,\n" +
                " SEX,\n" +
                " CREATOR_ID,\n" +
                " EMPLOYEE_ID,\n" +
                " SIGN_COUNTRY,\n" +
                " NATIONALITY_CN,\n" +
                " SIGN_COUNTRY_CN,\n" +
//                " EFFECTIVE_DATE_STR,\n" +
                " CN_NAME_FIRST,\n" +
                " CN_NAME_SECOND,\n" +
                " ZN_NAME_FIRST,\n" +
                " ZN_NAME_SECOND, \n" +
                " ETL_CREATE_TIME,\n" +
                " ETL_UPDATE_TIME,\n" +
                " ETL_DATE)" +
                " SELECT\n" +
                " ID,\n" +
                " CREATE_TIME,\n" +
                " DEL_STATUS,\n" +
                " MARK_CODE,\n" +
                " BIRTHDAY,\n" +
                " CERT_NAME,\n" +
                " CASE WHEN CERT_NO IS NOT NULL THEN sm4_encrypt(application_decrptor(CERT_NO, '" + Constants.DKHCL_AES_KEY_CRM_EMPLOYEE_CERT + "'), '" + Constants.SM4_KEY + "') END CERT_NO,\n" +
                " CERT_TYPE,\n" +
                " EFFECTIVE_DATE,\n" +
                " NATIONALITY,\n" +
                " SEX,\n" +
                " CREATOR_ID,\n" +
                " EMPLOYEE_ID,\n" +
                " SIGN_COUNTRY,\n" +
                " NATIONALITY_CN,\n" +
                " SIGN_COUNTRY_CN,\n" +
//                " EFFECTIVE_DATE_STR,\n" +
                " CN_NAME_FIRST,\n" +
                " CN_NAME_SECOND,\n" +
                " ZN_NAME_FIRST,\n" +
                " ZN_NAME_SECOND, \n" +
                " CURRENT_TIMESTAMP AS ETL_CREATE_TIME,\n" +
                " CURRENT_TIMESTAMP AS ETL_UPDATE_TIME,\n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                " FROM CRM_EMPLOYEE_CERT " +
                "WHERE ID > " + lastMaxId;

        log.info("ExtrctTOdsDkhclCrmEmployeeCert executeSql extract start");
        TableResult result = tEnv.executeSql(extractSql);
        log.info("ExtrctTOdsDkhclCrmEmployeeCert executeSql extract end");

        result.print();
        log.info("ExtrctTOdsDkhclCrmEmployeeCert data transfer end");
    }

}