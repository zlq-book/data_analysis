package com.travelsky.dataplatform.main.ods.v2.dkhcl;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.main.ods.v2.khbp.ExtractTOdsKfbpBusFare;
import com.travelsky.dataplatform.udf.ApplicationDecryptor;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.DorisUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;

public class ExtrctTOdsDkhclAirOrderPassenger {
    public static Logger log = LoggerFactory.getLogger(ExtrctTOdsDkhclAirOrderPassenger.class);

    public static void main(String[] args) throws Exception {
        log.info(" ExtrctTOdsDkhclAirOrderPassenger data transfer start");

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
        System.out.println(" ExtrctTOdsDkhclAirOrderPassenger etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);

        // === 查询 Doris 目标表当前最大 ID ===
        long lastMaxId = DorisUtils.queryMaxIdFromDoris("T_ODS_DKHCL_AIR_ORDER_PASSENGER");
        log.info("Last Max ID in Doris: " + lastMaxId);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, " ExtrctTOdsDkhclAirOrderPassenger");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册解密UDF
        tEnv.createTemporarySystemFunction("application_decrptor", ApplicationDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        log.info(" ExtrctTOdsDkhclAirOrderPassenger executeSql create table for source start");
        tEnv.executeSql("CREATE TABLE AIR_ORDER_PASSENGER (\n" +
                " ID BIGINT,\n" +
                " BIRTH_DAY TIMESTAMP(6),\n" +
                " BOOKING_SMS INT,\n" +
                " CARRIERCARDNO VARCHAR(200),\n" +
                " CERT_NO VARCHAR(64),\n" +
                " CERT_TYPE VARCHAR(20),\n" +
                " EMAIL VARCHAR(30),\n" +
                " EMP_NUMBER VARCHAR(100),\n" +
                " EMPLOYEE_ID BIGINT,\n" +
                " NAME VARCHAR(30),\n" +
                " PASS_TYPE VARCHAR(10),\n" +
                " PSGID VARCHAR(10),\n" +
                " SEX INT,\n" +
                " STATUS INT,\n" +
                " TELEPHONE VARCHAR(64),\n" +
                " TICKET_SMS INT,\n" +
                " COST_CENTER_ID BIGINT,\n" +
                " COST_CENTER_NAME VARCHAR(30),\n" +
                " DEPT_ID BIGINT,\n" +
                " DEPT_MARK_CODE VARCHAR(50),\n" +
                " DEPT_NAME VARCHAR(30),\n" +
                " PROJECT_CODE VARCHAR(30),\n" +
                " AIR_ORDER BIGINT,\n" +
                " CUSTOMER_LEVEL_CODE VARCHAR(30),\n" +
//                " IS_EMPLOYEE TINYINT,\n" +
                " PNR VARCHAR(10),\n" +
                " EFFECTIVE_DATE VARCHAR(10),\n" +
                " NATIONALITY VARCHAR(10),\n" +
                " NATIONALITY_VALUE VARCHAR(50),\n" +
                " SIGN_COUNTRY VARCHAR(10),\n" +
                " SIGN_COUNTRY_VALUE VARCHAR(50),\n" +
                " PSGNUM VARCHAR(10),\n" +
                " CN_NAME_FIRST VARCHAR(50),\n" +
                " CN_NAME_SECOND VARCHAR(50),\n" +
                " ZN_NAME_FIRST VARCHAR(100),\n" +
                " ZN_NAME_SECOND VARCHAR(100)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.DKHCL_IP + ":" + Constants.DKHCL_PORT + "/" + Constants.DKHCL_DB + "',\n" +
                "    'table-name' = '" + Constants.DKHCL_SCHEMA + ".AIR_ORDER_PASSENGER', \n" +
                "    'username' = '" + Constants.DKHCL_USER + "',\n" +
                "    'password' = '" + Constants.DKHCL_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        log.info(" ExtrctTOdsDkhclAirOrderPassenger executeSql create table for source end");
        //创建Doris目标表
        log.info(" ExtrctTOdsDkhclAirOrderPassenger executeSql create table for doris start");
        tEnv.executeSql("CREATE TABLE T_ODS_DKHCL_AIR_ORDER_PASSENGER (\n" +
                " ID BIGINT,\n" +
                " BIRTH_DAY TIMESTAMP(6),\n" +
                " BOOKING_SMS INT,\n" +
                " CARRIERCARDNO VARCHAR(600),\n" +
                " CERT_NO VARCHAR(192),\n" +
                " CERT_TYPE VARCHAR(60),\n" +
                " EMAIL VARCHAR(90),\n" +
                " EMP_NUMBER VARCHAR(300),\n" +
                " EMPLOYEE_ID BIGINT,\n" +
                " NAME VARCHAR(90),\n" +
                " PASS_TYPE VARCHAR(30),\n" +
                " PSGID VARCHAR(30),\n" +
                " SEX INT,\n" +
                " STATUS INT,\n" +
                " TELEPHONE VARCHAR(192),\n" +
                " TICKET_SMS INT,\n" +
                " COST_CENTER_ID BIGINT,\n" +
                " COST_CENTER_NAME VARCHAR(90),\n" +
                " DEPT_ID BIGINT,\n" +
                " DEPT_MARK_CODE VARCHAR(150),\n" +
                " DEPT_NAME VARCHAR(90),\n" +
                " PROJECT_CODE VARCHAR(90),\n" +
                " AIR_ORDER BIGINT,\n" +
                " CUSTOMER_LEVEL_CODE VARCHAR(90),\n" +
                " IS_EMPLOYEE TINYINT,\n" +
                " PNR VARCHAR(30),\n" +
                " EFFECTIVE_DATE VARCHAR(30),\n" +
                " NATIONALITY VARCHAR(30),\n" +
                " NATIONALITY_VALUE VARCHAR(150),\n" +
                " SIGN_COUNTRY VARCHAR(30),\n" +
                " SIGN_COUNTRY_VALUE VARCHAR(150),\n" +
                " PSGNUM VARCHAR(30),\n" +
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
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_DKHCL_AIR_ORDER_PASSENGER',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "',\n" +
                " 'sink.properties.read_json_by_line' = 'true',\n" +
                " 'sink.properties.format' = 'json',\n" +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        log.info(" ExtrctTOdsDkhclAirOrderPassenger executeSql create table for doris end");
        String extractSql = "INSERT INTO T_ODS_DKHCL_AIR_ORDER_PASSENGER(\n" +
                " ID,\n" +
                " BIRTH_DAY,\n" +
                " BOOKING_SMS,\n" +
                " CARRIERCARDNO,\n" +
                " CERT_NO,\n" +
                " CERT_TYPE,\n" +
                " EMAIL,\n" +
                " EMP_NUMBER,\n" +
                " EMPLOYEE_ID,\n" +
                " NAME,\n" +
                " PASS_TYPE,\n" +
                " PSGID,\n" +
                " SEX,\n" +
                " STATUS,\n" +
                " TELEPHONE,\n" +
                " TICKET_SMS,\n" +
                " COST_CENTER_ID,\n" +
                " COST_CENTER_NAME,\n" +
                " DEPT_ID,\n" +
                " DEPT_MARK_CODE,\n" +
                " DEPT_NAME,\n" +
                " PROJECT_CODE,\n" +
                " AIR_ORDER,\n" +
                " CUSTOMER_LEVEL_CODE,\n" +
//                " IS_EMPLOYEE,\n" +
                " PNR,\n" +
                " EFFECTIVE_DATE,\n" +
                " NATIONALITY,\n" +
                " NATIONALITY_VALUE,\n" +
                " SIGN_COUNTRY,\n" +
                " SIGN_COUNTRY_VALUE,\n" +
                " PSGNUM,\n" +
                " CN_NAME_FIRST,\n" +
                " CN_NAME_SECOND,\n" +
                " ZN_NAME_FIRST,\n" +
                " ZN_NAME_SECOND,\n" +
                " ETL_CREATE_TIME,\n" +
                " ETL_UPDATE_TIME,\n" +
                " ETL_DATE)" +

                " SELECT\n" +
                " ID,\n" +
                " BIRTH_DAY,\n" +
                " BOOKING_SMS,\n" +
                " CARRIERCARDNO,\n" +
                " sm4_encrypt(application_decrptor(CERT_NO, '" + Constants.DKHCL_AES_KEY_AIR_ORDER_PASSENGER + "'), '" + Constants.SM4_KEY + "') CERT_NO,\n" +
                " CERT_TYPE,\n" +
                " EMAIL,\n" +
                " EMP_NUMBER,\n" +
                " EMPLOYEE_ID,\n" +
                " NAME,\n" +
                " PASS_TYPE,\n" +
                " PSGID,\n" +
                " SEX,\n" +
                " STATUS,\n" +
                " sm4_encrypt(application_decrptor(TELEPHONE, '" + Constants.DKHCL_AES_KEY_AIR_ORDER_PASSENGER + "'), '" + Constants.SM4_KEY + "') TELEPHONE,\n" +
                " TICKET_SMS,\n" +
                " COST_CENTER_ID,\n" +
                " COST_CENTER_NAME,\n" +
                " DEPT_ID,\n" +
                " DEPT_MARK_CODE,\n" +
                " DEPT_NAME,\n" +
                " PROJECT_CODE,\n" +
                " AIR_ORDER,\n" +
                " CUSTOMER_LEVEL_CODE,\n" +
//                " IS_EMPLOYEE,\n" +
                " PNR,\n" +
                " EFFECTIVE_DATE,\n" +
                " NATIONALITY,\n" +
                " NATIONALITY_VALUE,\n" +
                " SIGN_COUNTRY,\n" +
                " SIGN_COUNTRY_VALUE,\n" +
                " PSGNUM,\n" +
                " CN_NAME_FIRST,\n" +
                " CN_NAME_SECOND,\n" +
                " ZN_NAME_FIRST,\n" +
                " ZN_NAME_SECOND, \n"+
                " CURRENT_TIMESTAMP AS ETL_CREATE_TIME,\n" +
                " CURRENT_TIMESTAMP AS ETL_UPDATE_TIME,\n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                " FROM AIR_ORDER_PASSENGER "+
                " WHERE ID > " + lastMaxId;

        log.info(" ExtrctTOdsDkhclAirOrderPassenger executeSql extract start");
        TableResult result = tEnv.executeSql(extractSql);
        log.info(" ExtrctTOdsDkhclAirOrderPassenger executeSql extract end");
        result.print();

        log.info(" ExtrctTOdsDkhclAirOrderPassenger data transfer end");


    }

}
