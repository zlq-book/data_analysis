package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.trp.usercenter.data.analysis.transform.hytd.SysRegisterToCertDimTrans;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * @author
 * @date 2025/7/2 9:31
 */
public class ExtractTOdsZxyhCrmCustomer {
    private static final Logger logger = LoggerFactory.getLogger(ExtractTOdsZxyhCrmCustomer.class);

    public static void main(String[] args) throws IOException {
        System.out.println("ExtractTOdsZxyhCrmCustomer data transfer start");
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        System.out.println("ExtractTOdsZxyhCrmCustomer etl_date:" + etlDate);
        String sm4key = "JUzgwCrDIT6v4SMg+BMX4A==";
        System.out.println("ExtractTOdsZxyhCrmCustomer getExecutionEnvironment start");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZxyhCrmCustomer");
        env.setParallelism(1);
        System.out.println("ExtractTOdsZxyhCrmCustomer getExecutionEnvironment end");
        System.out.println("ExtractTOdsZxyhCrmCustomer StreamTableEnvironment.create(env) start");
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        System.out.println("ExtractTOdsZxyhCrmCustomer StreamTableEnvironment.create(env) end");
        // 注册SM4加密UDF
        System.out.println("ExtractTOdsZxyhCrmCustomer register SM4 encrypt start");
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        System.out.println("ExtractTOdsZxyhCrmCustomer register SM4 encrypt end");
        System.out.println("ExtractTOdsZxyhCrmCustomer executeSql create table for source start");

        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游数据源表
        tEnv.executeSql("CREATE TABLE CRM_CUSTOMER (\n" +
                "    ID VARCHAR(96),\n" +
                "    CUSTOMER_ID VARCHAR(1500),\n" +
                "    CN_NAME VARCHAR(192),\n" +
                "    FIRST_CN_NAME VARCHAR(192),\n" +
                "    LAST_CN_NAME VARCHAR(192),\n" +
                "    ENG_NAME VARCHAR(192),\n" +
                "    FIRST_ENG_NAME VARCHAR(192),\n" +
                "    LAST_ENG_NAME VARCHAR(192),\n" +
                "    BIRTHDAY VARCHAR(6000),\n" +
                "    SEX CHAR(1),\n" +
                "    PROVINCE VARCHAR(300),\n" +
                "    `NATIONAL` VARCHAR(300),\n" +
                "    ETHNIC_GROUP VARCHAR(300),\n" +
                "    PRIMARY_PHONE_ID VARCHAR(96),\n" +
                "    REAL_NAME_FLAG CHAR(1),\n" +
                "    REAL_NAME_TIME VARCHAR(300),\n" +
                "    REAL_NAME_TYPE VARCHAR(6),\n" +
                "    REGIST_CHANNEL VARCHAR(6),\n" +
                "    POTENTIAL_FLAG CHAR(1),\n" +
                "    AGENT_FLAG CHAR(1),\n" +
                "    HISTORY_FLAG CHAR(1),\n" +
                "    CUSTOMER_STATUS CHAR(1),\n" +
                "    LAST_LOGIN_TIME TIMESTAMP(6),\n" +
                "    CUSTOMER_DESC VARCHAR(1500),\n" +
                "    COMMENTS VARCHAR(1500),\n" +
                "    CUSTOMER_SERVICE VARCHAR(12000),\n" +
                "    REGIST_DATE TIMESTAMP(6),\n" +
                "    TRUSTWORTHY CHAR(1),\n" +
                "    REGIST_IP VARCHAR(300),\n" +
                "    REFERRER VARCHAR(96),\n" +
                "    CUSTOMER_LEVEL CHAR(1),\n" +
                "    ENABLE CHAR(1),\n" +
                "    CREATE_TIME TIMESTAMP(6),\n" +
                "    UPDATE_TIME TIMESTAMP(6),\n" +
                "    CREATE_USER VARCHAR(96),\n" +
                "    UPDATE_USER VARCHAR(96),\n" +
                "    FILTER1 VARCHAR(600),\n" +
                "    FILTER2 VARCHAR(600),\n" +
                "    FILTER3 VARCHAR(600),\n" +
                "    FILTER4 VARCHAR(600),\n" +
                "    FILTER5 VARCHAR(600),\n" +
                "    FILTER6 VARCHAR(600),\n" +
                "    FILTER7 VARCHAR(600),\n" +
                "    FILTER8 VARCHAR(600),\n" +
                "    FILTER9 VARCHAR(600),\n" +
                "    VERIFY_CLV_FLAG CHAR(1),\n" +
                "    FOOD_PREF VARCHAR(600),\n" +
                "    SEAT_PREF VARCHAR(600),\n" +
                "    DEVICE_NO VARCHAR(600),\n" +
                "    FIRST_LOGIN_DATE TIMESTAMP(6),\n" +
                "    RETENTION_FLAG CHAR(1),\n" +
                "    WEB_IS_LIMITED CHAR(1),\n" +
                "    WEB_USER_STATUS CHAR(1),\n" +
                "    WEB_AUTHORIZE CHAR(1),\n" +
                "    REAL_NAME_CHANNEL VARCHAR(30),\n" +
                "    COUNTRY_AREA_CODE VARCHAR(30)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.ZXYH_IP + ":" + Constants.ZXYH_PORT + "/" + Constants.ZXYH_DB + "',\n" +
                "    'table-name' = '" + Constants.ZXYH_SCHEMA + ".CRM_CUSTOMER', \n" +
                "    'username' = '" + Constants.ZXYH_USER + "',\n" +
                "    'password' = '" + Constants.ZXYH_PWD + "'\n" +
                ",\n" + "  'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        System.out.println("ExtractTOdsZxyhCrmCustomer executeSql create table for source end");
        //创建Doris目标表
        System.out.println("ExtractTOdsZxyhCrmCustomer executeSql create table for doris start");
        tEnv.executeSql("CREATE TABLE T_ODS_ZXYH_CRM_CUSTOMER (\n" +
                "    ETL_DATE DATE,\n" +
                "    ID VARCHAR(96),\n" +
                "    CUSTOMER_ID VARCHAR(1500),\n" +
                "    CN_NAME VARCHAR(192),\n" +
                "    FIRST_CN_NAME VARCHAR(192),\n" +
                "    LAST_CN_NAME VARCHAR(192),\n" +
                "    ENG_NAME VARCHAR(192),\n" +
                "    FIRST_ENG_NAME VARCHAR(192),\n" +
                "    LAST_ENG_NAME VARCHAR(192),\n" +
                "    BIRTHDAY VARCHAR(6000),\n" +
                "    SEX CHAR(1),\n" +
                "    PROVINCE VARCHAR(300),\n" +
                "    `NATIONAL` VARCHAR(300),\n" +
                "    ETHNIC_GROUP VARCHAR(300),\n" +
                "    PRIMARY_PHONE_ID VARCHAR(96),\n" +
                "    REAL_NAME_FLAG CHAR(1),\n" +
                "    REAL_NAME_TIME VARCHAR(300),\n" +
                "    REAL_NAME_TYPE VARCHAR(6),\n" +
                "    REGIST_CHANNEL VARCHAR(6),\n" +
                "    POTENTIAL_FLAG CHAR(1),\n" +
                "    AGENT_FLAG CHAR(1),\n" +
                "    HISTORY_FLAG CHAR(1),\n" +
                "    CUSTOMER_STATUS CHAR(1),\n" +
                "    LAST_LOGIN_TIME TIMESTAMP(6),\n" +
                "    CUSTOMER_DESC VARCHAR(1500),\n" +
                "    COMMENTS VARCHAR(1500),\n" +
                "    CUSTOMER_SERVICE VARCHAR(12000),\n" +
                "    REGIST_DATE TIMESTAMP(6),\n" +
                "    TRUSTWORTHY CHAR(1),\n" +
                "    REGIST_IP VARCHAR(300),\n" +
                "    REFERRER VARCHAR(96),\n" +
                "    CUSTOMER_LEVEL CHAR(1),\n" +
                "    ENABLE CHAR(1),\n" +
                "    CREATE_TIME TIMESTAMP(6),\n" +
                "    UPDATE_TIME TIMESTAMP(6),\n" +
                "    CREATE_USER VARCHAR(96),\n" +
                "    UPDATE_USER VARCHAR(96),\n" +
                "    FILTER1 VARCHAR(600),\n" +
                "    FILTER2 VARCHAR(600),\n" +
                "    FILTER3 VARCHAR(600),\n" +
                "    FILTER4 VARCHAR(600),\n" +
                "    FILTER5 VARCHAR(600),\n" +
                "    FILTER6 VARCHAR(600),\n" +
                "    FILTER7 VARCHAR(600),\n" +
                "    FILTER8 VARCHAR(600),\n" +
                "    FILTER9 VARCHAR(600),\n" +
                "    VERIFY_CLV_FLAG CHAR(1),\n" +
                "    FOOD_PREF VARCHAR(600),\n" +
                "    SEAT_PREF VARCHAR(600),\n" +
                "    DEVICE_NO VARCHAR(600),\n" +
                "    FIRST_LOGIN_DATE TIMESTAMP(6),\n" +
                "    RETENTION_FLAG CHAR(1),\n" +
                "    WEB_IS_LIMITED CHAR(1),\n" +
                "    WEB_USER_STATUS CHAR(1),\n" +
                "    WEB_AUTHORIZE CHAR(1),\n" +
                "    REAL_NAME_CHANNEL VARCHAR(30),\n" +
                "    COUNTRY_AREA_CODE VARCHAR(30)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZXYH_CRM_CUSTOMER',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        System.out.println("ExtractTOdsZxyhCrmCustomer executeSql create table for doris end");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_ZXYH_CRM_CUSTOMER(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "CUSTOMER_ID,\n" +
                "CN_NAME,\n" +
                "FIRST_CN_NAME,\n" +
                "LAST_CN_NAME,\n" +
                "ENG_NAME,\n" +
                "FIRST_ENG_NAME,\n" +
                "LAST_ENG_NAME,\n" +
                "BIRTHDAY,\n" +
                "SEX,\n" +
                "PROVINCE,\n" +
                "`NATIONAL`,\n" +
                "ETHNIC_GROUP,\n" +
                "PRIMARY_PHONE_ID,\n" +
                "REAL_NAME_FLAG,\n" +
                "REAL_NAME_TIME,\n" +
                "REAL_NAME_TYPE,\n" +
                "REGIST_CHANNEL,\n" +
                "POTENTIAL_FLAG,\n" +
                "AGENT_FLAG,\n" +
                "HISTORY_FLAG,\n" +
                "CUSTOMER_STATUS,\n" +
                "LAST_LOGIN_TIME,\n" +
                "CUSTOMER_DESC,\n" +
                "COMMENTS,\n" +
                "CUSTOMER_SERVICE,\n" +
                "REGIST_DATE,\n" +
                "TRUSTWORTHY,\n" +
                "REGIST_IP,\n" +
                "REFERRER,\n" +
                "CUSTOMER_LEVEL,\n" +
                "ENABLE,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_USER,\n" +
                "FILTER1,\n" +
                "FILTER2,\n" +
                "FILTER3,\n" +
                "FILTER4,\n" +
                "FILTER5,\n" +
                "FILTER6,\n" +
                "FILTER7,\n" +
                "FILTER8,\n" +
                "FILTER9,\n" +
                "VERIFY_CLV_FLAG,\n" +
                "FOOD_PREF,\n" +
                "SEAT_PREF,\n" +
                "DEVICE_NO,\n" +
                "FIRST_LOGIN_DATE,\n" +
                "RETENTION_FLAG,\n" +
                "WEB_IS_LIMITED,\n" +
                "WEB_USER_STATUS,\n" +
                "WEB_AUTHORIZE,\n" +
                "REAL_NAME_CHANNEL,\n" +
                "COUNTRY_AREA_CODE)\n" +
                "SELECT " +
                        "DATE '" + etlDate + "' AS ETL_DATE," +
                        "ID,\n" +
                        "CUSTOMER_ID,\n" +
                        "CN_NAME,\n" +
                        "FIRST_CN_NAME,\n" +
                        "LAST_CN_NAME,\n" +
                        "ENG_NAME,\n" +
                        "FIRST_ENG_NAME,\n" +
                        "LAST_ENG_NAME,\n" +
                        "BIRTHDAY,\n" +
                        "SEX,\n" +
                        "PROVINCE,\n" +
                        "`NATIONAL`,\n" +
                        "ETHNIC_GROUP,\n" +
                        "PRIMARY_PHONE_ID,\n" +
                        "REAL_NAME_FLAG,\n" +
                        "REAL_NAME_TIME,\n" +
                        "REAL_NAME_TYPE,\n" +
                        "REGIST_CHANNEL,\n" +
                        "POTENTIAL_FLAG,\n" +
                        "AGENT_FLAG,\n" +
                        "HISTORY_FLAG,\n" +
                        "CUSTOMER_STATUS,\n" +
                        "LAST_LOGIN_TIME,\n" +
                        "CUSTOMER_DESC,\n" +
                        "COMMENTS,\n" +
                        "CUSTOMER_SERVICE,\n" +
                        "REGIST_DATE,\n" +
                        "TRUSTWORTHY,\n" +
                        "REGIST_IP,\n" +
                        "REFERRER,\n" +
                        "CUSTOMER_LEVEL,\n" +
                        "ENABLE,\n" +
                        "CREATE_TIME,\n" +
                        "UPDATE_TIME,\n" +
                        "CREATE_USER,\n" +
                        "UPDATE_USER,\n" +
                        "FILTER1,\n" +
                        "FILTER2,\n" +
                        "FILTER3,\n" +
                        "FILTER4,\n" +
                        "FILTER5,\n" +
                        "FILTER6,\n" +
                        "FILTER7,\n" +
                        "FILTER8,\n" +
                        "FILTER9,\n" +
                        "VERIFY_CLV_FLAG,\n" +
                        "FOOD_PREF,\n" +
                        "SEAT_PREF,\n" +
                        "DEVICE_NO,\n" +
                        "FIRST_LOGIN_DATE,\n" +
                        "RETENTION_FLAG,\n" +
                        "WEB_IS_LIMITED,\n" +
                        "WEB_USER_STATUS,\n" +
                        "WEB_AUTHORIZE,\n" +
                        "REAL_NAME_CHANNEL,\n" +
                        "COUNTRY_AREA_CODE\n" +
                        " FROM CRM_CUSTOMER "
                        + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                        +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        System.out.println("ExtractTOdsZxyhCrmCustomer executeSql extract start");
        System.out.println("extractSql:" + extractSql);
        TableResult result = tEnv.executeSql(extractSql);
        System.out.println("ExtractTOdsZxyhCrmCustomer executeSql extract end");

        result.print();
        System.out.println("ExtractTOdsZxyhCrmCustomer data transfer end");


    }
}
