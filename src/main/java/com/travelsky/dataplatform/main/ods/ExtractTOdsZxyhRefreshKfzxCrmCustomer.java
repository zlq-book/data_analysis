package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.UUID;

/**
 * @author
 * @date 2025/7/2 9:31
 */
public class ExtractTOdsZxyhRefreshKfzxCrmCustomer {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZxyhRefreshKfzxCrmCustomer");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE REFRESH_KFZX_CRM_CUSTOMER (\n" +
                "ID VARCHAR(96) ,\n" +
                "CUSTOMER_ID VARCHAR(96) ,\n" +
                "CN_NAME VARCHAR(192) ,\n" +
                "FIRST_CN_NAME VARCHAR(192) ,\n" +
                "LAST_CN_NAME VARCHAR(192) ,\n" +
                "ENG_NAME VARCHAR(192) ,\n" +
                "FIRST_ENG_NAME VARCHAR(192) ,\n" +
                "LAST_ENG_NAME VARCHAR(192) ,\n" +
                "BIRTHDAY VARCHAR(6000) ,\n" +
                "SEX VARCHAR(30) ,\n" +
                "PROVINCE VARCHAR(300) ,\n" +
                "`NATIONAL` VARCHAR(300) ,\n" +
                "ETHNIC_GROUP VARCHAR(300) ,\n" +
                "PRIMARY_PHONE_ID VARCHAR(96) ,\n" +
                "REAL_NAME_FLAG VARCHAR(30) ,\n" +
                "REAL_NAME_TIME VARCHAR(600) ,\n" +
                "REAL_NAME_TYPE VARCHAR(60) ,\n" +
                "REGIST_CHANNEL VARCHAR(60) ,\n" +
                "POTENTIAL_FLAG VARCHAR(30) ,\n" +
                "AGENT_FLAG VARCHAR(30) ,\n" +
                "HISTORY_FLAG VARCHAR(30) ,\n" +
                "CUSTOMER_STATUS VARCHAR(30) ,\n" +
                "LAST_LOGIN_TIME TIMESTAMP(6) ,\n" +
                "CUSTOMER_DESC VARCHAR(1500) ,\n" +
                "COMMENTS VARCHAR(1500) ,\n" +
                "ENABLE CHAR(1) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(96) ,\n" +
                "UPDATE_USER VARCHAR(96) ,\n" +
                "FILTER1 VARCHAR(600) ,\n" +
                "FILTER2 VARCHAR(600) ,\n" +
                "FILTER3 VARCHAR(600) ,\n" +
                "FILTER4 VARCHAR(600) ,\n" +
                "FILTER5 VARCHAR(600) ,\n" +
                "FILTER6 VARCHAR(600) ,\n" +
                "FILTER7 VARCHAR(600) ,\n" +
                "FILTER8 VARCHAR(600) ,\n" +
                "FILTER9 VARCHAR(600) ,\n" +
                "REFRESH_FLAG CHAR(1) ,\n" +
                "REGIST_DATE TIMESTAMP(6)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.ZXYH_IP+":"+Constants.ZXYH_PORT+"/"+Constants.ZXYH_DB+"',\n" +
                "    'table-name' = '"+Constants.ZXYH_SCHEMA+".REFRESH_KFZX_CRM_CUSTOMER', \n" +
                "    'username' = '"+Constants.ZXYH_USER+"',\n" +
                "    'password' = '"+Constants.ZXYH_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_ZXYH_REFRESH_KFZX_CRM_CUSTOMER (\n" +
                "    ETL_DATE DATE,\n" +
                "ID VARCHAR(96) ,\n" +
                "CUSTOMER_ID VARCHAR(96) ,\n" +
                "CN_NAME VARCHAR(192) ,\n" +
                "FIRST_CN_NAME VARCHAR(192) ,\n" +
                "LAST_CN_NAME VARCHAR(192) ,\n" +
                "ENG_NAME VARCHAR(192) ,\n" +
                "FIRST_ENG_NAME VARCHAR(192) ,\n" +
                "LAST_ENG_NAME VARCHAR(192) ,\n" +
                "BIRTHDAY VARCHAR(6000) ,\n" +
                "SEX VARCHAR(30) ,\n" +
                "PROVINCE VARCHAR(300) ,\n" +
                "`NATIONAL` VARCHAR(300) ,\n" +
                "ETHNIC_GROUP VARCHAR(300) ,\n" +
                "PRIMARY_PHONE_ID VARCHAR(96) ,\n" +
                "REAL_NAME_FLAG VARCHAR(30) ,\n" +
                "REAL_NAME_TIME VARCHAR(600) ,\n" +
                "REAL_NAME_TYPE VARCHAR(60) ,\n" +
                "REGIST_CHANNEL VARCHAR(60) ,\n" +
                "POTENTIAL_FLAG VARCHAR(30) ,\n" +
                "AGENT_FLAG VARCHAR(30) ,\n" +
                "HISTORY_FLAG VARCHAR(30) ,\n" +
                "CUSTOMER_STATUS VARCHAR(30) ,\n" +
                "LAST_LOGIN_TIME TIMESTAMP(6) ,\n" +
                "CUSTOMER_DESC VARCHAR(1500) ,\n" +
                "COMMENTS VARCHAR(1500) ,\n" +
                "ENABLE CHAR(1) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(96) ,\n" +
                "UPDATE_USER VARCHAR(96) ,\n" +
                "FILTER1 VARCHAR(600) ,\n" +
                "FILTER2 VARCHAR(600) ,\n" +
                "FILTER3 VARCHAR(600) ,\n" +
                "FILTER4 VARCHAR(600) ,\n" +
                "FILTER5 VARCHAR(600) ,\n" +
                "FILTER6 VARCHAR(600) ,\n" +
                "FILTER7 VARCHAR(600) ,\n" +
                "FILTER8 VARCHAR(600) ,\n" +
                "FILTER9 VARCHAR(600) ,\n" +
                "REFRESH_FLAG CHAR(1) ,\n" +
                "REGIST_DATE TIMESTAMP(6)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZXYH_REFRESH_KFZX_CRM_CUSTOMER',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_ZXYH_REFRESH_KFZX_CRM_CUSTOMER(" +
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
                "REFRESH_FLAG,\n" +
                "REGIST_DATE)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
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
                "REFRESH_FLAG,\n" +
                "REGIST_DATE\n" +
                " FROM REFRESH_KFZX_CRM_CUSTOMER "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
