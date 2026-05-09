package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.OracleAESDecryptor;
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
public class ExtractTOdsZxyhCrmCustomerPassengerInfo {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZxyhCrmCustomerPassengerInfo");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE CRM_CUSTOMER_PASSENGER_INFO (\n" +
                "ID VARCHAR(96) ,\n" +
                "PASSENGER_ID VARCHAR(96) ,\n" +
                "CUSTOMER_ID VARCHAR(600) ,\n" +
                "PASSENGER_NAME VARCHAR(300) ,\n" +
                "FIRST_CN_NAME VARCHAR(192) ,\n" +
                "LAST_CN_NAME VARCHAR(192) ,\n" +
                "ENG_NAME VARCHAR(192) ,\n" +
                "FIRST_ENG_NAME VARCHAR(192) ,\n" +
                "LAST_ENG_NAME VARCHAR(192) ,\n" +
                "PASSENGER_TYPE VARCHAR(30) ,\n" +
                "PHONE VARCHAR(300) ,\n" +
                "BIRTHDAY VARCHAR(300) ,\n" +
                "SEX VARCHAR(3) ,\n" +
                "COUNTRY VARCHAR(96) ,\n" +
                "GLOBAL_FLAG VARCHAR(3) ,\n" +
                "CHANNEL_TYPE VARCHAR(3) ,\n" +
                "COMMENTS VARCHAR(1500) ,\n" +
                "ENABLE CHAR(1) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(96) ,\n" +
                "UPDATE_USER VARCHAR(96) ,\n" +
                "MYSELF_FLAG VARCHAR(30)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.ZXYH_IP+":"+Constants.ZXYH_PORT+"/"+Constants.ZXYH_DB+"',\n" +
                "    'table-name' = '"+Constants.ZXYH_SCHEMA+".CRM_CUSTOMER_PASSENGER_INFO', \n" +
                "    'username' = '"+Constants.ZXYH_USER+"',\n" +
                "    'password' = '"+Constants.ZXYH_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_ZXYH_CRM_CUSTOMER_PASSENGER_INFO (\n" +
                "    ETL_DATE DATE,\n" +
                "ID VARCHAR(96) ,\n" +
                "PASSENGER_ID VARCHAR(96) ,\n" +
                "CUSTOMER_ID VARCHAR(600) ,\n" +
                "PASSENGER_NAME VARCHAR(300) ,\n" +
                "FIRST_CN_NAME VARCHAR(192) ,\n" +
                "LAST_CN_NAME VARCHAR(192) ,\n" +
                "ENG_NAME VARCHAR(192) ,\n" +
                "FIRST_ENG_NAME VARCHAR(192) ,\n" +
                "LAST_ENG_NAME VARCHAR(192) ,\n" +
                "PASSENGER_TYPE VARCHAR(30) ,\n" +
                "PHONE VARCHAR(768) ,\n" +
                "BIRTHDAY VARCHAR(300) ,\n" +
                "SEX VARCHAR(3) ,\n" +
                "COUNTRY VARCHAR(96) ,\n" +
                "GLOBAL_FLAG VARCHAR(3) ,\n" +
                "CHANNEL_TYPE VARCHAR(3) ,\n" +
                "COMMENTS VARCHAR(1500) ,\n" +
                "ENABLE CHAR(1) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(96) ,\n" +
                "UPDATE_USER VARCHAR(96) ,\n" +
                "MYSELF_FLAG VARCHAR(30)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZXYH_CRM_CUSTOMER_PASSENGER_INFO',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_ZXYH_CRM_CUSTOMER_PASSENGER_INFO(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "PASSENGER_ID,\n" +
                "CUSTOMER_ID,\n" +
                "PASSENGER_NAME,\n" +
                "FIRST_CN_NAME,\n" +
                "LAST_CN_NAME,\n" +
                "ENG_NAME,\n" +
                "FIRST_ENG_NAME,\n" +
                "LAST_ENG_NAME,\n" +
                "PASSENGER_TYPE,\n" +
                "PHONE,\n" +
                "BIRTHDAY,\n" +
                "SEX,\n" +
                "COUNTRY,\n" +
                "GLOBAL_FLAG,\n" +
                "CHANNEL_TYPE,\n" +
                "COMMENTS,\n" +
                "ENABLE,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_USER,\n" +
                "MYSELF_FLAG)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "PASSENGER_ID,\n" +
                "CUSTOMER_ID,\n" +
                "PASSENGER_NAME,\n" +
                "FIRST_CN_NAME,\n" +
                "LAST_CN_NAME,\n" +
                "ENG_NAME,\n" +
                "FIRST_ENG_NAME,\n" +
                "LAST_ENG_NAME,\n" +
                "PASSENGER_TYPE,\n" +
                "  sm4_encrypt(aes_decrypt(PHONE, '" + Constants.ZXYH_AES_KEY + "'), '" + sm4key + "') PHONE,\n" +
                "BIRTHDAY,\n" +
                "SEX,\n" +
                "COUNTRY,\n" +
                "GLOBAL_FLAG,\n" +
                "CHANNEL_TYPE,\n" +
                "COMMENTS,\n" +
                "ENABLE,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_USER,\n" +
                "MYSELF_FLAG\n" +
                " FROM CRM_CUSTOMER_PASSENGER_INFO "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
