package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.OracleAESDecryptor;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.UUID;

/**
 * @author
 * @date 2025/7/2 9:31
 */
public class ExtractTOdsZxyhRigGoldAccountHistory {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZxyhRigGoldAccountHistory");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE RIG_GOLD_ACCOUNT_HISTORY (\n" +
                "ID VARCHAR(96) ,\n" +
                "CUSTOMER_ID VARCHAR(600) ,\n" +
                "ACCOUNT_ID VARCHAR(600) ,\n" +
                "BUSINESS_TYPE VARCHAR(60) ,\n" +
                "BALANCE BIGINT ,\n" +
                "GOLD_ROAD VARCHAR(60) ,\n" +
                "RULE_ID VARCHAR(60) ,\n" +
                "RULE_NAME VARCHAR(60) ,\n" +
                "LAST_BALANCE BIGINT ,\n" +
                "ACCOUNT_BALANCE BIGINT ,\n" +
                "BUSINESS_DATE TIMESTAMP(6) ,\n" +
                "ENABLE CHAR(10) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_USER VARCHAR(384) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(384) ,\n" +
                "CERT_NO VARCHAR(192) ,\n" +
                "MOBILE VARCHAR(192) ,\n" +
                "ORDER_NO VARCHAR(300) ,\n" +
                "ROUTE VARCHAR(300) ,\n" +
                "FARE_BASIS_CODE VARCHAR(96) ,\n" +
                "FARE_BASIS_PRICE DECIMAL(10,2) ,\n" +
                "GOLD_RULE BIGINT,\n" +
                "CON_SIGN_DAYS VARCHAR(15) ,\n" +
                "NODE_DAY VARCHAR(30)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.ZXYH_IP+":"+Constants.ZXYH_PORT+"/"+Constants.ZXYH_DB+"',\n" +
                "    'table-name' = '"+Constants.ZXYH_SCHEMA+".RIG_GOLD_ACCOUNT_HISTORY', \n" +
                "    'username' = '"+Constants.ZXYH_USER+"',\n" +
                "    'password' = '"+Constants.ZXYH_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_ZXYH_RIG_GOLD_ACCOUNT_HISTORY (\n" +
                "    ETL_DATE DATE,\n" +
                "ID VARCHAR(96) ,\n" +
                "CUSTOMER_ID VARCHAR(600) ,\n" +
                "ACCOUNT_ID VARCHAR(600) ,\n" +
                "BUSINESS_TYPE VARCHAR(60) ,\n" +
                "BALANCE BIGINT ,\n" +
                "GOLD_ROAD VARCHAR(60) ,\n" +
                "RULE_ID VARCHAR(60) ,\n" +
                "RULE_NAME VARCHAR(60) ,\n" +
                "LAST_BALANCE BIGINT ,\n" +
                "ACCOUNT_BALANCE BIGINT ,\n" +
                "BUSINESS_DATE TIMESTAMP(6) ,\n" +
                "ENABLE CHAR(10) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_USER VARCHAR(384) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(384) ,\n" +
                "CERT_NO VARCHAR(768) ,\n" +
                "MOBILE VARCHAR(768) ,\n" +
                "ORDER_NO VARCHAR(300) ,\n" +
                "ROUTE VARCHAR(300) ,\n" +
                "FARE_BASIS_CODE VARCHAR(96) ,\n" +
                "FARE_BASIS_PRICE DECIMAL(10,2) ,\n" +
                "GOLD_RULE BIGINT,\n" +
                "CON_SIGN_DAYS VARCHAR(15) ,\n" +
                "NODE_DAY VARCHAR(30)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZXYH_RIG_GOLD_ACCOUNT_HISTORY',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_ZXYH_RIG_GOLD_ACCOUNT_HISTORY(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "CUSTOMER_ID,\n" +
                "ACCOUNT_ID,\n" +
                "BUSINESS_TYPE,\n" +
                "BALANCE,\n" +
                "GOLD_ROAD,\n" +
                "RULE_ID,\n" +
                "RULE_NAME,\n" +
                "LAST_BALANCE,\n" +
                "ACCOUNT_BALANCE,\n" +
                "BUSINESS_DATE,\n" +
                "ENABLE,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER,\n" +
                "CERT_NO,\n" +
                "MOBILE,\n" +
                "ORDER_NO,\n" +
                "ROUTE,\n" +
                "FARE_BASIS_CODE,\n" +
                "FARE_BASIS_PRICE,\n" +
                "GOLD_RULE,\n" +
                "CON_SIGN_DAYS,\n" +
                "NODE_DAY)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "CUSTOMER_ID,\n" +
                "ACCOUNT_ID,\n" +
                "BUSINESS_TYPE,\n" +
                "BALANCE,\n" +
                "GOLD_ROAD,\n" +
                "RULE_ID,\n" +
                "RULE_NAME,\n" +
                "LAST_BALANCE,\n" +
                "ACCOUNT_BALANCE,\n" +
                "BUSINESS_DATE,\n" +
                "ENABLE,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER,\n" +
                "  sm4_encrypt(aes_decrypt(CERT_NO, '" + Constants.ZXYH_AES_KEY + "'), '" + sm4key + "') CERT_NO,\n" +
                "  sm4_encrypt(aes_decrypt(MOBILE, '" + Constants.ZXYH_AES_KEY + "'), '" + sm4key + "') MOBILE,\n" +
                "ORDER_NO,\n" +
                "ROUTE,\n" +
                "FARE_BASIS_CODE,\n" +
                "FARE_BASIS_PRICE,\n" +
                "GOLD_RULE,\n" +
                "CON_SIGN_DAYS,\n" +
                "NODE_DAY\n" +
                " FROM RIG_GOLD_ACCOUNT_HISTORY "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
