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
public class ExtractTOdsZxyhRefreshKfzxCrmCAddress {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZxyhRefreshKfzxCrmCAddress");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE REFRESH_KFZX_CRM_C_ADDRESS (\n" +
                "ID VARCHAR(96) ,\n" +
                "CUSTOMER_ID VARCHAR(96) ,\n" +
                "ZIP_CODE VARCHAR(60) ,\n" +
                "COUNTRY_CODE VARCHAR(150) ,\n" +
                "COUNTRY_NAME VARCHAR(1500) ,\n" +
                "PROVINCE_CODE VARCHAR(150) ,\n" +
                "PROVINCE_NAME VARCHAR(1500) ,\n" +
                "CITY_CODE VARCHAR(150) ,\n" +
                "CITY_NAME VARCHAR(1500) ,\n" +
                "DETAIL_ADDRESS VARCHAR(1500) ,\n" +
                "FULL_ADDRESS VARCHAR(1500) ,\n" +
                "COMMENTS VARCHAR(1500) ,\n" +
                "ENABLE CHAR(1) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(96) ,\n" +
                "UPDATE_USER VARCHAR(96) ,\n" +
                "REFRESH_FLAG CHAR(1)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.ZXYH_IP+":"+Constants.ZXYH_PORT+"/"+Constants.ZXYH_DB+"',\n" +
                "    'table-name' = '"+Constants.ZXYH_SCHEMA+".REFRESH_KFZX_CRM_C_ADDRESS', \n" +
                "    'username' = '"+Constants.ZXYH_USER+"',\n" +
                "    'password' = '"+Constants.ZXYH_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_ZXYH_REFRESH_KFZX_CRM_C_ADDRESS (\n" +
                "    ETL_DATE DATE,\n" +
                "ID VARCHAR(96) ,\n" +
                "CUSTOMER_ID VARCHAR(96) ,\n" +
                "ZIP_CODE VARCHAR(60) ,\n" +
                "COUNTRY_CODE VARCHAR(150) ,\n" +
                "COUNTRY_NAME VARCHAR(1500) ,\n" +
                "PROVINCE_CODE VARCHAR(150) ,\n" +
                "PROVINCE_NAME VARCHAR(1500) ,\n" +
                "CITY_CODE VARCHAR(150) ,\n" +
                "CITY_NAME VARCHAR(1500) ,\n" +
                "DETAIL_ADDRESS VARCHAR(1500) ,\n" +
                "FULL_ADDRESS VARCHAR(1500) ,\n" +
                "COMMENTS VARCHAR(1500) ,\n" +
                "ENABLE CHAR(1) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(96) ,\n" +
                "UPDATE_USER VARCHAR(96) ,\n" +
                "REFRESH_FLAG CHAR(1)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZXYH_REFRESH_KFZX_CRM_C_ADDRESS',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_ZXYH_REFRESH_KFZX_CRM_C_ADDRESS(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "CUSTOMER_ID,\n" +
                "ZIP_CODE,\n" +
                "COUNTRY_CODE,\n" +
                "COUNTRY_NAME,\n" +
                "PROVINCE_CODE,\n" +
                "PROVINCE_NAME,\n" +
                "CITY_CODE,\n" +
                "CITY_NAME,\n" +
                "DETAIL_ADDRESS,\n" +
                "FULL_ADDRESS,\n" +
                "COMMENTS,\n" +
                "ENABLE,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_USER,\n" +
                "REFRESH_FLAG)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "CUSTOMER_ID,\n" +
                "ZIP_CODE,\n" +
                "COUNTRY_CODE,\n" +
                "COUNTRY_NAME,\n" +
                "PROVINCE_CODE,\n" +
                "PROVINCE_NAME,\n" +
                "CITY_CODE,\n" +
                "CITY_NAME,\n" +
                "DETAIL_ADDRESS,\n" +
                "FULL_ADDRESS,\n" +
                "COMMENTS,\n" +
                "ENABLE,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_USER,\n" +
                "REFRESH_FLAG\n" +
                " FROM REFRESH_KFZX_CRM_C_ADDRESS "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
