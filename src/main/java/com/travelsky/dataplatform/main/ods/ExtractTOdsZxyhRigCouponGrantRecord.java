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
public class ExtractTOdsZxyhRigCouponGrantRecord {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZxyhRigCouponGrantRecord");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE RIG_COUPON_GRANT_RECORD (\n" +
                "ID VARCHAR(96) ,\n" +
                "COUPON_ID VARCHAR(96) ,\n" +
                "GROUP_ID VARCHAR(96) ,\n" +
                "CUSTOMER_ID VARCHAR(600) ,\n" +
                "ENABLE CHAR(10) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_USER VARCHAR(384) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(384) ,\n" +
                "SEND_TIME TIMESTAMP(6) ,\n" +
                "BINDING_TIME TIMESTAMP(6) ,\n" +
                "UNLOCK_TIME TIMESTAMP(6) ,\n" +
                "CANCEL_TIME TIMESTAMP(6) ,\n" +
                "COUPON_STATUS VARCHAR(6) ,\n" +
                "TRP_COUPON_ID VARCHAR(96) ,\n" +
                "USE_CHANNEL VARCHAR(30) ,\n" +
                "EXP_DATE TIMESTAMP(6) ,\n" +
                "REAL_NAME_FLAG CHAR(1) ,\n" +
                "COUPON_BALANCE BIGINT ,\n" +
                "ORDER_NO VARCHAR(96) ,\n" +
                "MOBILE VARCHAR(600) ,\n" +
                "COMMENTS VARCHAR(1500) ,\n" +
                "COUPON_CODE VARCHAR(96) ,\n" +
                "COUPON_NAME VARCHAR(300) ,\n" +
                "CERT_NUMBER VARCHAR(600) ,\n" +
                "CUSTOMER_NAME VARCHAR(192) ,\n" +
                "BUSINESS_DATE TIMESTAMP(6) ,\n" +
                "INSTANCE_NO VARCHAR(96),\n" +
                "SEND_CHANNEL VARCHAR(30) ,\n" +
                "APPLY_LEG VARCHAR(100) ,\n" +
                "APPLY_CROWD VARCHAR(100) ,\n" +
                "USE_COUNT VARCHAR(100) ,\n" +
                "APPLY_TICKET_TYPE VARCHAR(100) ,\n" +
                "IS_ASSIGNEE VARCHAR(100) ,\n" +
                "AGE BIGINT ,\n" +
                "TIME_LIMIT VARCHAR(100) ,\n" +
                "FLIGHT_NO VARCHAR(100) ,\n" +
                "FLIGHT_DATE TIMESTAMP(6) ,\n" +
                "DEPARTURE_AIRPORT VARCHAR(100) ,\n" +
                "ARRIVE_AIRPORT VARCHAR(100) ,\n" +
                "CABIN VARCHAR(24) ,\n" +
                "REFUND_COUPON_CERT_NO VARCHAR(500) ,\n" +
                "REFUND_CAL_PRICE BIGINT ,\n" +
                "TICKET_NO VARCHAR(100) ,\n" +
                "OFFICE_CODE VARCHAR(100) ,\n" +
                "SUBMIT_REFUND_DATE TIMESTAMP(6) \n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.ZXYH_IP+":"+Constants.ZXYH_PORT+"/"+Constants.ZXYH_DB+"',\n" +
                "    'table-name' = '"+Constants.ZXYH_SCHEMA+".RIG_COUPON_GRANT_RECORD', \n" +
                "    'username' = '"+Constants.ZXYH_USER+"',\n" +
                "    'password' = '"+Constants.ZXYH_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_ZXYH_RIG_COUPON_GRANT_RECORD (\n" +
                "    ETL_DATE DATE,\n" +
                "ID VARCHAR(96) ,\n" +
                "COUPON_ID VARCHAR(96) ,\n" +
                "GROUP_ID VARCHAR(96) ,\n" +
                "CUSTOMER_ID VARCHAR(600) ,\n" +
                "ENABLE CHAR(10) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_USER VARCHAR(384) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(384) ,\n" +
                "SEND_TIME TIMESTAMP(6) ,\n" +
                "BINDING_TIME TIMESTAMP(6) ,\n" +
                "UNLOCK_TIME TIMESTAMP(6) ,\n" +
                "CANCEL_TIME TIMESTAMP(6) ,\n" +
                "COUPON_STATUS VARCHAR(6) ,\n" +
                "TRP_COUPON_ID VARCHAR(96) ,\n" +
                "USE_CHANNEL VARCHAR(30) ,\n" +
                "EXP_DATE TIMESTAMP(6) ,\n" +
                "REAL_NAME_FLAG CHAR(1) ,\n" +
                "COUPON_BALANCE BIGINT ,\n" +
                "ORDER_NO VARCHAR(96) ,\n" +
                "MOBILE VARCHAR(768) ,\n" +
                "COMMENTS VARCHAR(1500) ,\n" +
                "COUPON_CODE VARCHAR(96) ,\n" +
                "COUPON_NAME VARCHAR(300) ,\n" +
                "CERT_NUMBER VARCHAR(768) ,\n" +
                "CUSTOMER_NAME VARCHAR(192) ,\n" +
                "BUSINESS_DATE TIMESTAMP(6) ,\n" +
                "INSTANCE_NO VARCHAR(96),\n" +
                "SEND_CHANNEL VARCHAR(30) ,\n" +
                "APPLY_LEG VARCHAR(100) ,\n" +
                "APPLY_CROWD VARCHAR(100) ,\n" +
                "USE_COUNT VARCHAR(100) ,\n" +
                "APPLY_TICKET_TYPE VARCHAR(100) ,\n" +
                "IS_ASSIGNEE VARCHAR(100) ,\n" +
                "AGE BIGINT ,\n" +
                "TIME_LIMIT VARCHAR(100) ,\n" +
                "FLIGHT_NO VARCHAR(100) ,\n" +
                "FLIGHT_DATE TIMESTAMP(6) ,\n" +
                "DEPARTURE_AIRPORT VARCHAR(100) ,\n" +
                "ARRIVE_AIRPORT VARCHAR(100) ,\n" +
                "CABIN VARCHAR(24) ,\n" +
                "REFUND_COUPON_CERT_NO VARCHAR(500) ,\n" +
                "REFUND_CAL_PRICE BIGINT ,\n" +
                "TICKET_NO VARCHAR(100) ,\n" +
                "OFFICE_CODE VARCHAR(100) ,\n" +
                "SUBMIT_REFUND_DATE TIMESTAMP(6) \n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZXYH_RIG_COUPON_GRANT_RECORD',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_ZXYH_RIG_COUPON_GRANT_RECORD(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "COUPON_ID,\n" +
                "GROUP_ID,\n" +
                "CUSTOMER_ID,\n" +
                "ENABLE,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER,\n" +
                "SEND_TIME,\n" +
                "BINDING_TIME,\n" +
                "UNLOCK_TIME,\n" +
                "CANCEL_TIME,\n" +
                "COUPON_STATUS,\n" +
                "TRP_COUPON_ID,\n" +
                "USE_CHANNEL,\n" +
                "EXP_DATE,\n" +
                "REAL_NAME_FLAG,\n" +
                "COUPON_BALANCE,\n" +
                "ORDER_NO,\n" +
                "MOBILE,\n" +
                "COMMENTS,\n" +
                "COUPON_CODE,\n" +
                "COUPON_NAME,\n" +
                "CERT_NUMBER,\n" +
                "CUSTOMER_NAME,\n" +
                "BUSINESS_DATE,\n" +
                "INSTANCE_NO,\n" +
                "SEND_CHANNEL,\n" +
                "APPLY_LEG,\n" +
                "APPLY_CROWD,\n" +
                "USE_COUNT,\n" +
                "APPLY_TICKET_TYPE,\n" +
                "IS_ASSIGNEE,\n" +
                "AGE,\n" +
                "TIME_LIMIT,\n" +
                "FLIGHT_NO,\n" +
                "FLIGHT_DATE,\n" +
                "DEPARTURE_AIRPORT,\n" +
                "ARRIVE_AIRPORT,\n" +
                "CABIN,\n" +
                "REFUND_COUPON_CERT_NO,\n" +
                "REFUND_CAL_PRICE,\n" +
                "TICKET_NO,\n" +
                "OFFICE_CODE,\n" +
                "SUBMIT_REFUND_DATE)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "COUPON_ID,\n" +
                "GROUP_ID,\n" +
                "CUSTOMER_ID,\n" +
                "ENABLE,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER,\n" +
                "SEND_TIME,\n" +
                "BINDING_TIME,\n" +
                "UNLOCK_TIME,\n" +
                "CANCEL_TIME,\n" +
                "COUPON_STATUS,\n" +
                "TRP_COUPON_ID,\n" +
                "USE_CHANNEL,\n" +
                "EXP_DATE,\n" +
                "REAL_NAME_FLAG,\n" +
                "COUPON_BALANCE,\n" +
                "ORDER_NO,\n" +
                "  sm4_encrypt(aes_decrypt(MOBILE, '" + Constants.ZXYH_AES_KEY + "'), '" + sm4key + "') MOBILE,\n" +
                "COMMENTS,\n" +
                "COUPON_CODE,\n" +
                "COUPON_NAME,\n" +
                "  sm4_encrypt(aes_decrypt(CERT_NUMBER, '" + Constants.ZXYH_AES_KEY + "'), '" + sm4key + "') CERT_NUMBER,\n" +
                "CUSTOMER_NAME,\n" +
                "BUSINESS_DATE,\n" +
                "INSTANCE_NO,\n" +
                "SEND_CHANNEL,\n" +
                "APPLY_LEG,\n" +
                "APPLY_CROWD,\n" +
                "USE_COUNT,\n" +
                "APPLY_TICKET_TYPE,\n" +
                "IS_ASSIGNEE,\n" +
                "AGE,\n" +
                "TIME_LIMIT,\n" +
                "FLIGHT_NO,\n" +
                "FLIGHT_DATE,\n" +
                "DEPARTURE_AIRPORT,\n" +
                "ARRIVE_AIRPORT,\n" +
                "CABIN,\n" +
                "REFUND_COUPON_CERT_NO,\n" +
                "REFUND_CAL_PRICE,\n" +
                "TICKET_NO,\n" +
                "OFFICE_CODE,\n" +
                "SUBMIT_REFUND_DATE\n" +
                " FROM RIG_COUPON_GRANT_RECORD "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
