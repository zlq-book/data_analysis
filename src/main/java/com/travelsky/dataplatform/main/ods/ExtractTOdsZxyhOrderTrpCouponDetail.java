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
public class ExtractTOdsZxyhOrderTrpCouponDetail {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZxyhOrderTrpCouponDetail");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE ORDER_TRP_COUPON_DETAIL (\n" +
                "ID BIGINT ,\n" +
                "ORDER_NUMBER VARCHAR(150) ,\n" +
                "BANK_ORDER_NUMBER VARCHAR(150) ,\n" +
                "BANK_NAME VARCHAR(150) ,\n" +
                "CHANNEL VARCHAR(150) ,\n" +
                "CONTACT_NAME VARCHAR(150) ,\n" +
                "CONTACT_PHONE VARCHAR(150) ,\n" +
                "ORDER_DATE VARCHAR(150) ,\n" +
                "ORDER_STATUS VARCHAR(150) ,\n" +
                "COUPON_CODE VARCHAR(150) ,\n" +
                "COUPON_NAME VARCHAR(150) ,\n" +
                "COUPON_AMOUNT DECIMAL(10,2) ,\n" +
                "PAYMENT_AMOUNT DECIMAL(10,2) ,\n" +
                "PAYMENT_DATE VARCHAR(150) ,\n" +
                "PAYMENT_STATUS VARCHAR(150) ,\n" +
                "USERNAME VARCHAR(150) ,\n" +
                "ETL_INSERT_TIME TIMESTAMP(6) ,\n" +
                "ETL_UPDATE_TIME TIMESTAMP(6) ,\n" +
                "ENABLE CHAR(1) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(96) ,\n" +
                "UPDATE_USER VARCHAR(96),\n" +
                "ENCRYPT_FLAG VARCHAR(3)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.ZXYH_IP+":"+Constants.ZXYH_PORT+"/"+Constants.ZXYH_DB+"',\n" +
                "    'table-name' = '"+Constants.ZXYH_SCHEMA+".ORDER_TRP_COUPON_DETAIL', \n" +
                "    'username' = '"+Constants.ZXYH_USER+"',\n" +
                "    'password' = '"+Constants.ZXYH_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_ZXYH_ORDER_TRP_COUPON_DETAIL (\n" +
                "    ETL_DATE DATE,\n" +
                "ID BIGINT ,\n" +
                "ORDER_NUMBER VARCHAR(150) ,\n" +
                "BANK_ORDER_NUMBER VARCHAR(150) ,\n" +
                "BANK_NAME VARCHAR(150) ,\n" +
                "CHANNEL VARCHAR(150) ,\n" +
                "CONTACT_NAME VARCHAR(150) ,\n" +
                "CONTACT_PHONE VARCHAR(768) ,\n" +
                "ORDER_DATE VARCHAR(150) ,\n" +
                "ORDER_STATUS VARCHAR(150) ,\n" +
                "COUPON_CODE VARCHAR(150) ,\n" +
                "COUPON_NAME VARCHAR(150) ,\n" +
                "COUPON_AMOUNT DECIMAL(10,2) ,\n" +
                "PAYMENT_AMOUNT DECIMAL(10,2) ,\n" +
                "PAYMENT_DATE VARCHAR(150) ,\n" +
                "PAYMENT_STATUS VARCHAR(150) ,\n" +
                "USERNAME VARCHAR(150) ,\n" +
                "ETL_INSERT_TIME TIMESTAMP(6) ,\n" +
                "ETL_UPDATE_TIME TIMESTAMP(6) ,\n" +
                "ENABLE CHAR(1) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(96) ,\n" +
                "UPDATE_USER VARCHAR(96),\n" +
                "ENCRYPT_FLAG VARCHAR(3)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZXYH_ORDER_TRP_COUPON_DETAIL',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_ZXYH_ORDER_TRP_COUPON_DETAIL(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "ORDER_NUMBER,\n" +
                "BANK_ORDER_NUMBER,\n" +
                "BANK_NAME,\n" +
                "CHANNEL,\n" +
                "CONTACT_NAME,\n" +
                "CONTACT_PHONE,\n" +
                "ORDER_DATE,\n" +
                "ORDER_STATUS,\n" +
                "COUPON_CODE,\n" +
                "COUPON_NAME,\n" +
                "COUPON_AMOUNT,\n" +
                "PAYMENT_AMOUNT,\n" +
                "PAYMENT_DATE,\n" +
                "PAYMENT_STATUS,\n" +
                "USERNAME,\n" +
                "ETL_INSERT_TIME,\n" +
                "ETL_UPDATE_TIME,\n" +
                "ENABLE,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_USER,\n" +
                "ENCRYPT_FLAG)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "CAST(ID AS BIGINT) ID,\n" +
                "ORDER_NUMBER,\n" +
                "BANK_ORDER_NUMBER,\n" +
                "BANK_NAME,\n" +
                "CHANNEL,\n" +
                "CONTACT_NAME,\n" +
                "  sm4_encrypt(aes_decrypt(CONTACT_PHONE, '" + Constants.ZXYH_AES_KEY + "'), '" + sm4key + "') CONTACT_PHONE,\n" +
                "ORDER_DATE,\n" +
                "ORDER_STATUS,\n" +
                "COUPON_CODE,\n" +
                "COUPON_NAME,\n" +
                "COUPON_AMOUNT,\n" +
                "PAYMENT_AMOUNT,\n" +
                "PAYMENT_DATE,\n" +
                "PAYMENT_STATUS,\n" +
                "USERNAME,\n" +
                "ETL_INSERT_TIME,\n" +
                "ETL_UPDATE_TIME,\n" +
                "ENABLE,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_USER,\n" +
                "ENCRYPT_FLAG\n" +
                " FROM ORDER_TRP_COUPON_DETAIL "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
