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
public class ExtractTOdsZxyhOrderTrpTicketDetail {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZxyhOrderTrpTicketDetail");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE ORDER_TRP_TICKET_DETAIL (\n" +
                "ID BIGINT ,\n" +
                "ORDER_NUM VARCHAR(600) ,\n" +
                "BANK_ORDER_NUM VARCHAR(60) ,\n" +
                "PNR VARCHAR(60) ,\n" +
                "TICKET_NUM VARCHAR(60) ,\n" +
                "FLIGHT_NUM VARCHAR(60) ,\n" +
                "SEGMENT_CODE VARCHAR(60) ,\n" +
                "SEGMENT VARCHAR(600) ,\n" +
                "CABIN VARCHAR(60) ,\n" +
                "DEPART_TIME VARCHAR(150) ,\n" +
                "PASSENGER_NAME VARCHAR(600) ,\n" +
                "ORIG_FARE DECIMAL(10,2) ,\n" +
                "DISC_FARE DECIMAL(10,2) ,\n" +
                "APT_CONSTR_FEE DECIMAL(10,2) ,\n" +
                "FUEL_SURCHARGE DECIMAL(10,2) ,\n" +
                "OTHER_TAXES DECIMAL(10,2) ,\n" +
                "INSURANCE_FEE DECIMAL(10,2) ,\n" +
                "AMT_PAYABLE DECIMAL(10,2) ,\n" +
                "PROD_NAME VARCHAR(60) ,\n" +
                "ORDER_DATE VARCHAR(150) ,\n" +
                "TKT_ISSUE_DATE VARCHAR(150) ,\n" +
                "ORDER_STATUS VARCHAR(60) ,\n" +
                "ORDER_SOURCE VARCHAR(60) ,\n" +
                "ORDER_CHANNEL VARCHAR(150) ,\n" +
                "ORDER_USERNAME VARCHAR(150) ,\n" +
                "REG_USER_NAME VARCHAR(150) ,\n" +
                "ORDER_USER_PHONE VARCHAR(60) ,\n" +
                "ORDER_USER_EMAIL VARCHAR(150) ,\n" +
                "BANK_NAME VARCHAR(150) ,\n" +
                "ACT_CARRIER VARCHAR(60) ,\n" +
                "VIP_NUM VARCHAR(60) ,\n" +
                "FF_NUM VARCHAR(60) ,\n" +
                "PAYMENT_TIME VARCHAR(150) ,\n" +
                "TKT_PURCHASE_PROC VARCHAR(150) ,\n" +
                "PASSENGER_TYPE VARCHAR(60) ,\n" +
                "ID_TYPE VARCHAR(150) ,\n" +
                "ID_NUMBER VARCHAR(60) ,\n" +
                "TRAVEL_TYPE VARCHAR(60) ,\n" +
                "TKT_TYPE VARCHAR(60) ,\n" +
                "IS_DIRECT_RED VARCHAR(60) ,\n" +
                "CONTACT_NAME VARCHAR(150) ,\n" +
                "CONTACT_PHONE VARCHAR(60) ,\n" +
                "SITE VARCHAR(60) ,\n" +
                "`LANGUAGE` VARCHAR(60) ,\n" +
                "CURRENCY VARCHAR(60) ,\n" +
                "COUPON_NAME VARCHAR(150) ,\n" +
                "COUPON_AMT DECIMAL(10,2) ,\n" +
                "COUPON_CODE VARCHAR(60) ,\n" +
                "COUPON_CODE_NUM VARCHAR(60) ,\n" +
                "DIRECT_RED_AMT DECIMAL(10,2) ,\n" +
                "STATIC_PROMO_RED_AMT DECIMAL(10,2) ,\n" +
                "IS_STATIC_PROMO_RED VARCHAR(60) ,\n" +
                "FARE_BASIS VARCHAR(60) ,\n" +
                "IS_INSUR_GIFT VARCHAR(60) ,\n" +
                "INSUR_GIFT_NAME VARCHAR(60) ,\n" +
                "IS_COUPON_GIFT VARCHAR(60) ,\n" +
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
                "    'table-name' = '"+Constants.ZXYH_SCHEMA+".ORDER_TRP_TICKET_DETAIL', \n" +
                "    'username' = '"+Constants.ZXYH_USER+"',\n" +
                "    'password' = '"+Constants.ZXYH_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_ZXYH_ORDER_TRP_TICKET_DETAIL (\n" +
                "    ETL_DATE DATE,\n" +
                "ID BIGINT ,\n" +
                "ORDER_NUM VARCHAR(600) ,\n" +
                "BANK_ORDER_NUM VARCHAR(60) ,\n" +
                "PNR VARCHAR(60) ,\n" +
                "TICKET_NUM VARCHAR(60) ,\n" +
                "FLIGHT_NUM VARCHAR(60) ,\n" +
                "SEGMENT_CODE VARCHAR(60) ,\n" +
                "SEGMENT VARCHAR(600) ,\n" +
                "CABIN VARCHAR(60) ,\n" +
                "DEPART_TIME VARCHAR(150) ,\n" +
                "PASSENGER_NAME VARCHAR(600) ,\n" +
                "ORIG_FARE DECIMAL(10,2) ,\n" +
                "DISC_FARE DECIMAL(10,2) ,\n" +
                "APT_CONSTR_FEE DECIMAL(10,2) ,\n" +
                "FUEL_SURCHARGE DECIMAL(10,2) ,\n" +
                "OTHER_TAXES DECIMAL(10,2) ,\n" +
                "INSURANCE_FEE DECIMAL(10,2) ,\n" +
                "AMT_PAYABLE DECIMAL(10,2) ,\n" +
                "PROD_NAME VARCHAR(60) ,\n" +
                "ORDER_DATE VARCHAR(150) ,\n" +
                "TKT_ISSUE_DATE VARCHAR(150) ,\n" +
                "ORDER_STATUS VARCHAR(60) ,\n" +
                "ORDER_SOURCE VARCHAR(60) ,\n" +
                "ORDER_CHANNEL VARCHAR(150) ,\n" +
                "ORDER_USERNAME VARCHAR(150) ,\n" +
                "REG_USER_NAME VARCHAR(150) ,\n" +
                "ORDER_USER_PHONE VARCHAR(60) ,\n" +
                "ORDER_USER_EMAIL VARCHAR(150) ,\n" +
                "BANK_NAME VARCHAR(150) ,\n" +
                "ACT_CARRIER VARCHAR(60) ,\n" +
                "VIP_NUM VARCHAR(60) ,\n" +
                "FF_NUM VARCHAR(768) ,\n" +
                "PAYMENT_TIME VARCHAR(150) ,\n" +
                "TKT_PURCHASE_PROC VARCHAR(150) ,\n" +
                "PASSENGER_TYPE VARCHAR(60) ,\n" +
                "ID_TYPE VARCHAR(150) ,\n" +
                "ID_NUMBER VARCHAR(60) ,\n" +
                "TRAVEL_TYPE VARCHAR(60) ,\n" +
                "TKT_TYPE VARCHAR(60) ,\n" +
                "IS_DIRECT_RED VARCHAR(60) ,\n" +
                "CONTACT_NAME VARCHAR(150) ,\n" +
                "CONTACT_PHONE VARCHAR(768) ,\n" +
                "SITE VARCHAR(60) ,\n" +
                "`LANGUAGE` VARCHAR(60) ,\n" +
                "CURRENCY VARCHAR(60) ,\n" +
                "COUPON_NAME VARCHAR(150) ,\n" +
                "COUPON_AMT DECIMAL(10,2) ,\n" +
                "COUPON_CODE VARCHAR(60) ,\n" +
                "COUPON_CODE_NUM VARCHAR(60) ,\n" +
                "DIRECT_RED_AMT DECIMAL(10,2) ,\n" +
                "STATIC_PROMO_RED_AMT DECIMAL(10,2) ,\n" +
                "IS_STATIC_PROMO_RED VARCHAR(60) ,\n" +
                "FARE_BASIS VARCHAR(60) ,\n" +
                "IS_INSUR_GIFT VARCHAR(60) ,\n" +
                "INSUR_GIFT_NAME VARCHAR(60) ,\n" +
                "IS_COUPON_GIFT VARCHAR(60) ,\n" +
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
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZXYH_ORDER_TRP_TICKET_DETAIL',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_ZXYH_ORDER_TRP_TICKET_DETAIL(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "ORDER_NUM,\n" +
                "BANK_ORDER_NUM,\n" +
                "PNR,\n" +
                "TICKET_NUM,\n" +
                "FLIGHT_NUM,\n" +
                "SEGMENT_CODE,\n" +
                "SEGMENT,\n" +
                "CABIN,\n" +
                "DEPART_TIME,\n" +
                "PASSENGER_NAME,\n" +
                "ORIG_FARE,\n" +
                "DISC_FARE,\n" +
                "APT_CONSTR_FEE,\n" +
                "FUEL_SURCHARGE,\n" +
                "OTHER_TAXES,\n" +
                "INSURANCE_FEE,\n" +
                "AMT_PAYABLE,\n" +
                "PROD_NAME,\n" +
                "ORDER_DATE,\n" +
                "TKT_ISSUE_DATE,\n" +
                "ORDER_STATUS,\n" +
                "ORDER_SOURCE,\n" +
                "ORDER_CHANNEL,\n" +
                "ORDER_USERNAME,\n" +
                "REG_USER_NAME,\n" +
                "ORDER_USER_PHONE,\n" +
                "ORDER_USER_EMAIL,\n" +
                "BANK_NAME,\n" +
                "ACT_CARRIER,\n" +
                "VIP_NUM,\n" +
                "FF_NUM,\n" +
                "PAYMENT_TIME,\n" +
                "TKT_PURCHASE_PROC,\n" +
                "PASSENGER_TYPE,\n" +
                "ID_TYPE,\n" +
                "ID_NUMBER,\n" +
                "TRAVEL_TYPE,\n" +
                "TKT_TYPE,\n" +
                "IS_DIRECT_RED,\n" +
                "CONTACT_NAME,\n" +
                "CONTACT_PHONE,\n" +
                "SITE,\n" +
                "`LANGUAGE`,\n" +
                "CURRENCY,\n" +
                "COUPON_NAME,\n" +
                "COUPON_AMT,\n" +
                "COUPON_CODE,\n" +
                "COUPON_CODE_NUM,\n" +
                "DIRECT_RED_AMT,\n" +
                "STATIC_PROMO_RED_AMT,\n" +
                "IS_STATIC_PROMO_RED,\n" +
                "FARE_BASIS,\n" +
                "IS_INSUR_GIFT,\n" +
                "INSUR_GIFT_NAME,\n" +
                "IS_COUPON_GIFT,\n" +
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
                "ORDER_NUM,\n" +
                "BANK_ORDER_NUM,\n" +
                "PNR,\n" +
                "TICKET_NUM,\n" +
                "FLIGHT_NUM,\n" +
                "SEGMENT_CODE,\n" +
                "SEGMENT,\n" +
                "CABIN,\n" +
                "DEPART_TIME,\n" +
                "PASSENGER_NAME,\n" +
                "ORIG_FARE,\n" +
                "DISC_FARE,\n" +
                "APT_CONSTR_FEE,\n" +
                "FUEL_SURCHARGE,\n" +
                "OTHER_TAXES,\n" +
                "INSURANCE_FEE,\n" +
                "AMT_PAYABLE,\n" +
                "PROD_NAME,\n" +
                "ORDER_DATE,\n" +
                "TKT_ISSUE_DATE,\n" +
                "ORDER_STATUS,\n" +
                "ORDER_SOURCE,\n" +
                "ORDER_CHANNEL,\n" +
                "ORDER_USERNAME,\n" +
                "REG_USER_NAME,\n" +
                "ORDER_USER_PHONE,\n" +
                "ORDER_USER_EMAIL,\n" +
                "BANK_NAME,\n" +
                "ACT_CARRIER,\n" +
                "VIP_NUM,\n" +
                "  sm4_encrypt(aes_decrypt(FF_NUM, '" + Constants.ZXYH_AES_KEY + "'), '" + sm4key + "') FF_NUM,\n" +
                "PAYMENT_TIME,\n" +
                "TKT_PURCHASE_PROC,\n" +
                "PASSENGER_TYPE,\n" +
                "ID_TYPE,\n" +
                "ID_NUMBER,\n" +
                "TRAVEL_TYPE,\n" +
                "TKT_TYPE,\n" +
                "IS_DIRECT_RED,\n" +
                "CONTACT_NAME,\n" +
                "  sm4_encrypt(aes_decrypt(CONTACT_PHONE, '" + Constants.ZXYH_AES_KEY + "'), '" + sm4key + "') CONTACT_PHONE,\n" +
                "SITE,\n" +
                "`LANGUAGE`,\n" +
                "CURRENCY,\n" +
                "COUPON_NAME,\n" +
                "COUPON_AMT,\n" +
                "COUPON_CODE,\n" +
                "COUPON_CODE_NUM,\n" +
                "DIRECT_RED_AMT,\n" +
                "STATIC_PROMO_RED_AMT,\n" +
                "IS_STATIC_PROMO_RED,\n" +
                "FARE_BASIS,\n" +
                "IS_INSUR_GIFT,\n" +
                "INSUR_GIFT_NAME,\n" +
                "IS_COUPON_GIFT,\n" +
                "ETL_INSERT_TIME,\n" +
                "ETL_UPDATE_TIME,\n" +
                "ENABLE,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_USER,\n" +
                "ENCRYPT_FLAG\n" +
                " FROM ORDER_TRP_TICKET_DETAIL "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
