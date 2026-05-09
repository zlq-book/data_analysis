package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.OracleAESDecryptor;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.UUID;

/**
 * @author kuangaihua
 * @date 2025/6/26 16:29
 */
public class ExtractTOdsWxapCheckinTrpOrder {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsWxapCheckinTrpOrder");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);

        // ====================== 1. 构建上游JDBC数据源表SQL（StringBuffer拼接）======================
        StringBuffer createSourceTableSql = new StringBuffer();
        createSourceTableSql.append("CREATE TABLE CHECKIN_TRP_ORDER (")
                .append("    ID BIGINT,")
                .append("    ORDER_NO VARCHAR(96),")
                .append("    ORDER_AMOUNT BIGINT,")
                .append("    SEAT_TYPE TINYINT,")
                .append("    ORDER_STATUS TINYINT,")
                .append("    ORDER_TIME TIMESTAMP(6),")
                .append("    CONTACT_PERSON VARCHAR(96),")
                .append("    CONTACT_PHONE VARCHAR(192),")
                .append("    CREATE_TIME TIMESTAMP(6),")
                .append("    UPDATE_TIME TIMESTAMP(6),")
                .append("    PAY_TYPE TINYINT,")
                .append("    CUSTOMER_ID VARCHAR(96),")
                .append("    OPEN_ID VARCHAR(96),")
                .append("    CHANNEL TINYINT,")
                .append("    PAY_NO VARCHAR(96),")
                .append("    THIRD_PAY_NO VARCHAR(96),")
                .append("    TRP_PAY_NO VARCHAR(96),")
                .append("    PAY_STATUS TINYINT,")
                .append("    TRP_PAY_STATUS TINYINT")
                .append(") WITH (")
                .append("    'connector' = 'jdbc',")
                .append("    'url' = 'jdbc:oracle:thin:@//").append(Constants.WXAP_IP).append(":").append(Constants.WXAP_PORT).append("/").append(Constants.WXAP_DB).append("',")
                .append("    'table-name' = '").append(Constants.WXAP_SCHEMA).append(".CHECKIN_TRP_ORDER', ")
                .append("    'username' = '").append(Constants.WXAP_USER).append("',")
                .append("    'password' = '").append(Constants.WXAP_PWD).append("',")
                .append("    'driver' = '").append(Constants.ORACLE_DRIVER).append("'")
                .append(")");
        tEnv.executeSql(createSourceTableSql.toString());

        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();

        // ====================== 2. 构建Doris目标表SQL（StringBuffer拼接）======================
        StringBuffer createSinkTableSql = new StringBuffer();
        createSinkTableSql.append("CREATE TABLE T_ODS_WXAP_CHECKIN_TRP_ORDER (")
                .append("    ETL_DATE DATE,")
                .append("    ID BIGINT,")
                .append("    ORDER_NO VARCHAR(96),")
                .append("    ORDER_AMOUNT BIGINT,")
                .append("    SEAT_TYPE TINYINT,")
                .append("    ORDER_STATUS TINYINT,")
                .append("    ORDER_TIME TIMESTAMP(6),")
                .append("    CONTACT_PERSON VARCHAR(96),")
                .append("    CONTACT_PHONE VARCHAR(192),")
                .append("    CREATE_TIME TIMESTAMP(6),")
                .append("    UPDATE_TIME TIMESTAMP(6),")
                .append("    PAY_TYPE TINYINT,")
                .append("    CUSTOMER_ID VARCHAR(96),")
                .append("    OPEN_ID VARCHAR(96),")
                .append("    CHANNEL TINYINT,")
                .append("    PAY_NO VARCHAR(96),")
                .append("    THIRD_PAY_NO VARCHAR(96),")
                .append("    TRP_PAY_NO VARCHAR(96),")
                .append("    PAY_STATUS TINYINT,")
                .append("    TRP_PAY_STATUS TINYINT")
                .append(") WITH (")
                .append(" 'connector' = 'doris',")
                .append("'fenodes' = '").append(Constants.DORIS_FE_IP).append(":").append(Constants.DORIS_FE_PORT).append("',") // 替换为 Doris FE 的地址和端口
                .append("'benodes' = '").append(Constants.DORIS_BE_IP).append(":").append(Constants.DORIS_BE_PORT).append("',") // 替换为 Doris BE 的地址和端口
                .append(" 'table.identifier' = '").append(Constants.ODS_DB).append(".T_ODS_WXAP_CHECKIN_TRP_ORDER',")
                .append("'sink.label-prefix' = '").append(timestamp).append(uuid).append("',")
                .append(" 'sink.properties.read_json_by_line' = 'true',")
                .append(" 'sink.properties.format' = 'json',")
                .append("    'username' = '").append(Constants.ODS_USER).append("',")
                .append("    'password' = '").append(Constants.ODS_PWD).append("'")
                .append(")");
        tEnv.executeSql(createSinkTableSql.toString());

        // ====================== 3. 构建数据抽取INSERT SQL（StringBuffer拼接）======================
        StringBuffer extractSql = new StringBuffer();
        extractSql.append("INSERT INTO T_ODS_WXAP_CHECKIN_TRP_ORDER(")
                .append("ETL_DATE, ID, ORDER_NO, ORDER_AMOUNT, SEAT_TYPE, ORDER_STATUS, ORDER_TIME, CONTACT_PERSON, ")
                .append("CONTACT_PHONE, CREATE_TIME, UPDATE_TIME, PAY_TYPE, CUSTOMER_ID, OPEN_ID, CHANNEL, ")
                .append("PAY_NO, THIRD_PAY_NO, TRP_PAY_NO, PAY_STATUS, TRP_PAY_STATUS)")
                .append(" SELECT ")
                .append("CAST('").append(etlDate).append("' AS DATE) ETL_DATE,")
                .append("CAST(ID AS BIGINT) ID,")
                .append("ORDER_NO, ORDER_AMOUNT, SEAT_TYPE, ORDER_STATUS, ORDER_TIME, CONTACT_PERSON, ")
                .append("sm4_encrypt(aes_decrypt(CONTACT_PHONE, '").append(Constants.WXAP_AES_KEY).append("'), '").append(Constants.SM4_KEY).append("') CONTACT_PHONE,")
                .append("CREATE_TIME, UPDATE_TIME, PAY_TYPE, CUSTOMER_ID, OPEN_ID, CHANNEL, ")
                .append("PAY_NO, THIRD_PAY_NO, TRP_PAY_NO, PAY_STATUS, TRP_PAY_STATUS ")
                .append("FROM CHECKIN_TRP_ORDER ")
                .append("WHERE CREATE_TIME BETWEEN TIMESTAMP '").append(etlDate).append(" 00:00:00' AND TIMESTAMP '").append(etlDate).append(" 23:59:59.999' ")
                .append("OR UPDATE_TIME BETWEEN TIMESTAMP '").append(etlDate).append(" 00:00:00' AND TIMESTAMP '").append(etlDate).append(" 23:59:59.999'");

        // ====================== 4. 构建备用抽取SQL（extractSql1，StringBuffer拼接）======================
        StringBuffer extractSql1 = new StringBuffer();
        extractSql1.append("SELECT ")
                .append("CAST('").append(etlDate).append("' AS DATE) ETL_DATE,")
                .append("CAST(ID AS BIGINT) ID, ")
                .append("ORDER_NO, ORDER_AMOUNT, SEAT_TYPE, ORDER_STATUS, ORDER_TIME, CONTACT_PERSON, CONTACT_PHONE, ")
                .append("CREATE_TIME, UPDATE_TIME, PAY_TYPE, ")
                .append("sm4_encrypt(CUSTOMER_ID, '").append(sm4key).append("') CUSTOMER_ID, ")
                .append("OPEN_ID, CHANNEL, PAY_NO, THIRD_PAY_NO, TRP_PAY_NO, PAY_STATUS, TRP_PAY_STATUS ")
                .append("FROM CHECKIN_TRP_ORDER ")
                .append("WHERE CAST(CREATE_TIME AS DATE) = '").append(etlDate).append("' ")
                .append("OR CAST(UPDATE_TIME AS DATE) = '").append(etlDate).append("'");

        // 执行数据插入
        TableResult result = tEnv.executeSql(extractSql.toString());
        result.print();
    }
}