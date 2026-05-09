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
 * @author kuangaihua
 * @date 2025/6/26 16:29
 */
public class ExtractTOdsWxapCheckinSypr {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsWxapCheckinSypr");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);

        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE CHECKIN_SYPR (\n" +
                "  ID BIGINT,\n" +
                "  CI_ID BIGINT,\n" +
                "  CHECKIN_NO STRING,\n" +
                "  TKT_NUMBER STRING,\n" +
                "  PNR STRING,\n" +
                "  PSR_NAME STRING,\n" +
                "  PSR_EN_NAME STRING,\n" +
                "  CERTIFICATE_TYPE STRING,\n" +
                "  CERTIFICATE_NUMBER STRING,\n" +
                "  PHONE STRING,\n" +
                "  ASR_SEAT STRING,\n" +
                "  CABIN_TYPE STRING,\n" +
                "  SPEICIAL_SVC STRING,\n" +
                "  ASR_STATUS STRING,\n" +
                "  HOST_NUM STRING,\n" +
                "  TOUR_INDEX STRING,\n" +
                "  CHD_FLAG STRING,\n" +
                "  CHD STRING,\n" +
                "  SEAT_NO STRING,\n" +
                "  BOARD_STREAM STRING,\n" +
                "  BOARDING_NUMBER STRING,\n" +
                "  CHECK_CODE STRING,\n" +
                "  CHECKIN_NUM TINYINT,\n" +
                "  CHANGE_SEAT_NO_NUM TINYINT,\n" +
                "  CHECKIN_STATUS TINYINT,\n" +
                "  CHECKIN_TYPE TINYINT,\n" +
                "  FARE_BASIS STRING,\n" +
                "  DIFF_CHECKIN TINYINT,\n" +
                "  SECURITY_CHECK_TIME TIMESTAMP(6),\n" +
                "  CHECKIN_TIME TIMESTAMP(6),\n" +
                "  WRAP_COUNT INT,\n" +
                "  FAIL_REASON STRING,\n" +
                "  HANDLE_TYPE TINYINT,\n" +
                "  CREATE_TIME TIMESTAMP(6),\n" +
                "  UPDATE_TIME TIMESTAMP(6),\n" +
                "  CREATE_BY STRING,\n" +
                "  UPDATE_BY STRING,\n" +
                "  AUTO_CHECKIN_TIME TIMESTAMP(6),\n" +
                "  LAST_CHECKIN_CHANNEL TINYINT\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.WXAP_IP + ":" + Constants.WXAP_PORT + "/" + Constants.WXAP_DB + "',\n" +
                "    'table-name' = '" + Constants.WXAP_SCHEMA + ".CHECKIN_SYPR', \n" +
                "    'username' = '" + Constants.WXAP_USER + "',\n" +
                "    'password' = '" + Constants.WXAP_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_WXAP_CHECKIN_SYPR (\n" +
                "    ETL_DATE DATE,\n" +
                "  ID BIGINT,\n" +
                "  CI_ID BIGINT,\n" +
                "  CHECKIN_NO STRING,\n" +
                "  TKT_NUMBER STRING,\n" +
                "  PNR STRING,\n" +
                "  PSR_NAME STRING,\n" +
                "  PSR_EN_NAME STRING,\n" +
                "  CERTIFICATE_TYPE STRING,\n" +
                "  CERTIFICATE_NUMBER STRING,\n" +
                "  PHONE STRING,\n" +
                "  ASR_SEAT STRING,\n" +
                "  CABIN_TYPE STRING,\n" +
                "  SPEICIAL_SVC STRING,\n" +
                "  ASR_STATUS STRING,\n" +
                "  HOST_NUM STRING,\n" +
                "  TOUR_INDEX STRING,\n" +
                "  CHD_FLAG STRING,\n" +
                "  CHD STRING,\n" +
                "  SEAT_NO STRING,\n" +
                "  BOARD_STREAM STRING,\n" +
                "  BOARDING_NUMBER STRING,\n" +
                "  CHECK_CODE STRING,\n" +
                "  CHECKIN_NUM TINYINT,\n" +
                "  CHANGE_SEAT_NO_NUM TINYINT,\n" +
                "  CHECKIN_STATUS TINYINT,\n" +
                "  CHECKIN_TYPE TINYINT,\n" +
                "  FARE_BASIS STRING,\n" +
                "  DIFF_CHECKIN TINYINT,\n" +
                "  SECURITY_CHECK_TIME TIMESTAMP(6),\n" +
                "  CHECKIN_TIME TIMESTAMP(6),\n" +
                "  WRAP_COUNT INT,\n" +
                "  FAIL_REASON STRING,\n" +
                "  HANDLE_TYPE TINYINT,\n" +
                "  CREATE_TIME TIMESTAMP(6),\n" +
                "  UPDATE_TIME TIMESTAMP(6),\n" +
                "  CREATE_BY STRING,\n" +
                "  UPDATE_BY STRING,\n" +
                "  AUTO_CHECKIN_TIME TIMESTAMP(6),\n" +
                "  LAST_CHECKIN_CHANNEL TINYINT\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_WXAP_CHECKIN_SYPR',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        String extractSql = "INSERT INTO T_ODS_WXAP_CHECKIN_SYPR(" +
                "ETL_DATE\n" +
                ",ID\n" +
                ",CI_ID\n" +
                ",CHECKIN_NO\n" +
                ",TKT_NUMBER\n" +
                ",PNR\n" +
                ",PSR_NAME\n" +
                ",PSR_EN_NAME\n" +
                ",CERTIFICATE_TYPE\n" +
                ",CERTIFICATE_NUMBER\n" +
                ",PHONE\n" +
                ",ASR_SEAT\n" +
                ",CABIN_TYPE\n" +
                ",SPEICIAL_SVC\n" +
                ",ASR_STATUS\n" +
                ",HOST_NUM\n" +
                ",TOUR_INDEX\n" +
                ",CHD_FLAG\n" +
                ",CHD\n" +
                ",SEAT_NO\n" +
                ",BOARD_STREAM\n" +
                ",BOARDING_NUMBER\n" +
                ",CHECK_CODE\n" +
                ",CHECKIN_NUM\n" +
                ",CHANGE_SEAT_NO_NUM\n" +
                ",CHECKIN_STATUS\n" +
                ",CHECKIN_TYPE\n" +
                ",FARE_BASIS\n" +
                ",DIFF_CHECKIN\n" +
                ",SECURITY_CHECK_TIME\n" +
                ",CHECKIN_TIME\n" +
                ",WRAP_COUNT\n" +
                ",FAIL_REASON\n" +
                ",HANDLE_TYPE\n" +
                ",CREATE_TIME\n" +
                ",UPDATE_TIME\n" +
                ",CREATE_BY\n" +
                ",UPDATE_BY\n" +
                ",AUTO_CHECKIN_TIME\n" +
                ",LAST_CHECKIN_CHANNEL\n)  " +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE" +
                ",ID\n" +
                ",CI_ID\n" +
                ",CHECKIN_NO\n" +
                ",TKT_NUMBER\n" +
                ",PNR\n" +
                ",PSR_NAME\n" +
                ",PSR_EN_NAME\n" +
                ",CERTIFICATE_TYPE\n" +
                ",sm4_encrypt(aes_decrypt(CERTIFICATE_NUMBER, '" + Constants.WXAP_AES_KEY + "'), '" + Constants.SM4_KEY + "') CERTIFICATE_NUMBER\n" +
                ",sm4_encrypt(aes_decrypt(PHONE, '" + Constants.WXAP_AES_KEY + "'), '" + Constants.SM4_KEY + "') PHONE\n" +
                ",ASR_SEAT\n" +
                ",CABIN_TYPE\n" +
                ",SPEICIAL_SVC\n" +
                ",ASR_STATUS\n" +
                ",HOST_NUM\n" +
                ",TOUR_INDEX\n" +
                ",CHD_FLAG\n" +
                ",CHD\n" +
                ",SEAT_NO\n" +
                ",BOARD_STREAM\n" +
                ",BOARDING_NUMBER\n" +
                ",CHECK_CODE\n" +
                ",CHECKIN_NUM\n" +
                ",CHANGE_SEAT_NO_NUM\n" +
                ",CHECKIN_STATUS\n" +
                ",CHECKIN_TYPE\n" +
                ",FARE_BASIS\n" +
                ",DIFF_CHECKIN\n" +
                ",SECURITY_CHECK_TIME\n" +
                ",CHECKIN_TIME\n" +
                ",WRAP_COUNT\n" +
                ",FAIL_REASON\n" +
                ",HANDLE_TYPE\n" +
                ",CREATE_TIME\n" +
                ",UPDATE_TIME\n" +
                ",CREATE_BY\n" +
                ",UPDATE_BY\n" +
                ",AUTO_CHECKIN_TIME\n" +
                ",LAST_CHECKIN_CHANNEL\n" +
                " FROM CHECKIN_SYPR "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' " +
                " OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        System.out.println(extractSql);
        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }

}
