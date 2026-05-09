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
public class ExtractTOdsWxapWxCheckinInsure {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsWxapWxCheckinInsure");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);

        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE WX_CHECKIN_INSURE (\n" +
                "ID VARCHAR(96), " +
                "FID VARCHAR(96), " +
                "UNIONID VARCHAR(96), " +
                "INSID VARCHAR(96), " +
                "FLIGHTNO VARCHAR(90), " +
                "DEPDATE TIMESTAMP(6), " +
                "ARRDATE TIMESTAMP(6), " +
                "DEP VARCHAR(60), " +
                "ARR VARCHAR(60), " +
                "DEPNAME VARCHAR(150), " +
                "ARRNAME VARCHAR(150), " +
                "CHECKID VARCHAR(96), " +
                "CERTNAME VARCHAR(150), " +
                "CERTTYPE VARCHAR(30), " +
                "CERTNO VARCHAR(300), " +
                "MOBILE VARCHAR(300), " +
                "PRODUCTNAME VARCHAR(150), " +
                "PRICE BIGINT, " +
                "TOTAL BIGINT, " +
                "PAYID VARCHAR(150), " +
                "PAYTIME BIGINT, " +
                "OSTATUS TINYINT, " +
                "CREATE_TIME TIMESTAMP(6), " +
                "DELETE_FLAG TINYINT, " +
                "FIDDISPLAY SMALLINT, " +
                "PAYLOGID VARCHAR(96), " +
                "OPENID VARCHAR(96), " +
                "INVOICE STRING, " +
                "ETN VARCHAR(60), " +
                "PAYSTATUS TINYINT, " +
                "INSURE_TYPE TINYINT, " +
                "UPDATE_TIME TIMESTAMP(6), " +
                "CERTEMAIL VARCHAR(150), " +
                "CERTBIRTHDAY VARCHAR(60), " +
                "POLICYID VARCHAR(96), " +
                "ORDER_NO VARCHAR(150), " +
                "CABIN VARCHAR(12), " +
                "POLICY_RESULT STRING, " +
                "POLICY_TYPE TINYINT, " +
                "INSURE_MOLD TINYINT, " +
                "FLIGHT_INDEX TINYINT, " +
                "CHANNEL_TYPE TINYINT " +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.WXAP_IP + ":" + Constants.WXAP_PORT + "/" + Constants.WXAP_DB + "',\n" +
                "    'table-name' = '" + Constants.WXAP_SCHEMA + ".WX_CHECKIN_INSURE', \n" +
                "    'username' = '" + Constants.WXAP_USER + "',\n" +
                "    'password' = '" + Constants.WXAP_PWD + "'\n" +
                ",\n" + "  'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_WXAP_WX_CHECKIN_INSURE (\n" +
                "ETL_DATE DATE, " +
                "ID VARCHAR(96), " +
                "FID VARCHAR(96), " +
                "UNIONID VARCHAR(96), " +
                "INSID VARCHAR(96), " +
                "FLIGHTNO VARCHAR(90), " +
                "DEPDATE TIMESTAMP(6), " +
                "ARRDATE TIMESTAMP(6), " +
                "DEP VARCHAR(60), " +
                "ARR VARCHAR(60), " +
                "DEPNAME VARCHAR(150), " +
                "ARRNAME VARCHAR(150), " +
                "CHECKID VARCHAR(96), " +
                "CERTNAME VARCHAR(150), " +
                "CERTTYPE VARCHAR(30), " +
                "CERTNO VARCHAR(300), " +
                "MOBILE VARCHAR(300), " +
                "PRODUCTNAME VARCHAR(150), " +
                "PRICE BIGINT, " +
                "TOTAL BIGINT, " +
                "PAYID VARCHAR(150), " +
                "PAYTIME BIGINT, " +
                "OSTATUS TINYINT, " +
                "CREATE_TIME TIMESTAMP(6), " +
                "DELETE_FLAG TINYINT, " +
                "FIDDISPLAY SMALLINT, " +
                "PAYLOGID VARCHAR(96), " +
                "OPENID VARCHAR(96), " +
                "INVOICE STRING, " +
                "ETN VARCHAR(60), " +
                "PAYSTATUS TINYINT, " +
                "INSURE_TYPE TINYINT, " +
                "UPDATE_TIME TIMESTAMP(6), " +
                "CERTEMAIL VARCHAR(150), " +
                "CERTBIRTHDAY VARCHAR(60), " +
                "POLICYID VARCHAR(96), " +
                "ORDER_NO VARCHAR(150), " +
                "CABIN VARCHAR(12), " +
                "POLICY_RESULT STRING, " +
                "POLICY_TYPE TINYINT, " +
                "INSURE_MOLD TINYINT, " +
                "FLIGHT_INDEX TINYINT, " +
                "CHANNEL_TYPE TINYINT " +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_WXAP_WX_CHECKIN_INSURE',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        String extractSql = "INSERT INTO T_ODS_WXAP_WX_CHECKIN_INSURE(" +
                "ETL_DATE\n" +
                ",ID\n" +
                ",FID\n" +
                ",UNIONID\n" +
                ",INSID\n" +
                ",FLIGHTNO\n" +
                ",DEPDATE\n" +
                ",ARRDATE\n" +
                ",DEP\n" +
                ",ARR\n" +
                ",DEPNAME\n" +
                ",ARRNAME\n" +
                ",CHECKID\n" +
                ",CERTNAME\n" +
                ",CERTTYPE\n" +
                ",CERTNO\n" +
                ",MOBILE\n" +
                ",PRODUCTNAME\n" +
                ",PRICE\n" +
                ",TOTAL\n" +
                ",PAYID\n" +
                ",PAYTIME\n" +
                ",OSTATUS\n" +
                ",CREATE_TIME\n" +
                ",DELETE_FLAG\n" +
                ",FIDDISPLAY\n" +
                ",PAYLOGID\n" +
                ",OPENID\n" +
                ",INVOICE\n" +
                ",ETN\n" +
                ",PAYSTATUS\n" +
                ",INSURE_TYPE\n" +
                ",UPDATE_TIME\n" +
                ",CERTEMAIL\n" +
                ",CERTBIRTHDAY\n" +
                ",POLICYID\n" +
                ",ORDER_NO\n" +
                ",CABIN\n" +
                ",POLICY_RESULT\n" +
                ",POLICY_TYPE\n" +
                ",INSURE_MOLD\n" +
                ",FLIGHT_INDEX\n" +
                ",CHANNEL_TYPE\n)  " +
                "SELECT CAST('" + etlDate + "' AS DATE) ETL_DATE" +
                ",ID\n" +
                ",FID\n" +
                ",UNIONID\n" +
                ",INSID\n" +
                ",FLIGHTNO\n" +
                ",DEPDATE\n" +
                ",ARRDATE\n" +
                ",DEP\n" +
                ",ARR\n" +
                ",DEPNAME\n" +
                ",ARRNAME\n" +
                ",CHECKID\n" +
                ",CERTNAME\n" +
                ",CERTTYPE\n" +
                ",sm4_encrypt(aes_decrypt(CERTNO, '" + Constants.WXAP_AES_KEY + "'), '" + Constants.SM4_KEY + "') CERTNO\n" +
                ",sm4_encrypt(aes_decrypt(MOBILE, '" + Constants.WXAP_AES_KEY + "'), '" + Constants.SM4_KEY + "') MOBILE\n" +
                ",PRODUCTNAME\n" +
                ",PRICE\n" +
                ",TOTAL\n" +
                ",sm4_encrypt (PAYID,'" + Constants.SM4_KEY + "') PAYID\n" +
                ",PAYTIME\n" +
                ",OSTATUS\n" +
                ",CREATE_TIME\n" +
                ",DELETE_FLAG\n" +
                ",FIDDISPLAY\n" +
                ",PAYLOGID\n" +
                ",OPENID\n" +
                ",INVOICE\n" +
                ",ETN\n" +
                ",PAYSTATUS\n" +
                ",INSURE_TYPE\n" +
                ",UPDATE_TIME\n" +
                ",CERTEMAIL\n" +
                ",CERTBIRTHDAY\n" +
                ",POLICYID\n" +
                ",ORDER_NO\n" +
                ",CABIN\n" +
                ",POLICY_RESULT\n" +
                ",POLICY_TYPE\n" +
                ",INSURE_MOLD\n" +
                ",FLIGHT_INDEX\n" +
                ",CHANNEL_TYPE\n" +
                " FROM WX_CHECKIN_INSURE "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' " +
                " OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        TableResult result = tEnv.executeSql(extractSql);

        result.print();

    }

}
