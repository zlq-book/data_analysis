package com.travelsky.dataplatform.main.ods.v2.Jssc;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.OracleAESDecryptor;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.time.LocalDate;
import java.util.UUID;

/**
 * * 第一次取SELL_DATE是2022年1月1日后的，后续每天更新近2天数据
 */
public class ExtractTOdsJsscTbOtaOrder {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        // 将字符串转换为 LocalDate
        LocalDate currentDate = LocalDate.parse(etlDate);
        // 计算二天前的日期
        LocalDate twoDaysAgo = currentDate.minusDays(2);
        // 格式化为字符串
        String twoDaysAgoStr = twoDaysAgo.toString();
        String startDate = twoDaysAgoStr;
        String endDate = etlDate;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsJsscTbOtaOrder");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE TB_OTA_ORDER (\n" +
                "    ID                    BIGINT,\n" +
                "    ORDER_NO              VARCHAR(30),\n" +
                "    STATUS                VARCHAR(30),\n" +
                "    `COUNT`               VARCHAR(30),\n" +
                "    TOTALAMOUNT           BIGINT,\n" +
                "    PRODUCT_TYPE          BIGINT,\n" +
                "    SELL_DATE             TIMESTAMP(6),\n" +
                "    CHANNELID             VARCHAR(30),\n" +
                "    CHANNEL               VARCHAR(30),\n" +
                "    MOBILE                VARCHAR(30),\n" +
                "    ISSENDMESSAGE         VARCHAR(30),\n" +
                "    SERIALNO              VARCHAR(50),\n" +
                "    PAY_DATE              TIMESTAMP(6),\n" +
                "    INVOICETITLE          VARCHAR(30),\n" +
                "    INVOICEDATE           VARCHAR(30),\n" +
                "    DELIVERY_NO           VARCHAR(100),\n" +
                "    ORDER_DISPATCH_STATUS BIGINT,\n" +
                "    BILL_DELIVERY_NO      VARCHAR(30),\n" +
                "    BILL_DISPATCH_STATUS  BIGINT," +
                "    PAY_TYPE               VARCHAR(150)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.JSSC_IP + ":" + Constants.JSSC_PORT + "/" + Constants.JSSC_DB + "',\n" +
                "    'table-name' = '" + Constants.JSSC_SCHEMA + ".TB_OTA_ORDER', \n" +
                "    'username' = '" + Constants.JSSC_USER + "',\n" +
                "    'password' = '" + Constants.JSSC_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_JSSC_TB_OTA_ORDER (\n" +
                "    ID                    BIGINT,\n" +
                "    ORDER_NO              VARCHAR(30),\n" +
                "    STATUS                VARCHAR(30),\n" +
                "    `COUNT`               VARCHAR(30),\n" +
                "    TOTALAMOUNT           BIGINT,\n" +
                "    PRODUCT_TYPE          BIGINT,\n" +
                "    SELL_DATE             TIMESTAMP(6),\n" +
                "    CHANNELID             VARCHAR(30),\n" +
                "    CHANNEL               VARCHAR(30),\n" +
                "    MOBILE                VARCHAR(30),\n" +
                "    ISSENDMESSAGE         VARCHAR(30),\n" +
                "    SERIALNO              VARCHAR(50),\n" +
                "    PAY_DATE              TIMESTAMP(6),\n" +
                "    INVOICETITLE          VARCHAR(30),\n" +
                "    INVOICEDATE           VARCHAR(30),\n" +
                "    DELIVERY_NO           VARCHAR(100),\n" +
                "    ORDER_DISPATCH_STATUS BIGINT,\n" +
                "    BILL_DELIVERY_NO      VARCHAR(30),\n" +
                "    BILL_DISPATCH_STATUS  BIGINT," +
                "    PAY_TYPE               VARCHAR(150)," +
                "    ETL_DATE DATE" +
                //") WITH (\n" +
                //"    'connector' = 'jdbc',\n" +
                //"    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
                //"    'table-name' = 'T_ODS_JSSC_TB_OTA_ORDER',\n" +
                //"    'username' = '" + Constants.ODS_USER + "',\n" +
                //"    'password' = '" + Constants.ODS_PWD + "'\n" +
                //",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
                //")");
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "  'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "',\n" +
                "  'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "',\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_JSSC_TB_OTA_ORDER',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        String extractSql = "INSERT INTO T_ODS_JSSC_TB_OTA_ORDER (" +
                "    ETL_DATE,\n" +
                "    ID,\n" +
                "    ORDER_NO,\n" +
                "    STATUS,\n" +
                "    `COUNT`,\n" +
                "    TOTALAMOUNT,\n" +
                "    PRODUCT_TYPE,\n" +
                "    SELL_DATE,\n" +
                "    CHANNELID,\n" +
                "    CHANNEL,\n" +
                "    MOBILE,\n" +
                "    ISSENDMESSAGE,\n" +
                "    SERIALNO,\n" +
                "    PAY_DATE,\n" +
                "    INVOICETITLE,\n" +
                "    INVOICEDATE,\n" +
                "    DELIVERY_NO,\n" +
                "    ORDER_DISPATCH_STATUS,\n" +
                "    BILL_DELIVERY_NO,\n" +
                "    BILL_DISPATCH_STATUS," +
                "    PAY_TYPE)" +
                "    SELECT \n" +
                "    CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "    ID,\n" +
                "    ORDER_NO,\n" +
                "    STATUS,\n" +
                "    `COUNT`,\n" +
                "    TOTALAMOUNT,\n" +
                "    PRODUCT_TYPE,\n" +
                "    SELL_DATE,\n" +
                "    CHANNELID,\n" +
                "    CHANNEL,\n" +
                "    sm4_encrypt(MOBILE, '" + Constants.SM4_KEY + "') MOBILE, \n" +
                "    ISSENDMESSAGE,\n" +
                "    SERIALNO,\n" +
                "    PAY_DATE,\n" +
                "    INVOICETITLE,\n" +
                "    INVOICEDATE,\n" +
                "    DELIVERY_NO,\n" +
                "    ORDER_DISPATCH_STATUS,\n" +
                "    BILL_DELIVERY_NO,\n" +
                "    BILL_DISPATCH_STATUS,\n " +
                "    PAY_TYPE " +
                " FROM TB_OTA_ORDER"
                + " WHERE  SELL_DATE BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' ";
        TableResult result = tEnv.executeSql(extractSql);

        result.print();

    }
}
