package com.travelsky.dataplatform.main.ods.v2.Jssc;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.time.LocalDate;
import java.util.UUID;

/**
 * * 第一次取UPGRADE_TIME和UPGFADED_TIME是2022年1月1日后的，
 * * 后续每天按照创建时间UPGRADE_TIME和UPGFADED_TIME近一个月的全量更新到ODS，再将仅一个月的更新到事实表
 */
public class ExtractTOdsJsscTbExternalOrders {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        // 将字符串转换为 LocalDate
        LocalDate currentDate = LocalDate.parse(etlDate);
        // 计算一个月前的日期
        LocalDate oneMonthAgo = currentDate.minusMonths(1);
        // 格式化为字符串
        String oneMonthAgoStr = oneMonthAgo.toString();
        String startDate = oneMonthAgoStr;
        String endDate = etlDate;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsJsscTbExternalOrders");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        //创建上游ORACLE数据源表（按最新字段）
        tEnv.executeSql("CREATE TABLE TB_EXTERNAL_ORDERS (\n" +
                "    UPGRADE_ORDER_ID VARCHAR(20),\n" +
                "    ORDER_NO         VARCHAR(30),\n" +
                "    FLT_DATE         VARCHAR(20),\n" +
                "    FLT_NUM          VARCHAR(20),\n" +
                "    ORIG             VARCHAR(10),\n" +
                "    DEST             VARCHAR(10),\n" +
                "    TICKET_NUM       VARCHAR(20),\n" +
                "    UPGRADE_PRICE    VARCHAR(20),\n" +
                "    BEFORE_CABIN     VARCHAR(10),\n" +
                "    BEFORE_SEAT      VARCHAR(20),\n" +
                "    AFTER_CABIN      VARCHAR(5),\n" +
                "    PSGNAME          VARCHAR(50),\n" +
                "    FLY_MILEAGE      VARCHAR(20),\n" +
                "    USER_ID          VARCHAR(30),\n" +
                "    ACCT             VARCHAR(20),\n" +
                "    TAKE_OFF         VARCHAR(100),\n" +
                "    UPGRADE_TIME     VARCHAR(20),\n" +
                "    MOBILE           VARCHAR(400),\n" +
                "    UPGRADE_STATUS   VARCHAR(5),\n" +
                "    OPERATOR         VARCHAR(20),\n" +
                "    CABIN_CREW_NAME  VARCHAR(420),\n" +
                "    PAY_TYPE         VARCHAR(10),\n" +
                "    UPGRADE_ORDER_NO VARCHAR(30),\n" +
                "    COUON_NO         VARCHAR(20),\n" +
                "    PAY_PRICE        VARCHAR(20),\n" +
                "    PAY_STATUS       VARCHAR(5),\n" +
                "    PAY_TIME         VARCHAR(20),\n" +
                "    REMARK           VARCHAR(800),\n" +
                "    INVOICE_STATE    VARCHAR(5),\n" +
                "    TRADE_NO         VARCHAR(30),\n" +
                "    PRODUCT_NO       VARCHAR(10),\n" +
                "    PRODUCT_NAME     VARCHAR(60),\n" +
                "    AFTER_SEAT       VARCHAR(20),\n" +
                "    PSG_CARDID       VARCHAR(500),\n" +
                "    SALE_MAN         VARCHAR(20),\n" +
                "    UPGFADED_TIME    VARCHAR(100),\n" +
                "    EMD_TICKET       VARCHAR(400),\n" +
                "    CHANNEL_TYPE     VARCHAR(10),\n" +
                "    DPI_IDX          VARCHAR(30),\n" +
                "    PAYCHANNEL       VARCHAR(20),\n" +
                "    MAKEUPFLAG       VARCHAR(5),\n" +
                "    CARDUSEDNUM      VARCHAR(20),\n" +
                "    CARDAVAILNUM     VARCHAR(20),\n" +
                "    ORDER_STATUS     VARCHAR(5),\n" +
                "    IS_POST          VARCHAR(5),\n" +
                "    SERIAL_NO        VARCHAR(100),\n" +
                "    FLY_TYPE         VARCHAR(10),\n" +
                "    CREATE_TIME      VARCHAR(25)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.JSSC_IP + ":" + Constants.JSSC_PORT + "/" + Constants.JSSC_DB + "',\n" +
                "    'table-name' = '" + Constants.JSSC_SCHEMA + ".TB_EXTERNAL_ORDERS', \n" +
                "    'username' = '" + Constants.JSSC_USER + "',\n" +
                "    'password' = '" + Constants.JSSC_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        //创建Doris/MySQL目标表（按最新字段）
        tEnv.executeSql("CREATE TABLE T_ODS_JSSC_TB_EXTERNAL_ORDERS (\n" +
                "    UPGRADE_ORDER_ID VARCHAR(20),\n" +
                "    ORDER_NO         VARCHAR(30),\n" +
                "    FLT_DATE         VARCHAR(20),\n" +
                "    FLT_NUM          VARCHAR(20),\n" +
                "    ORIG             VARCHAR(10),\n" +
                "    DEST             VARCHAR(10),\n" +
                "    TICKET_NUM       VARCHAR(20),\n" +
                "    UPGRADE_PRICE    VARCHAR(20),\n" +
                "    BEFORE_CABIN     VARCHAR(10),\n" +
                "    BEFORE_SEAT      VARCHAR(20),\n" +
                "    AFTER_CABIN      VARCHAR(5),\n" +
                "    PSGNAME          VARCHAR(50),\n" +
                "    FLY_MILEAGE      VARCHAR(20),\n" +
                "    USER_ID          VARCHAR(30),\n" +
                "    ACCT             VARCHAR(20),\n" +
                "    TAKE_OFF         VARCHAR(100),\n" +
                "    UPGRADE_TIME     TIMESTAMP(6),\n" +
                "    MOBILE           VARCHAR(400),\n" +
                "    UPGRADE_STATUS   VARCHAR(5),\n" +
                "    OPERATOR         VARCHAR(20),\n" +
                "    CABIN_CREW_NAME  VARCHAR(420),\n" +
                "    PAY_TYPE         VARCHAR(10),\n" +
                "    UPGRADE_ORDER_NO VARCHAR(30),\n" +
                "    COUON_NO         VARCHAR(20),\n" +
                "    PAY_PRICE        VARCHAR(20),\n" +
                "    PAY_STATUS       VARCHAR(5),\n" +
                "    PAY_TIME         VARCHAR(20),\n" +
                "    REMARK           VARCHAR(800),\n" +
                "    INVOICE_STATE    VARCHAR(5),\n" +
                "    TRADE_NO         VARCHAR(30),\n" +
                "    PRODUCT_NO       VARCHAR(10),\n" +
                "    PRODUCT_NAME     VARCHAR(60),\n" +
                "    AFTER_SEAT       VARCHAR(20),\n" +
                "    PSG_CARDID       VARCHAR(500),\n" +
                "    SALE_MAN         VARCHAR(20),\n" +
                "    UPGFADED_TIME    TIMESTAMP(6),\n" +
                "    EMD_TICKET       VARCHAR(400),\n" +
                "    CHANNEL_TYPE     VARCHAR(10),\n" +
                "    DPI_IDX          VARCHAR(30),\n" +
                "    PAYCHANNEL       VARCHAR(20),\n" +
                "    MAKEUPFLAG       VARCHAR(5),\n" +
                "    CARDUSEDNUM      VARCHAR(20),\n" +
                "    CARDAVAILNUM     VARCHAR(20),\n" +
                "    ORDER_STATUS     VARCHAR(5),\n" +
                "    IS_POST          VARCHAR(5),\n" +
                "    SERIAL_NO        VARCHAR(100),\n" +
                "    FLY_TYPE         VARCHAR(10),\n" +
                "    CREATE_TIME      VARCHAR(25),\n" +
                "    ETL_DATE         DATE\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
                "    'table-name' = 'T_ODS_JSSC_TB_EXTERNAL_ORDERS',\n" +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ",\n" + "  'driver' = '" + Constants.MYSQL_DRIVER + "'" +
                ")");
        //") WITH (\n" +
        //" 'connector' = 'doris',\n" +
        //"  'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "',\n" +
        //"  'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "',\n" +
        //" 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_JSSC_TB_EXTERNAL_ORDERS',\n" +
        //"'sink.label-prefix' = '" + timestamp + uuid + "'," +
        //" 'sink.properties.read_json_by_line' = 'true'," +
        //" 'sink.properties.format' = 'json'," +
        //"    'username' = '" + Constants.ODS_USER + "',\n" +
        //"    'password' = '" + Constants.ODS_PWD + "'\n" +
        //")");
        String extractSql = "INSERT INTO T_ODS_JSSC_TB_EXTERNAL_ORDERS (" +
                "    ETL_DATE,\n" +
                "    UPGRADE_ORDER_ID,\n" +
                "    ORDER_NO,\n" +
                "    FLT_DATE,\n" +
                "    FLT_NUM,\n" +
                "    ORIG,\n" +
                "    DEST,\n" +
                "    TICKET_NUM,\n" +
                "    UPGRADE_PRICE,\n" +
                "    BEFORE_CABIN,\n" +
                "    BEFORE_SEAT,\n" +
                "    AFTER_CABIN,\n" +
                "    PSGNAME,\n" +
                "    FLY_MILEAGE,\n" +
                "    USER_ID,\n" +
                "    ACCT,\n" +
                "    TAKE_OFF,\n" +
                "    UPGRADE_TIME,\n" +
                "    MOBILE,\n" +
                "    UPGRADE_STATUS,\n" +
                "    OPERATOR,\n" +
                "    CABIN_CREW_NAME,\n" +
                "    PAY_TYPE,\n" +
                "    UPGRADE_ORDER_NO,\n" +
                "    COUON_NO,\n" +
                "    PAY_PRICE,\n" +
                "    PAY_STATUS,\n" +
                "    PAY_TIME,\n" +
                "    REMARK,\n" +
                "    INVOICE_STATE,\n" +
                "    TRADE_NO,\n" +
                "    PRODUCT_NO,\n" +
                "    PRODUCT_NAME,\n" +
                "    AFTER_SEAT,\n" +
                "    PSG_CARDID,\n" +
                "    SALE_MAN,\n" +
                "    UPGFADED_TIME,\n" +
                "    EMD_TICKET,\n" +
                "    CHANNEL_TYPE,\n" +
                "    DPI_IDX,\n" +
                "    PAYCHANNEL,\n" +
                "    MAKEUPFLAG,\n" +
                "    CARDUSEDNUM,\n" +
                "    CARDAVAILNUM,\n" +
                "    ORDER_STATUS,\n" +
                "    IS_POST,\n" +
                "    SERIAL_NO,\n" +
                "    FLY_TYPE,\n" +
                "    CREATE_TIME)  " +
                "    SELECT \n" +
                "    CAST('" + etlDate + "' AS DATE) ETL_DATE,\n" +
                "    UPGRADE_ORDER_ID,\n" +
                "    ORDER_NO,\n" +
                "    FLT_DATE,\n" +
                "    FLT_NUM,\n" +
                "    ORIG,\n" +
                "    DEST,\n" +
                "    TICKET_NUM,\n" +
                "    UPGRADE_PRICE,\n" +
                "    BEFORE_CABIN,\n" +
                "    BEFORE_SEAT,\n" +
                "    AFTER_CABIN,\n" +
                "    PSGNAME,\n" +
                "    FLY_MILEAGE,\n" +
                "    USER_ID,\n" +
                "    ACCT,\n" +
                "    TAKE_OFF,\n" +
                "CASE \n" +
                "    WHEN UPGRADE_TIME IS NOT NULL AND CHAR_LENGTH(UPGRADE_TIME) = 16 THEN TO_TIMESTAMP(CONCAT(UPGRADE_TIME, ':00'), 'yyyy-MM-dd HH:mm:ss') \n" +
                "    WHEN UPGRADE_TIME IS NOT NULL AND CHAR_LENGTH(UPGRADE_TIME) = 19 THEN TO_TIMESTAMP(UPGRADE_TIME, 'yyyy-MM-dd HH:mm:ss') \n" +
                "    ELSE NULL \n" +
                "END AS UPGRADE_TIME,\n" +
                "    sm4_encrypt(MOBILE, '" + Constants.SM4_KEY + "') MOBILE, \n" +
                "    UPGRADE_STATUS,\n" +
                "    OPERATOR,\n" +
                "    CABIN_CREW_NAME,\n" +
                "    PAY_TYPE,\n" +
                "    UPGRADE_ORDER_NO,\n" +
                "    COUON_NO,\n" +
                "    PAY_PRICE,\n" +
                "    PAY_STATUS,\n" +
                "    PAY_TIME,\n" +
                "    REMARK,\n" +
                "    INVOICE_STATE,\n" +
                "    TRADE_NO,\n" +
                "    PRODUCT_NO,\n" +
                "    PRODUCT_NAME,\n" +
                "    AFTER_SEAT,\n" +
                "    sm4_encrypt(PSG_CARDID, '" + Constants.SM4_KEY + "') PSG_CARDID, \n" +
                "    SALE_MAN,\n" +
                "CASE \n" +
                "    WHEN UPGFADED_TIME IS NOT NULL AND CHAR_LENGTH(UPGFADED_TIME) = 16 THEN TO_TIMESTAMP(CONCAT(UPGFADED_TIME, ':00'), 'yyyy-MM-dd HH:mm:ss') \n" +
                "    WHEN UPGFADED_TIME IS NOT NULL AND CHAR_LENGTH(UPGFADED_TIME) = 19 THEN TO_TIMESTAMP(UPGFADED_TIME, 'yyyy-MM-dd HH:mm:ss') \n" +
                "    ELSE NULL \n" +
                "END AS UPGFADED_TIME,\n" +
                "    EMD_TICKET,\n" +
                "    CHANNEL_TYPE,\n" +
                "    DPI_IDX,\n" +
                "    PAYCHANNEL,\n" +
                "    MAKEUPFLAG,\n" +
                "    CARDUSEDNUM,\n" +
                "    CARDAVAILNUM,\n" +
                "    ORDER_STATUS,\n" +
                "    IS_POST,\n" +
                "    SERIAL_NO,\n" +
                "    FLY_TYPE,\n" +
                "    CREATE_TIME " +
                " FROM TB_EXTERNAL_ORDERS "
                + " WHERE  (UPGRADE_TIME >= '" + startDate + " 00:00:00' AND UPGRADE_TIME <= '" + endDate + " 23:59:59' "
                + " OR UPGFADED_TIME >= '" + startDate + " 00:00:00' AND UPGFADED_TIME <= '" + endDate + " 23:59:59')"
                ;
        TableResult result = tEnv.executeSql(extractSql);

        result.print();

    }
}
