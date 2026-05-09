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
 * *第一次取SELLDATE是2022年1月1日后的，后续每天更新近2天数据
 */
public class ExtractTOdsJsscTbOrder {
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
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsJsscTbOrder");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE TB_ORDER (\n" +
                "    ID                  BIGINT,\n" +
                "    BOOKCOUNT           BIGINT,\n" +
                "    HASBILL             TINYINT,\n" +
                "    ISSENDMESSAGE       TINYINT,\n" +
                "    SELLDATE            TIMESTAMP(6),\n" +
                "    SOLACOUNT           BIGINT,\n" +
                "    STATUS              BIGINT,\n" +
                "    TOTALAMOUNT         BIGINT,\n" +
                "    BILL_ID             BIGINT,\n" +
                "    CUSTOMER_ID         BIGINT,\n" +
                "    USER_ID             BIGINT,\n" +
                "    ORDERNO             VARCHAR(60),\n" +
                "    DELIVERYNO          VARCHAR(40),\n" +
                "    BILLDELIVERYNO      VARCHAR(40),\n" +
                "    ORDERTYPE           VARCHAR(255),\n" +
                "    ORGANIZE_ID         BIGINT,\n" +
                "    ADDRESS_ID          BIGINT,\n" +
                "    SATUSE              BIGINT,\n" +
                "    TOCUSTOMER          TINYINT,\n" +
                "    UPGRADECOUNTTYPE    TINYINT,\n" +
                "    BILLDISPATCHSTATUS  TINYINT,\n" +
                "    ORDERDISPATCHSTATUS TINYINT,\n" +
                "    EXPORTDATE          TIMESTAMP(6),\n" +
                "    EXPORTDATE2         TIMESTAMP(6),\n" +
                "    SCANDATE            TIMESTAMP(6),\n" +
                "    QRCODEOFEMPLOYEE_ID BIGINT,\n" +
                "    CLICK_PAY_DATE      TIMESTAMP(6),\n" +
                "    PRODUCTTYPE         BIGINT,\n" +
                "    USERID_TRANSFER     BIGINT" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.JSSC_IP + ":" + Constants.JSSC_PORT + "/" + Constants.JSSC_DB + "',\n" +
                "    'table-name' = '" + Constants.JSSC_SCHEMA + ".TB_ORDER', \n" +
                "    'username' = '" + Constants.JSSC_USER + "',\n" +
                "    'password' = '" + Constants.JSSC_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_JSSC_TB_ORDER (\n" +
                "    ID                  BIGINT,\n" +
                "    BOOKCOUNT           BIGINT,\n" +
                "    HASBILL             TINYINT,\n" +
                "    ISSENDMESSAGE       TINYINT,\n" +
                "    SELLDATE            TIMESTAMP(6),\n" +
                "    SOLACOUNT           BIGINT,\n" +
                "    STATUS              BIGINT,\n" +
                "    TOTALAMOUNT         BIGINT,\n" +
                "    BILL_ID             BIGINT,\n" +
                "    CUSTOMER_ID         BIGINT,\n" +
                "    USER_ID             BIGINT,\n" +
                "    ORDERNO             VARCHAR(60),\n" +
                "    DELIVERYNO          VARCHAR(40),\n" +
                "    BILLDELIVERYNO      VARCHAR(40),\n" +
                "    ORDERTYPE           VARCHAR(255),\n" +
                "    ORGANIZE_ID         BIGINT,\n" +
                "    ADDRESS_ID          BIGINT,\n" +
                "    SATUSE              BIGINT,\n" +
                "    TOCUSTOMER          TINYINT,\n" +
                "    UPGRADECOUNTTYPE    TINYINT,\n" +
                "    BILLDISPATCHSTATUS  TINYINT,\n" +
                "    ORDERDISPATCHSTATUS TINYINT,\n" +
                "    EXPORTDATE          TIMESTAMP(6),\n" +
                "    EXPORTDATE2         TIMESTAMP(6),\n" +
                "    SCANDATE            TIMESTAMP(6),\n" +
                "    QRCODEOFEMPLOYEE_ID BIGINT,\n" +
                "    CLICK_PAY_DATE      TIMESTAMP(6),\n" +
                "    PRODUCTTYPE         BIGINT,\n" +
                "    USERID_TRANSFER     BIGINT," +
                "    ETL_DATE DATE" +
                //") WITH (\n" +
                //"    'connector' = 'jdbc',\n" +
                //"    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
                //"    'table-name' = 'T_ODS_JSSC_TB_ORDER',\n" +
                //"    'username' = '" + Constants.ODS_USER + "',\n" +
                //"    'password' = '" + Constants.ODS_PWD + "'\n" +
                //",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
                //")");
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "  'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "',\n" +
                "  'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "',\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_JSSC_TB_ORDER',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        String extractSql = "INSERT INTO T_ODS_JSSC_TB_ORDER (" +
                "    ETL_DATE,\n" +
                "    ID,\n" +
                "    BOOKCOUNT,\n" +
                "    HASBILL,\n" +
                "    ISSENDMESSAGE,\n" +
                "    SELLDATE,\n" +
                "    SOLACOUNT,\n" +
                "    STATUS,\n" +
                "    TOTALAMOUNT,\n" +
                "    BILL_ID,\n" +
                "    CUSTOMER_ID,\n" +
                "    USER_ID,\n" +
                "    ORDERNO,\n" +
                "    DELIVERYNO,\n" +
                "    BILLDELIVERYNO,\n" +
                "    ORDERTYPE,\n" +
                "    ORGANIZE_ID,\n" +
                "    ADDRESS_ID,\n" +
                "    SATUSE,\n" +
                "    TOCUSTOMER,\n" +
                "    UPGRADECOUNTTYPE,\n" +
                "    BILLDISPATCHSTATUS,\n" +
                "    ORDERDISPATCHSTATUS,\n" +
                "    EXPORTDATE,\n" +
                "    EXPORTDATE2,\n" +
                "    SCANDATE,\n" +
                "    QRCODEOFEMPLOYEE_ID,\n" +
                "    CLICK_PAY_DATE,\n" +
                "    PRODUCTTYPE,\n" +
                "    USERID_TRANSFER)  " +
                "    SELECT \n" +
                "    CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "    ID,\n" +
                "    BOOKCOUNT,\n" +
                "    HASBILL,\n" +
                "    ISSENDMESSAGE,\n" +
                "    SELLDATE,\n" +
                "    SOLACOUNT,\n" +
                "    STATUS,\n" +
                "    TOTALAMOUNT,\n" +
                "    BILL_ID,\n" +
                "    CUSTOMER_ID,\n" +
                "    USER_ID,\n" +
                "    ORDERNO,\n" +
                "    DELIVERYNO,\n" +
                "    BILLDELIVERYNO,\n" +
                "    ORDERTYPE,\n" +
                "    ORGANIZE_ID,\n" +
                "    ADDRESS_ID,\n" +
                "    SATUSE,\n" +
                "    TOCUSTOMER,\n" +
                "    UPGRADECOUNTTYPE,\n" +
                "    BILLDISPATCHSTATUS,\n" +
                "    ORDERDISPATCHSTATUS,\n" +
                "    EXPORTDATE,\n" +
                "    EXPORTDATE2,\n" +
                "    SCANDATE,\n" +
                "    QRCODEOFEMPLOYEE_ID,\n" +
                "    CLICK_PAY_DATE,\n" +
                "    PRODUCTTYPE,\n" +
                "    USERID_TRANSFER " +
                " FROM TB_ORDER"
                + " WHERE  SELLDATE BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' ";
        TableResult result = tEnv.executeSql(extractSql);

        result.print();

    }
}
