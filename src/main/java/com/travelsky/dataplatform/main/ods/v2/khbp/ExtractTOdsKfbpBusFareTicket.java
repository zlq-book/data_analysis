package com.travelsky.dataplatform.main.ods.v2.khbp;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * @author
 * @date 2025/9/25 16:29
 */
public class ExtractTOdsKfbpBusFareTicket {
    public static Logger log = LoggerFactory.getLogger(ExtractTOdsKfbpBusFareTicket.class);
    public static void main(String[] args) throws IOException {
        log.info(" ExtractTOdsKfbpBusFareTicket data transfer start");
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String startDate = etlDate;
        String endDate = etlDate;
        if (args.length > 2) {
            startDate = args[1];
            endDate = args[2];
        }
        log.info("ExtractTOdsKfbpBusOrder etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, " ExtractTOdsKfbpBusFareTicket");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        log.info(" ExtractTOdsKfbpBusFareTicket executeSql create table for source start");
        tEnv.executeSql("CREATE TABLE BUS_FARE_TICKET (\n" +
                "    `PKID` VARCHAR(255) NOT NULL,\n" +
                "    `CREATE_BY` VARCHAR(40),\n" +
                "    `CREATE_DATE` TIMESTAMP,\n" +
                "    `CREATE_NAME` VARCHAR(40),\n" +
                "    `UPDATE_BY` VARCHAR(40),\n" +
                "    `UPDATE_DATE` TIMESTAMP,\n" +
                "    `UPDATE_NAME` VARCHAR(40),\n" +
                "    `PAY_BALANCE_STATUS` INT,\n" +
                "    `PAY_CONFIRM_STATUS` INT,\n" +
                "    `PAY_TRADE_NO` VARCHAR(16),\n" +
                "    `PRICE_ALL_FEE` DECIMAL(19, 2),\n" +
                "    `PRICE_BUILD_FEE` DECIMAL(19, 2),\n" +
                "    `PRICE_CABIN_PRICE` DECIMAL(19, 2),\n" +
                "    `PRICE_CURRENCY` INT,\n" +
                "    `PRICE_EXCHANGE_RATE` DECIMAL(10, 4),\n" +
                "    `PRICE_FUEL_FEE` DECIMAL(19, 2),\n" +
                "    `PRICE_OB_FEE` DECIMAL(19, 2),\n" +
                "    `PRICE_TICKET_PRICE` DECIMAL(19, 2),\n" +
                "    `PRICE_TOTAL_PRICE` DECIMAL(19, 2),\n" +
                "    `TICKET_NO` VARCHAR(16),\n" +
                "    `FARE_PKID` VARCHAR(255),\n" +
                "    `ORDER_PKID` VARCHAR(255),\n" +
                "    `IS_VOID` tinyint,  -- NUMBER(1,0) to BOOLEAN\n" +
                "    `BILL_TIME` TIMESTAMP,\n" +
                "    `IS_REGISTRATION` tinyint" +

                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.KHBP_IP + ":" + Constants.KHBP_PORT + "/" + Constants.KHBP_DB + "',\n" +
                "    'table-name' = '" + Constants.KHBP_SCHEMA + ".BUS_FARE_TICKET', \n" +
                "    'username' = '" + Constants.KHBP_USER + "',\n" +
                "    'password' = '" + Constants.KHBP_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        log.info(" ExtractTOdsKfbpBusFareTicket executeSql create table for source end");
        //创建Doris目标表
        log.info(" ExtractTOdsKfbpBusFareTicket executeSql create table for doris start");
        tEnv.executeSql("CREATE TABLE T_ODS_KFBP_BUS_FARE_TICKET (\n" +
                "  `PKID` VARCHAR(1000) NOT NULL,\n" +
                "  `CREATE_BY` VARCHAR(200),\n" +
                "  `CREATE_DATE` TIMESTAMP(3),\n" +
                "  `CREATE_NAME` VARCHAR(200),\n" +
                "  `UPDATE_BY` VARCHAR(200),\n" +
                "  `UPDATE_DATE` TIMESTAMP(3),\n" +
                "  `UPDATE_NAME` VARCHAR(200),\n" +
                "  `PAY_BALANCE_STATUS` INT,\n" +
                "  `PAY_CONFIRM_STATUS` INT,\n" +
                "  `PAY_TRADE_NO` VARCHAR(80),\n" +
                "  `PRICE_ALL_FEE` DECIMAL(19, 2),\n" +
                "  `PRICE_BUILD_FEE` DECIMAL(19, 2),\n" +
                "  `PRICE_CABIN_PRICE` DECIMAL(19, 2),\n" +
                "  `PRICE_CURRENCY` INT,\n" +
                "  `PRICE_EXCHANGE_RATE` DECIMAL(10, 4),\n" +
                "  `PRICE_FUEL_FEE` DECIMAL(19, 2),\n" +
                "  `PRICE_OB_FEE` DECIMAL(19, 2),\n" +
                "  `PRICE_TICKET_PRICE` DECIMAL(19, 2),\n" +
                "  `PRICE_TOTAL_PRICE` DECIMAL(19, 2),\n" +
                "  `TICKET_NO` VARCHAR(80),\n" +
                "  `FARE_PKID` VARCHAR(1275),\n" +
                "  `ORDER_PKID` VARCHAR(1275),\n" +
                "  `IS_VOID` tinyint,  -- tinyint 转换为 BOOLEAN\n" +
                "  `BILL_TIME` TIMESTAMP(3),\n" +
                "  `IS_REGISTRATION` tinyint,\n" +
                "  `ETL_CREATE_TIME` TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                "  `ETL_UPDATE_TIME` TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                "  `ETL_DATE` date  NULL COMMENT '数据ETL日期'" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_KFBP_BUS_FARE_TICKET',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //") WITH (\n" +
        //"    'connector' = 'jdbc',\n" +
        //"    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
        //"    'table-name' = 'T_ODS_KFBP_BUS_FARE_TICKET', -- 替换为实际的表名\n" +
        //"    'username' = '"+Constants.ODS_USER+"',\n" +
        //"    'password' = '"+Constants.ODS_PWD+"'\n" +
        //",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
        //")");
        log.info(" ExtractTOdsKfbpBusFareTicket executeSql create table for doris end");
        String extractSql = "INSERT INTO T_ODS_KFBP_BUS_FARE_TICKET(\n" +
                " PKID,\n" +
                " CREATE_BY,\n" +
                " CREATE_DATE,\n" +
                " CREATE_NAME,\n" +
                " UPDATE_BY,\n" +
                " UPDATE_DATE,\n" +
                " UPDATE_NAME,\n" +
                " PAY_BALANCE_STATUS,\n" +
                " PAY_CONFIRM_STATUS,\n" +
                " PAY_TRADE_NO,\n" +
                " PRICE_ALL_FEE,\n" +
                " PRICE_BUILD_FEE,\n" +
                " PRICE_CABIN_PRICE,\n" +
                " PRICE_CURRENCY,\n" +
                " PRICE_EXCHANGE_RATE,\n" +
                " PRICE_FUEL_FEE,\n" +
                " PRICE_OB_FEE,\n" +
                " PRICE_TICKET_PRICE,\n" +
                " PRICE_TOTAL_PRICE,\n" +
                " TICKET_NO,\n" +
                " FARE_PKID,\n" +
                " ORDER_PKID,\n" +
                " IS_VOID,\n" +
                " BILL_TIME,\n" +
                " IS_REGISTRATION,\n" +
                " ETL_CREATE_TIME,\n" +
                " ETL_UPDATE_TIME,\n" +
                " ETL_DATE)" +

                "SELECT\n" +
                " PKID,\n" +
                " CREATE_BY,\n" +
                " CREATE_DATE,\n" +
                " CREATE_NAME,\n" +
                " UPDATE_BY,\n" +
                " UPDATE_DATE,\n" +
                " UPDATE_NAME,\n" +
                " PAY_BALANCE_STATUS,\n" +
                " PAY_CONFIRM_STATUS,\n" +
                " PAY_TRADE_NO,\n" +
                " PRICE_ALL_FEE,\n" +
                " PRICE_BUILD_FEE,\n" +
                " PRICE_CABIN_PRICE,\n" +
                " PRICE_CURRENCY,\n" +
                " PRICE_EXCHANGE_RATE,\n" +
                " PRICE_FUEL_FEE,\n" +
                " PRICE_OB_FEE,\n" +
                " PRICE_TICKET_PRICE,\n" +
                " PRICE_TOTAL_PRICE,\n" +
                " TICKET_NO,\n" +
                " FARE_PKID,\n" +
                " ORDER_PKID,\n" +
                " IS_VOID,\n" +
                " BILL_TIME,\n" +
                " IS_REGISTRATION,\n" +
                " CURRENT_TIMESTAMP AS ETL_CREATE_TIME,\n" +
                " CURRENT_TIMESTAMP AS ETL_UPDATE_TIME,\n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                " FROM BUS_FARE_TICKET "
                + " WHERE  CREATE_DATE BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  UPDATE_DATE BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;
        log.info(" ExtractTOdsKfbpBusFareTicket executeSql extract start");
        TableResult result = tEnv.executeSql(extractSql);
        log.info(" ExtractTOdsKfbpBusFareTicket executeSql extract end");

        result.print();

        log.info(" ExtractTOdsKfbpBusFareTicket data transfer end");


    }

}
