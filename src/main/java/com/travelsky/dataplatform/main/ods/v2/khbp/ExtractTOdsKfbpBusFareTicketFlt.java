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
public class  ExtractTOdsKfbpBusFareTicketFlt {
    public static Logger log = LoggerFactory.getLogger(ExtractTOdsKfbpBusFareTicketFlt.class);
    public static void main(String[] args) throws IOException {
        log.info("  ExtractTOdsKfbpBusFareTicketFlt data transfer start");
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
        CheckpointUtils.setCheckpoint(env, "  ExtractTOdsKfbpBusFareTicketFlt");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        log.info("  ExtractTOdsKfbpBusFareTicketFlt executeSql create table for source start");
        tEnv.executeSql("CREATE TABLE BUS_FARE_TICKET_FLT (\n" +
                "    `PKID` VARCHAR(255) NOT NULL,\n" +
                "    `CREATE_BY` VARCHAR(40),\n" +
                "    `CREATE_DATE` TIMESTAMP,\n" +
                "    `CREATE_NAME` VARCHAR(40),\n" +
                "    `UPDATE_BY` VARCHAR(40),\n" +
                "    `UPDATE_DATE` TIMESTAMP,\n" +
                "    `UPDATE_NAME` VARCHAR(40),\n" +
                "    `COUPON_NUMER` INT,\n" +
                "    `FLT_ACTION_CODE` VARCHAR(6),\n" +
                "    `FLT_AIRPLANE_TYPE` VARCHAR(8),\n" +
                "    `FLT_ARRIVAL_TERMINAL` VARCHAR(4),\n" +
                "    `FLT_ARRIVE_CITY` VARCHAR(8),\n" +
                "    `FLT_ARRIVE_DAY` TIMESTAMP(3),\n" +
                "    `FLT_ARRIVE_TIME` VARCHAR(4),\n" +
                "    `FLT_CABIN` VARCHAR(4),\n" +
                "    `FLT_DEPARTURE_DAY` TIMESTAMP(3),\n" +
                "    `FLT_DEPARTURE_TERMINAL` VARCHAR(4),\n" +
                "    `FLT_DEPARTURE_TIME` VARCHAR(4),\n" +
                "    `FLT_FLIGHT_NO` VARCHAR(8),\n" +
                "    `FLT_IS_ARNK`   tinyINT,\n" +
                "    `FLT_IS_SHARE_FLIGHT`   tinyINT,\n" +
                "    `FLT_REAL_FLIGHT_NO` VARCHAR(8),\n" +
                "    `FLT_SEGMENT_STATUS` INT,\n" +
                "    `FLT_START_CITY` VARCHAR(8),\n" +
                "    `PRICE_ALL_FEE` DECIMAL(19, 2),\n" +
                "    `PRICE_BUILD_FEE` DECIMAL(19, 2),\n" +
                "    `PRICE_CABIN_PRICE` DECIMAL(19, 2),\n" +
                "    `PRICE_CURRENCY` INT,\n" +
                "    `PRICE_EXCHANGE_RATE` DECIMAL(10, 4),\n" +
                "    `PRICE_FUEL_FEE` DECIMAL(19, 2),\n" +
                "    `PRICE_OB_FEE` DECIMAL(19, 2),\n" +
                "    `PRICE_TICKET_PRICE` DECIMAL(19, 2),\n" +
                "    `PRICE_TOTAL_PRICE` DECIMAL(19, 2),\n" +
                "    `TRAVELLED_DAY` TIMESTAMP(3),\n" +
                "    `TRAVELLED_STATUE` INT,\n" +
                "    `FARE_PKID` VARCHAR(255),\n" +
                "    `FARE_TICKET_PKID` VARCHAR(255),\n" +
                "    `ORDER_PKID` VARCHAR(255),\n" +
                "    `FLT_INDEX_IN_PNR` VARCHAR(3),\n" +
                "    `FLIGHT_PKID` VARCHAR(255),\n" +
                "    `IS_PRICE_IGNORE`   tinyINT,\n" +
                "    `INSURE_EXPECT_POLICY_NO` VARCHAR(255),\n" +
                "    `INSURE_EXPECT_SERIAL_NO` VARCHAR(255),\n" +
                "    `INSURE_EXPECT_STATUS` INT,\n" +
                "    `INSURE_DELAY_SERIAL_NO` VARCHAR(255),\n" +
                "    `INSURE_DELAY_POLICY_NO` VARCHAR(255),\n" +
                "    `INSURE_DELAY_STATUS` INT,\n" +
                "    `INSURE_ALL_RISKS_SERIAL_NO` VARCHAR(255),\n" +
                "    `INSURE_ALL_RISKS_POLICY_NO` VARCHAR(255),\n" +
                "    `INSURE_ALL_RISKS_STATUS` INT,\n" +
                "    `TICKET_NO` VARCHAR(16)" +

                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.KHBP_IP + ":" + Constants.KHBP_PORT + "/" + Constants.KHBP_DB + "',\n" +
                "    'table-name' = '" + Constants.KHBP_SCHEMA + ".BUS_FARE_TICKET_FLT', \n" +
                "    'username' = '" + Constants.KHBP_USER + "',\n" +
                "    'password' = '" + Constants.KHBP_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        log.info("  ExtractTOdsKfbpBusFareTicketFlt executeSql create table for source end");
        //创建Doris目标表
        log.info("  ExtractTOdsKfbpBusFareTicketFlt executeSql create table for doris start");
        tEnv.executeSql("CREATE TABLE `T_ODS_KFBP_BUS_FARE_TICKET_FLT` (\n" +
                "  `PKID` VARCHAR(1000) NOT NULL COMMENT '主键',\n" +
                "  `CREATE_BY` VARCHAR(200) COMMENT '创建人ID',\n" +
                "  `CREATE_DATE` TIMESTAMP(3) COMMENT '创建时间',\n" +
                "  `CREATE_NAME` VARCHAR(200) COMMENT '创建人名称',\n" +
                "  `UPDATE_BY` VARCHAR(200) COMMENT '修改人',\n" +
                "  `UPDATE_DATE` TIMESTAMP(3) COMMENT '修改时间',\n" +
                "  `UPDATE_NAME` VARCHAR(200) COMMENT '修改人姓名',\n" +
                "  `COUPON_NUMER` INT COMMENT '票联',\n" +
                "  `FLT_ACTION_CODE` VARCHAR(30) COMMENT 'PNR航段状态',\n" +
                "  `FLT_AIRPLANE_TYPE` VARCHAR(40) COMMENT '机型',\n" +
                "  `FLT_ARRIVAL_TERMINAL` VARCHAR(20) COMMENT '到达航站楼',\n" +
                "  `FLT_ARRIVE_CITY` VARCHAR(40) COMMENT '到达城市',\n" +
                "  `FLT_ARRIVE_DAY` TIMESTAMP(3) COMMENT '到达日期',\n" +
                "  `FLT_ARRIVE_TIME` VARCHAR(20) COMMENT '到达时刻',\n" +
                "  `FLT_CABIN` VARCHAR(20) COMMENT '舱位',\n" +
                "  `FLT_DEPARTURE_DAY` TIMESTAMP(3) COMMENT '出发日期',\n" +
                "  `FLT_DEPARTURE_TERMINAL` VARCHAR(20) COMMENT '出发航站楼',\n" +
                "  `FLT_DEPARTURE_TIME` VARCHAR(20) COMMENT '出发时刻',\n" +
                "  `FLT_FLIGHT_NO` VARCHAR(40) COMMENT '航班号(市场方)',\n" +
                "  `FLT_IS_ARNK`   tinyINT COMMENT '是否缺口段',\n" +
                "  `FLT_IS_SHARE_FLIGHT`   tinyINT COMMENT '是否共享航班',\n" +
                "  `FLT_REAL_FLIGHT_NO` VARCHAR(40) COMMENT '航班号(承运方)',\n" +
                "  `FLT_SEGMENT_STATUS` INT COMMENT '客票航段状态',\n" +
                "  `FLT_START_CITY` VARCHAR(40) COMMENT '出发城市',\n" +
                "  `PRICE_ALL_FEE` DECIMAL(19, 2) COMMENT '总税',\n" +
                "  `PRICE_BUILD_FEE` DECIMAL(19, 2) COMMENT '机场建设费',\n" +
                "  `PRICE_CABIN_PRICE` DECIMAL(19, 2) COMMENT '舱位原价',\n" +
                "  `PRICE_CURRENCY` INT COMMENT '支付币种',\n" +
                "  `PRICE_EXCHANGE_RATE` DECIMAL(10, 4) COMMENT '汇率',\n" +
                "  `PRICE_FUEL_FEE` DECIMAL(19, 2) COMMENT '机场燃油费',\n" +
                "  `PRICE_OB_FEE` DECIMAL(19, 2) COMMENT '换开变更费',\n" +
                "  `PRICE_TICKET_PRICE` DECIMAL(19, 2) COMMENT '机票价',\n" +
                "  `PRICE_TOTAL_PRICE` DECIMAL(19, 2) COMMENT '总价',\n" +
                "  `TRAVELLED_DAY` TIMESTAMP(3) COMMENT '成行日期',\n" +
                "  `TRAVELLED_STATUE` INT COMMENT '成行状态',\n" +
                "  `FARE_PKID` VARCHAR(1275) COMMENT '旅客ID',\n" +
                "  `FARE_TICKET_PKID` VARCHAR(1275) COMMENT '票号ID',\n" +
                "  `ORDER_PKID` VARCHAR(1275) COMMENT '订单ID',\n" +
                "  `FLT_INDEX_IN_PNR` VARCHAR(15) COMMENT '在pnr中的序号',\n" +
                "  `FLIGHT_PKID` VARCHAR(1275) COMMENT '航班表外键',\n" +
                "  `IS_PRICE_IGNORE`   tinyINT COMMENT '该票号航段下的价格忽略（可能全算入其他航段）',\n" +
                "  `INSURE_EXPECT_POLICY_NO` VARCHAR(1275) COMMENT '航意险保单号',\n" +
                "  `INSURE_EXPECT_SERIAL_NO` VARCHAR(1275) COMMENT '航意险序列号',\n" +
                "  `INSURE_EXPECT_STATUS` INT COMMENT '航意险保险状态',\n" +
                "  `INSURE_DELAY_SERIAL_NO` VARCHAR(1275) COMMENT '航延险序列号',\n" +
                "  `INSURE_DELAY_POLICY_NO` VARCHAR(1275) COMMENT '航延险保单号',\n" +
                "  `INSURE_DELAY_STATUS` INT COMMENT '航延险状态',\n" +
                "  `INSURE_ALL_RISKS_SERIAL_NO` VARCHAR(1275) COMMENT '综合险序列号',\n" +
                "  `INSURE_ALL_RISKS_POLICY_NO` VARCHAR(1275) COMMENT '综合险保单号',\n" +
                "  `INSURE_ALL_RISKS_STATUS` INT COMMENT '综合险状态',\n" +
                "  `TICKET_NO` VARCHAR(80) COMMENT '票号',\n" +
                "  `ETL_CREATE_TIME` TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                "  `ETL_UPDATE_TIME` TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                "  `ETL_DATE` date  NULL COMMENT '数据ETL日期'" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_KFBP_BUS_FARE_TICKET_FLT',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //") WITH (\n" +
        //"    'connector' = 'jdbc',\n" +
        //"    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
        //"    'table-name' = 'T_ODS_KFBP_BUS_FARE_TICKET_FLT', -- 替换为实际的表名\n" +
        //"    'username' = '"+Constants.ODS_USER+"',\n" +
        //"    'password' = '"+Constants.ODS_PWD+"'\n" +
        //",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
        //")");
        log.info("  ExtractTOdsKfbpBusFareTicketFlt executeSql create table for doris end");
        String extractSql = "INSERT INTO T_ODS_KFBP_BUS_FARE_TICKET_FLT(\n" +
                "  PKID,\n" +
                "  CREATE_BY,\n" +
                "  CREATE_DATE,\n" +
                "  CREATE_NAME,\n" +
                "  UPDATE_BY,\n" +
                "  UPDATE_DATE,\n" +
                "  UPDATE_NAME,\n" +
                "  COUPON_NUMER,\n" +
                "  FLT_ACTION_CODE,\n" +
                "  FLT_AIRPLANE_TYPE,\n" +
                "  FLT_ARRIVAL_TERMINAL,\n" +
                "  FLT_ARRIVE_CITY,\n" +
                "  FLT_ARRIVE_DAY,\n" +
                "  FLT_ARRIVE_TIME,\n" +
                "  FLT_CABIN,\n" +
                "  FLT_DEPARTURE_DAY,\n" +
                "  FLT_DEPARTURE_TERMINAL,\n" +
                "  FLT_DEPARTURE_TIME,\n" +
                "  FLT_FLIGHT_NO,\n" +
                "  FLT_IS_ARNK,\n" +
                "  FLT_IS_SHARE_FLIGHT,\n" +
                "  FLT_REAL_FLIGHT_NO,\n" +
                "  FLT_SEGMENT_STATUS,\n" +
                "  FLT_START_CITY,\n" +
                "  PRICE_ALL_FEE,\n" +
                "  PRICE_BUILD_FEE,\n" +
                "  PRICE_CABIN_PRICE,\n" +
                "  PRICE_CURRENCY,\n" +
                "  PRICE_EXCHANGE_RATE,\n" +
                "  PRICE_FUEL_FEE,\n" +
                "  PRICE_OB_FEE,\n" +
                "  PRICE_TICKET_PRICE,\n" +
                "  PRICE_TOTAL_PRICE,\n" +
                "  TRAVELLED_DAY,\n" +
                "  TRAVELLED_STATUE,\n" +
                "  FARE_PKID,\n" +
                "  FARE_TICKET_PKID,\n" +
                "  ORDER_PKID,\n" +
                "  FLT_INDEX_IN_PNR,\n" +
                "  FLIGHT_PKID,\n" +
                "  IS_PRICE_IGNORE,\n" +
                "  INSURE_EXPECT_POLICY_NO,\n" +
                "  INSURE_EXPECT_SERIAL_NO,\n" +
                "  INSURE_EXPECT_STATUS,\n" +
                "  INSURE_DELAY_SERIAL_NO,\n" +
                "  INSURE_DELAY_POLICY_NO,\n" +
                "  INSURE_DELAY_STATUS,\n" +
                "  INSURE_ALL_RISKS_SERIAL_NO,\n" +
                "  INSURE_ALL_RISKS_POLICY_NO,\n" +
                "  INSURE_ALL_RISKS_STATUS,\n" +
                "  TICKET_NO,\n" +
                "  ETL_CREATE_TIME,\n" +
                "  ETL_UPDATE_TIME,\n" +
                "  ETL_DATE)" +

                "SELECT\n" +
                "  PKID,\n" +
                "  CREATE_BY,\n" +
                "  CREATE_DATE,\n" +
                "  CREATE_NAME,\n" +
                "  UPDATE_BY,\n" +
                "  UPDATE_DATE,\n" +
                "  UPDATE_NAME,\n" +
                "  COUPON_NUMER,\n" +
                "  FLT_ACTION_CODE,\n" +
                "  FLT_AIRPLANE_TYPE,\n" +
                "  FLT_ARRIVAL_TERMINAL,\n" +
                "  FLT_ARRIVE_CITY,\n" +
                "  FLT_ARRIVE_DAY,\n" +
                "  FLT_ARRIVE_TIME,\n" +
                "  FLT_CABIN,\n" +
                "  FLT_DEPARTURE_DAY,\n" +
                "  FLT_DEPARTURE_TERMINAL,\n" +
                "  FLT_DEPARTURE_TIME,\n" +
                "  FLT_FLIGHT_NO,\n" +
                "  FLT_IS_ARNK,\n" +
                "  FLT_IS_SHARE_FLIGHT,\n" +
                "  FLT_REAL_FLIGHT_NO,\n" +
                "  FLT_SEGMENT_STATUS,\n" +
                "  FLT_START_CITY,\n" +
                "  PRICE_ALL_FEE,\n" +
                "  PRICE_BUILD_FEE,\n" +
                "  PRICE_CABIN_PRICE,\n" +
                "  PRICE_CURRENCY,\n" +
                "  PRICE_EXCHANGE_RATE,\n" +
                "  PRICE_FUEL_FEE,\n" +
                "  PRICE_OB_FEE,\n" +
                "  PRICE_TICKET_PRICE,\n" +
                "  PRICE_TOTAL_PRICE,\n" +
                "  TRAVELLED_DAY,\n" +
                "  TRAVELLED_STATUE,\n" +
                "  FARE_PKID,\n" +
                "  FARE_TICKET_PKID,\n" +
                "  ORDER_PKID,\n" +
                "  FLT_INDEX_IN_PNR,\n" +
                "  FLIGHT_PKID,\n" +
                "  IS_PRICE_IGNORE,\n" +
                "  INSURE_EXPECT_POLICY_NO,\n" +
                "  INSURE_EXPECT_SERIAL_NO,\n" +
                "  INSURE_EXPECT_STATUS,\n" +
                "  INSURE_DELAY_SERIAL_NO,\n" +
                "  INSURE_DELAY_POLICY_NO,\n" +
                "  INSURE_DELAY_STATUS,\n" +
                "  INSURE_ALL_RISKS_SERIAL_NO,\n" +
                "  INSURE_ALL_RISKS_POLICY_NO,\n" +
                "  INSURE_ALL_RISKS_STATUS,\n" +
                "  TICKET_NO,\n" +
                " CURRENT_TIMESTAMP AS ETL_CREATE_TIME,\n" +
                " CURRENT_TIMESTAMP AS ETL_UPDATE_TIME,\n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                " FROM BUS_FARE_TICKET_FLT "
                + " WHERE  CREATE_DATE BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  UPDATE_DATE BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;
        log.info("  ExtractTOdsKfbpBusFareTicketFlt executeSql extract start");
        TableResult result = tEnv.executeSql(extractSql);
        log.info("  ExtractTOdsKfbpBusFareTicketFlt executeSql extract end");

        result.print();

        log.info("  ExtractTOdsKfbpBusFareTicketFlt data transfer end");


    }

}
