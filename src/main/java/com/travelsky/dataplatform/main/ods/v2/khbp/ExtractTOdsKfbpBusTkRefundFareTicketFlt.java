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
 * @date 2025/9/29 16:29
 */
public class ExtractTOdsKfbpBusTkRefundFareTicketFlt {
    public static Logger log = LoggerFactory.getLogger(ExtractTOdsKfbpBusTkRefundFareTicketFlt.class);
    public static void main(String[] args) throws IOException {
        log.info("ExtractTOdsKfbpBusTkRefundFareTicketFlt data transfer start");
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
        log.info("ExtractTOdsKfbpBusTkRefundFareTicketFlt etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsKfbpBusTkRefundFareTicketFlt");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        log.info("ExtractTOdsKfbpBusTkRefundFareTicketFlt executeSql create table for source start");
        tEnv.executeSql("CREATE TABLE `BUS_TK_REFUND_FARE_TICKET_FLT` (\n" +
                "    `PKID` VARCHAR(255),\n" +
                "    `CREATE_BY` VARCHAR(40),\n" +
                "    `CREATE_DATE` TIMESTAMP,\n" +
                "    `CREATE_NAME` VARCHAR(40),\n" +
                "    `UPDATE_BY` VARCHAR(40),\n" +
                "    `UPDATE_DATE` TIMESTAMP,\n" +
                "    `UPDATE_NAME` VARCHAR(40),\n" +
                "    `ARRIVE_CITY` VARCHAR(8),\n" +
                "    `CABIN` VARCHAR(4),\n" +
                "    `DEPARTURE_DAY` TIMESTAMP(3),\n" +
                "    `FARE_NAME` VARCHAR(255),\n" +
                "    `FLIGHT_NO` VARCHAR(8),\n" +
                "    `IS_ALL_SGM`  tinyint,\n" +
                "    `OPEN_TICKET_DATE` TIMESTAMP,\n" +
                "    `RATE` DECIMAL(19, 2),\n" +
                "    `RET_PRICE_ALL_FEE` DECIMAL(19, 2),\n" +
                "    `RET_PRICE_BUILD_FEE` DECIMAL(19, 2),\n" +
                "    `RET_PRICE_CABIN_PRICE` DECIMAL(19, 2),\n" +
                "    `RET_PRICE_CURRENCY` INT,       -- Convert NUMBER(10,0) to INT\n" +
                "    `RET_PRICE_EXCHANGE_RATE` DECIMAL(10, 4),\n" +
                "    `RET_PRICE_FUEL_FEE` DECIMAL(19, 2),\n" +
                "    `RET_PRICE_OB_FEE` DECIMAL(19, 2),\n" +
                "    `RET_PRICE_TICKET_PRICE` DECIMAL(19, 2),\n" +
                "    `RET_PRICE_TOTAL_PRICE` DECIMAL(19, 2),\n" +
                "    `SEGMENT_IDX` VARCHAR(255),\n" +
                "    `SEGMENT_STATUS` INT,         -- Convert NUMBER(10,0) to INT\n" +
                "    `START_CITY` VARCHAR(8),\n" +
                "    `TICKET_NO` VARCHAR(16),\n" +
                "    `FARE_TICKET_PKID` VARCHAR(255),\n" +
                "    `FLIGHT_PKID` VARCHAR(255),\n" +
                "    `TK_REFUND_PKID` VARCHAR(255),\n" +
                "    `REC_TOTAL_FEE` DECIMAL(19, 2),\n" +
                "    `REFUND_TOTAL_FEE` DECIMAL(19, 2),\n" +
                "    `REMARK` VARCHAR(1000),\n" +
                "    `TRFD_SN` VARCHAR(30),\n" +
                "    `OTHER_TAX_DETAIL` VARCHAR(500),\n" +
                "    `INSURE_ORDER_REFUND_PKID` VARCHAR(255),\n" +
                "    `PAY_CONFIRM_STATUS` INT,   -- Convert NUMBER(10,0) to INT\n" +
                "    `PAY_TRADE_NO` VARCHAR(16),\n" +
                "    `BILL_TIME` TIMESTAMP,\n" +
                "    `HIS_TICKET_NO` VARCHAR(500),\n" +
                "    `WRONG_MAC_FEE` DECIMAL(19, 2),\n" +
                "    `RET_TOTAL_PRICE_ALL_FEE` DECIMAL(19, 2),\n" +
                "    `RET_TOTAL_PRICE_BUILD_FEE` DECIMAL(19, 2),\n" +
                "    `RET_TOTAL_PRICE_CABIN_PRICE` DECIMAL(19, 2),\n" +
                "    `RET_TOTAL_PRICE_CURRENCY` INT,       -- Convert NUMBER(10,0) to INT\n" +
                "    `RET_TOTAL_PRICE_FUEL_FEE` DECIMAL(19, 2),\n" +
                "    `RET_TOTAL_PRICE_OB_FEE` DECIMAL(19, 2),\n" +
                "    `RET_TOTAL_PRICE_TICKET_PRICE` DECIMAL(19, 2),\n" +
                "    `RET_TOTAL_PRICE_TOTAL_PRICE` DECIMAL(19, 2),\n" +
                "    `RET_USE_PRICE_ALL_FEE` DECIMAL(19, 2),\n" +
                "    `RET_USE_PRICE_BUILD_FEE` DECIMAL(19, 2),\n" +
                "    `RET_USE_PRICE_CABIN_PRICE` DECIMAL(19, 2),\n" +
                "    `RET_USE_PRICE_CURRENCY` INT,       -- Convert NUMBER(10,0) to INT\n" +
                "    `RET_USE_PRICE_FUEL_FEE` DECIMAL(19, 2),\n" +
                "    `RET_USE_PRICE_OB_FEE` DECIMAL(19, 2),\n" +
                "    `RET_USE_PRICE_TICKET_PRICE` DECIMAL(19, 2),\n" +
                "    `RET_USE_PRICE_TOTAL_PRICE` DECIMAL(19, 2),\n" +
                "    `RET_TOTAL_TAX_DETAIL` VARCHAR(500),\n" +
                "    `RET_USE_TAX_DETAIL` VARCHAR(500),\n" +
                "    `XSFSI_INFO` VARCHAR(2000),\n" +
                "    `REC_AGENCY_FEE` DECIMAL(8, 2),\n" +
                "    `FARE_TYPE` INT" +

                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.KHBP_IP + ":" + Constants.KHBP_PORT + "/" + Constants.KHBP_DB + "',\n" +
                "    'table-name' = '" + Constants.KHBP_SCHEMA + ".BUS_TK_REFUND_FARE_TICKET_FLT', \n" +
                "    'username' = '" + Constants.KHBP_USER + "',\n" +
                "    'password' = '" + Constants.KHBP_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        log.info("ExtractTOdsKfbpBusTkRefundFareTicketFlt executeSql create table for source end");
        //创建Doris目标表
        log.info("ExtractTOdsKfbpBusTkRefundFareTicketFlt executeSql create table for doris start");
        tEnv.executeSql("CREATE TABLE `T_ODS_KFBP_BUS_TK_REFUND_FARE_TICKET_FLT` (\n" +
                "  `PKID` VARCHAR(1000) NOT NULL COMMENT '主键',\n" +
                "  `CREATE_BY` VARCHAR(200) COMMENT '创建人ID',\n" +
                "  `CREATE_DATE` TIMESTAMP(3) COMMENT '创建时间',\n" +
                "  `CREATE_NAME` VARCHAR(200) COMMENT '创建人名称',\n" +
                "  `UPDATE_BY` VARCHAR(200) COMMENT '修改人',\n" +
                "  `UPDATE_DATE` TIMESTAMP(3) COMMENT '修改时间',\n" +
                "  `UPDATE_NAME` VARCHAR(200) COMMENT '修改人姓名',\n" +
                "  `ARRIVE_CITY` VARCHAR(40) COMMENT '到达城市',\n" +
                "  `CABIN` VARCHAR(20) COMMENT '舱位',\n" +
                "  `DEPARTURE_DAY` TIMESTAMP(3) COMMENT '出发日期',\n" +
                "  `FARE_NAME` VARCHAR(1275) COMMENT '旅客姓名',\n" +
                "  `FLIGHT_NO` VARCHAR(40) COMMENT '航班号(市场方)',\n" +
                "  `IS_ALL_SGM`  tinyint COMMENT '是否选定了全部航段',\n" +
                "  `OPEN_TICKET_DATE` TIMESTAMP(3) COMMENT '出票日期',\n" +
                "  `RATE` DECIMAL(19, 2) COMMENT '手续费率',\n" +
                "  `RET_PRICE_ALL_FEE` DECIMAL(19, 2) COMMENT '总税',\n" +
                "  `RET_PRICE_BUILD_FEE` DECIMAL(19, 2) COMMENT '机场建设费',\n" +
                "  `RET_PRICE_CABIN_PRICE` DECIMAL(19, 2) COMMENT '舱位原价',\n" +
                "  `RET_PRICE_CURRENCY` INT COMMENT '支付币种',\n" +
                "  `RET_PRICE_EXCHANGE_RATE` DECIMAL(10, 4) COMMENT '汇率',\n" +
                "  `RET_PRICE_FUEL_FEE` DECIMAL(19, 2) COMMENT '机场燃油费',\n" +
                "  `RET_PRICE_OB_FEE` DECIMAL(19, 2) COMMENT '换开变更费',\n" +
                "  `RET_PRICE_TICKET_PRICE` DECIMAL(19, 2) COMMENT '机票价',\n" +
                "  `RET_PRICE_TOTAL_PRICE` DECIMAL(19, 2) COMMENT '总价',\n" +
                "  `SEGMENT_IDX` VARCHAR(1275) COMMENT '操作航段在detr中的序号',\n" +
                "  `SEGMENT_STATUS` INT COMMENT '客票航段状态',\n" +
                "  `START_CITY` VARCHAR(40) COMMENT '出发城市',\n" +
                "  `TICKET_NO` VARCHAR(80) COMMENT '票号',\n" +
                "  `FARE_TICKET_PKID` VARCHAR(1275) COMMENT '票号ID',\n" +
                "  `FLIGHT_PKID` VARCHAR(1275) COMMENT '航班ID',\n" +
                "  `TK_REFUND_PKID` VARCHAR(1275) COMMENT '退票ID',\n" +
                "  `REC_TOTAL_FEE` DECIMAL(19, 2) COMMENT '退票费',\n" +
                "  `REFUND_TOTAL_FEE` DECIMAL(19, 2) COMMENT '应退款',\n" +
                "  `REMARK` VARCHAR(1000) COMMENT '备注',\n" +
                "  `TRFD_SN` VARCHAR(150) COMMENT 'trfd退票单号',\n" +
                "  `OTHER_TAX_DETAIL` VARCHAR(2000) COMMENT '其他税费明细',\n" +
                "  `INSURE_ORDER_REFUND_PKID` VARCHAR(1275) COMMENT '保险订单退款ID',\n" +
                "  `PAY_CONFIRM_STATUS` INT COMMENT '开账状态',\n" +
                "  `PAY_TRADE_NO` VARCHAR(80) COMMENT '开账流水号',\n" +
                "  `BILL_TIME` TIMESTAMP(3) COMMENT '开账日期',\n" +
                "  `HIS_TICKET_NO` VARCHAR(2000) COMMENT '历史票号',\n" +
                "  `WRONG_MAC_FEE` DECIMAL(19, 2) COMMENT '误机费',\n" +
                "  `RET_TOTAL_PRICE_ALL_FEE` DECIMAL(19, 2) COMMENT '总税',\n" +
                "  `RET_TOTAL_PRICE_BUILD_FEE` DECIMAL(19, 2) COMMENT '机场建设费',\n" +
                "  `RET_TOTAL_PRICE_CABIN_PRICE` DECIMAL(19, 2) COMMENT '舱位原价',\n" +
                "  `RET_TOTAL_PRICE_CURRENCY` INT COMMENT '支付币种',\n" +
                "  `RET_TOTAL_PRICE_FUEL_FEE` DECIMAL(19, 2) COMMENT '机场燃油费',\n" +
                "  `RET_TOTAL_PRICE_OB_FEE` DECIMAL(19, 2) COMMENT '换开变更费',\n" +
                "  `RET_TOTAL_PRICE_TICKET_PRICE` DECIMAL(19, 2) COMMENT '机票价',\n" +
                "  `RET_TOTAL_PRICE_TOTAL_PRICE` DECIMAL(19, 2) COMMENT '总价',\n" +
                "  `RET_USE_PRICE_ALL_FEE` DECIMAL(19, 2) COMMENT '总税',\n" +
                "  `RET_USE_PRICE_BUILD_FEE` DECIMAL(19, 2) COMMENT '机场建设费',\n" +
                "  `RET_USE_PRICE_CABIN_PRICE` DECIMAL(19, 2) COMMENT '舱位原价',\n" +
                "  `RET_USE_PRICE_CURRENCY` INT COMMENT '支付币种',\n" +
                "  `RET_USE_PRICE_FUEL_FEE` DECIMAL(19, 2) COMMENT '机场燃油费',\n" +
                "  `RET_USE_PRICE_OB_FEE` DECIMAL(19, 2) COMMENT '换开变更费',\n" +
                "  `RET_USE_PRICE_TICKET_PRICE` DECIMAL(19, 2) COMMENT '机票价',\n" +
                "  `RET_USE_PRICE_TOTAL_PRICE` DECIMAL(19, 2) COMMENT '总价',\n" +
                "  `RET_TOTAL_TAX_DETAIL` VARCHAR(2000) COMMENT '总的其他税费明细',\n" +
                "  `RET_USE_TAX_DETAIL` VARCHAR(2000) COMMENT '已使用的其他税费明细',\n" +
                "  `XSFSI_INFO` VARCHAR(2000) COMMENT '已使用票面信息的报文',\n" +
                "  `REC_AGENCY_FEE` DECIMAL(8, 2) COMMENT '代理费',\n" +
                "  `FARE_TYPE` INT COMMENT '旅客类型',\n" +
                "  `EXCHANGED_IND` VARCHAR(50) COMMENT '所退机票是否为换开后机票Y是N否',\n" +
                "  `IRR_TICKET_NUMBER` VARCHAR(2000) COMMENT '航变机票号',\n" +
                "  `IRR_AFFECTED_TICKET_NUMBER` VARCHAR(2000) COMMENT '航变关联机票号',\n" +
                "  `ETL_CREATE_TIME` TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                "  `ETL_UPDATE_TIME` TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                "  `ETL_DATE` date  NULL COMMENT '数据ETL日期'" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_KFBP_BUS_TK_REFUND_FARE_TICKET_FLT',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //") WITH (\n" +
        //"    'connector' = 'jdbc',\n" +
        //"    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
        //"    'table-name' = 'T_ODS_KFBP_BUS_TK_REFUND_FARE_TICKET_FLT', -- 替换为实际的表名\n" +
        //"    'username' = '"+Constants.ODS_USER+"',\n" +
        //"    'password' = '"+Constants.ODS_PWD+"'\n" +
        //",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
        //")");
        log.info("ExtractTOdsKfbpBusTkRefundFareTicketFlt executeSql create table for doris end");
        String extractSql = "INSERT INTO `T_ODS_KFBP_BUS_TK_REFUND_FARE_TICKET_FLT` \n" +
                " (`PKID`, \n" +
                " `CREATE_BY`, \n" +
                " `CREATE_DATE`, \n" +
                " `CREATE_NAME`, \n" +
                " `UPDATE_BY`, \n" +
                " `UPDATE_DATE`, \n" +
                " `UPDATE_NAME`, \n" +
                " `ARRIVE_CITY`, \n" +
                " `CABIN`, \n" +
                " `DEPARTURE_DAY`, \n" +
                " `FARE_NAME`, \n" +
                " `FLIGHT_NO`, \n" +
                " `IS_ALL_SGM`, \n" +
                " `OPEN_TICKET_DATE`, \n" +
                " `RATE`, \n" +
                " `RET_PRICE_ALL_FEE`, \n" +
                " `RET_PRICE_BUILD_FEE`, \n" +
                " `RET_PRICE_CABIN_PRICE`, \n" +
                " `RET_PRICE_CURRENCY`, \n" +
                " `RET_PRICE_EXCHANGE_RATE`, \n" +
                " `RET_PRICE_FUEL_FEE`, \n" +
                " `RET_PRICE_OB_FEE`, \n" +
                " `RET_PRICE_TICKET_PRICE`, \n" +
                " `RET_PRICE_TOTAL_PRICE`, \n" +
                " `SEGMENT_IDX`, \n" +
                " `SEGMENT_STATUS`, \n" +
                " `START_CITY`, \n" +
                " `TICKET_NO`, \n" +
                " `FARE_TICKET_PKID`, \n" +
                " `FLIGHT_PKID`, \n" +
                " `TK_REFUND_PKID`, \n" +
                " `REC_TOTAL_FEE`, \n" +
                " `REFUND_TOTAL_FEE`, \n" +
                " `REMARK`, \n" +
                " `TRFD_SN`, \n" +
                " `OTHER_TAX_DETAIL`, \n" +
                " `INSURE_ORDER_REFUND_PKID`, \n" +
                " `PAY_CONFIRM_STATUS`, \n" +
                " `PAY_TRADE_NO`, \n" +
                " `BILL_TIME`, \n" +
                " `HIS_TICKET_NO`, \n" +
                " `WRONG_MAC_FEE`, \n" +
                " `RET_TOTAL_PRICE_ALL_FEE`, \n" +
                " `RET_TOTAL_PRICE_BUILD_FEE`, \n" +
                " `RET_TOTAL_PRICE_CABIN_PRICE`, \n" +
                " `RET_TOTAL_PRICE_CURRENCY`, \n" +
                " `RET_TOTAL_PRICE_FUEL_FEE`, \n" +
                " `RET_TOTAL_PRICE_OB_FEE`, \n" +
                " `RET_TOTAL_PRICE_TICKET_PRICE`, \n" +
                " `RET_TOTAL_PRICE_TOTAL_PRICE`, \n" +
                " `RET_USE_PRICE_ALL_FEE`, \n" +
                " `RET_USE_PRICE_BUILD_FEE`, \n" +
                " `RET_USE_PRICE_CABIN_PRICE`, \n" +
                " `RET_USE_PRICE_CURRENCY`, \n" +
                " `RET_USE_PRICE_FUEL_FEE`, \n" +
                " `RET_USE_PRICE_OB_FEE`, \n" +
                " `RET_USE_PRICE_TICKET_PRICE`, \n" +
                " `RET_USE_PRICE_TOTAL_PRICE`, \n" +
                " `RET_TOTAL_TAX_DETAIL`, \n" +
                " `RET_USE_TAX_DETAIL`, \n" +
                " `XSFSI_INFO`, \n" +
                " `REC_AGENCY_FEE`, \n" +
                " `FARE_TYPE`, \n" +
                " `ETL_CREATE_TIME`, \n" +
                " `ETL_UPDATE_TIME`, \n" +
                " `ETL_DATE`)" +

                "SELECT\n" +
                " PKID,\n" +
                " CREATE_BY,\n" +
                " CREATE_DATE,\n" +
                " CREATE_NAME,\n" +
                " UPDATE_BY,\n" +
                " UPDATE_DATE,\n" +
                " UPDATE_NAME,\n" +
                " ARRIVE_CITY,\n" +
                " CABIN,\n" +
                " DEPARTURE_DAY,\n" +
                " FARE_NAME,\n" +
                " FLIGHT_NO,\n" +
                " IS_ALL_SGM,\n" +
                " OPEN_TICKET_DATE,\n" +
                " RATE,\n" +
                " RET_PRICE_ALL_FEE,\n" +
                " RET_PRICE_BUILD_FEE,\n" +
                " RET_PRICE_CABIN_PRICE,\n" +
                " RET_PRICE_CURRENCY,\n" +
                " RET_PRICE_EXCHANGE_RATE,\n" +
                " RET_PRICE_FUEL_FEE,\n" +
                " RET_PRICE_OB_FEE,\n" +
                " RET_PRICE_TICKET_PRICE,\n" +
                " RET_PRICE_TOTAL_PRICE,\n" +
                " SEGMENT_IDX,\n" +
                " SEGMENT_STATUS,\n" +
                " START_CITY,\n" +
                " TICKET_NO,\n" +
                " FARE_TICKET_PKID,\n" +
                " FLIGHT_PKID,\n" +
                " TK_REFUND_PKID,\n" +
                " REC_TOTAL_FEE,\n" +
                " REFUND_TOTAL_FEE,\n" +
                " REMARK,\n" +
                " TRFD_SN,\n" +
                " OTHER_TAX_DETAIL,\n" +
                " INSURE_ORDER_REFUND_PKID,\n" +
                " PAY_CONFIRM_STATUS,\n" +
                " PAY_TRADE_NO,\n" +
                " BILL_TIME,\n" +
                " HIS_TICKET_NO,\n" +
                " WRONG_MAC_FEE,\n" +
                " RET_TOTAL_PRICE_ALL_FEE,\n" +
                " RET_TOTAL_PRICE_BUILD_FEE,\n" +
                " RET_TOTAL_PRICE_CABIN_PRICE,\n" +
                " RET_TOTAL_PRICE_CURRENCY,\n" +
                " RET_TOTAL_PRICE_FUEL_FEE,\n" +
                " RET_TOTAL_PRICE_OB_FEE,\n" +
                " RET_TOTAL_PRICE_TICKET_PRICE,\n" +
                " RET_TOTAL_PRICE_TOTAL_PRICE,\n" +
                " RET_USE_PRICE_ALL_FEE,\n" +
                " RET_USE_PRICE_BUILD_FEE,\n" +
                " RET_USE_PRICE_CABIN_PRICE,\n" +
                " RET_USE_PRICE_CURRENCY,\n" +
                " RET_USE_PRICE_FUEL_FEE,\n" +
                " RET_USE_PRICE_OB_FEE,\n" +
                " RET_USE_PRICE_TICKET_PRICE,\n" +
                " RET_USE_PRICE_TOTAL_PRICE,\n" +
                " RET_TOTAL_TAX_DETAIL,\n" +
                " RET_USE_TAX_DETAIL,\n" +
                " XSFSI_INFO,\n" +
                " REC_AGENCY_FEE,\n" +
                " FARE_TYPE,\n" +
                " CURRENT_TIMESTAMP AS ETL_CREATE_TIME,\n" +
                " CURRENT_TIMESTAMP AS ETL_UPDATE_TIME,\n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                " FROM BUS_TK_REFUND_FARE_TICKET_FLT "
                + " WHERE  CREATE_DATE BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  UPDATE_DATE BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;
        log.info("ExtractTOdsKfbpBusTkRefundFareTicketFlt executeSql extract start");
        TableResult result = tEnv.executeSql(extractSql);
        log.info("ExtractTOdsKfbpBusTkRefundFareTicketFlt executeSql extract end");

        result.print();

        log.info("ExtractTOdsKfbpBusTkRefundFareTicketFlt data transfer end");


    }

}
