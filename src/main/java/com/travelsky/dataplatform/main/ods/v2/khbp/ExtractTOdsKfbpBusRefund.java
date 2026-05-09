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
public class ExtractTOdsKfbpBusRefund {
    public static Logger log = LoggerFactory.getLogger(ExtractTOdsKfbpBusRefund.class);
    public static void main(String[] args) throws IOException {
        log.info("ExtractTOdsKfbpBusRefund data transfer start");
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
        log.info("ExtractTOdsKfbpBusRefund etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsKfbpBusRefund");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        log.info("ExtractTOdsKfbpBusRefund executeSql create table for source start");
        tEnv.executeSql("CREATE TABLE BUS_REFUND (\n" +
                "    REFUNDTYPE VARCHAR(31),\n" +
                "    PKID VARCHAR(255),\n" +
                "    CREATE_BY VARCHAR(40),\n" +
                "    CREATE_DATE TIMESTAMP,\n" +
                "    CREATE_NAME VARCHAR(40),\n" +
                "    UPDATE_BY VARCHAR(40),\n" +
                "    UPDATE_DATE TIMESTAMP,\n" +
                "    UPDATE_NAME VARCHAR(40),\n" +
                "    AUDIT_MEMO VARCHAR(2000),\n" +
                "    AUDITING_THROUGH_DATE TIMESTAMP,\n" +
                "    CONTACT_EMAIL VARCHAR(64),\n" +
                "    CONTACT_ID_CODE VARCHAR(255),\n" +
                "    CONTACT_MOBILE VARCHAR(32),\n" +
                "    CONTACT_NAME VARCHAR(32),\n" +
                "    CONTACT_PHONE VARCHAR(32),\n" +
                "    COURIER_NUMBER VARCHAR(64),\n" +
                "    CUR_TEL VARCHAR(20),\n" +
                "    DATA_COMPLETE INT,\n" +
                "    MEMO VARCHAR(2000),\n" +
                "    NEED_REFUND_AMOUT DECIMAL(10, 2),\n" +
                "    REASON VARCHAR(255),\n" +
                "    REFUND_DATA_FLAG INT,\n" +
                "    REFUND_DATA_TYPE VARCHAR(255),\n" +
                "    REFUND_FEE DECIMAL(10, 2),\n" +
                "    REFUND_SN VARCHAR(255),\n" +
                "    REFUND_STATUS INT,\n" +
                "    REFUNDEDMENT_DATE TIMESTAMP,\n" +
                "    REFUNDMENT_STATUS INT,\n" +
                "    SALE_CHANNEL INT,\n" +
                "    SUTMIT_USER_INFO_BUS_DEPT_ID INT,\n" +
                "    SUTMIT_USER_INFO_DEPT_ID INT,\n" +
                "    SUTMIT_USER_INFO_LOGIN VARCHAR(255),\n" +
                "    SUTMIT_USER_INFO_NAME VARCHAR(255),\n" +
                "    RET_PRICE_ALL_FEE DECIMAL(19, 2),\n" +
                "    RET_PRICE_BUILD_FEE DECIMAL(19, 2),\n" +
                "    RET_PRICE_CABIN_PRICE DECIMAL(19, 2),\n" +
                "    RET_PRICE_CURRENCY INT,\n" +
                "    RET_PRICE_EXCHANGE_RATE DECIMAL(10, 4),\n" +
                "    RET_PRICE_FUEL_FEE DECIMAL(19, 2),\n" +
                "    RET_PRICE_OB_FEE DECIMAL(19, 2),\n" +
                "    RET_PRICE_TICKET_PRICE DECIMAL(19, 2),\n" +
                "    RET_PRICE_TOTAL_PRICE DECIMAL(19, 2),\n" +
                "    AUDIT_USER_USER_ID INT,\n" +
                "    ORDER_PKID VARCHAR(255),\n" +
                "    SUBMIT_USER_USER_ID INT,\n" +
                "    TK_ORDER_PKID VARCHAR(255),\n" +
                "    DATA_PRINT INT,\n" +
                "    MAIL_MATERIAL VARCHAR(255),\n" +
                "    SHIP_ADDRESS VARCHAR(64),\n" +
                "    SHIP_MEMO VARCHAR(2000),\n" +
                "    SHIP_MOBILE VARCHAR(16),\n" +
                "    SHIP_NAME VARCHAR(40),\n" +
                "    SHIP_PHONE VARCHAR(16),\n" +
                "    SHIP_SEND_DATE TIMESTAMP(3),\n" +
                "    SHIP_SEND_TIME VARCHAR(16),\n" +
                "    SHIP_SHIP_MAIL_TYPE INT,\n" +
                "    SHIP_SHIP_STATUS INT,\n" +
                "    SHIP_SHIP_TYPE INT,\n" +
                "    SHIP_ZIP_CODE VARCHAR(8),\n" +
                "    SHIP_DEPT_GROUP_ID INT,\n" +
                "    REFUND_TRFD_STATUS INT,\n" +
                "    INSURE_ORDER_PKID VARCHAR(255),\n" +
                "    SHIP_IS_URGENCY  tinyint,\n" +
                "    TK_REFUND_FARE_TICKET_FLT_PKID VARCHAR(255),\n" +
                "    REFUND_MAIL_INVOICE_TITLE VARCHAR(30),\n" +
                "    REFUND_MAIL_REFUND VARCHAR(20),\n" +
                "    REFUND_MAIL_REFUNDABLE VARCHAR(20),\n" +
                "    REFUND_MAIL_ISSUE_TICKET VARCHAR(10),\n" +
                "    REFUND_MAIL_DRAWER VARCHAR(10),\n" +
                "    REFUND_MAIL_AGGREGATE_AMOUNT VARCHAR(20),\n" +
                "    REFUND_MEMO VARCHAR(2000),\n" +
                "    OTHER_FLAG  tinyint,\n" +
                "    VOLUNTARILY_FLAG  tinyint,\n" +
                "    REFUND_MAIL_TAXPAYER_NO VARCHAR(20),\n" +
                "    REFUND_MAIL_INVOICE_TYPE INT,\n" +
                "    SHIP_EMS_MAIL_TYPE INT,\n" +
                "    SHIP_EMSREMARK VARCHAR(2000),\n" +
                "    REFUND_MAIL_BUILD_FEE VARCHAR(20),\n" +
                "    REFUND_MAIL_FUEL_FEE VARCHAR(20),\n" +
                "    REFUND_MAIL_INSURE_FEE VARCHAR(20),\n" +
                "    OPER_FLAG  tinyint,\n" +
                "    OFFLINE_TICKET_REFUND  tinyint,\n" +
                "    REFUND_MARK_STATUS  tinyint,\n" +
                "    REFUND_AGENCY_FEE DECIMAL(10, 2),\n" +
                "    REFUND_CATEGORY SMALLINT,\n" +
                "    TICKETNO_OR_ORDERNO VARCHAR(1000),\n" +
                "    CONTACT_BIRTH DATE,\n" +
                "    OCRP_ID VARCHAR(100),\n" +
                "    BUSINESS_TYPE VARCHAR(10),\n" +
                "    DI_IND VARCHAR(10),\n" +
                "    REFUND_TICKET_NUMBER VARCHAR(1000),\n" +
                "    SUPPLY_FLAG  tinyint,\n" +
                "    CONNECT_NUMBER VARCHAR(1000),\n" +
                "    REFUND_PNR VARCHAR(100),\n" +
                "    EXCHANGED_IND VARCHAR(10),\n" +
                "    IRR_TICKET_NUMBER VARCHAR(500),\n" +
                "    IRR_AFFECTED_TICKET_NUMBER VARCHAR(500)" +

                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.KHBP_IP + ":" + Constants.KHBP_PORT + "/" + Constants.KHBP_DB + "',\n" +
                "    'table-name' = '" + Constants.KHBP_SCHEMA + ".BUS_REFUND', \n" +
                "    'username' = '" + Constants.KHBP_USER + "',\n" +
                "    'password' = '" + Constants.KHBP_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        log.info("ExtractTOdsKfbpBusRefund executeSql create table for source end");
        //创建Doris目标表
        log.info("ExtractTOdsKfbpBusRefund executeSql create table for doris start");
        tEnv.executeSql("CREATE TABLE `T_ODS_KFBP_BUS_REFUND` (\n" +
                "    `PKID` VARCHAR(1000) NOT NULL COMMENT '主键',\n" +
                "    `REFUNDTYPE` VARCHAR(155) COMMENT '退票类型',\n" +
                "    `CREATE_BY` VARCHAR(200) COMMENT '创建人ID',\n" +
                "    `CREATE_DATE` TIMESTAMP(3) COMMENT '创建时间',\n" +
                "    `CREATE_NAME` VARCHAR(200) COMMENT '创建人名称',\n" +
                "    `UPDATE_BY` VARCHAR(200) COMMENT '修改人',\n" +
                "    `UPDATE_DATE` TIMESTAMP(3) COMMENT '修改时间',\n" +
                "    `UPDATE_NAME` VARCHAR(200) COMMENT '修改人姓名',\n" +
                "    `AUDIT_MEMO` VARCHAR(2000) COMMENT '审核备注',\n" +
                "    `AUDITING_THROUGH_DATE` TIMESTAMP(3) COMMENT '审核通过时间',\n" +
                "    `CONTACT_EMAIL` VARCHAR(320) COMMENT '联系人邮箱',\n" +
                "    `CONTACT_ID_CODE` VARCHAR(1275) COMMENT '证件号码',\n" +
                "    `CONTACT_MOBILE` VARCHAR(160) COMMENT '联系人手机',\n" +
                "    `CONTACT_NAME` VARCHAR(160) COMMENT '联系人姓名',\n" +
                "    `CONTACT_PHONE` VARCHAR(160) COMMENT '联系人电话',\n" +
                "    `COURIER_NUMBER` VARCHAR(320) COMMENT '快递单号',\n" +
                "    `CUR_TEL` VARCHAR(100) COMMENT '当前来电',\n" +
                "    `DATA_COMPLETE` INT COMMENT '是否资料齐全',\n" +
                "    `MEMO` VARCHAR(2000) COMMENT '备注',\n" +
                "    `NEED_REFUND_AMOUT` DECIMAL(10, 2) COMMENT '应退款',\n" +
                "    `REASON` VARCHAR(1275) COMMENT '退款原因',\n" +
                "    `REFUND_DATA_FLAG` INT COMMENT '是否需要退票审核资料',\n" +
                "    `REFUND_DATA_TYPE` VARCHAR(1275) COMMENT '所需退票资料',\n" +
                "    `REFUND_FEE` DECIMAL(10, 2) COMMENT '退款手续费',\n" +
                "    `REFUND_SN` VARCHAR(1275) COMMENT '退款单号',\n" +
                "    `REFUND_STATUS` INT COMMENT '退款申请单状态',\n" +
                "    `REFUNDEDMENT_DATE` TIMESTAMP(3) COMMENT '完成退款时间',\n" +
                "    `REFUNDMENT_STATUS` INT COMMENT '退款状态',\n" +
                "    `SALE_CHANNEL` INT COMMENT '销售渠道',\n" +
                "    `SUTMIT_USER_INFO_BUS_DEPT_ID` INT COMMENT '用户登陆名称',\n" +
                "    `SUTMIT_USER_INFO_DEPT_ID` INT COMMENT '用户所属当前部门ID',\n" +
                "    `SUTMIT_USER_INFO_LOGIN` VARCHAR(1275) COMMENT '用户登陆名称',\n" +
                "    `SUTMIT_USER_INFO_NAME` VARCHAR(1275) COMMENT '用户名',\n" +
                "    `RET_PRICE_ALL_FEE` DECIMAL(19, 2) COMMENT '总税',\n" +
                "    `RET_PRICE_BUILD_FEE` DECIMAL(19, 2) COMMENT '机场建设费',\n" +
                "    `RET_PRICE_CABIN_PRICE` DECIMAL(19, 2) COMMENT '舱位原价',\n" +
                "    `RET_PRICE_CURRENCY` INT COMMENT '支付币种',\n" +
                "    `RET_PRICE_EXCHANGE_RATE` DECIMAL(10, 4) COMMENT '汇率',\n" +
                "    `RET_PRICE_FUEL_FEE` DECIMAL(19, 2) COMMENT '机场燃油费',\n" +
                "    `RET_PRICE_OB_FEE` DECIMAL(19, 2) COMMENT '换开变更费',\n" +
                "    `RET_PRICE_TICKET_PRICE` DECIMAL(19, 2) COMMENT '机票价',\n" +
                "    `RET_PRICE_TOTAL_PRICE` DECIMAL(19, 2) COMMENT '总价',\n" +
                "    `AUDIT_USER_USER_ID` INT COMMENT '审核人',\n" +
                "    `ORDER_PKID` VARCHAR(1275) COMMENT '订单ID',\n" +
                "    `SUBMIT_USER_USER_ID` INT COMMENT '提交人',\n" +
                "    `TK_ORDER_PKID` VARCHAR(1275) COMMENT '机票订单ID',\n" +
                "    `DATA_PRINT` INT COMMENT '是否资料打印',\n" +
                "    `MAIL_MATERIAL` VARCHAR(1275) COMMENT '邮寄资料',\n" +
                "    `SHIP_ADDRESS` VARCHAR(320) COMMENT '收货地址',\n" +
                "    `SHIP_MEMO` VARCHAR(2000) COMMENT '备注',\n" +
                "    `SHIP_MOBILE` VARCHAR(80) COMMENT '收货手机',\n" +
                "    `SHIP_NAME` VARCHAR(200) COMMENT '收货人姓名',\n" +
                "    `SHIP_PHONE` VARCHAR(80) COMMENT '收货电话',\n" +
                "    `SHIP_SEND_DATE` TIMESTAMP(3) COMMENT '配送日期',\n" +
                "    `SHIP_SEND_TIME` VARCHAR(80) COMMENT '送票时间',\n" +
                "    `SHIP_SHIP_MAIL_TYPE` INT COMMENT '邮寄方式',\n" +
                "    `SHIP_SHIP_STATUS` INT COMMENT '配送状态',\n" +
                "    `SHIP_SHIP_TYPE` INT COMMENT '配送方式',\n" +
                "    `SHIP_ZIP_CODE` VARCHAR(40) COMMENT '收货邮编',\n" +
                "    `SHIP_DEPT_GROUP_ID` INT COMMENT '配送部门id',\n" +
                "    `REFUND_TRFD_STATUS` INT COMMENT 'TRFD状态',\n" +
                "    `INSURE_ORDER_PKID` VARCHAR(1275) COMMENT '保险订单ID',\n" +
                "    `SHIP_IS_URGENCY`  tinyint COMMENT '是否紧急邮寄派送',\n" +
                "    `TK_REFUND_FARE_TICKET_FLT_PKID` VARCHAR(1275) COMMENT '退票航班ID',\n" +
                "    `REFUND_MAIL_INVOICE_TITLE` VARCHAR(150) COMMENT '发票抬头',\n" +
                "    `REFUND_MAIL_REFUND` VARCHAR(100) COMMENT '退票费',\n" +
                "    `REFUND_MAIL_REFUNDABLE` VARCHAR(100) COMMENT '应退金额',\n" +
                "    `REFUND_MAIL_ISSUE_TICKET` VARCHAR(50) COMMENT '出票方',\n" +
                "    `REFUND_MAIL_DRAWER` VARCHAR(50) COMMENT '出款方',\n" +
                "    `REFUND_MAIL_AGGREGATE_AMOUNT` VARCHAR(100) COMMENT '支付总金额',\n" +
                "    `REFUND_MEMO` VARCHAR(2000) COMMENT '退票邮寄备注',\n" +
                "    `OTHER_FLAG`  tinyint COMMENT '是否退农行/跨行储蓄卡',\n" +
                "    `VOLUNTARILY_FLAG`  tinyint COMMENT '自愿/非自愿: 1自愿2非自愿',\n" +
                "    `REFUND_MAIL_TAXPAYER_NO` VARCHAR(100) COMMENT '纳税人识别号',\n" +
                "    `REFUND_MAIL_INVOICE_TYPE` INT COMMENT '发票类型',\n" +
                "    `SHIP_EMS_MAIL_TYPE` INT COMMENT 'EMS项目',\n" +
                "    `SHIP_EMSREMARK` VARCHAR(2000) COMMENT 'EMS备注',\n" +
                "    `REFUND_MAIL_BUILD_FEE` VARCHAR(100) COMMENT '民航发展基金',\n" +
                "    `REFUND_MAIL_FUEL_FEE` VARCHAR(100) COMMENT '燃油附加费',\n" +
                "    `REFUND_MAIL_INSURE_FEE` VARCHAR(100) COMMENT '保险费用',\n" +
                "    `OPER_FLAG`  tinyint COMMENT '操作标记0暂不处理1正常处理',\n" +
                "    `OFFLINE_TICKET_REFUND`  tinyint COMMENT '退票渠道：0白屏退票1线下退票2中台退票',\n" +
                "    `REFUND_MARK_STATUS`  tinyint COMMENT '退票状态0未退票1已退票',\n" +
                "    `REFUND_AGENCY_FEE` DECIMAL(10, 2) COMMENT '代理费手续费',\n" +
                "    `REFUND_CATEGORY` INT COMMENT '退款类型',\n" +
                "    `TICKETNO_OR_ORDERNO` VARCHAR(500) COMMENT '关联票号/订单号',\n" +
                "    `OCRP_ID` VARCHAR(500) COMMENT '中台的退单号',\n" +
                "    `BUSINESS_TYPE` VARCHAR(50) COMMENT '业务类型2小时错购D2/I2退票券R非连续非自愿AB',\n" +
                "    `DI_IND` VARCHAR(50) COMMENT '类型标识国内D国际I',\n" +
                "    `REFUND_TICKET_NUMBER` VARCHAR(2000) COMMENT '中台的退票票号',\n" +
                "    `SUPPLY_FLAG`  tinyint COMMENT '是否已补充退款信息0否1是',\n" +
                "    `CONTACT_BIRTH` DATE COMMENT '联系人出生日期',\n" +
                "    `CONNECT_NUMBER` VARCHAR(2000) COMMENT '中台的联票票号',\n" +
                "    `REFUND_PNR` VARCHAR(500) COMMENT '中台的PNR',\n" +
                "    `EXCHANGED_IND` VARCHAR(50) COMMENT '所退机票是否为换开后机票Y是N否',\n" +
                "    `IRR_TICKET_NUMBER` VARCHAR(2000) COMMENT '航变机票号',\n" +
                "    `IRR_AFFECTED_TICKET_NUMBER` VARCHAR(2000) COMMENT '航变关联机票号',\n" +
                "  `ETL_CREATE_TIME` TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                "  `ETL_UPDATE_TIME` TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                "  `ETL_DATE` date  NULL COMMENT '数据ETL日期'" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_KFBP_BUS_REFUND',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //") WITH (\n" +
        //"    'connector' = 'jdbc',\n" +
        //"    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
        //"    'table-name' = 'T_ODS_KFBP_BUS_REFUND', -- 替换为实际的表名\n" +
        //"    'username' = '"+Constants.ODS_USER+"',\n" +
        //"    'password' = '"+Constants.ODS_PWD+"'\n" +
        //",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
        //")");
        log.info("ExtractTOdsKfbpBusRefund executeSql create table for doris end");
        String extractSql = "INSERT INTO `T_ODS_KFBP_BUS_REFUND` \n" +
                " (`PKID`, \n" +
                " `REFUNDTYPE`, \n" +
                " `CREATE_BY`, \n" +
                " `CREATE_DATE`, \n" +
                " `CREATE_NAME`, \n" +
                " `UPDATE_BY`, \n" +
                " `UPDATE_DATE`, \n" +
                " `UPDATE_NAME`, \n" +
                " `AUDIT_MEMO`, \n" +
                " `AUDITING_THROUGH_DATE`, \n" +
                " `CONTACT_EMAIL`, \n" +
                " `CONTACT_ID_CODE`, \n" +
                " `CONTACT_MOBILE`, \n" +
                " `CONTACT_NAME`, \n" +
                " `CONTACT_PHONE`, \n" +
                " `COURIER_NUMBER`, \n" +
                " `CUR_TEL`, \n" +
                " `DATA_COMPLETE`, \n" +
                " `MEMO`, \n" +
                " `NEED_REFUND_AMOUT`, \n" +
                " `REASON`, \n" +
                " `REFUND_DATA_FLAG`, \n" +
                " `REFUND_DATA_TYPE`, \n" +
                " `REFUND_FEE`, \n" +
                " `REFUND_SN`, \n" +
                " `REFUND_STATUS`, \n" +
                " `REFUNDEDMENT_DATE`, \n" +
                " `REFUNDMENT_STATUS`, \n" +
                " `SALE_CHANNEL`, \n" +
                " `SUTMIT_USER_INFO_BUS_DEPT_ID`, \n" +
                " `SUTMIT_USER_INFO_DEPT_ID`, \n" +
                " `SUTMIT_USER_INFO_LOGIN`, \n" +
                " `SUTMIT_USER_INFO_NAME`, \n" +
                " `RET_PRICE_ALL_FEE`, \n" +
                " `RET_PRICE_BUILD_FEE`, \n" +
                " `RET_PRICE_CABIN_PRICE`, \n" +
                " `RET_PRICE_CURRENCY`, \n" +
                " `RET_PRICE_EXCHANGE_RATE`, \n" +
                " `RET_PRICE_FUEL_FEE`, \n" +
                " `RET_PRICE_OB_FEE`, \n" +
                " `RET_PRICE_TICKET_PRICE`, \n" +
                " `RET_PRICE_TOTAL_PRICE`, \n" +
                " `AUDIT_USER_USER_ID`, \n" +
                " `ORDER_PKID`, \n" +
                " `SUBMIT_USER_USER_ID`, \n" +
                " `TK_ORDER_PKID`, \n" +
                " `DATA_PRINT`, \n" +
                " `MAIL_MATERIAL`, \n" +
                " `SHIP_ADDRESS`, \n" +
                " `SHIP_MEMO`, \n" +
                " `SHIP_MOBILE`, \n" +
                " `SHIP_NAME`, \n" +
                " `SHIP_PHONE`, \n" +
                " `SHIP_SEND_DATE`, \n" +
                " `SHIP_SEND_TIME`, \n" +
                " `SHIP_SHIP_MAIL_TYPE`, \n" +
                " `SHIP_SHIP_STATUS`, \n" +
                " `SHIP_SHIP_TYPE`, \n" +
                " `SHIP_ZIP_CODE`, \n" +
                " `SHIP_DEPT_GROUP_ID`, \n" +
                " `REFUND_TRFD_STATUS`, \n" +
                " `INSURE_ORDER_PKID`, \n" +
                " `SHIP_IS_URGENCY`, \n" +
                " `TK_REFUND_FARE_TICKET_FLT_PKID`, \n" +
                " `REFUND_MAIL_INVOICE_TITLE`, \n" +
                " `REFUND_MAIL_REFUND`, \n" +
                " `REFUND_MAIL_REFUNDABLE`, \n" +
                " `REFUND_MAIL_ISSUE_TICKET`, \n" +
                " `REFUND_MAIL_DRAWER`, \n" +
                " `REFUND_MAIL_AGGREGATE_AMOUNT`, \n" +
                " `REFUND_MEMO`, \n" +
                " `OTHER_FLAG`, \n" +
                " `VOLUNTARILY_FLAG`, \n" +
                " `REFUND_MAIL_TAXPAYER_NO`, \n" +
                " `REFUND_MAIL_INVOICE_TYPE`, \n" +
                " `SHIP_EMS_MAIL_TYPE`, \n" +
                " `SHIP_EMSREMARK`, \n" +
                " `REFUND_MAIL_BUILD_FEE`, \n" +
                " `REFUND_MAIL_FUEL_FEE`, \n" +
                " `REFUND_MAIL_INSURE_FEE`, \n" +
                " `OPER_FLAG`, \n" +
                " `OFFLINE_TICKET_REFUND`, \n" +
                " `REFUND_MARK_STATUS`, \n" +
                " `REFUND_AGENCY_FEE`, \n" +
                " `REFUND_CATEGORY`, \n" +
                " `TICKETNO_OR_ORDERNO`, \n" +
                " `CONTACT_BIRTH`, \n" +
                " `OCRP_ID`, \n" +
                " `BUSINESS_TYPE`, \n" +
                " `DI_IND`, \n" +
                " `REFUND_TICKET_NUMBER`, \n" +
                " `SUPPLY_FLAG`, \n" +
                " `CONNECT_NUMBER`, \n" +
                " `REFUND_PNR`, \n" +
                " `EXCHANGED_IND`, \n" +
                " `IRR_TICKET_NUMBER`, \n" +
                " `IRR_AFFECTED_TICKET_NUMBER`, \n" +
                " `ETL_CREATE_TIME`, \n" +
                " `ETL_UPDATE_TIME`, \n" +
                " `ETL_DATE`)" +

                "SELECT\n" +
                " PKID,\n" +
                " REFUNDTYPE,\n" +
                " CREATE_BY,\n" +
                " CREATE_DATE,\n" +
                " CREATE_NAME,\n" +
                " UPDATE_BY,\n" +
                " UPDATE_DATE,\n" +
                " UPDATE_NAME,\n" +
                " AUDIT_MEMO,\n" +
                " AUDITING_THROUGH_DATE,\n" +
                " CONTACT_EMAIL,\n" +
                " sm4_encrypt(CONTACT_ID_CODE, '" + Constants.SM4_KEY + "') CONTACT_ID_CODE,\n" +
                " sm4_encrypt(CONTACT_MOBILE, '" + Constants.SM4_KEY + "') CONTACT_MOBILE,\n" +
                " CONTACT_NAME,\n" +
                " sm4_encrypt(CONTACT_PHONE, '" + Constants.SM4_KEY + "') CONTACT_PHONE,\n" +
                " COURIER_NUMBER,\n" +
                " sm4_encrypt(CUR_TEL, '" + Constants.SM4_KEY + "') CUR_TEL,\n" +
                " DATA_COMPLETE,\n" +
                " MEMO,\n" +
                " NEED_REFUND_AMOUT,\n" +
                " REASON,\n" +
                " REFUND_DATA_FLAG,\n" +
                " REFUND_DATA_TYPE,\n" +
                " REFUND_FEE,\n" +
                " REFUND_SN,\n" +
                " REFUND_STATUS,\n" +
                " REFUNDEDMENT_DATE,\n" +
                " REFUNDMENT_STATUS,\n" +
                " SALE_CHANNEL,\n" +
                " SUTMIT_USER_INFO_BUS_DEPT_ID,\n" +
                " SUTMIT_USER_INFO_DEPT_ID,\n" +
                " SUTMIT_USER_INFO_LOGIN,\n" +
                " SUTMIT_USER_INFO_NAME,\n" +
                " RET_PRICE_ALL_FEE,\n" +
                " RET_PRICE_BUILD_FEE,\n" +
                " RET_PRICE_CABIN_PRICE,\n" +
                " RET_PRICE_CURRENCY,\n" +
                " RET_PRICE_EXCHANGE_RATE,\n" +
                " RET_PRICE_FUEL_FEE,\n" +
                " RET_PRICE_OB_FEE,\n" +
                " RET_PRICE_TICKET_PRICE,\n" +
                " RET_PRICE_TOTAL_PRICE,\n" +
                " AUDIT_USER_USER_ID,\n" +
                " ORDER_PKID,\n" +
                " SUBMIT_USER_USER_ID,\n" +
                " TK_ORDER_PKID,\n" +
                " DATA_PRINT,\n" +
                " MAIL_MATERIAL,\n" +
                " SHIP_ADDRESS,\n" +
                " SHIP_MEMO,\n" +
                " SHIP_MOBILE,\n" +
                " SHIP_NAME,\n" +
                " SHIP_PHONE,\n" +
                " SHIP_SEND_DATE,\n" +
                " SHIP_SEND_TIME,\n" +
                " SHIP_SHIP_MAIL_TYPE,\n" +
                " SHIP_SHIP_STATUS,\n" +
                " SHIP_SHIP_TYPE,\n" +
                " SHIP_ZIP_CODE,\n" +
                " SHIP_DEPT_GROUP_ID,\n" +
                " REFUND_TRFD_STATUS,\n" +
                " INSURE_ORDER_PKID,\n" +
                " SHIP_IS_URGENCY,\n" +
                " TK_REFUND_FARE_TICKET_FLT_PKID,\n" +
                " REFUND_MAIL_INVOICE_TITLE,\n" +
                " REFUND_MAIL_REFUND,\n" +
                " REFUND_MAIL_REFUNDABLE,\n" +
                " REFUND_MAIL_ISSUE_TICKET,\n" +
                " REFUND_MAIL_DRAWER,\n" +
                " REFUND_MAIL_AGGREGATE_AMOUNT,\n" +
                " REFUND_MEMO,\n" +
                " OTHER_FLAG,\n" +
                " VOLUNTARILY_FLAG,\n" +
                " REFUND_MAIL_TAXPAYER_NO,\n" +
                " REFUND_MAIL_INVOICE_TYPE,\n" +
                " SHIP_EMS_MAIL_TYPE,\n" +
                " SHIP_EMSREMARK,\n" +
                " REFUND_MAIL_BUILD_FEE,\n" +
                " REFUND_MAIL_FUEL_FEE,\n" +
                " REFUND_MAIL_INSURE_FEE,\n" +
                " OPER_FLAG,\n" +
                " OFFLINE_TICKET_REFUND,\n" +
                " REFUND_MARK_STATUS,\n" +
                " REFUND_AGENCY_FEE,\n" +
                " REFUND_CATEGORY,\n" +
                " TICKETNO_OR_ORDERNO,\n" +
                " CONTACT_BIRTH,\n" +
                " OCRP_ID,\n" +
                " BUSINESS_TYPE,\n" +
                " DI_IND,\n" +
                " REFUND_TICKET_NUMBER,\n" +
                " SUPPLY_FLAG,\n" +
                " CONNECT_NUMBER,\n" +
                " REFUND_PNR,\n" +
                " EXCHANGED_IND,\n" +
                " IRR_TICKET_NUMBER,\n" +
                " IRR_AFFECTED_TICKET_NUMBER,\n" +
                " CURRENT_TIMESTAMP AS ETL_CREATE_TIME,\n" +
                " CURRENT_TIMESTAMP AS ETL_UPDATE_TIME,\n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                " FROM BUS_REFUND "
                + " WHERE  CREATE_DATE BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  UPDATE_DATE BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;
        log.info("ExtractTOdsKfbpBusRefund executeSql extract start");
        TableResult result = tEnv.executeSql(extractSql);
        log.info("ExtractTOdsKfbpBusRefund executeSql extract end");

        result.print();

        log.info("ExtractTOdsKfbpBusRefund data transfer end");


    }

}
