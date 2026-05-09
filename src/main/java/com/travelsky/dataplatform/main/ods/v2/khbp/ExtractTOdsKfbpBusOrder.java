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
public class ExtractTOdsKfbpBusOrder {
    public static Logger log = LoggerFactory.getLogger(ExtractTOdsKfbpBusOrder.class);
    public static void main(String[] args) throws IOException {
        log.info("ExtractTOdsKfbpBusOrder data transfer start");
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
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsKfbpBusOrder");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        log.info("ExtractTOdsKfbpBusOrder executeSql create table for source start");
        tEnv.executeSql("CREATE TABLE BUS_ORDER (\n" +
                "    ORDER_TYPE VARCHAR(31),\n" +
                "    PKID VARCHAR(255),\n" +
                "    CREATE_BY VARCHAR(40),\n" +
                "    CREATE_DATE TIMESTAMP,\n" +
                "    CREATE_NAME VARCHAR(40),\n" +
                "    UPDATE_BY VARCHAR(40),\n" +
                "    UPDATE_DATE TIMESTAMP,\n" +
                "    UPDATE_NAME VARCHAR(40),\n" +
                "    BIG_CUS_INFO_BIG_CUS_TYPE INT,\n" +
                "    BIG_CUS_INFO_CARD_NO VARCHAR(64),\n" +
                "    BIG_CUS_INFO_DEP_NAME VARCHAR(64),\n" +
                "    BIG_CUS_INFO_FIRM_NAME VARCHAR(64),\n" +
                "    BIG_CUS_INFO_ID_NUMBER VARCHAR(64),\n" +
                "    BIG_CUS_INFO_IS_BIG_CUS tinyint,\n" +
                "    BIG_CUS_INFO_MANANGER_NAME VARCHAR(16),\n" +
                "    BIG_CUS_INFO_MEET_TITLE VARCHAR(32),\n" +
                "    BIG_CUS_INFO_PARBU VARCHAR(16),\n" +
                "    CASH_CASH_STATUS INT,\n" +
                "    CASH_CURRENCY INT,\n" +
                "    CASH_DELIVERY_FEE DECIMAL(19, 2),\n" +
                "    CASH_DELIVERY_FEE_CUR DECIMAL(19, 2),\n" +
                "    CASH_FPF_CARD_NO VARCHAR(16),\n" +
                "    CASH_FPF_NAME VARCHAR(255),\n" +
                "    CASH_FPF_TAX_SCORE INT,\n" +
                "    CASH_FPF_TICKET_DISCOUNT DECIMAL(19, 2),\n" +
                "    CASH_FPF_TICKET_SCORE INT,\n" +
                "    CASH_IS_FPF tinyint,\n" +
                "    CASH_IS_FPF_MIX tinyint,\n" +
                "    CASH_PAID_AMOUNT DECIMAL(19, 2),\n" +
                "    CASH_PAID_AMOUNT_CUR DECIMAL(19, 2),\n" +
                "    CASH_PAY_ADDRESS INT,\n" +
                "    CASH_TOTAL_ORDER_AMOUNT DECIMAL(19, 2),\n" +
                "    CASH_TOTAL_ORDER_AMOUNT_CUR DECIMAL(19, 2),\n" +
                "    CASH_TOTAL_PRODUCT_PRICE DECIMAL(19, 2),\n" +
                "    CASH_TOTAL_PRODUCT_PRICE_CUR DECIMAL(19, 2),\n" +
                "    CHINS_ID VARCHAR(16),\n" +
                "    CONTACT_EMAIL VARCHAR(64),\n" +
                "    CONTACT_ID_CODE VARCHAR(255),\n" +
                "    CONTACT_MOBILE VARCHAR(32),\n" +
                "    CONTACT_NAME VARCHAR(32),\n" +
                "    CONTACT_PHONE VARCHAR(32),\n" +
                "    CUR_TEL VARCHAR(16),\n" +
                "    GOV_INFO_BIN VARCHAR(16),\n" +
                "    GOV_INFO_BUGDET_NAME VARCHAR(64),\n" +
                "    GOV_INFO_IS_GOV tinyINT,\n" +
                "    IS_URGENCY tinyINT,\n" +
                "    MEMO VARCHAR(4000),\n" +
                "    ORDER_SN VARCHAR(30),\n" +
                "    ORDER_SOURCE INT,\n" +
                "    ORDER_STATUS INT,\n" +
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
                "    SUTMIT_USER_INFO_BUS_DEPT_ID INT,\n" +
                "    SUTMIT_USER_INFO_DEPT_ID INT,\n" +
                "    SUTMIT_USER_INFO_LOGIN VARCHAR(255),\n" +
                "    SUTMIT_USER_INFO_NAME VARCHAR(255),\n" +
                "    BUS_ORDER_STATUS INT,\n" +
                "    BUS_ORDER_TYPE INT,\n" +
                "    CLEAN_QUEUE_CHANGE_TYPE INT,\n" +
                "    IS_ADD_INF tinyINT,\n" +
                "    OPEN_TICKET_LIMIT_DATE TIMESTAMP,\n" +
                "    OT_USER_INFO_BUS_DEPT_ID INT,\n" +
                "    OT_USER_INFO_DEPT_ID INT,\n" +
                "    OT_USER_INFO_LOGIN VARCHAR(255),\n" +
                "    OT_USER_INFO_NAME VARCHAR(255),\n" +
                "    OTHER_PRODUCT_TYPE INT,\n" +
                "    PNR VARCHAR(10),\n" +
                "    PRICE_ALL_FEE DECIMAL(19, 2),\n" +
                "    PRICE_BUILD_FEE DECIMAL(19, 2),\n" +
                "    PRICE_CABIN_PRICE DECIMAL(19, 2),\n" +
                "    PRICE_CURRENCY INT,\n" +
                "    PRICE_EXCHANGE_RATE DECIMAL(10, 4),\n" +
                "    PRICE_FUEL_FEE DECIMAL(19, 2),\n" +
                "    PRICE_OB_FEE DECIMAL(19, 2),\n" +
                "    PRICE_TICKET_PRICE DECIMAL(19, 2),\n" +
                "    PRICE_TOTAL_PRICE DECIMAL(19, 2),\n" +
                "    TICKET_TYPE INT,\n" +
                "    TK_ORDER_STATUS INT,\n" +
                "    TK_ORDER_TYPE INT,\n" +
                "    ALL_RISKS_INSURE_FEE DECIMAL(19, 2),\n" +
                "    ALL_RISKS_INSURE_INSURE_COUNT DECIMAL(19, 2),\n" +
                "    ALL_RISKS_INSURE_TOTAL_FEE DECIMAL(19, 2),\n" +
                "    DELAY_INSURE_FEE DECIMAL(19, 2),\n" +
                "    DELAY_INSURE_INSURE_COUNT DECIMAL(19, 2),\n" +
                "    DELAY_INSURE_TOTAL_FEE DECIMAL(19, 2),\n" +
                "    EXPECT_INSURE_FEE DECIMAL(19, 2),\n" +
                "    EXPECT_INSURE_INSURE_COUNT DECIMAL(19, 2),\n" +
                "    EXPECT_INSURE_TOTAL_FEE DECIMAL(19, 2),\n" +
                "    FLT_COUNT DECIMAL(19, 2),\n" +
                "    INSURE_DAYS INT,\n" +
                "    INSURE_SOURCE INT,\n" +
                "    INSURE_STATUS INT,\n" +
                "    OVERSEA_FEE DECIMAL(19, 2),\n" +
                "    POLICY_NO VARCHAR(20),\n" +
                "    TOTAL_FEE DECIMAL(19, 2),\n" +
                "    CUR_INCOME_NAME VARCHAR(255),\n" +
                "    POST_NO VARCHAR(255),\n" +
                "    CHANGE_ORDER_SN VARCHAR(255),\n" +
                "    CHANGE_ORDER_STATUS INT,\n" +
                "    IS_FEE tinyINT,\n" +
                "    PNR_CHANGE_AFTER VARCHAR(255),\n" +
                "    CASH_DEPT_GROUP_ID INT,\n" +
                "    GAIN_USER_USER_ID INT,\n" +
                "    SHIP_DEPT_GROUP_ID INT,\n" +
                "    SUBMIT_USER_USER_ID INT,\n" +
                "    UNION_ORDER_PKID VARCHAR(255),\n" +
                "    OT_USER_USER_ID INT,\n" +
                "    FARE_PKID VARCHAR(255),\n" +
                "    ORDER_PKID VARCHAR(255),\n" +
                "    SEAT_NO VARCHAR(255),\n" +
                "    FLIGHT_PKID VARCHAR(255),\n" +
                "    SEAT_ORDER_STATUS INT,\n" +
                "    RECEIPT_TYPE TINYINT,\n" +
                "    CASH_PAID_TIME TIMESTAMP,\n" +
                "    EMD_NO VARCHAR(16),\n" +
                "    SEAT_ORDER_REFUND_PKID VARCHAR(255),\n" +
                "    IS_PNR_BY_WHITE tinyINT,\n" +
                "    IS_FICTITIOUS_ORDER tinyINT,\n" +
                "    SHIP_IS_URGENCY tinyINT,\n" +
                "    OVERSEA_INSURE_STATUS INT,\n" +
                "    EMD_TYPE INT,\n" +
                "    PRODUCT_PKID VARCHAR(255),\n" +
                "    CASH_FPF_TAX_PRICE DECIMAL(19, 2),\n" +
                "    IS_ONLY_ORDER tinyINT,\n" +
                "    CASH_SCORE_DISCOUNT DECIMAL(19, 2),\n" +
                "    CHANGE_FK_TK_ORDER_PKID VARCHAR(255),\n" +
                "    REISSUE_TYPE INT,\n" +
                "    REFUND_MAIL_INVOICE_TITLE VARCHAR(100),\n" +
                "    REFUND_MAIL_REFUND VARCHAR(20),\n" +
                "    REFUND_MAIL_REFUNDABLE VARCHAR(20),\n" +
                "    REFUND_MAIL_ISSUE_TICKET VARCHAR(10),\n" +
                "    REFUND_MAIL_DRAWER VARCHAR(10),\n" +
                "    REFUND_MAIL_AGGREGATE_AMOUNT VARCHAR(20),\n" +
                "    PRINT_TKT_ORIGIN INT,\n" +
                "    LUAAGE_ORDER_STATUS INT,\n" +
                "    EMD_SEAT_ORDER_REFUND_PKID VARCHAR(255),\n" +
                "    EMD_LUAAGE_ORDER_REFUND_PKID VARCHAR(255),\n" +
                "    ESS_ORDER_PKID VARCHAR(255),\n" +
                "    TEAM_NUM VARCHAR(10),\n" +
                "    CASH_CURRENCY_CODE VARCHAR(4),\n" +
                "    CASH_EXCHANGE_RATE DECIMAL(19, 4),\n" +
                "    PRINT_TKT_PENDING_DATE TIMESTAMP,\n" +
                "    PRINT_TKT_COMPLETED_DATE TIMESTAMP,\n" +
                "    EMD_FLIGHT_NO VARCHAR(8),\n" +
                "    EMD_FARE_NAME VARCHAR(255),\n" +
                "    REFUND_MAIL_TAXPAYER_NO VARCHAR(20),\n" +
                "    CASH_FPF_SEASON VARCHAR(16),\n" +
                "    REFUND_MAIL_INVOICE_TYPE INT,\n" +
                "    IS_TKEN VARCHAR(3),\n" +
                "    PRINT_TKT_COMPLETED_NAME VARCHAR(30),\n" +
                "    PRINT_MAIL_NAME VARCHAR(30),\n" +
                "    PRINT_MAIL_DATE TIMESTAMP,\n" +
                "    CASH_CREDIT_SCORE INT,\n" +
                "    SHIP_EMS_MAIL_TYPE INT,\n" +
                "    SHIP_EMSREMARK VARCHAR(2000),\n" +
                "    APPLYFOR_MEMO VARCHAR(4000),\n" +
                "    APPLYFOR_PHONE VARCHAR(20),\n" +
                "    APPLYFOR_STATUS VARCHAR(3),\n" +
                "    OFFICE VARCHAR(255),\n" +
                "    CASH_SPECIAL_AMOUNT DECIMAL(19, 2),\n" +
                "    IS_MANUAL tinyINT,\n" +
                "    IS_TURNDOWN tinyINT,\n" +
                "    REFUND_MAIL_BUILD_FEE VARCHAR(20),\n" +
                "    REFUND_MAIL_FUEL_FEE VARCHAR(20),\n" +
                "    IS_MANUAL_FPF tinyINT,\n" +
                "    REFUND_MAIL_INSURE_FEE VARCHAR(20),\n" +
                "    INSURE_SUC_DATE TIMESTAMP,\n" +
                "    INSURE_REFUNDED_DATE TIMESTAMP,\n" +
                "    IS_HAS_INSURE_FAIL tinyINT,\n" +
                "    INSURE_ALLRISKS_TYPE INT,\n" +
                "    REISSUE_INFO2 VARCHAR(4000),\n" +
                "    REISSUE_INFO STRING,\n" +
                "    SUMMARY_SHORT_CODE VARCHAR(512),\n" +
                "    SUMMARY_URL VARCHAR(512),\n" +
                "    PRINT_REPEAT VARCHAR(2),\n" +
                "    CONTACE_BIRTH DATE,\n" +
                "    CONTACT_BIRTH DATE" +

                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.KHBP_IP + ":" + Constants.KHBP_PORT + "/" + Constants.KHBP_DB + "',\n" +
                "    'table-name' = '" + Constants.KHBP_SCHEMA + ".BUS_ORDER', \n" +
                "    'username' = '" + Constants.KHBP_USER + "',\n" +
                "    'password' = '" + Constants.KHBP_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        log.info("ExtractTOdsKfbpBusOrder executeSql create table for source end");
        //创建Doris目标表
        log.info("ExtractTOdsKfbpBusOrder executeSql create table for doris start");
        tEnv.executeSql("CREATE TABLE T_ODS_KFBP_BUS_ORDER (\n" +
                "  `PKID` STRING COMMENT '主键',\n" +
                "  `ORDER_TYPE` STRING COMMENT '订单类型',\n" +
                "  `CREATE_BY` STRING COMMENT '创建人ID',\n" +
                "  `CREATE_DATE` TIMESTAMP(3) COMMENT '创建时间',\n" +
                "  `CREATE_NAME` STRING COMMENT '创建人名称',\n" +
                "  `UPDATE_BY` STRING COMMENT '修改人',\n" +
                "  `UPDATE_DATE` TIMESTAMP(3) COMMENT '修改时间',\n" +
                "  `UPDATE_NAME` STRING COMMENT '修改人姓名',\n" +
                "  `BIG_CUS_INFO_BIG_CUS_TYPE` INT COMMENT '大客户类型',\n" +
                "  `BIG_CUS_INFO_CARD_NO` STRING COMMENT '大客户协议号',\n" +
                "  `BIG_CUS_INFO_DEP_NAME` STRING COMMENT '所属部门',\n" +
                "  `BIG_CUS_INFO_FIRM_NAME` STRING COMMENT '大客户公司名',\n" +
                "  `BIG_CUS_INFO_ID_NUMBER` STRING COMMENT '大客户主键',\n" +
                "  `BIG_CUS_INFO_IS_BIG_CUS` tinyINT COMMENT '是否大客户订单',\n" +
                "  `BIG_CUS_INFO_MANANGER_NAME` STRING COMMENT '大客户经理名称',\n" +
                "  `BIG_CUS_INFO_MEET_TITLE` STRING COMMENT '会议主题',\n" +
                "  `BIG_CUS_INFO_PARBU` STRING COMMENT '大客户结算代码',\n" +
                "  `CASH_CASH_STATUS` INT COMMENT '支付状态',\n" +
                "  `CASH_CURRENCY` INT COMMENT '支付时的币种',\n" +
                "  `CASH_DELIVERY_FEE` DECIMAL(22, 2) COMMENT '配送费用(人民币)',\n" +
                "  `CASH_DELIVERY_FEE_CUR` DECIMAL(22, 2) COMMENT '配送费用(支付时币种)',\n" +
                "  `CASH_FPF_CARD_NO` STRING COMMENT '常客卡号',\n" +
                "  `CASH_FPF_NAME` STRING COMMENT '常客卡用户名',\n" +
                "  `CASH_FPF_TAX_SCORE` INT COMMENT '所需积分(税费)',\n" +
                "  `CASH_FPF_TICKET_DISCOUNT` DECIMAL(22, 2) COMMENT '折扣',\n" +
                "  `CASH_FPF_TICKET_SCORE` INT COMMENT '所需积分(票面)',\n" +
                "  `CASH_IS_FPF` tinyINT COMMENT '是否积分支付',\n" +
                "  `CASH_IS_FPF_MIX` tinyINT COMMENT '积分支付是否混合支付',\n" +
                "  `CASH_PAID_AMOUNT` DECIMAL(22, 2) COMMENT '已付金额(人民币)',\n" +
                "  `CASH_PAID_AMOUNT_CUR` DECIMAL(22, 2) COMMENT '已付金额(支付时币种)',\n" +
                "  `CASH_PAY_ADDRESS` INT COMMENT '支付时的地址',\n" +
                "  `CASH_TOTAL_ORDER_AMOUNT` DECIMAL(22, 2) COMMENT '订单总额(人民币)',\n" +
                "  `CASH_TOTAL_ORDER_AMOUNT_CUR` DECIMAL(22, 2) COMMENT '订单总额(支付时币种)',\n" +
                "  `CASH_TOTAL_PRODUCT_PRICE` DECIMAL(22, 2) COMMENT '总商品价格(人民币)',\n" +
                "  `CASH_TOTAL_PRODUCT_PRICE_CUR` DECIMAL(22, 2) COMMENT '总商品价格(支付时币种)',\n" +
                "  `CHINS_ID` STRING COMMENT '话务系统取到ID',\n" +
                "  `CONTACT_EMAIL` STRING COMMENT '联系人邮箱',\n" +
                "  `CONTACT_ID_CODE` STRING COMMENT '证件号码',\n" +
                "  `CONTACT_MOBILE` STRING COMMENT '联系人手机',\n" +
                "  `CONTACT_NAME` STRING COMMENT '联系人姓名',\n" +
                "  `CONTACT_PHONE` STRING COMMENT '联系人电话',\n" +
                "  `CUR_TEL` STRING COMMENT '当前来电号码',\n" +
                "  `GOV_INFO_BIN` STRING COMMENT '政府卡BIN',\n" +
                "  `GOV_INFO_BUGDET_NAME` STRING COMMENT '预算单位名称',\n" +
                "  `GOV_INFO_IS_GOV` tinyINT COMMENT '是否政府采购',\n" +
                "  `IS_URGENCY` tinyINT COMMENT '是否紧急订单',\n" +
                "  `MEMO` STRING COMMENT '备注',\n" +
                "  `ORDER_SN` STRING COMMENT '订单号',\n" +
                "  `ORDER_SOURCE` INT COMMENT '订单来源',\n" +
                "  `ORDER_STATUS` INT COMMENT '订单状态',\n" +
                "  `SHIP_ADDRESS` STRING COMMENT '收货地址',\n" +
                "  `SHIP_MEMO` STRING COMMENT '备注',\n" +
                "  `SHIP_MOBILE` STRING COMMENT '收货手机',\n" +
                "  `SHIP_NAME` STRING COMMENT '收货人姓名',\n" +
                "  `SHIP_PHONE` STRING COMMENT '收货电话',\n" +
                "  `SHIP_SEND_DATE` TIMESTAMP(3) COMMENT '配送日期',\n" +
                "  `SHIP_SEND_TIME` STRING COMMENT '送票时间',\n" +
                "  `SHIP_SHIP_MAIL_TYPE` INT COMMENT '邮寄方式',\n" +
                "  `SHIP_SHIP_STATUS` INT COMMENT '配送状态',\n" +
                "  `SHIP_SHIP_TYPE` INT COMMENT '配送方式',\n" +
                "  `SHIP_ZIP_CODE` STRING COMMENT '收货邮编',\n" +
                "  `SUTMIT_USER_INFO_BUS_DEPT_ID` INT COMMENT '用户登陆名称',\n" +
                "  `SUTMIT_USER_INFO_DEPT_ID` INT COMMENT '用户所属当前部门ID',\n" +
                "  `SUTMIT_USER_INFO_LOGIN` STRING COMMENT '用户登陆名称',\n" +
                "  `SUTMIT_USER_INFO_NAME` STRING COMMENT '用户名',\n" +
                "  `BUS_ORDER_STATUS` INT COMMENT '便捷巴士单状态',\n" +
                "  `BUS_ORDER_TYPE` INT COMMENT '便捷巴士单类型',\n" +
                "  `CLEAN_QUEUE_CHANGE_TYPE` INT COMMENT '清Q变动类型',\n" +
                "  `IS_ADD_INF` tinyINT COMMENT '是否补开婴儿',\n" +
                "  `OPEN_TICKET_LIMIT_DATE` TIMESTAMP(3) COMMENT '出票时限',\n" +
                "  `OT_USER_INFO_BUS_DEPT_ID` INT COMMENT '用户登陆名称',\n" +
                "  `OT_USER_INFO_DEPT_ID` INT COMMENT '用户所属当前部门ID',\n" +
                "  `OT_USER_INFO_LOGIN` STRING COMMENT '用户登陆名称',\n" +
                "  `OT_USER_INFO_NAME` STRING COMMENT '用户名',\n" +
                "  `OTHER_PRODUCT_TYPE` INT COMMENT '其它服务',\n" +
                "  `PNR` STRING COMMENT 'PNR号',\n" +
                "  `PRICE_ALL_FEE` DECIMAL(22, 2) COMMENT '总税',\n" +
                "  `PRICE_BUILD_FEE` DECIMAL(22, 2) COMMENT '机场建设费',\n" +
                "  `PRICE_CABIN_PRICE` DECIMAL(22, 2) COMMENT '舱位原价',\n" +
                "  `PRICE_CURRENCY` INT COMMENT '支付币种',\n" +
                "  `PRICE_EXCHANGE_RATE` DECIMAL(14, 4) COMMENT '汇率',\n" +
                "  `PRICE_FUEL_FEE` DECIMAL(22, 2) COMMENT '机场燃油费',\n" +
                "  `PRICE_OB_FEE` DECIMAL(22, 2) COMMENT '换开变更费',\n" +
                "  `PRICE_TICKET_PRICE` DECIMAL(22, 2) COMMENT '机票价',\n" +
                "  `PRICE_TOTAL_PRICE` DECIMAL(22, 2) COMMENT '总价',\n" +
                "  `TICKET_TYPE` INT COMMENT '机票类型',\n" +
                "  `TK_ORDER_STATUS` INT COMMENT '机票订单状态',\n" +
                "  `TK_ORDER_TYPE` INT COMMENT '机票订单类型',\n" +
                "  `ALL_RISKS_INSURE_FEE` DECIMAL(22, 2) COMMENT '保费(单价)',\n" +
                "  `ALL_RISKS_INSURE_INSURE_COUNT` DECIMAL(22, 2) COMMENT '保险份数',\n" +
                "  `ALL_RISKS_INSURE_TOTAL_FEE` DECIMAL(22, 2) COMMENT '总保费',\n" +
                "  `DELAY_INSURE_FEE` DECIMAL(22, 2) COMMENT '保费(单价)',\n" +
                "  `DELAY_INSURE_INSURE_COUNT` DECIMAL(22, 2) COMMENT '保险份数',\n" +
                "  `DELAY_INSURE_TOTAL_FEE` DECIMAL(22, 2) COMMENT '总保费',\n" +
                "  `EXPECT_INSURE_FEE` DECIMAL(22, 2) COMMENT '保费(单价)',\n" +
                "  `EXPECT_INSURE_INSURE_COUNT` DECIMAL(22, 2) COMMENT '保险份数',\n" +
                "  `EXPECT_INSURE_TOTAL_FEE` DECIMAL(22, 2) COMMENT '总保费',\n" +
                "  `FLT_COUNT` DECIMAL(22, 2) COMMENT '航段数',\n" +
                "  `INSURE_DAYS` INT COMMENT '天数(境外险使用)',\n" +
                "  `INSURE_SOURCE` INT COMMENT '保险来源',\n" +
                "  `INSURE_STATUS` INT COMMENT '航联保险状态',\n" +
                "  `OVERSEA_FEE` DECIMAL(22, 2) COMMENT '境外险保费',\n" +
                "  `POLICY_NO` STRING COMMENT '保险号，退保需要使用',\n" +
                "  `TOTAL_FEE` DECIMAL(22, 2) COMMENT '总保费',\n" +
                "  `CUR_INCOME_NAME` STRING COMMENT '来电姓名',\n" +
                "  `POST_NO` STRING COMMENT '快递单号',\n" +
                "  `CHANGE_ORDER_SN` STRING COMMENT '变更单号',\n" +
                "  `CHANGE_ORDER_STATUS` INT COMMENT '变更单状态',\n" +
                "  `IS_FEE` tinyINT COMMENT '是否免费变更',\n" +
                "  `PNR_CHANGE_AFTER` STRING COMMENT '变更后的pnr',\n" +
                "  `CASH_DEPT_GROUP_ID` INT COMMENT '支付部门ID',\n" +
                "  `GAIN_USER_USER_ID` INT COMMENT '领用人',\n" +
                "  `SHIP_DEPT_GROUP_ID` INT COMMENT '配送部门ID',\n" +
                "  `SUBMIT_USER_USER_ID` INT COMMENT '提交人',\n" +
                "  `UNION_ORDER_PKID` STRING COMMENT '统一收款单ID',\n" +
                "  `OT_USER_USER_ID` INT COMMENT '预定人',\n" +
                "  `FARE_PKID` STRING COMMENT '旅客ID',\n" +
                "  `ORDER_PKID` STRING COMMENT '付费EMD订单属于的机票订单ID',\n" +
                "  `SEAT_NO` STRING COMMENT '选座座位号',\n" +
                "  `FLIGHT_PKID` STRING COMMENT '订单ID',\n" +
                "  `SEAT_ORDER_STATUS` INT COMMENT '选座订单状态',\n" +
                "  `RECEIPT_TYPE` TINYINT COMMENT '收款类型',\n" +
                "  `CASH_PAID_TIME` TIMESTAMP COMMENT '最新支付时间',\n" +
                "  `EMD_NO` STRING COMMENT 'emd票号',\n" +
                "  `SEAT_ORDER_REFUND_PKID` STRING COMMENT '退座单ID',\n" +
                "  `IS_PNR_BY_WHITE` tinyINT COMMENT '是否白屏产生PNR',\n" +
                "  `IS_FICTITIOUS_ORDER` tinyINT COMMENT '是否虚拟订单',\n" +
                "  `SHIP_IS_URGENCY` tinyINT COMMENT '是否紧急邮寄派送',\n" +
                "  `OVERSEA_INSURE_STATUS` INT COMMENT '境外险状态',\n" +
                "  `EMD_TYPE` INT COMMENT 'emd类型(1选座2行李)',\n" +
                "  `PRODUCT_PKID` STRING COMMENT '产品ID',\n" +
                "  `CASH_FPF_TAX_PRICE` DECIMAL(22, 2) COMMENT '税费总额',\n" +
                "  `IS_ONLY_ORDER` tinyINT COMMENT '是否单独购保单，选座单等',\n" +
                "  `CASH_SCORE_DISCOUNT` DECIMAL(22, 2) COMMENT '积分折扣',\n" +
                "  `CHANGE_FK_TK_ORDER_PKID` STRING COMMENT '变更单属于的机票订单ID',\n" +
                "  `REISSUE_TYPE` INT COMMENT '改期类型',\n" +
                "  `REFUND_MAIL_INVOICE_TITLE` STRING COMMENT '发票抬头',\n" +
                "  `REFUND_MAIL_REFUND` STRING COMMENT '退票费',\n" +
                "  `REFUND_MAIL_REFUNDABLE` STRING COMMENT '应退金额',\n" +
                "  `REFUND_MAIL_ISSUE_TICKET` STRING COMMENT '出票方',\n" +
                "  `REFUND_MAIL_DRAWER` STRING COMMENT '出款方',\n" +
                "  `REFUND_MAIL_AGGREGATE_AMOUNT` STRING COMMENT '支付总金额',\n" +
                "  `PRINT_TKT_ORIGIN` INT COMMENT '行程单来源',\n" +
                "  `LUAAGE_ORDER_STATUS` INT COMMENT '行李订单状态',\n" +
                "  `EMD_SEAT_ORDER_REFUND_PKID` STRING COMMENT '退选座单ID',\n" +
                "  `EMD_LUAAGE_ORDER_REFUND_PKID` STRING COMMENT '退行李单ID',\n" +
                "  `ESS_ORDER_PKID` STRING COMMENT '选座单关联机票ID',\n" +
                "  `TEAM_NUM` STRING COMMENT '团队人数',\n" +
                "  `CASH_CURRENCY_CODE` STRING COMMENT '货币代码',\n" +
                "  `CASH_EXCHANGE_RATE` DECIMAL(23, 4) COMMENT '以人民币汇率支付货币为1 人民币为1*exchangeRate',\n" +
                "  `PRINT_TKT_PENDING_DATE` TIMESTAMP COMMENT '状态变为待处理的时间',\n" +
                "  `PRINT_TKT_COMPLETED_DATE` TIMESTAMP COMMENT '状态变为已完成的时间',\n" +
                "  `EMD_FLIGHT_NO` STRING COMMENT 'EMD对应的航班号',\n" +
                "  `EMD_FARE_NAME` STRING COMMENT 'EMD对应的旅客姓名',\n" +
                "  `REFUND_MAIL_TAXPAYER_NO` STRING COMMENT '纳税人识别号',\n" +
                "  `CASH_FPF_SEASON` STRING COMMENT '季节',\n" +
                "  `REFUND_MAIL_INVOICE_TYPE` INT COMMENT '发票类型',\n" +
                "  `IS_TKEN` STRING COMMENT '是否处理Tkne',\n" +
                "  `PRINT_TKT_COMPLETED_NAME` STRING COMMENT '状态变为已完成的账号',\n" +
                "  `PRINT_MAIL_NAME` STRING COMMENT '打印邮寄快递账号',\n" +
                "  `PRINT_MAIL_DATE` TIMESTAMP COMMENT '打印邮寄快递时间',\n" +
                "  `CASH_CREDIT_SCORE` INT COMMENT '透支积分',\n" +
                "  `SHIP_EMS_MAIL_TYPE` INT COMMENT 'EMS项目',\n" +
                "  `SHIP_EMSREMARK` STRING COMMENT 'EMS备注',\n" +
                "  `APPLYFOR_MEMO` STRING COMMENT '产品备注',\n" +
                "  `APPLYFOR_PHONE` STRING COMMENT '产品联系人',\n" +
                "  `APPLYFOR_STATUS` STRING COMMENT '产品状态',\n" +
                "  `OFFICE` STRING COMMENT '产品状态',\n" +
                "  `CASH_SPECIAL_AMOUNT` DECIMAL(22, 2) COMMENT '附加其它价格',\n" +
                "  `IS_MANUAL` tinyINT COMMENT '是否手工补价',\n" +
                "  `IS_TURNDOWN` tinyINT COMMENT '是否拒绝',\n" +
                "  `REFUND_MAIL_BUILD_FEE` STRING COMMENT '民航发展基金',\n" +
                "  `REFUND_MAIL_FUEL_FEE` STRING COMMENT '燃油附加费',\n" +
                "  `IS_MANUAL_FPF` tinyINT COMMENT '是否线下常客扣减',\n" +
                "  `REFUND_MAIL_INSURE_FEE` STRING COMMENT '保险费用',\n" +
                "  `INSURE_SUC_DATE` TIMESTAMP COMMENT '购保成功时间',\n" +
                "  `INSURE_REFUNDED_DATE` TIMESTAMP COMMENT '完成退保时间',\n" +
                "  `IS_HAS_INSURE_FAIL` tinyINT COMMENT '是否包含购保失败的旅客保单',\n" +
                "  `INSURE_ALLRISKS_TYPE` INT COMMENT '综合险类型',\n" +
                "  `REISSUE_INFO2` STRING COMMENT '改期相关信息',\n" +
                "  `REISSUE_INFO` STRING COMMENT '改期相关信息1',\n" +
                "  `SUMMARY_SHORT_CODE` STRING COMMENT '短信概要内容h5Url code',\n" +
                "  `SUMMARY_URL` STRING COMMENT '概要短信内容H5url',\n" +
                "  `PRINT_REPEAT` STRING COMMENT '打印单是否二次登记',\n" +
                "  `CONTACE_BIRTH` DATE COMMENT '联系人出生日期',\n" +
                "  `CONTACT_BIRTH` DATE COMMENT '联系人出生日期',\n" +
                "  `ETL_CREATE_TIME` TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                "  `ETL_UPDATE_TIME` TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                "  `ETL_DATE` date  NULL COMMENT '数据ETL日期'" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_KFBP_BUS_ORDER',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //") WITH (\n" +
        //"    'connector' = 'jdbc',\n" +
        //"    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
        //"    'table-name' = 'T_ODS_KFBP_BUS_ORDER', -- 替换为实际的表名\n" +
        //"    'username' = '"+Constants.ODS_USER+"',\n" +
        //"    'password' = '"+Constants.ODS_PWD+"'\n" +
        //",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
        //")");
        log.info("ExtractTOdsKfbpBusOrder executeSql create table for doris end");
        String extractSql = "INSERT INTO T_ODS_KFBP_BUS_ORDER(\n" +
                " PKID,\n" +
                " ORDER_TYPE,\n" +
                " CREATE_BY,\n" +
                " CREATE_DATE,\n" +
                " CREATE_NAME,\n" +
                " UPDATE_BY,\n" +
                " UPDATE_DATE,\n" +
                " UPDATE_NAME,\n" +
                " BIG_CUS_INFO_BIG_CUS_TYPE,\n" +
                " BIG_CUS_INFO_CARD_NO,\n" +
                " BIG_CUS_INFO_DEP_NAME,\n" +
                " BIG_CUS_INFO_FIRM_NAME,\n" +
                " BIG_CUS_INFO_ID_NUMBER,\n" +
                " BIG_CUS_INFO_IS_BIG_CUS,\n" +
                " BIG_CUS_INFO_MANANGER_NAME,\n" +
                " BIG_CUS_INFO_MEET_TITLE,\n" +
                " BIG_CUS_INFO_PARBU,\n" +
                " CASH_CASH_STATUS,\n" +
                " CASH_CURRENCY,\n" +
                " CASH_DELIVERY_FEE,\n" +
                " CASH_DELIVERY_FEE_CUR,\n" +
                " CASH_FPF_CARD_NO,\n" +
                " CASH_FPF_NAME,\n" +
                " CASH_FPF_TAX_SCORE,\n" +
                " CASH_FPF_TICKET_DISCOUNT,\n" +
                " CASH_FPF_TICKET_SCORE,\n" +
                " CASH_IS_FPF,\n" +
                " CASH_IS_FPF_MIX,\n" +
                " CASH_PAID_AMOUNT,\n" +
                " CASH_PAID_AMOUNT_CUR,\n" +
                " CASH_PAY_ADDRESS,\n" +
                " CASH_TOTAL_ORDER_AMOUNT,\n" +
                " CASH_TOTAL_ORDER_AMOUNT_CUR,\n" +
                " CASH_TOTAL_PRODUCT_PRICE,\n" +
                " CASH_TOTAL_PRODUCT_PRICE_CUR,\n" +
                " CHINS_ID,\n" +
                " CONTACT_EMAIL,\n" +
                " CONTACT_ID_CODE,\n" +
                " CONTACT_MOBILE,\n" +
                " CONTACT_NAME,\n" +
                " CONTACT_PHONE,\n" +
                " CUR_TEL,\n" +
                " GOV_INFO_BIN,\n" +
                " GOV_INFO_BUGDET_NAME,\n" +
                " GOV_INFO_IS_GOV,\n" +
                " IS_URGENCY,\n" +
                " MEMO,\n" +
                " ORDER_SN,\n" +
                " ORDER_SOURCE,\n" +
                " ORDER_STATUS,\n" +
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
                " SUTMIT_USER_INFO_BUS_DEPT_ID,\n" +
                " SUTMIT_USER_INFO_DEPT_ID,\n" +
                " SUTMIT_USER_INFO_LOGIN,\n" +
                " SUTMIT_USER_INFO_NAME,\n" +
                " BUS_ORDER_STATUS,\n" +
                " BUS_ORDER_TYPE,\n" +
                " CLEAN_QUEUE_CHANGE_TYPE,\n" +
                " IS_ADD_INF,\n" +
                " OPEN_TICKET_LIMIT_DATE,\n" +
                " OT_USER_INFO_BUS_DEPT_ID,\n" +
                " OT_USER_INFO_DEPT_ID,\n" +
                " OT_USER_INFO_LOGIN,\n" +
                " OT_USER_INFO_NAME,\n" +
                " OTHER_PRODUCT_TYPE,\n" +
                " PNR,\n" +
                " PRICE_ALL_FEE,\n" +
                " PRICE_BUILD_FEE,\n" +
                " PRICE_CABIN_PRICE,\n" +
                " PRICE_CURRENCY,\n" +
                " PRICE_EXCHANGE_RATE,\n" +
                " PRICE_FUEL_FEE,\n" +
                " PRICE_OB_FEE,\n" +
                " PRICE_TICKET_PRICE,\n" +
                " PRICE_TOTAL_PRICE,\n" +
                " TICKET_TYPE,\n" +
                " TK_ORDER_STATUS,\n" +
                " TK_ORDER_TYPE,\n" +
                " ALL_RISKS_INSURE_FEE,\n" +
                " ALL_RISKS_INSURE_INSURE_COUNT,\n" +
                " ALL_RISKS_INSURE_TOTAL_FEE,\n" +
                " DELAY_INSURE_FEE,\n" +
                " DELAY_INSURE_INSURE_COUNT,\n" +
                " DELAY_INSURE_TOTAL_FEE,\n" +
                " EXPECT_INSURE_FEE,\n" +
                " EXPECT_INSURE_INSURE_COUNT,\n" +
                " EXPECT_INSURE_TOTAL_FEE,\n" +
                " FLT_COUNT,\n" +
                " INSURE_DAYS,\n" +
                " INSURE_SOURCE,\n" +
                " INSURE_STATUS,\n" +
                " OVERSEA_FEE,\n" +
                " POLICY_NO,\n" +
                " TOTAL_FEE,\n" +
                " CUR_INCOME_NAME,\n" +
                " POST_NO,\n" +
                " CHANGE_ORDER_SN,\n" +
                " CHANGE_ORDER_STATUS,\n" +
                " IS_FEE,\n" +
                " PNR_CHANGE_AFTER,\n" +
                " CASH_DEPT_GROUP_ID,\n" +
                " GAIN_USER_USER_ID,\n" +
                " SHIP_DEPT_GROUP_ID,\n" +
                " SUBMIT_USER_USER_ID,\n" +
                " UNION_ORDER_PKID,\n" +
                " OT_USER_USER_ID,\n" +
                " FARE_PKID,\n" +
                " ORDER_PKID,\n" +
                " SEAT_NO,\n" +
                " FLIGHT_PKID,\n" +
                " SEAT_ORDER_STATUS,\n" +
                " RECEIPT_TYPE,\n" +
                " CASH_PAID_TIME,\n" +
                " EMD_NO,\n" +
                " SEAT_ORDER_REFUND_PKID,\n" +
                " IS_PNR_BY_WHITE,\n" +
                " IS_FICTITIOUS_ORDER,\n" +
                " SHIP_IS_URGENCY,\n" +
                " OVERSEA_INSURE_STATUS,\n" +
                " EMD_TYPE,\n" +
                " PRODUCT_PKID,\n" +
                " CASH_FPF_TAX_PRICE,\n" +
                " IS_ONLY_ORDER,\n" +
                " CASH_SCORE_DISCOUNT,\n" +
                " CHANGE_FK_TK_ORDER_PKID,\n" +
                " REISSUE_TYPE,\n" +
                " REFUND_MAIL_INVOICE_TITLE,\n" +
                " REFUND_MAIL_REFUND,\n" +
                " REFUND_MAIL_REFUNDABLE,\n" +
                " REFUND_MAIL_ISSUE_TICKET,\n" +
                " REFUND_MAIL_DRAWER,\n" +
                " REFUND_MAIL_AGGREGATE_AMOUNT,\n" +
                " PRINT_TKT_ORIGIN,\n" +
                " LUAAGE_ORDER_STATUS,\n" +
                " EMD_SEAT_ORDER_REFUND_PKID,\n" +
                " EMD_LUAAGE_ORDER_REFUND_PKID,\n" +
                " ESS_ORDER_PKID,\n" +
                " TEAM_NUM,\n" +
                " CASH_CURRENCY_CODE,\n" +
                " CASH_EXCHANGE_RATE,\n" +
                " PRINT_TKT_PENDING_DATE,\n" +
                " PRINT_TKT_COMPLETED_DATE,\n" +
                " EMD_FLIGHT_NO,\n" +
                " EMD_FARE_NAME,\n" +
                " REFUND_MAIL_TAXPAYER_NO,\n" +
                " CASH_FPF_SEASON,\n" +
                " REFUND_MAIL_INVOICE_TYPE,\n" +
                " IS_TKEN,\n" +
                " PRINT_TKT_COMPLETED_NAME,\n" +
                " PRINT_MAIL_NAME,\n" +
                " PRINT_MAIL_DATE,\n" +
                " CASH_CREDIT_SCORE,\n" +
                " SHIP_EMS_MAIL_TYPE,\n" +
                " SHIP_EMSREMARK,\n" +
                " APPLYFOR_MEMO,\n" +
                " APPLYFOR_PHONE,\n" +
                " APPLYFOR_STATUS,\n" +
                " OFFICE,\n" +
                " CASH_SPECIAL_AMOUNT,\n" +
                " IS_MANUAL,\n" +
                " IS_TURNDOWN,\n" +
                " REFUND_MAIL_BUILD_FEE,\n" +
                " REFUND_MAIL_FUEL_FEE,\n" +
                " IS_MANUAL_FPF,\n" +
                " REFUND_MAIL_INSURE_FEE,\n" +
                " INSURE_SUC_DATE,\n" +
                " INSURE_REFUNDED_DATE,\n" +
                " IS_HAS_INSURE_FAIL,\n" +
                " INSURE_ALLRISKS_TYPE,\n" +
                " REISSUE_INFO2,\n" +
                " REISSUE_INFO,\n" +
                " SUMMARY_SHORT_CODE,\n" +
                " SUMMARY_URL,\n" +
                " PRINT_REPEAT,\n" +
                " CONTACE_BIRTH,\n" +
                " CONTACT_BIRTH,\n" +
                "ETL_CREATE_TIME,\n" +
                "ETL_UPDATE_TIME,\n" +
                "ETL_DATE)" +

                "SELECT\n" +
                " PKID,\n" +
                " ORDER_TYPE,\n" +
                " CREATE_BY,\n" +
                " CREATE_DATE,\n" +
                " CREATE_NAME,\n" +
                " UPDATE_BY,\n" +
                " UPDATE_DATE,\n" +
                " UPDATE_NAME,\n" +
                " BIG_CUS_INFO_BIG_CUS_TYPE,\n" +
                " BIG_CUS_INFO_CARD_NO,\n" +
                " BIG_CUS_INFO_DEP_NAME,\n" +
                " BIG_CUS_INFO_FIRM_NAME,\n" +
                " BIG_CUS_INFO_ID_NUMBER,\n" +
                " BIG_CUS_INFO_IS_BIG_CUS,\n" +
                " BIG_CUS_INFO_MANANGER_NAME,\n" +
                " BIG_CUS_INFO_MEET_TITLE,\n" +
                " BIG_CUS_INFO_PARBU,\n" +
                " CASH_CASH_STATUS,\n" +
                " CASH_CURRENCY,\n" +
                " CASH_DELIVERY_FEE,\n" +
                " CASH_DELIVERY_FEE_CUR,\n" +
                " CASH_FPF_CARD_NO,\n" +
                " CASH_FPF_NAME,\n" +
                " CASH_FPF_TAX_SCORE,\n" +
                " CASH_FPF_TICKET_DISCOUNT,\n" +
                " CASH_FPF_TICKET_SCORE,\n" +
                " CASH_IS_FPF,\n" +
                " CASH_IS_FPF_MIX,\n" +
                " CASH_PAID_AMOUNT,\n" +
                " CASH_PAID_AMOUNT_CUR,\n" +
                " CASH_PAY_ADDRESS,\n" +
                " CASH_TOTAL_ORDER_AMOUNT,\n" +
                " CASH_TOTAL_ORDER_AMOUNT_CUR,\n" +
                " CASH_TOTAL_PRODUCT_PRICE,\n" +
                " CASH_TOTAL_PRODUCT_PRICE_CUR,\n" +
                " CHINS_ID,\n" +
                " CONTACT_EMAIL,\n" +
                " sm4_encrypt(CONTACT_ID_CODE, '" + Constants.SM4_KEY + "') CONTACT_ID_CODE,\n" +
                " sm4_encrypt(CONTACT_MOBILE, '" + Constants.SM4_KEY + "') CONTACT_MOBILE,\n" +
                " CONTACT_NAME,\n" +
                " sm4_encrypt(CONTACT_PHONE, '" + Constants.SM4_KEY + "') CONTACT_PHONE,\n" +
                " sm4_encrypt(CUR_TEL, '" + Constants.SM4_KEY + "') CUR_TEL,\n" +
                " GOV_INFO_BIN,\n" +
                " GOV_INFO_BUGDET_NAME,\n" +
                " GOV_INFO_IS_GOV,\n" +
                " IS_URGENCY,\n" +
                " MEMO,\n" +
                " ORDER_SN,\n" +
                " ORDER_SOURCE,\n" +
                " ORDER_STATUS,\n" +
                " SHIP_ADDRESS,\n" +
                " SHIP_MEMO,\n" +
                " sm4_encrypt(SHIP_MOBILE, '" + Constants.SM4_KEY + "') SHIP_MOBILE,\n" +
                " SHIP_NAME,\n" +
                " sm4_encrypt(SHIP_PHONE, '" + Constants.SM4_KEY + "') SHIP_PHONE,\n" +
                " SHIP_SEND_DATE,\n" +
                " SHIP_SEND_TIME,\n" +
                " SHIP_SHIP_MAIL_TYPE,\n" +
                " SHIP_SHIP_STATUS,\n" +
                " SHIP_SHIP_TYPE,\n" +
                " SHIP_ZIP_CODE,\n" +
                " SUTMIT_USER_INFO_BUS_DEPT_ID,\n" +
                " SUTMIT_USER_INFO_DEPT_ID,\n" +
                " SUTMIT_USER_INFO_LOGIN,\n" +
                " SUTMIT_USER_INFO_NAME,\n" +
                " BUS_ORDER_STATUS,\n" +
                " BUS_ORDER_TYPE,\n" +
                " CLEAN_QUEUE_CHANGE_TYPE,\n" +
                " IS_ADD_INF,\n" +
                " OPEN_TICKET_LIMIT_DATE,\n" +
                " OT_USER_INFO_BUS_DEPT_ID,\n" +
                " OT_USER_INFO_DEPT_ID,\n" +
                " OT_USER_INFO_LOGIN,\n" +
                " OT_USER_INFO_NAME,\n" +
                " OTHER_PRODUCT_TYPE,\n" +
                " PNR,\n" +
                " PRICE_ALL_FEE,\n" +
                " PRICE_BUILD_FEE,\n" +
                " PRICE_CABIN_PRICE,\n" +
                " PRICE_CURRENCY,\n" +
                " PRICE_EXCHANGE_RATE,\n" +
                " PRICE_FUEL_FEE,\n" +
                " PRICE_OB_FEE,\n" +
                " PRICE_TICKET_PRICE,\n" +
                " PRICE_TOTAL_PRICE,\n" +
                " TICKET_TYPE,\n" +
                " TK_ORDER_STATUS,\n" +
                " TK_ORDER_TYPE,\n" +
                " ALL_RISKS_INSURE_FEE,\n" +
                " ALL_RISKS_INSURE_INSURE_COUNT,\n" +
                " ALL_RISKS_INSURE_TOTAL_FEE,\n" +
                " DELAY_INSURE_FEE,\n" +
                " DELAY_INSURE_INSURE_COUNT,\n" +
                " DELAY_INSURE_TOTAL_FEE,\n" +
                " EXPECT_INSURE_FEE,\n" +
                " EXPECT_INSURE_INSURE_COUNT,\n" +
                " EXPECT_INSURE_TOTAL_FEE,\n" +
                " FLT_COUNT,\n" +
                " INSURE_DAYS,\n" +
                " INSURE_SOURCE,\n" +
                " INSURE_STATUS,\n" +
                " OVERSEA_FEE,\n" +
                " POLICY_NO,\n" +
                " TOTAL_FEE,\n" +
                " CUR_INCOME_NAME,\n" +
                " POST_NO,\n" +
                " CHANGE_ORDER_SN,\n" +
                " CHANGE_ORDER_STATUS,\n" +
                " IS_FEE,\n" +
                " PNR_CHANGE_AFTER,\n" +
                " CASH_DEPT_GROUP_ID,\n" +
                " GAIN_USER_USER_ID,\n" +
                " SHIP_DEPT_GROUP_ID,\n" +
                " SUBMIT_USER_USER_ID,\n" +
                " UNION_ORDER_PKID,\n" +
                " OT_USER_USER_ID,\n" +
                " FARE_PKID,\n" +
                " ORDER_PKID,\n" +
                " SEAT_NO,\n" +
                " FLIGHT_PKID,\n" +
                " SEAT_ORDER_STATUS,\n" +
                " RECEIPT_TYPE,\n" +
                " CASH_PAID_TIME,\n" +
                " EMD_NO,\n" +
                " SEAT_ORDER_REFUND_PKID,\n" +
                " IS_PNR_BY_WHITE,\n" +
                " IS_FICTITIOUS_ORDER,\n" +
                " SHIP_IS_URGENCY,\n" +
                " OVERSEA_INSURE_STATUS,\n" +
                " EMD_TYPE,\n" +
                " PRODUCT_PKID,\n" +
                " CASH_FPF_TAX_PRICE,\n" +
                " IS_ONLY_ORDER,\n" +
                " CASH_SCORE_DISCOUNT,\n" +
                " CHANGE_FK_TK_ORDER_PKID,\n" +
                " REISSUE_TYPE,\n" +
                " REFUND_MAIL_INVOICE_TITLE,\n" +
                " REFUND_MAIL_REFUND,\n" +
                " REFUND_MAIL_REFUNDABLE,\n" +
                " REFUND_MAIL_ISSUE_TICKET,\n" +
                " REFUND_MAIL_DRAWER,\n" +
                " REFUND_MAIL_AGGREGATE_AMOUNT,\n" +
                " PRINT_TKT_ORIGIN,\n" +
                " LUAAGE_ORDER_STATUS,\n" +
                " EMD_SEAT_ORDER_REFUND_PKID,\n" +
                " EMD_LUAAGE_ORDER_REFUND_PKID,\n" +
                " ESS_ORDER_PKID,\n" +
                " TEAM_NUM,\n" +
                " CASH_CURRENCY_CODE,\n" +
                " CASH_EXCHANGE_RATE,\n" +
                " PRINT_TKT_PENDING_DATE,\n" +
                " PRINT_TKT_COMPLETED_DATE,\n" +
                " EMD_FLIGHT_NO,\n" +
                " EMD_FARE_NAME,\n" +
                " REFUND_MAIL_TAXPAYER_NO,\n" +
                " CASH_FPF_SEASON,\n" +
                " REFUND_MAIL_INVOICE_TYPE,\n" +
                " IS_TKEN,\n" +
                " PRINT_TKT_COMPLETED_NAME,\n" +
                " PRINT_MAIL_NAME,\n" +
                " PRINT_MAIL_DATE,\n" +
                " CASH_CREDIT_SCORE,\n" +
                " SHIP_EMS_MAIL_TYPE,\n" +
                " SHIP_EMSREMARK,\n" +
                " APPLYFOR_MEMO,\n" +
                " APPLYFOR_PHONE,\n" +
                " APPLYFOR_STATUS,\n" +
                " OFFICE,\n" +
                " CASH_SPECIAL_AMOUNT,\n" +
                " IS_MANUAL,\n" +
                " IS_TURNDOWN,\n" +
                " REFUND_MAIL_BUILD_FEE,\n" +
                " REFUND_MAIL_FUEL_FEE,\n" +
                " IS_MANUAL_FPF,\n" +
                " REFUND_MAIL_INSURE_FEE,\n" +
                " INSURE_SUC_DATE,\n" +
                " INSURE_REFUNDED_DATE,\n" +
                " IS_HAS_INSURE_FAIL,\n" +
                " INSURE_ALLRISKS_TYPE,\n" +
                " REISSUE_INFO2,\n" +
                " REISSUE_INFO,\n" +
                " SUMMARY_SHORT_CODE,\n" +
                " SUMMARY_URL,\n" +
                " PRINT_REPEAT,\n" +
                " CONTACE_BIRTH,\n" +
                " CONTACT_BIRTH,\n" +
                " CURRENT_TIMESTAMP AS ETL_CREATE_TIME,\n" +
                " CURRENT_TIMESTAMP AS ETL_UPDATE_TIME,\n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                " FROM BUS_ORDER "
                + " WHERE  CREATE_DATE BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  UPDATE_DATE BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;
        log.info("ExtractTOdsKfbpBusOrder executeSql extract start");
        TableResult result = tEnv.executeSql(extractSql);
        log.info("ExtractTOdsKfbpBusOrder executeSql extract end");

        result.print();

        log.info("ExtractTOdsKfbpBusOrder data transfer end");


    }

}
