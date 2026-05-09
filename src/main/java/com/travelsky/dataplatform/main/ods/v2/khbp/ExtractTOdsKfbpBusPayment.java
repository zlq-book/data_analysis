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
public class ExtractTOdsKfbpBusPayment {
    public static Logger log = LoggerFactory.getLogger(ExtractTOdsKfbpBusPayment.class);
    public static void main(String[] args) throws IOException {
        log.info("ExtractTOdsKfbpBusPayment data transfer start");
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
        log.info("ExtractTOdsKfbpBusPayment etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsKfbpBusPayment");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        log.info("ExtractTOdsKfbpBusPayment executeSql create table for source start");
        tEnv.executeSql("CREATE TABLE BUS_PAYMENT (\n" +
                "    PKID VARCHAR(255),\n" +
                "    CREATE_BY VARCHAR(40),\n" +
                "    CREATE_DATE TIMESTAMP,\n" +
                "    CREATE_NAME VARCHAR(40),\n" +
                "    UPDATE_BY VARCHAR(40),\n" +
                "    UPDATE_DATE TIMESTAMP,\n" +
                "    UPDATE_NAME VARCHAR(40),\n" +
                "    AUTH_ID VARCHAR(32),\n" +
                "    BANK_NAME VARCHAR(100),\n" +
                "    BATCH_NUM VARCHAR(32),\n" +
                "    CARD_BIND_TEL VARCHAR(16),\n" +
                "    CARD_CITY VARCHAR(32),\n" +
                "    CARD_CVV VARCHAR(64),\n" +
                "    CARD_IDENTY_CODE VARCHAR(255),\n" +
                "    CARD_NO VARCHAR(255),\n" +
                "    CARD_PROVINCE VARCHAR(32),\n" +
                "    CARD_VALID_THRU VARCHAR(60),\n" +
                "    CUS_ID_CARD VARCHAR(24),\n" +
                "    CUS_NAME VARCHAR(60),\n" +
                "    IS_IVR tinyint,\n" +
                "    IS_OLD_CUS tinyint,\n" +
                "    IVR_STATUS INT,\n" +
                "    MEMO VARCHAR(2000),\n" +
                "    PAID_TIME TIMESTAMP,\n" +
                "    PAY_ID_TYPE INT,\n" +
                "    PAYMENT_METHOD INT,\n" +
                "    PAYMENT_OPERATE_STATUS INT,\n" +
                "    PAYMENT_SN VARCHAR(16),\n" +
                "    PAYMENT_STATUS INT,\n" +
                "    PAYMENT_TYPE INT,\n" +
                "    REMITS_BANK_NAME VARCHAR(100),\n" +
                "    REMITS_NAME VARCHAR(100),\n" +
                "    REMITS_TIME VARCHAR(100),\n" +
                "    RETURN_CODE VARCHAR(300),\n" +
                "    RETURN_RESULT VARCHAR(1000),\n" +
                "    SUB_USER_INFO_BUS_DEPT_ID INT,\n" +
                "    SUB_USER_INFO_DEPT_ID INT,\n" +
                "    SUB_USER_INFO_LOGIN VARCHAR(255),\n" +
                "    SUB_USER_INFO_NAME VARCHAR(255),\n" +
                "    SUBMIT_TIME TIMESTAMP,\n" +
                "    TOTAL_AMOUNT DECIMAL(8, 2),\n" +
                "    TRADE_NO VARCHAR(50),\n" +
                "    SUB_USER_USER_ID INT,\n" +
                "    UNION_ORDER_PKID VARCHAR(255),\n" +
                "    PAYMENT_CHANNEL VARCHAR(20),\n" +
                "    REPEAL_TIME TIMESTAMP,\n" +
                "    WEIXIN_URL VARCHAR(300),\n" +
                "    AGENCY_PAY_URL VARCHAR(1000),\n" +
                "    AGENCY_PAY_OPENID VARCHAR(500),\n" +
                "    BILL_ADDRESS VARCHAR(200),\n" +
                "    BILL_CITY VARCHAR(200),\n" +
                "    BILL_COUNTRY_CODE VARCHAR(8),\n" +
                "    BILL_EMAIL VARCHAR(200),\n" +
                "    BILL_FIRST_NAME VARCHAR(50),\n" +
                "    BILL_LAST_NAME VARCHAR(50),\n" +
                "    BILL_POSTAL_CODE VARCHAR(50),\n" +
                "    BILL_STATE VARCHAR(50),\n" +
                "    ALIPAY_NO VARCHAR(255),\n" +
                "    REMIT_METHOD VARCHAR(20)\n" +

                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.KHBP_IP + ":" + Constants.KHBP_PORT + "/" + Constants.KHBP_DB + "',\n" +
                "    'table-name' = '" + Constants.KHBP_SCHEMA + ".BUS_PAYMENT', \n" +
                "    'username' = '" + Constants.KHBP_USER + "',\n" +
                "    'password' = '" + Constants.KHBP_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        log.info("ExtractTOdsKfbpBusPayment executeSql create table for source end");
        //创建Doris目标表
        log.info("ExtractTOdsKfbpBusPayment executeSql create table for doris start");
        tEnv.executeSql("CREATE TABLE `T_ODS_KFBP_BUS_PAYMENT` (\n" +
                "  `PKID` VARCHAR(1275) NOT NULL COMMENT '主键',\n" +
                "  `CREATE_BY` VARCHAR(200) COMMENT '创建人ID',\n" +
                "  `CREATE_DATE` TIMESTAMP(3) COMMENT '创建时间',\n" +
                "  `CREATE_NAME` VARCHAR(200) COMMENT '创建人名称',\n" +
                "  `UPDATE_BY` VARCHAR(200) COMMENT '修改人',\n" +
                "  `UPDATE_DATE` TIMESTAMP(3) COMMENT '修改时间',\n" +
                "  `UPDATE_NAME` VARCHAR(200) COMMENT '修改人姓名',\n" +
                "  `AUTH_ID` VARCHAR(160) COMMENT '授权号',\n" +
                "  `BANK_NAME` VARCHAR(500) COMMENT '银行名称',\n" +
                "  `BATCH_NUM` VARCHAR(160) COMMENT '批次号',\n" +
                "  `CARD_BIND_TEL` VARCHAR(80) COMMENT '卡绑定电话',\n" +
                "  `CARD_CITY` VARCHAR(160) COMMENT '卡开户市',\n" +
                "  `CARD_CVV` VARCHAR(320) COMMENT 'CVV',\n" +
                "  `CARD_IDENTY_CODE` VARCHAR(1275) COMMENT '验证码,密码',\n" +
                "  `CARD_NO` VARCHAR(1275) COMMENT '支付卡号',\n" +
                "  `CARD_PROVINCE` VARCHAR(160) COMMENT '卡开户省',\n" +
                "  `CARD_VALID_THRU` VARCHAR(300) COMMENT '有效期',\n" +
                "  `CUS_ID_CARD` VARCHAR(120) COMMENT '持卡人证件号',\n" +
                "  `CUS_NAME` VARCHAR(300) COMMENT '持卡人姓名',\n" +
                "  `IS_IVR` tinyint COMMENT '是否使用IVR',\n" +
                "  `IS_OLD_CUS` tinyint COMMENT '是否老客户',\n" +
                "  `IVR_STATUS` INT COMMENT '提交ivr状态',\n" +
                "  `MEMO` VARCHAR(10000) COMMENT '备注',\n" +
                "  `PAID_TIME` TIMESTAMP(3) COMMENT '成功支付时间',\n" +
                "  `PAY_ID_TYPE` INT COMMENT '持卡人证件类型',\n" +
                "  `PAYMENT_METHOD` INT COMMENT '平台支付类型',\n" +
                "  `PAYMENT_OPERATE_STATUS` INT COMMENT '操作状态',\n" +
                "  `PAYMENT_SN` VARCHAR(80) COMMENT '系统产生的支付编号',\n" +
                "  `PAYMENT_STATUS` INT COMMENT '支付状态',\n" +
                "  `PAYMENT_TYPE` INT COMMENT '支付方式',\n" +
                "  `REMITS_BANK_NAME` VARCHAR(500) COMMENT '汇款银行',\n" +
                "  `REMITS_NAME` VARCHAR(500) COMMENT '汇款人',\n" +
                "  `REMITS_TIME` VARCHAR(500) COMMENT '汇款时间',\n" +
                "  `RETURN_CODE` VARCHAR(1500) COMMENT '支付返回结果代码',\n" +
                "  `RETURN_RESULT` VARCHAR(5000) COMMENT '支付返回结果',\n" +
                "  `SUB_USER_INFO_BUS_DEPT_ID` INT COMMENT '用户登陆名称',\n" +
                "  `SUB_USER_INFO_DEPT_ID` INT COMMENT '用户所属当前部门ID',\n" +
                "  `SUB_USER_INFO_LOGIN` VARCHAR(1275) COMMENT '用户登陆名称',\n" +
                "  `SUB_USER_INFO_NAME` VARCHAR(1275) COMMENT '用户名',\n" +
                "  `SUBMIT_TIME` TIMESTAMP(3) COMMENT '提交支付时间',\n" +
                "  `TOTAL_AMOUNT` DECIMAL(10, 2) COMMENT '支付金额',\n" +
                "  `TRADE_NO` VARCHAR(250) COMMENT '第三方支付交易号',\n" +
                "  `SUB_USER_USER_ID` INT COMMENT '提交人',\n" +
                "  `UNION_ORDER_PKID` VARCHAR(1275) COMMENT '统一收款单ID',\n" +
                "  `PAYMENT_CHANNEL` VARCHAR(100) COMMENT '统一支付平台的支付通道',\n" +
                "  `REPEAL_TIME` TIMESTAMP(3) COMMENT '成功撤销时间',\n" +
                "  `WEIXIN_URL` VARCHAR(1500) COMMENT '生成微信二维码URL(对外映射url)',\n" +
                "  `AGENCY_PAY_URL` VARCHAR(5000) COMMENT '生成付款链接url',\n" +
                "  `AGENCY_PAY_OPENID` VARCHAR(2500) COMMENT '生成付款链接微信支付需要的openid',\n" +
                "  `BILL_ADDRESS` VARCHAR(1000) COMMENT '账单地址',\n" +
                "  `BILL_CITY` VARCHAR(1000) COMMENT '账单地址市',\n" +
                "  `BILL_COUNTRY_CODE` VARCHAR(40) COMMENT '账单地址国家编码',\n" +
                "  `BILL_EMAIL` VARCHAR(1000) COMMENT '账单邮箱',\n" +
                "  `BILL_FIRST_NAME` VARCHAR(250) COMMENT '账单姓',\n" +
                "  `BILL_LAST_NAME` VARCHAR(250) COMMENT '账单名',\n" +
                "  `BILL_POSTAL_CODE` VARCHAR(250) COMMENT '账单邮编',\n" +
                "  `BILL_STATE` VARCHAR(250) COMMENT '账单州',\n" +
                "  `ALIPAY_NO` VARCHAR(1275) COMMENT '支付宝号',\n" +
                "  `REMIT_METHOD` VARCHAR(100) COMMENT '线下支付方式'," +
                "  `ETL_CREATE_TIME` TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                "  `ETL_UPDATE_TIME` TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                "  `ETL_DATE` date  NULL COMMENT '数据ETL日期'" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_KFBP_BUS_PAYMENT',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //") WITH (\n" +
        //"    'connector' = 'jdbc',\n" +
        //"    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
        //"    'table-name' = 'T_ODS_KFBP_BUS_PAYMENT', -- 替换为实际的表名\n" +
        //"    'username' = '"+Constants.ODS_USER+"',\n" +
        //"    'password' = '"+Constants.ODS_PWD+"'\n" +
        //",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
        //")");
        log.info("ExtractTOdsKfbpBusPayment executeSql create table for doris end");
        String extractSql = "INSERT INTO `T_ODS_KFBP_BUS_PAYMENT` \n" +
                " (`PKID`, \n" +
                " `CREATE_BY`, \n" +
                " `CREATE_DATE`, \n" +
                " `CREATE_NAME`, \n" +
                " `UPDATE_BY`, \n" +
                " `UPDATE_DATE`, \n" +
                " `UPDATE_NAME`, \n" +
                " `AUTH_ID`, \n" +
                " `BANK_NAME`, \n" +
                " `BATCH_NUM`, \n" +
                " `CARD_BIND_TEL`, \n" +
                " `CARD_CITY`, \n" +
                " `CARD_CVV`, \n" +
                " `CARD_IDENTY_CODE`, \n" +
                " `CARD_NO`, \n" +
                " `CARD_PROVINCE`, \n" +
                " `CARD_VALID_THRU`, \n" +
                " `CUS_ID_CARD`, \n" +
                " `CUS_NAME`, \n" +
                " `IS_IVR`, \n" +
                " `IS_OLD_CUS`, \n" +
                " `IVR_STATUS`, \n" +
                " `MEMO`, \n" +
                " `PAID_TIME`, \n" +
                " `PAY_ID_TYPE`, \n" +
                " `PAYMENT_METHOD`, \n" +
                " `PAYMENT_OPERATE_STATUS`, \n" +
                " `PAYMENT_SN`, \n" +
                " `PAYMENT_STATUS`, \n" +
                " `PAYMENT_TYPE`, \n" +
                " `REMITS_BANK_NAME`, \n" +
                " `REMITS_NAME`, \n" +
                " `REMITS_TIME`, \n" +
                " `RETURN_CODE`, \n" +
                " `RETURN_RESULT`, \n" +
                " `SUB_USER_INFO_BUS_DEPT_ID`, \n" +
                " `SUB_USER_INFO_DEPT_ID`, \n" +
                " `SUB_USER_INFO_LOGIN`, \n" +
                " `SUB_USER_INFO_NAME`, \n" +
                " `SUBMIT_TIME`, \n" +
                " `TOTAL_AMOUNT`, \n" +
                " `TRADE_NO`, \n" +
                " `SUB_USER_USER_ID`, \n" +
                " `UNION_ORDER_PKID`, \n" +
                " `PAYMENT_CHANNEL`, \n" +
                " `REPEAL_TIME`, \n" +
                " `WEIXIN_URL`, \n" +
                " `AGENCY_PAY_URL`, \n" +
                " `AGENCY_PAY_OPENID`, \n" +
                " `BILL_ADDRESS`, \n" +
                " `BILL_CITY`, \n" +
                " `BILL_COUNTRY_CODE`, \n" +
                " `BILL_EMAIL`, \n" +
                " `BILL_FIRST_NAME`, \n" +
                " `BILL_LAST_NAME`, \n" +
                " `BILL_POSTAL_CODE`, \n" +
                " `BILL_STATE`, \n" +
                " `ALIPAY_NO`, \n" +
                " `REMIT_METHOD`, \n" +
                " `ETL_CREATE_TIME`, \n" +
                " `ETL_UPDATE_TIME`, \n" +
                " `ETL_DATE`) " +

                "SELECT\n" +
                " PKID,\n" +
                " CREATE_BY,\n" +
                " CREATE_DATE,\n" +
                " CREATE_NAME,\n" +
                " UPDATE_BY,\n" +
                " UPDATE_DATE,\n" +
                " UPDATE_NAME,\n" +
                " AUTH_ID,\n" +
                " BANK_NAME,\n" +
                " BATCH_NUM,\n" +
                " sm4_encrypt(CARD_BIND_TEL, '" + Constants.SM4_KEY + "') CARD_BIND_TEL,\n" +
                " CARD_CITY,\n" +
                " CARD_CVV,\n" +
                " CARD_IDENTY_CODE,\n" +
                " CARD_NO,\n" +
                " CARD_PROVINCE,\n" +
                " CARD_VALID_THRU,\n" +
                " sm4_encrypt(CUS_ID_CARD, '" + Constants.SM4_KEY + "') CUS_ID_CARD,\n" +
                " CUS_NAME,\n" +
                " IS_IVR,\n" +
                " IS_OLD_CUS,\n" +
                " IVR_STATUS,\n" +
                " MEMO,\n" +
                " PAID_TIME,\n" +
                " PAY_ID_TYPE,\n" +
                " PAYMENT_METHOD,\n" +
                " PAYMENT_OPERATE_STATUS,\n" +
                " PAYMENT_SN,\n" +
                " PAYMENT_STATUS,\n" +
                " PAYMENT_TYPE,\n" +
                " REMITS_BANK_NAME,\n" +
                " REMITS_NAME,\n" +
                " REMITS_TIME,\n" +
                " RETURN_CODE,\n" +
                " RETURN_RESULT,\n" +
                " SUB_USER_INFO_BUS_DEPT_ID,\n" +
                " SUB_USER_INFO_DEPT_ID,\n" +
                " SUB_USER_INFO_LOGIN,\n" +
                " SUB_USER_INFO_NAME,\n" +
                " SUBMIT_TIME,\n" +
                " TOTAL_AMOUNT,\n" +
                " TRADE_NO,\n" +
                " SUB_USER_USER_ID,\n" +
                " UNION_ORDER_PKID,\n" +
                " PAYMENT_CHANNEL,\n" +
                " REPEAL_TIME,\n" +
                " WEIXIN_URL,\n" +
                " AGENCY_PAY_URL,\n" +
                " AGENCY_PAY_OPENID,\n" +
                " BILL_ADDRESS,\n" +
                " BILL_CITY,\n" +
                " BILL_COUNTRY_CODE,\n" +
                " BILL_EMAIL,\n" +
                " BILL_FIRST_NAME,\n" +
                " BILL_LAST_NAME,\n" +
                " BILL_POSTAL_CODE,\n" +
                " BILL_STATE,\n" +
                " ALIPAY_NO,\n" +
                " REMIT_METHOD,\n" +
                " CURRENT_TIMESTAMP AS ETL_CREATE_TIME,\n" +
                " CURRENT_TIMESTAMP AS ETL_UPDATE_TIME,\n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                " FROM BUS_PAYMENT "
                + " WHERE  CREATE_DATE BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  UPDATE_DATE BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;
        log.info("ExtractTOdsKfbpBusPayment executeSql extract start");
        TableResult result = tEnv.executeSql(extractSql);
        log.info("ExtractTOdsKfbpBusPayment executeSql extract end");

        result.print();

        log.info("ExtractTOdsKfbpBusPayment data transfer end");


    }

}
