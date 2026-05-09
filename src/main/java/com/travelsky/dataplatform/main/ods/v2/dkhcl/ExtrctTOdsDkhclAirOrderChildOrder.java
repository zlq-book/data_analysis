package com.travelsky.dataplatform.main.ods.v2.dkhcl;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.main.ods.v2.khbp.ExtractTOdsKfbpBusFare;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

public class ExtrctTOdsDkhclAirOrderChildOrder {

    public static Logger log = LoggerFactory.getLogger(ExtrctTOdsDkhclAirOrderChildOrder.class);
    public static void main(String[] args) throws IOException {
        log.info(" ExtrctTOdsDkhclAirOrderChildOrder data transfer start");
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
        System.out.println(" ExtrctTOdsDkhclAirOrderChildOrder etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, " ExtrctTOdsDkhclAirOrderChildOrder");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        System.out.println(" ExtrctTOdsDkhclAirOrderChildOrder executeSql create table for source start");
        tEnv.executeSql("CREATE TABLE AIR_ORDER_CHILD_ORDER (\n" +
                " ID BIGINT,\n" +
                " PNR VARCHAR(20),\n" +
                " PNRXML VARCHAR(3800),\n" +
                " CARD_TYPE VARCHAR(20),\n" +
                " CHANGE_OR_UP INT,\n" +
                " CREATE_ORDER_TYPE INT,\n" +
                " CREATE_TIME TIMESTAMP(6),\n" +
                " CREATOR_ID BIGINT,\n" +
                " CREATOR_NAME VARCHAR(30),\n" +
                " CUR_OPERATOR_ID BIGINT,\n" +
                " CUR_OPERATOR_NAME VARCHAR(30),\n" +
                " CUR_OPERATOR_TIME TIMESTAMP(6),\n" +
                " DELIVERY_TYPE VARCHAR(30),\n" +
                " EMPLOYEE_NO VARCHAR(30),\n" +
                " FN VARCHAR(10),\n" +
                " HAVING_NOSHOW INT,\n" +
                " LAST_TICKET_LIMIT TIMESTAMP(6),\n" +
                " MARK_CODE VARCHAR(100),\n" +
                " ORDER_NO VARCHAR(20),\n" +
                " ORDER_REMARK VARCHAR(200),\n" +
                " ORDER_SOURCE INT,\n" +
                " ORDER_STATUS INT,\n" +
                " ORDER_TYPE VARCHAR(20),\n" +
                " ORI_ORDER_NO VARCHAR(20),\n" +
                " PAY_BANK VARCHAR(50),\n" +
                " PAY_BANK_CODE VARCHAR(30),\n" +
                " PAY_BANK_NAME_CODE VARCHAR(30),\n" +
                " PAY_MENT VARCHAR(20),\n" +
                " PAY_NAME VARCHAR(50),\n" +
                " PAY_STATUS INT,\n" +
                " PRINT_NO VARCHAR(20),\n" +
                " REMARKS VARCHAR(500),\n" +
                " TICKET_MAN_ID BIGINT,\n" +
                " TICKET_MAN_NAME VARCHAR(30),\n" +
                " TICKET_OUT_DATE TIMESTAMP(6),\n" +
                " TICKET_TYPE INT,\n" +
                " TRAVEL_MATTERS VARCHAR(500),\n" +
                " VERSION INT,\n" +
                " APP_REMARKS VARCHAR(500),\n" +
                " APPROVE_DEPT_ID BIGINT,\n" +
                " APPROVE_DEPT_NAME VARCHAR(30),\n" +
                " APPROVE_STATUS INT,\n" +
                " BOOK_WAY VARCHAR(20),\n" +
                " CHECK_NOTE VARCHAR(200),\n" +
                " CHECK_STATUS INT,\n" +
                " COMPANY_ID BIGINT,\n" +
                " COMPANY_NAME VARCHAR(80),\n" +
                " CONTACT_TEL VARCHAR(64),\n" +
                " CUS_BIG_CODE VARCHAR(10),\n" +
                " CUST_ORDER_NO VARCHAR(30),\n" +
                " DELI_ADDRESS VARCHAR(50),\n" +
                " DELI_DATE VARCHAR(20),\n" +
                " DELIVARY_STATUS INT,\n" +
                " DELIVERY_NO VARCHAR(300),\n" +
                " DELIVERY_ORG VARCHAR(30),\n" +
                " FACE_MARK_CODE VARCHAR(100),\n" +
                " IS_URGENT_ORDER INT,\n" +
                " LOWEST_FARES VARCHAR(300),\n" +
                " NEED_CHECK INT,\n" +
                " PAY_INFO_COLLET TINYINT,\n" +
                " REVIEW_DATE TIMESTAMP(6),\n" +
                " REVIEW_ID BIGINT,\n" +
                " REVIEW_NAME VARCHAR(100),\n" +
                " REVIEW_NO VARCHAR(30),\n" +
                " SIGN_MAN VARCHAR(30),\n" +
                " YPOSITION VARCHAR(300),\n" +
                " ZIP_CODE VARCHAR(20),\n" +
                " BIG_ORDER_NO BIGINT,\n" +
                " DELI_WAY VARCHAR(20),\n" +
                " IS_HAVINSUR TINYINT,\n" +
                " DELIVERY_ROUTE_NO VARCHAR(300),\n" +
                " IS_REVOCATION TINYINT,\n" +
                " FULL_PRICE_TICKET TINYINT,\n" +
                " MANUA_STATUS TINYINT,\n" +
                " GUESTBOOK BIGINT,\n" +
                " PROVIDER TINYINT,\n" +
                " OFFICE_USER_NAME VARCHAR(20),\n" +
                " SEND_MESSAGE VARCHAR(20),\n" +
                " IS_FAIL TINYINT,\n" +
                " FAIL_MESSAGE VARCHAR(50),\n" +
                " IS_TRP VARCHAR(2),\n" +
                " IS_CANCEL_PNR INT" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.DKHCL_IP + ":" + Constants.DKHCL_PORT + "/" + Constants.DKHCL_DB + "',\n" +
                "    'table-name' = '" + Constants.DKHCL_SCHEMA + ".AIR_ORDER_CHILD_ORDER', \n" +
                "    'username' = '" + Constants.DKHCL_USER + "',\n" +
                "    'password' = '" + Constants.DKHCL_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        System.out.println(" ExtrctTOdsDkhclAirOrderChildOrder executeSql create table for source end");
        //创建Doris目标表
        System.out.println(" ExtrctTOdsDkhclAirOrderChildOrder executeSql create table for doris start");
        tEnv.executeSql("CREATE TABLE T_ODS_DKHCL_AIR_ORDER_CHILD_ORDER (\n" +
                " ID BIGINT,\n" +
                " PNR VARCHAR(60),\n" +
                " PNRXML VARCHAR(11400),\n" +
                " CARD_TYPE VARCHAR(60),\n" +
                " CHANGE_OR_UP INT,\n" +
                " CREATE_ORDER_TYPE INT,\n" +
                " CREATE_TIME TIMESTAMP(6),\n" +
                " CREATOR_ID BIGINT,\n" +
                " CREATOR_NAME VARCHAR(90),\n" +
                " CUR_OPERATOR_ID BIGINT,\n" +
                " CUR_OPERATOR_NAME VARCHAR(90),\n" +
                " CUR_OPERATOR_TIME TIMESTAMP(6),\n" +
                " DELIVERY_TYPE VARCHAR(90),\n" +
                " EMPLOYEE_NO VARCHAR(90),\n" +
                " FN VARCHAR(30),\n" +
                " HAVING_NOSHOW INT,\n" +
                " LAST_TICKET_LIMIT TIMESTAMP(6),\n" +
                " MARK_CODE VARCHAR(300),\n" +
                " ORDER_NO VARCHAR(60),\n" +
                " ORDER_REMARK VARCHAR(600),\n" +
                " ORDER_SOURCE INT,\n" +
                " ORDER_STATUS INT,\n" +
                " ORDER_TYPE VARCHAR(60),\n" +
                " ORI_ORDER_NO VARCHAR(60),\n" +
                " PAY_BANK VARCHAR(150),\n" +
                " PAY_BANK_CODE VARCHAR(90),\n" +
                " PAY_BANK_NAME_CODE VARCHAR(90),\n" +
                " PAY_MENT VARCHAR(60),\n" +
                " PAY_NAME VARCHAR(150),\n" +
                " PAY_STATUS INT,\n" +
                " PRINT_NO VARCHAR(60),\n" +
                " REMARKS VARCHAR(1500),\n" +
                " TICKET_MAN_ID BIGINT,\n" +
                " TICKET_MAN_NAME VARCHAR(90),\n" +
                " TICKET_OUT_DATE TIMESTAMP(6),\n" +
                " TICKET_TYPE INT,\n" +
                " TRAVEL_MATTERS VARCHAR(1500),\n" +
                " VERSION INT,\n" +
                " APP_REMARKS VARCHAR(1500),\n" +
                " APPROVE_DEPT_ID BIGINT,\n" +
                " APPROVE_DEPT_NAME VARCHAR(90),\n" +
                " APPROVE_STATUS INT,\n" +
                " BOOK_WAY VARCHAR(60),\n" +
                " CHECK_NOTE VARCHAR(600),\n" +
                " CHECK_STATUS INT,\n" +
                " COMPANY_ID BIGINT,\n" +
                " COMPANY_NAME VARCHAR(240),\n" +
                " CONTACT_TEL VARCHAR(192),\n" +
                " CUS_BIG_CODE VARCHAR(30),\n" +
                " CUST_ORDER_NO VARCHAR(90),\n" +
                " DELI_ADDRESS VARCHAR(150),\n" +
                " DELI_DATE VARCHAR(60),\n" +
                " DELIVARY_STATUS INT,\n" +
                " DELIVERY_NO VARCHAR(900),\n" +
                " DELIVERY_ORG VARCHAR(90),\n" +
                " FACE_MARK_CODE VARCHAR(300),\n" +
                " IS_URGENT_ORDER INT,\n" +
                " LOWEST_FARES VARCHAR(900),\n" +
                " NEED_CHECK INT,\n" +
                " PAY_INFO_COLLET TINYINT,\n" +
                " REVIEW_DATE TIMESTAMP(6),\n" +
                " REVIEW_ID BIGINT,\n" +
                " REVIEW_NAME VARCHAR(300),\n" +
                " REVIEW_NO VARCHAR(90),\n" +
                " SIGN_MAN VARCHAR(90),\n" +
                " YPOSITION VARCHAR(900),\n" +
                " ZIP_CODE VARCHAR(60),\n" +
                " BIG_ORDER_NO BIGINT,\n" +
                " DELI_WAY VARCHAR(60),\n" +
                " IS_HAVINSUR TINYINT,\n" +
                " DELIVERY_ROUTE_NO VARCHAR(900),\n" +
                " IS_REVOCATION TINYINT,\n" +
                " FULL_PRICE_TICKET TINYINT,\n" +
                " MANUA_STATUS TINYINT,\n" +
                " GUESTBOOK BIGINT,\n" +
                " PROVIDER TINYINT,\n" +
                " OFFICE_USER_NAME VARCHAR(60),\n" +
                " SEND_MESSAGE VARCHAR(60),\n" +
                " IS_FAIL TINYINT,\n" +
                " FAIL_MESSAGE VARCHAR(150),\n" +
                " IS_TRP VARCHAR(6),\n" +
                " IS_CANCEL_PNR INT,\n" +
                " ETL_CREATE_TIME TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                " ETL_UPDATE_TIME TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                " ETL_DATE date  NULL COMMENT '数据ETL日期'" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "',\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "',\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_DKHCL_AIR_ORDER_CHILD_ORDER',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "',\n" +
                " 'sink.properties.read_json_by_line' = 'true',\n" +
                " 'sink.properties.format' = 'json',\n" +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        System.out.println(" ExtrctTOdsDkhclAirOrderChildOrder executeSql create table for doris end");
        String extractSql = "INSERT INTO T_ODS_DKHCL_AIR_ORDER_CHILD_ORDER(\n" +
                " ID,\n" +
                " PNR,\n" +
                " PNRXML,\n" +
                " CARD_TYPE,\n" +
                " CHANGE_OR_UP,\n" +
                " CREATE_ORDER_TYPE,\n" +
                " CREATE_TIME,\n" +
                " CREATOR_ID,\n" +
                " CREATOR_NAME,\n" +
                " CUR_OPERATOR_ID,\n" +
                " CUR_OPERATOR_NAME,\n" +
                " CUR_OPERATOR_TIME,\n" +
                " DELIVERY_TYPE,\n" +
                " EMPLOYEE_NO,\n" +
                " FN,\n" +
                " HAVING_NOSHOW,\n" +
                " LAST_TICKET_LIMIT,\n" +
                " MARK_CODE,\n" +
                " ORDER_NO,\n" +
                " ORDER_REMARK,\n" +
                " ORDER_SOURCE,\n" +
                " ORDER_STATUS,\n" +
                " ORDER_TYPE,\n" +
                " ORI_ORDER_NO,\n" +
                " PAY_BANK,\n" +
                " PAY_BANK_CODE,\n" +
                " PAY_BANK_NAME_CODE,\n" +
                " PAY_MENT,\n" +
                " PAY_NAME,\n" +
                " PAY_STATUS,\n" +
                " PRINT_NO,\n" +
                " REMARKS,\n" +
                " TICKET_MAN_ID,\n" +
                " TICKET_MAN_NAME,\n" +
                " TICKET_OUT_DATE,\n" +
                " TICKET_TYPE,\n" +
                " TRAVEL_MATTERS,\n" +
                " VERSION,\n" +
                " APP_REMARKS,\n" +
                " APPROVE_DEPT_ID,\n" +
                " APPROVE_DEPT_NAME,\n" +
                " APPROVE_STATUS,\n" +
                " BOOK_WAY,\n" +
                " CHECK_NOTE,\n" +
                " CHECK_STATUS,\n" +
                " COMPANY_ID,\n" +
                " COMPANY_NAME,\n" +
                " CONTACT_TEL,\n" +
                " CUS_BIG_CODE,\n" +
                " CUST_ORDER_NO,\n" +
                " DELI_ADDRESS,\n" +
                " DELI_DATE,\n" +
                " DELIVARY_STATUS,\n" +
                " DELIVERY_NO,\n" +
                " DELIVERY_ORG,\n" +
                " FACE_MARK_CODE,\n" +
                " IS_URGENT_ORDER,\n" +
                " LOWEST_FARES,\n" +
                " NEED_CHECK,\n" +
                " PAY_INFO_COLLET,\n" +
                " REVIEW_DATE,\n" +
                " REVIEW_ID,\n" +
                " REVIEW_NAME,\n" +
                " REVIEW_NO,\n" +
                " SIGN_MAN,\n" +
                " YPOSITION,\n" +
                " ZIP_CODE,\n" +
                " BIG_ORDER_NO,\n" +
                " DELI_WAY,\n" +
                " IS_HAVINSUR,\n" +
                " DELIVERY_ROUTE_NO,\n" +
                " IS_REVOCATION,\n" +
                " FULL_PRICE_TICKET,\n" +
                " MANUA_STATUS,\n" +
                " GUESTBOOK,\n" +
                " PROVIDER,\n" +
                " OFFICE_USER_NAME,\n" +
                " SEND_MESSAGE,\n" +
                " IS_FAIL,\n" +
                " FAIL_MESSAGE,\n" +
                " IS_TRP,\n" +
                " IS_CANCEL_PNR,\n" +
                " ETL_CREATE_TIME,\n" +
                " ETL_UPDATE_TIME,\n" +
                " ETL_DATE)" +

                " SELECT\n" +
                " ID,\n" +
                " PNR,\n" +
                " PNRXML,\n" +
                " CARD_TYPE,\n" +
                " CHANGE_OR_UP,\n" +
                " CREATE_ORDER_TYPE,\n" +
                " CREATE_TIME,\n" +
                " CREATOR_ID,\n" +
                " CREATOR_NAME,\n" +
                " CUR_OPERATOR_ID,\n" +
                " CUR_OPERATOR_NAME,\n" +
                " CUR_OPERATOR_TIME,\n" +
                " DELIVERY_TYPE,\n" +
                " EMPLOYEE_NO,\n" +
                " FN,\n" +
                " HAVING_NOSHOW,\n" +
                " LAST_TICKET_LIMIT,\n" +
                " MARK_CODE,\n" +
                " ORDER_NO,\n" +
                " ORDER_REMARK,\n" +
                " ORDER_SOURCE,\n" +
                " ORDER_STATUS,\n" +
                " ORDER_TYPE,\n" +
                " ORI_ORDER_NO,\n" +
                " PAY_BANK,\n" +
                " PAY_BANK_CODE,\n" +
                " PAY_BANK_NAME_CODE,\n" +
                " PAY_MENT,\n" +
                " PAY_NAME,\n" +
                " PAY_STATUS,\n" +
                " PRINT_NO,\n" +
                " REMARKS,\n" +
                " TICKET_MAN_ID,\n" +
                " TICKET_MAN_NAME,\n" +
                " TICKET_OUT_DATE,\n" +
                " TICKET_TYPE,\n" +
                " TRAVEL_MATTERS,\n" +
                " VERSION,\n" +
                " APP_REMARKS,\n" +
                " APPROVE_DEPT_ID,\n" +
                " APPROVE_DEPT_NAME,\n" +
                " APPROVE_STATUS,\n" +
                " BOOK_WAY,\n" +
                " CHECK_NOTE,\n" +
                " CHECK_STATUS,\n" +
                " COMPANY_ID,\n" +
                " COMPANY_NAME,\n" +
                " CONTACT_TEL,\n" +
                " CUS_BIG_CODE,\n" +
                " CUST_ORDER_NO,\n" +
                " DELI_ADDRESS,\n" +
                " DELI_DATE,\n" +
                " DELIVARY_STATUS,\n" +
                " DELIVERY_NO,\n" +
                " DELIVERY_ORG,\n" +
                " FACE_MARK_CODE,\n" +
                " IS_URGENT_ORDER,\n" +
                " LOWEST_FARES,\n" +
                " NEED_CHECK,\n" +
                " PAY_INFO_COLLET,\n" +
                " REVIEW_DATE,\n" +
                " REVIEW_ID,\n" +
                " REVIEW_NAME,\n" +
                " REVIEW_NO,\n" +
                " SIGN_MAN,\n" +
                " YPOSITION,\n" +
                " ZIP_CODE,\n" +
                " BIG_ORDER_NO,\n" +
                " DELI_WAY,\n" +
                " IS_HAVINSUR,\n" +
                " DELIVERY_ROUTE_NO,\n" +
                " IS_REVOCATION,\n" +
                " FULL_PRICE_TICKET,\n" +
                " MANUA_STATUS,\n" +
                " GUESTBOOK,\n" +
                " PROVIDER,\n" +
                " OFFICE_USER_NAME,\n" +
                " SEND_MESSAGE,\n" +
                " IS_FAIL,\n" +
                " FAIL_MESSAGE,\n" +
                " IS_TRP,\n" +
                " IS_CANCEL_PNR,\n" +
                " CURRENT_TIMESTAMP AS ETL_CREATE_TIME,\n" +
                " CURRENT_TIMESTAMP AS ETL_UPDATE_TIME,\n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                " FROM AIR_ORDER_CHILD_ORDER \n" +
                " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' ";
        System.out.println(" ExtrctTOdsDkhclAirOrderChildOrder executeSql extract start");
        TableResult result = tEnv.executeSql(extractSql);
        System.out.println(" ExtrctTOdsDkhclAirOrderChildOrder executeSql extract end");

        result.print();

        System.out.println(" ExtrctTOdsDkhclAirOrderChildOrder data transfer end");


    }

}
