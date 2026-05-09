package com.travelsky.dataplatform.constans;

import com.travelsky.dataplatform.utils.GetTableSql;

public class DkhclCreateToSql {
    public static final String AIR_ORDER_CHILD_ORDER = "CREATE TABLE T_ODS_DKHCL_AIR_ORDER_CHILD_ORDER (\n" +
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

            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_DKHCL_AIR_ORDER_CHILD_ORDER", Constants.ODS_USER, Constants.ODS_PWD)

            + ")";
}
