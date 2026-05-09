package com.travelsky.dataplatform.constans;

import com.travelsky.dataplatform.utils.GetTableSql;

public class TrpCreateToSql {
    public static final String UPLOAD_B2C_ORDERINFO = "CREATE TABLE T_ODS_SC_UPLOAD_B2C_ORDERINFO (\n" +
            "ROW_ID VARCHAR(100) ,\n" +
            "ID BIGINT ,\n" +
            "ORDER_NO VARCHAR(54) ,\n" +
            "BANK_ORDER_NO VARCHAR(210) ,\n" +
            "VOUCHER_NO VARCHAR(30) ,\n" +
            "PNR VARCHAR(18) ,\n" +
            "TICKET_NO VARCHAR(42) ,\n" +
            "FLIGHT_NO VARCHAR(30) ,\n" +
            "UP_LOCATION VARCHAR(60) ,\n" +
            "DEST_LOCATION VARCHAR(60) ,\n" +
            "CABIN VARCHAR(15) ,\n" +
            "TAKEOFF_TIME TIMESTAMP(6) ,\n" +
            "PASS_NAME VARCHAR(108) ,\n" +
            "ID_NO VARCHAR(256) ,\n" +
            "ORIGINAL_FARE DECIMAL(10,2) ,\n" +
            "DISCOUNT_FARE DECIMAL(10,2) ,\n" +
            "CTAX DECIMAL(10,2) ,\n" +
            "FTAX DECIMAL(10,2) ,\n" +
            "FARE_OTHER DECIMAL(10,2) ,\n" +
            "INSURANCE DECIMAL(10,2) ,\n" +
            "FARE_TOTAL DECIMAL(10,2) ,\n" +
            "PRODUCT_NAME VARCHAR(60) ,\n" +
            "ORDER_DATE TIMESTAMP(6) ,\n" +
            "ORDER_STATUS VARCHAR(60) ,\n" +
            "ORDER_SOURCE DECIMAL(1,0) ,\n" +
            "ORDER_USER VARCHAR(150) ,\n" +
            "REGISTER_USER VARCHAR(150) ,\n" +
            "ORDER_MOBILE VARCHAR(60) ,\n" +
            "ORDER_EMAIL VARCHAR(256) ,\n" +
            "BANK_NAME VARCHAR(90) ,\n" +
            "R_CARRIER VARCHAR(12) ,\n" +
            "GUEST_SCODE VARCHAR(300) ,\n" +
            "CONTACT_NAME VARCHAR(90) ,\n" +
            "MEM_NUM VARCHAR(256) ,\n" +
            "FILE_DATE VARCHAR(900) ,\n" +
            "ETL_INSERT_TIME TIMESTAMP(6) ,\n" +
            "FLIGHT_SEGMENT VARCHAR(300) ,\n" +
            "FLIGHT_SEGMENT_CODE VARCHAR(120) ,\n" +
            "TK_TIME TIMESTAMP(6) ,\n" +
            "ORDER_CHANNEL VARCHAR(120) ,\n" +
            "PAY_TIME TIMESTAMP(6) ,\n" +
            "TK_PROCEDURE VARCHAR(90) ,\n" +
            "PASSENGER_TYPE VARCHAR(30) ,\n" +
            "ID_TYPE VARCHAR(90) ,\n" +
            "TRIP_TYPE VARCHAR(30) ,\n" +
            "D_OR_I VARCHAR(30) ,\n" +
            "REDUCTION VARCHAR(30) ,\n" +
            "COUPON_NO VARCHAR(90) ,\n" +
            "CONN_MOBILE VARCHAR(256) ,\n" +
            "SITE VARCHAR(30) ,\n" +
            "`LANGUAGE` VARCHAR(30) ,\n" +
            "CURRENCY_TYPE VARCHAR(15) ,\n" +
            "TEL_PHONE VARCHAR(60) ,\n" +
            "COUPON_NAME VARCHAR(150) ,\n" +
            "COUPON_FARE VARCHAR(30) ,\n" +
            "COUPON_NUM VARCHAR(120) ,\n" +
            "FARE_ZHIJIAN VARCHAR(30) ,\n" +
            "FARE_JTCXZJ VARCHAR(30) ,\n" +
            "IS_JTCXZJ VARCHAR(60) ,\n" +
            "FAREBASIS VARCHAR(120) ,\n" +
            "IS_ZSBX VARCHAR(12) ,\n" +
            "NAME_ZSBX VARCHAR(300) ,\n" +
            "IS_ZSKQ VARCHAR(12) ,\n" +
            "  `ETL_CREATE_TIME` TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
            "  `ETL_UPDATE_TIME` TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
            "  `ETL_DATE` date  NULL COMMENT '数据ETL日期'" +
            ") WITH (\n" +

            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_SC_UPLOAD_B2C_ORDERINFO", Constants.ODS_USER, Constants.ODS_PWD)

            + ")";
    public static final String SC_SALE_DETAIL = "CREATE TABLE T_ODS_TRP_SC_SALE_DETAIL (\n" +
            "  `ID` bigint ,\n" +
            "  `ORDER_NUMBER` varchar(500) ,\n" +
            "  `BANK_ORDER_NUMBER` varchar(500) ,\n" +
            "  `PNR` varchar(500) ,\n" +
            "  `TICKET_NUMBER` varchar(500) ,\n" +
            "  `FLIGHT_NUMBER` varchar(500)  ,\n" +
            "  `SEGMENT_THREE_CODE` varchar(500)  ,\n" +
            "  `SEGMENT` varchar(500) ,\n" +
            "  `CABIN` varchar(500)  ,\n" +
            "  `DEPARTURE_TIME` varchar(500) ,\n" +
            "  `PASSENGER_NAME` varchar(500)  ,\n" +
            "  `PRICE_BEFORE_DISCOUNT` decimal(20,2)  ,\n" +
            "  `PRICE_AFTER_DISCOUNT` decimal(20,2)  ,\n" +
            "  `AIRPORT_CONSTRUCTION_FEE` decimal(20,2)  ,\n" +
            "  `FUEL_SURCHARGE` decimal(20,2)  ,\n" +
            "  `OTHER_TAX` decimal(20,2)  ,\n" +
            "  `INSURANCE_FEE` decimal(20,2)  ,\n" +
            "  `AMOUNT_PAYABLE` decimal(20,2)  ,\n" +
            "  `PRODUCT_NAME` varchar(500)  ,\n" +
            "  `BOOKING_DATE` varchar(500)  ,\n" +
            "  `TICKETING_DATE` varchar(500)  ,\n" +
            "  `BOOKING_STATUS` varchar(500)  ,\n" +
            "  `ORDER_SOURCE` varchar(500)  ,\n" +
            "  `ORDER_CHANNEL` varchar(500)  ,\n" +
            "  `BOOKING_USERNAME` varchar(500)  ,\n" +
            "  `REGISTERED_USER_NAME` varchar(500)  ,\n" +
            "  `BOOKING_USER_PHONE` varchar(500)  ,\n" +
            "  `BOOKING_USER_EMAIL` varchar(500)  ,\n" +
            "  `BANK_NAME` varchar(500)  ,\n" +
            "  `ACTUAL_CARRIER` varchar(500)  ,\n" +
            "  `BIG_CUSTOMER_NUMBER` varchar(500)  ,\n" +
            "  `FREQUENT_FLYER_CARD_NO` varchar(500)  ,\n" +
            "  `PAYMENT_TIME` varchar(500)  ,\n" +
            "  `PURCHASE_PROCESS` varchar(500)  ,\n" +
            "  `PASSENGER_TYPE` varchar(500)  ,\n" +
            "  `ID_TYPE` varchar(500)  ,\n" +
            "  `ID_NUMBER` varchar(500)  ,\n" +
            "  `ITINERARY_TYPE` varchar(500)  ,\n" +
            "  `TICKET_NATURE` varchar(500)  ,\n" +
            "  `IS_DIRECT_DISCOUNT` varchar(500)  ,\n" +
            "  `CONTACT_NAME` varchar(500)  ,\n" +
            "  `CONTACT_PHONE` varchar(500)  ,\n" +
            "  `SITE` varchar(500)  ,\n" +
            "  `LANGUAGE` varchar(500)  ,\n" +
            "  `CURRENCY` varchar(500)  ,\n" +
            "  `COUPON_NAME` varchar(500)  ,\n" +
            "  `COUPON_AMOUNT` decimal(20,2)  ,\n" +
            "  `COUPON_CODE` varchar(500)  ,\n" +
            "  `VOUCHER_CODE` varchar(500)  ,\n" +
            "  `DIRECT_DISCOUNT_AMOUNT` decimal(20,2)  ,\n" +
            "  `STATIC_PROMOTION_DIRECT_DISCOUNT_AMOUNT` decimal(20,2)  ,\n" +
            "  `IS_STATIC_PROMOTION_DIRECT_DISCOUNT` varchar(500)  ,\n" +
            "  `FARE_BASIS` varchar(500)  ,\n" +
            "  `IS_GIFT_INSURANCE` varchar(500)  ,\n" +
            "  `GIFT_INSURANCE_NAME` varchar(500)  ,\n" +
            "  `IS_GIFT_COUPON` varchar(500)  ,\n" +
            "  `IS_MEMBER_EXCLUSIVE_DIRECT_DISCOUNT` varchar(500)  ,\n" +
            "  `IS_REAL_NAME_EXCLUSIVE_DIRECT_DISCOUNT` varchar(500)  ,\n" +
            "  `AIRLINE` varchar(500)  ,\n" +
            "  `MILEAGE_DEDUCTION_AMOUNT` decimal(20,2)  ,\n" +
            "  `ETL_CREATE_TIME` TIMESTAMP(6)  ,\n" +
            "  `ETL_UPDATE_TIME` TIMESTAMP(6)  ,\n" +
            "  `ETL_DATE` date " +
            ") WITH (\n" +

            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_TRP_SC_SALE_DETAIL", Constants.ODS_USER, Constants.ODS_PWD)

            + ")";
    public static final String AIR_REFUND_DETAIL = "CREATE TABLE T_ODS_TRP_AIR_REFUND_DETAIL (\n" +
            "  `ID` bigint ,\n" +
            "  `ORDER_DATE` varchar(500)  ,\n" +
            "  `ORDER_NUMBER` varchar(500)  ,\n" +
            "  `PNR` varchar(500)  ,\n" +
            "  `TICKET_NATURE` varchar(500)  ,\n" +
            "  `TICKET_NUMBER` varchar(500)  ,\n" +
            "  `PASSENGER_NAME` varchar(500)  ,\n" +
            "  `SEGMENT_SEQUENCE_NUMBER` varchar(500)  ,\n" +
            "  `FLIGHT_NUMBER` varchar(500)  ,\n" +
            "  `PAYMENT_BANK` varchar(500)  ,\n" +
            "  `BANK_ORDER_NUMBER` varchar(500)  ,\n" +
            "  `PAYMENT_AMOUNT` decimal(20,2)  ,\n" +
            "  `REFUND_AMOUNT` decimal(20,2)  ,\n" +
            "  `TICKET_REFUND_AMOUNT` decimal(20,2)  ,\n" +
            "  `TICKET_REFUND_RATE` decimal(10,6)  ,\n" +
            "  `TICKET_REFUND_FEE` decimal(20,2)  ,\n" +
            "  `AIRPORT_CONSTRUCTION_REFUND` decimal(20,2)  ,\n" +
            "  `FUEL_SURCHARGE_REFUND` decimal(20,2)  ,\n" +
            "  `OTHER_TAX_REFUND` decimal(20,2)  ,\n" +
            "  `TOTAL_TAX_REFUND` decimal(20,2)  ,\n" +
            "  `TOTAL_TAX_REFUND_FEE` decimal(20,2)  ,\n" +
            "  `INSURANCE_REFUND_AMOUNT` decimal(20,2)  ,\n" +
            "  `SECOND_REVIEW_EXECUTOR` varchar(500)  ,\n" +
            "  `SECOND_REVIEW_TIME` varchar(500)  ,\n" +
            "  `SECOND_REVIEW_OPINION` varchar(500)  ,\n" +
            "  `REFUND_EXECUTOR` varchar(500)  ,\n" +
            "  `TICKET_REFUND_COMPLETION_TIME` varchar(500)  ,\n" +
            "  `REFUND_REVIEW_OPINION` varchar(500)  ,\n" +
            "  `PAYMENT_TIME` varchar(500)  ,\n" +
            "  `TICKET_REFUND_NATURE` varchar(500)  ,\n" +
            "  `TICKET_REFUND_APPLICANT` varchar(500)  ,\n" +
            "  `TICKET_REFUND_APPLICATION_TIME` varchar(500)  ,\n" +
            "  `PNR_CANCELLATION_TIME` varchar(500)  ,\n" +
            "  `TICKET_REFUND_REASON` varchar(500)  ,\n" +
            "  `CURRENCY` varchar(500)  ,\n" +
            "  `SETTLEMENT_NUMBER` varchar(500)  ,\n" +
            "  `TICKET_REFUND_NUMBER` varchar(500)  ,\n" +
            "  `TICKET_REFUND_STATUS` varchar(500)  ,\n" +
            "  `CHANNEL` varchar(500)  ,\n" +
            "  `SITE` varchar(500)  ,\n" +
            "  `TICKET_REFUND_TIME` varchar(500)  ,\n" +
            "  `REFUND_INITIATION_TIME` varchar(500)  ,\n" +
            "  `TICKET_TYPE` varchar(500)  ,\n" +
            "  `POINTS_REFUND_STATUS` varchar(500)  ,\n" +
            "  `MILEAGE_PAYMENT_ORDER_NUMBER` varchar(500)  ,\n" +
            "  `PAID_MILEAGE` decimal(20,2)  ,\n" +
            "  `REFUNDED_MILEAGE` decimal(20,2)  ,\n" +
            "  `MILEAGE_FEE` decimal(20,2)  ,\n" +
            "  `POINTS_REFUND_EXECUTOR` varchar(500)  ,\n" +
            "  `POINTS_REFUND_COMPLETION_TIME` varchar(500)  ,\n" +
            "  `POINTS_REFUND_REVIEW_OPINION` varchar(500)  ,\n" +
            "  `TICKET_REFUND_APPLICATION_CHANNEL` varchar(500)  ,\n" +
            "  `DEPARTURE_DATE` varchar(500)  ,\n" +
            "  `DEPARTURE_CITY_CODE` varchar(500)  ,\n" +
            "  `ARRIVAL_CITY_CODE` varchar(500)  ,\n" +
            "  `ACCOUNT_USERNAME` varchar(500)  ,\n" +
            "  `ETL_CREATE_TIME` TIMESTAMP(6)  ,\n" +
            "  `ETL_UPDATE_TIME` TIMESTAMP(6)  ,\n" +
            "  `ETL_DATE` date  " +
            ") WITH (\n" +
            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_TRP_AIR_REFUND_DETAIL", Constants.ODS_USER, Constants.ODS_PWD)
            + ")";

    public static final String COUPON_SALE_DETAIL = "CREATE TABLE T_ODS_TRP_COUPON_SALE_DETAIL (\n" +
            "  `ID` bigint,\n" +
            "  `ORDER_NUMBER` varchar(500),\n" +
            "  `BANK_ORDER_NUMBER` varchar(500),\n" +
            "  `BANK_NAME` varchar(500),\n" +
            "  `CHANNEL` varchar(500),\n" +
            "  `CONTACT_NAME` varchar(500),\n" +
            "  `CONTACT_PHONE_NUMBER` varchar(500),\n" +
            "  `ORDER_DATE` varchar(500),\n" +
            "  `ORDER_STATUS` varchar(500),\n" +
            "  `VOUCHER_CODE` varchar(500),\n" +
            "  `VOUCHER_NAME` varchar(500),\n" +
            "  `VOUCHER_AMOUNT` decimal(20,2),\n" +
            "  `PAYMENT_AMOUNT` decimal(20,2),\n" +
            "  `PAYMENT_DATE` varchar(500),\n" +
            "  `PAYMENT_STATUS` varchar(500),\n" +
            "  `USERNAME` varchar(500),\n" +
            "  `ETL_CREATE_TIME` TIMESTAMP(6),\n" +
            "  `ETL_UPDATE_TIME` TIMESTAMP(6),\n" +
            "  `ETL_DATE` date\n" +
            ") WITH (\n" +
            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_TRP_COUPON_SALE_DETAIL", Constants.ODS_USER, Constants.ODS_PWD)
            + ")";

    public static final String COUPON_REFUND_DETAIL = "CREATE TABLE T_ODS_TRP_COUPON_REFUND_DETAIL (\n" +
            "  `ID` bigint,\n" +
            "  `ORDER_NUMBER` varchar(500),\n" +
            "  `BANK_ORDER_NUMBER` varchar(500),\n" +
            "  `BANK_NAME` varchar(500),\n" +
            "  `CHANNEL` varchar(500),\n" +
            "  `CONTACT_NAME` varchar(500),\n" +
            "  `CONTACT_PHONE_NUMBER` varchar(500),\n" +
            "  `ORDER_DATE` varchar(500),\n" +
            "  `ORDER_STATUS` varchar(500),\n" +
            "  `VOUCHER_CODE` varchar(500),\n" +
            "  `VOUCHER_NAME` varchar(500),\n" +
            "  `VOUCHER_AMOUNT` decimal(20,2),\n" +
            "  `PAYMENT_AMOUNT` decimal(20,2),\n" +
            "  `REFUND_AMOUNT` decimal(20,2),\n" +
            "  `REFUND_DATE` varchar(500),\n" +
            "  `REFUND_STATUS` varchar(500),\n" +
            "  `USERNAME` varchar(500),\n" +
            "  `ETL_CREATE_TIME` TIMESTAMP(6),\n" +
            "  `ETL_UPDATE_TIME` TIMESTAMP(6),\n" +
            "  `ETL_DATE` date\n" +
            ") WITH (\n" +
            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_TRP_COUPON_REFUND_DETAIL", Constants.ODS_USER, Constants.ODS_PWD)
            + ")";

    public static final String INSURANCE_SALE_DETAIL = "CREATE TABLE T_ODS_TRP_INSURANCE_SALE_DETAIL (\n" +
            "  ID bigint ,\n" +
            "ORDER_NUMBER varchar(500) ,\n" +
            "PASSENGER_NAME varchar(500) ,\n" +
            "BOOKING_TIME varchar(500) ,\n" +
            "BANK_ORDER_NUMBER varchar(500) ,\n" +
            "BANK_NAME varchar(500) ,\n" +
            "PNR varchar(500) ,\n" +
            "TICKET_NUMBER varchar(500) ,\n" +
            "TICKET_NATURE varchar(500) ,\n" +
            "ACCOUNT_USERNAME varchar(500) ,\n" +
            "DEPARTURE_DATE varchar(500) ,\n" +
            "DEPARTURE_CITY varchar(500) ,\n" +
            "ARRIVAL_CITY varchar(500) ,\n" +
            "ID_TYPE varchar(500) ,\n" +
            "ID_NUMBER varchar(500) ,\n" +
            "INSURANCE_POLICY_NUMBER varchar(500) ,\n" +
            "INSURANCE_FEE decimal(20,2) ,\n" +
            "PAYMENT_TIME varchar(500) ,\n" +
            "INSURANCE_PURCHASE_SUCCESS_TIME varchar(500) ,\n" +
            "INSURANCE_PURCHASE_TYPE varchar(500) ,\n" +
            "INSURANCE_STATUS varchar(500) ,\n" +
            "REMARKS varchar(500) ,\n" +
            "INSURANCE_TYPE varchar(500) ,\n" +
            "CHANNEL_SOURCE varchar(500) ,\n" +
            "INSURANCE_PURCHASE_STATUS varchar(500) ,\n" +
            "INSURANCE_PURCHASE_INITIATION_TIME varchar(500) ,\n" +
            "INSURANCE_PURCHASE_FAILURE_TIME varchar(500) ,\n" +
            "SITE varchar(500) ,\n" +
            "CURRENCY varchar(500) ,\n" +
            "AIRLINE_ORDER_NUMBER varchar(500) ,\n" +
            "ETL_CREATE_TIME TIMESTAMP(6) ,\n" +
            "ETL_UPDATE_TIME TIMESTAMP(6) ,\n" +
            "ETL_DATE date " +
            ") WITH (\n" +
            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_TRP_INSURANCE_SALE_DETAIL", Constants.ODS_USER, Constants.ODS_PWD)
            + ")";
}
