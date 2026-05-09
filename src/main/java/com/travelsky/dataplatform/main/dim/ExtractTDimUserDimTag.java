package com.travelsky.dataplatform.main.dim;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据需求-智能标签体系-用户标签
 * @date 2025/8/1
 */
public class ExtractTDimUserDimTag {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = "JUzgwCrDIT6v4SMg+BMX4A==";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);

//创建上游数据源表
        /*tEnv.executeSql("CREATE TABLE T_DIM_USER_DIM (\n" +
                "  PK_ID varchar(256) NOT NULL,\n" +
                "  CRM_CUSTOMER_ID varchar(32),\n" +
                "  LY_VIP_ID varchar(32),\n" +
                "  LY_MEMBER_ID varchar(32),\n" +
                "  SYS_REGISTER_ID varchar(32),\n" +
                "  FFP_REGISTER_ID varchar(32),\n" +
                "  HISTORICAL_FFP_ID varchar(32),\n" +
                "  ALIPAY_ID varchar(64),\n" +
                "  WECHAT_ID varchar(64),\n" +
                "  DOUYIN_ID varchar(64),\n" +
                "  CN_NAME varchar(100),\n" +
                "  EN_NAME varchar(100),\n" +
                "  SEX char(1),\n" +
                "  USER_TYPE varchar(20),\n" +
                "  BIRTHDAY datetime(3),\n" +
                "  PROVINCE varchar(32),\n" +
                "  CITY varchar(32),\n" +
                "  NATIONALITY varchar(64),\n" +
                "  ETHNICITY varchar(32),\n" +
                "  EMPLOYER varchar(128),\n" +
                "  MOBILE_PHONE varchar(128),\n" +
                "  MOBILE_NUMBER_RELIABILITY varchar(32),\n" +
                "  EMAIL varchar(256),\n" +
                "  ID_CARD varchar(128),\n" +
                "  PASSPORT varchar(128),\n" +
                "  SEAMAN_ID varchar(128),\n" +
                "  ALIEN_PERMIT varchar(128),\n" +
                "  DIPLOMATIC_STAFF_CERTIFICATE varchar(128),\n" +
                "  PERMANENT_RESIDENT_ID varchar(128),\n" +
                "  CIVILIAN_STAFF_ID varchar(128),\n" +
                "  STAFF_ID varchar(128),\n" +
                "  OFFICER_ID_CARD varchar(128),\n" +
                "  ARMED_POLICE_OFFICER varchar(128),\n" +
                "  ARMED_POLICE_SOLDIER varchar(128),\n" +
                "  CIVILIAN_OFFICIAL_ID varchar(128),\n" +
                "  CONSCRIPT_SOLDIER_ID varchar(128),\n" +
                "  NON_COMMISSIONED_OFFICER_ID varchar(128),\n" +
                "  HK_MACAO_RESIDENT_PERMIT varchar(128),\n" +
                "  TAIWAN_RESIDENT_TRAVEL_PERMIT varchar(128),\n" +
                "  BLIND_PASSENGER boolean,\n" +
                "  DEAF_PASSENGER boolean,\n" +
                "  IS_DIRECT_USER boolean,\n" +
                "  IS_OFFICIAL_WEBSITE_NON_REGISTERED boolean,\n" +
                "  IS_DOUYIN_CARD_PURCHASER boolean,\n" +
                "  DIRECT_USER_STATUS varchar(20),\n" +
                "  DIRECT_REGISTER_DATE date,\n" +
                "  LAST_LOGIN_TIME datetime(3),\n" +
                "  DIRECT_VERIFIED_FLAG boolean,\n" +
                "  DIRECT_VERIFY_DATE date,\n" +
                "  IS_BLACKLIST_USER boolean,\n" +
                "  STUDENT_FLAG boolean,\n" +
                "  TEACHER_FLAG boolean,\n" +
                "  AGENT_FLAG boolean,\n" +
                "  IS_KEY_ACCOUNT boolean,\n" +
                "  KEY_ACCOUNT_NUMBER varchar(32),\n" +
                "  IS_FREQUENT_TRAVELER boolean,\n" +
                "  FREQUENT_TRAVELER_CARDNO varchar(128),\n" +
                "  FREQUENT_TRAVELER_LEVEL varchar(64),\n" +
                "  YJ_CARD_NUMBER varchar(128),\n" +
                "  YJ_CARD_EXPIREDATE date,\n" +
                "  DEV_CHANNEL_ONE varchar(64),\n" +
                "  DEV_CHANNEL_TWO varchar(64),\n" +
                "  DEV_CHANNEL_THREE varchar(64),\n" +
                "  DEV_CHANNEL_FOUR varchar(64),\n" +
                "  FT_REGISTER_TIME datetime(3),\n" +
                "  ACCEPT_SMS_MARKETING boolean,\n" +
                "  ACCEPT_EMAIL_MARKETING boolean,\n" +
                "  PARENT_FT_CARDNO varchar(128),\n" +
                "  IS_LY_USER boolean,\n" +
                "  LY_REGISTER_TIME datetime(3),\n" +
                "  LY_CARD_NUMBER varchar(128),\n" +
                "  LY_USER_LEVEL varchar(20),\n" +
                "  LY_USER_STATUS varchar(20),\n" +
                "  LY_REGISTER_STATUS varchar(20),\n" +
                "  LY_VERIFY_STATUS varchar(20),\n" +
                "  LY_LIFETIME_POINTS int,\n" +
                "  LY_AVAILABLE_POINTS int,\n" +
                "  IS_HIGH_TRAVELER boolean,\n" +
                "  HIGH_TRAVELER_TYPE varchar(20),\n" +
                "  HIGH_TRAVELER_TIER varchar(20),\n" +
                "  TIER_PRESTIGE_EXPIREDATE date,\n" +
                "  TIER_HONOR_EXPIREDATE date,\n" +
                "  HIGH_TRAVELER_DS varchar(20),\n" +
                "  IS_YJ_PERSONNEL boolean,\n" +
                "  VIP_FLAG varchar(64),\n" +
                "  CIP_FLAG varchar(64),\n" +
                "  VVIP_FLAG varchar(64),\n" +
                "  FOOD_PREFERENCE varchar(128),\n" +
                "  SEAT_PREFERENCE varchar(64),\n" +
                "  TEMP_FOOD_PREFERENCE varchar(128),\n" +
                "  TEMP_SEAT_PREFERENCE varchar(64),\n" +
                "  BEVERAGE_PREFERENCE varchar(128),\n" +
                "  LONG_TERM_SEAT_PREFERENCE varchar(64),\n" +
                "  FIRST_CLASS_LOUNGE_PREFERENCE varchar(64),\n" +
                "  MERGED_TO_USERID varchar(256),\n" +
                "  IS_VALID_USER boolean,\n" +
                "  IS_FREQUENT_FLYER_NUMBER_VERIFIED boolean,\n" +
                "  CREATE_TIME datetime(3),\n" +
                "  UPDATE_TIME datetime(3),\n" +
                //新增字段部分
                "HAS_REGISTERED_FREQUENT_TRAVELER VARCHAR(5),\n" +
                "LAST_REGISTRATION_TIME_FREQUENT_TRAVELER DATE,\n" +
                "MILEAGE_EXCHANGE_COUNT_12M_AIR INT,\n" +
                "MILEAGE_EXCHANGE_AMOUNT_12M_AIR DECIMAL(10,1),\n" +
                "MILEAGE_EXCHANGE_COUNT_12M_NON_AIR INT,\n" +
                "MILEAGE_EXCHANGE_AMOUNT_12M_NON_AIR DECIMAL(10,1),\n" +
                "AVERAGE_ADVANCE_PURCHASE_TIME_LAST_12M DECIMAL(10,1),\n" +
                "AVERAGE_ADVANCE_CHECK_IN_TIME_LAST_12M DECIMAL(10,1),\n" +
                "ANCILLARY_PRODUCT_PREFERENCE_CATEGORY VARCHAR(50),\n" +
                "INSURANCE_PURCHASE_COUNT_LAST_12M INT,\n" +
                "PAID_SEAT_SELECTION_PURCHASE_COUNT_LAST_12M INT,\n" +
                "PREPAID_BAGGAGE_PURCHASE_COUNT_LAST_12M INT,\n" +
                "UPGRADE_VOUCHER_USAGE_COUNT_LAST_12M INT,\n" +
                "IN_FLIGHT_UPGRADE_COUNT_BUSINESS_LAST_12M INT,\n" +
                "IN_FLIGHT_UPGRADE_COUNT_PREMIUM_ECONOMY_LAST_12M INT,\n" +
                "BOARDING_GATE_UPGRADE_COUNT_BUSINESS_LAST_12M INT,\n" +
                "BOARDING_GATE_UPGRADE_COUNT_PREMIUM_ECONOMY_LAST_12M INT,\n" +
                "UPGRADE_VOUCHER_USAGE_AMOUNT_LAST_12M DECIMAL(18,2),\n" +
                "IN_FLIGHT_UPGRADE_AMOUNT_BUSINESS_LAST_12M DECIMAL(18,2),\n" +
                "IN_FLIGHT_UPGRADE_AMOUNT_PREMIUM_ECONOMY_LAST_12M DECIMAL(18,2),\n" +
                "BOARDING_GATE_UPGRADE_AMOUNT_BUSINESS_LAST_12M DECIMAL(18,2),\n" +
                "BOARDING_GATE_UPGRADE_AMOUNT_PREMIUM_ECONOMY_LAST_12M DECIMAL(18,2),\n" +
                "TOTAL_UPGRADE_VOUCHER_USAGE_COUNT INT,\n" +
                "TOTAL_IN_FLIGHT_UPGRADE_COUNT_PREMIUM_ECONOMY INT,\n" +
                "TOTAL_IN_FLIGHT_UPGRADE_COUNT_BUSINESS INT,\n" +
                "TOTAL_BOARDING_GATE_UPGRADE_COUNT_PREMIUM_ECONOMY INT,\n" +
                "TOTAL_BOARDING_GATE_UPGRADE_COUNT_BUSINESS INT,\n" +
                "TOTAL_UPGRADE_VOUCHER_USAGE_AMOUNT DECIMAL(18,2),\n" +
                "TOTAL_IN_FLIGHT_UPGRADE_AMOUNT_PREMIUM_ECONOMY DECIMAL(18,2),\n" +
                "TOTAL_IN_FLIGHT_UPGRADE_AMOUNT_BUSINESS DECIMAL(18,2),\n" +
                "TOTAL_UPGRADE_CABIN_PREFERENCE VARCHAR(20),\n" +
                "UPGRADE_CABIN_PREFERENCE_LAST_12M VARCHAR(20),\n" +
                "COUNTRY_VISITED_COUNT INT,\n" +
                "CITY_VISITED_COUNT INT,\n" +
                "MOST_COMMON_DOMESTIC_DEPARTURE_LOCATION_ALL_TIME VARCHAR(50),\n" +
                "MOST_COMMON_INTERNATIONAL_DEPARTURE_LOCATION_ALL_TIME VARCHAR(50),\n" +
                "MOST_COMMON_DOMESTIC_ARRIVAL_LOCATION_ALL_TIME VARCHAR(50),\n" +
                "MOST_COMMON_INTERNATIONAL_ARRIVAL_LOCATION_ALL_TIME VARCHAR(50),\n" +
                "TOTAL_TRAVEL_SEGMENTS_ALL_TIME INT,\n" +
                "SUSPECTED_AGENT VARCHAR(5),\n" +
                "IS_HIGH_VALUE_FREQUENT_TRAVELER_MEMBER VARCHAR(5),\n" +
                "CUSTOMER_LIFE_CYCLE_LABEL_DIRECT_SALES VARCHAR(30),\n" +
                "CUSTOMER_CHURN_LABEL VARCHAR(30),\n" +
                "CUSTOMER_FLIGHT_PURCHASE_RATE_REPURCHASE_RATE DECIMAL(5,2),\n" +
                "PAYMENT_METHOD_PREFERENCE VARCHAR(20),\n" +
                "FLIGHT_BOOKING_COUNT_LAST_30_DAYS INT,\n" +
                "TOTAL_TICKET_ISSUANCE_SEGMENTS_ALL_TIME INT,\n" +
                "TOTAL_TICKET_ISSUANCE_AMOUNT_INTERNATIONAL_ALL_TIME DECIMAL(18,2),\n" +
                "TOTAL_TICKET_ISSUANCE_AMOUNT_DOMESTIC_ALL_TIME DECIMAL(18,2),\n" +
                "TOTAL_INSURANCE_PRODUCT_PURCHASE_AMOUNT_ALL_TIME DECIMAL(18,2),\n" +
                "TOTAL_PAID_SEAT_SELECTION_PRODUCT_PURCHASE_AMOUNT_ALL_TIME DECIMAL(18,2),\n" +
                "TOTAL_PREPAID_BAGGAGE_PRODUCT_PURCHASE_AMOUNT_ALL_TIME DECIMAL(18,2),\n" +
                "CABIN_CLASS_PREFERENCE_LAST_1_YEAR VARCHAR(20),\n" +
                "TICKET_PURCHASE_SEGMENTS_COUNT_LAST_1_YEAR INT,\n" +
                "TICKET_PURCHASE_AMOUNT_INTERNATIONAL_LAST_1_YEAR DECIMAL(18,2),\n" +
                "TICKET_PURCHASE_AMOUNT_DOMESTIC_LAST_1_YEAR DECIMAL(18,2),\n" +
                "INSURANCE_PRODUCT_PURCHASE_AMOUNT_LAST_1_YEAR DECIMAL(18,2),\n" +
                "PAID_SEAT_SELECTION_PRODUCT_PURCHASE_AMOUNT_LAST_1_YEAR DECIMAL(18,2),\n" +
                "PREPAID_BAGGAGE_PRODUCT_PURCHASE_AMOUNT_LAST_1_YEAR DECIMAL(18,2),\n" +
                "GROUP_TICKET_PURCHASE_ISSUANCE_COUNT_LAST_1_YEAR INT,\n" +
                "GROUP_TICKET_PURCHASE_ISSUANCE_COUNT_ALL_TIME INT,\n" +
                "TOTAL_DOMESTIC_TICKET_FACE_VALUE_CARRIED_BY_SHANDONG_AIRLINES_LAST_3_YEARS DECIMAL(18,2),\n" +
                "TOTAL_INTERNATIONAL_TICKET_FACE_VALUE_CARRIED_BY_SHANDONG_AIRLINES_LAST_3_YEARS DECIMAL(18,2),\n" +
                "TOTAL_INSURANCE_PRODUCT_CONSUMPTION_AMOUNT_LAST_3_YEARS DECIMAL(18,2),\n" +
                "TOTAL_PAID_SEAT_SELECTION_PRODUCT_CONSUMPTION_AMOUNT_LAST_3_YEARS DECIMAL(18,2),\n" +
                "TOTAL_PREPAID_BAGGAGE_PRODUCT_CONSUMPTION_AMOUNT_LAST_3_YEARS DECIMAL(18,2),\n" +
                "TOTAL_UNACCOMPANIED_MINOR_EMD_PRODUCT_CONSUMPTION_AMOUNT_LAST_3_YEARS DECIMAL(18,2),\n" +
                "FLIGHT_CONSUMPTION_RANK_LAST_3_YEARS INT,\n" +
                "TOTAL_CONSUMPTION_AMOUNT_ALL_TIME DECIMAL(18,2),\n" +
                "TOTAL_CONSUMPTION_RANK_ALL_TIME INT,\n" +
                "FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS INT,\n" +
                "CUSTOMER_CONTRIBUTION VARCHAR(50),\n" +
                "TRAVELER_SOURCE_LOCATION VARCHAR(50),\n" +
                "TRAVELER_PREFERRED_DESTINATION_CITY VARCHAR(50),\n" +
                "TRAVELER_DEPARTURE_UPGRADE_PREFERENCE INT,\n" +
                "TRAVELER_DOMESTIC_TICKET_DISCOUNT_DEVIATION DECIMAL(5,2),\n" +
                "FREQUENT_TRAVELER_VALUE DECIMAL(18,2),\n" +
                "TRAVELER_COMPREHENSIVE_VALUE DECIMAL(18,2),\n" +
                "TRAVELER_CONSUMPTION_VALUE DECIMAL(18,2),\n" +
                "TRAVELER_FLIGHT_ACTIVITY_LEVEL INT,\n" +
                "HAS_MULTIPLE_FREQUENT_TRAVELER_CARD_NUMBERS VARCHAR(5),\n" +
                "HAS_MULTIPLE_CUSTOMER_IDS VARCHAR(5),\n" +
                "SUMMER_TRAVEL_COUNT_ALL_TIME INT,\n" +
                "SPRING_FESTIVAL_TRAVEL_COUNT_ALL_TIME INT,\n" +
                "HOLIDAY_TRAVEL_COUNT_ALL_TIME INT,\n" +
                "TRAVEL_SEGMENTS_WITH_ELDERLY_ALL_TIME INT,\n" +
                "TRAVEL_SEGMENTS_WITH_CHILDREN_ALL_TIME INT,\n" +
                "SELF_BOOKING_TRAVEL_SEGMENTS_ALL_TIME INT,\n" +
                "COUPON_USAGE_AMOUNT_LAST_12M DECIMAL(18,2),\n" +
                "COUPON_USAGE_COUNT_LAST_12M INT\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.DIM_DB + "',\n" +
                "    'table-name' = 'T_DIM_USER_DIM', -- 替换为实际的表名\n" +
                "    'username' = '" + Constants.DIM_USER + "',\n" +
                "    'password' = '" + Constants.DIM_PWD + "'\n" +
                ")");*/
        //数据抽取sql
        /*String extractSql =
                "INSERT INTO T_DIM_USER_DIM(" +
                        "  `PK_ID`,\n" +
                        "  `CRM_CUSTOMER_ID`,\n" +
                        "  `LY_VIP_ID`,\n" +
                        "  `LY_MEMBER_ID`,\n" +
                        "  `SYS_REGISTER_ID`,\n" +
                        "  `FFP_REGISTER_ID`,\n" +
                        "  `HISTORICAL_FFP_ID`,\n" +
                        "  `ALIPAY_ID`,\n" +
                        "  `WECHAT_ID`,\n" +
                        "  `DOUYIN_ID`,\n" +
                        "  `CN_NAME`,\n" +
                        "  `EN_NAME`,\n" +
                        "  `SEX`,\n" +
                        "  `USER_TYPE`,\n" +
                        "  `BIRTHDAY`,\n" +
                        "  `PROVINCE`,\n" +
                        "  `CITY`,\n" +
                        "  `NATIONALITY`,\n" +
                        "  `ETHNICITY`,\n" +
                        "  `EMPLOYER`,\n" +
                        "  `MOBILE_PHONE`,\n" +
                        "  `MOBILE_NUMBER_RELIABILITY`,\n" +
                        "  `EMAIL`,\n" +
                        "  `ID_CARD`,\n" +
                        "  `PASSPORT`,\n" +
                        "  `SEAMAN_ID`,\n" +
                        "  `ALIEN_PERMIT`,\n" +
                        "  `DIPLOMATIC_STAFF_CERTIFICATE`,\n" +
                        "  `PERMANENT_RESIDENT_ID`,\n" +
                        "  `CIVILIAN_STAFF_ID`,\n" +
                        "  `STAFF_ID`,\n" +
                        "  `OFFICER_ID_CARD`,\n" +
                        "  `ARMED_POLICE_OFFICER`,\n" +
                        "  `ARMED_POLICE_SOLDIER`,\n" +
                        "  `CIVILIAN_OFFICIAL_ID`,\n" +
                        "  `CONSCRIPT_SOLDIER_ID`,\n" +
                        "  `NON_COMMISSIONED_OFFICER_ID`,\n" +
                        "  `HK_MACAO_RESIDENT_PERMIT`,\n" +
                        "  `TAIWAN_RESIDENT_TRAVEL_PERMIT`,\n" +
                        "  `BLIND_PASSENGER`,\n" +
                        "  `DEAF_PASSENGER`,\n" +
                        "  `IS_DIRECT_USER`,\n" +
                        "  `IS_OFFICIAL_WEBSITE_NON_REGISTERED`,\n" +
                        "  `IS_DOUYIN_CARD_PURCHASER`,\n" +
                        "  `DIRECT_USER_STATUS`,\n" +
                        "  `DIRECT_REGISTER_DATE`,\n" +
                        "  `LAST_LOGIN_TIME`,\n" +
                        "  `DIRECT_VERIFIED_FLAG`,\n" +
                        "  `DIRECT_VERIFY_DATE`,\n" +
                        "  `IS_BLACKLIST_USER`,\n" +
                        "  `STUDENT_FLAG`,\n" +
                        "  `TEACHER_FLAG`,\n" +
                        "  `AGENT_FLAG`,\n" +
                        "  `IS_KEY_ACCOUNT`,\n" +
                        "  `KEY_ACCOUNT_NUMBER`,\n" +
                        "  `IS_FREQUENT_TRAVELER`,\n" +
                        "  `FREQUENT_TRAVELER_CARDNO`,\n" +
                        "  `FREQUENT_TRAVELER_LEVEL`,\n" +
                        "  `YJ_CARD_NUMBER`,\n" +
                        "  `YJ_CARD_EXPIREDATE`,\n" +
                        "  `DEV_CHANNEL_ONE`,\n" +
                        "  `DEV_CHANNEL_TWO`,\n" +
                        "  `DEV_CHANNEL_THREE`,\n" +
                        "  `DEV_CHANNEL_FOUR`,\n" +
                        "  `FT_REGISTER_TIME`,\n" +
                        "  `ACCEPT_SMS_MARKETING`,\n" +
                        "  `ACCEPT_EMAIL_MARKETING`,\n" +
                        "  `PARENT_FT_CARDNO`,\n" +
                        "  `IS_LY_USER`,\n" +
                        "  `LY_REGISTER_TIME`,\n" +
                        "  `LY_CARD_NUMBER`,\n" +
                        "  `LY_USER_LEVEL`,\n" +
                        "  `LY_USER_STATUS`,\n" +
                        "  `LY_REGISTER_STATUS`,\n" +
                        "  `LY_VERIFY_STATUS`,\n" +
                        "  `LY_LIFETIME_POINTS`,\n" +
                        "  `LY_AVAILABLE_POINTS`,\n" +
                        "  `IS_HIGH_TRAVELER`,\n" +
                        "  `HIGH_TRAVELER_TYPE`,\n" +
                        "  `HIGH_TRAVELER_TIER`,\n" +
                        "  `TIER_PRESTIGE_EXPIREDATE`,\n" +
                        "  `TIER_HONOR_EXPIREDATE`,\n" +
                        "  `HIGH_TRAVELER_DS`,\n" +
                        "  `IS_YJ_PERSONNEL`,\n" +
                        "  `VIP_FLAG`,\n" +
                        "  `CIP_FLAG`,\n" +
                        "  `VVIP_FLAG`,\n" +
                        "  `FOOD_PREFERENCE`,\n" +
                        "  `SEAT_PREFERENCE`,\n" +
                        "  `TEMP_FOOD_PREFERENCE`,\n" +
                        "  `TEMP_SEAT_PREFERENCE`,\n" +
                        "  `BEVERAGE_PREFERENCE`,\n" +
                        "  `LONG_TERM_SEAT_PREFERENCE`,\n" +
                        "  `FIRST_CLASS_LOUNGE_PREFERENCE`,\n" +
                        "  `MERGED_TO_USERID`,\n" +
                        "  `IS_VALID_USER`,\n" +
                        "  `IS_FREQUENT_FLYER_NUMBER_VERIFIED`,\n" +
                        "  `CREATE_TIME`,\n" +
                        "  `UPDATE_TIME`)    \n" +
                        "SELECT " +
                        "  `PK_ID`,\n" +
                        "  `CRM_CUSTOMER_ID`,\n" +
                        "  `LY_VIP_ID`,\n" +
                        "  `LY_MEMBER_ID`,\n" +
                        "  `SYS_REGISTER_ID`,\n" +
                        "  `FFP_REGISTER_ID`,\n" +
                        "  `HISTORICAL_FFP_ID`,\n" +
                        "  `ALIPAY_ID`,\n" +
                        "  `WECHAT_ID`,\n" +
                        "  `DOUYIN_ID`,\n" +
                        "  `CN_NAME`,\n" +
                        "  `EN_NAME`,\n" +
                        "  `SEX`,\n" +
                        "  `USER_TYPE`,\n" +
                        "  `BIRTHDAY`,\n" +
                        "  `PROVINCE`,\n" +
                        "  `CITY`,\n" +
                        "  `NATIONALITY`,\n" +
                        "  `ETHNICITY`,\n" +
                        "  `EMPLOYER`,\n" +
                        "  `MOBILE_PHONE`,\n" +
                        "  `MOBILE_NUMBER_RELIABILITY`,\n" +
                        "  `EMAIL`,\n" +
                        "  `ID_CARD`,\n" +
                        "  `PASSPORT`,\n" +
                        "  `SEAMAN_ID`,\n" +
                        "  `ALIEN_PERMIT`,\n" +
                        "  `DIPLOMATIC_STAFF_CERTIFICATE`,\n" +
                        "  `PERMANENT_RESIDENT_ID`,\n" +
                        "  `CIVILIAN_STAFF_ID`,\n" +
                        "  `STAFF_ID`,\n" +
                        "  `OFFICER_ID_CARD`,\n" +
                        "  `ARMED_POLICE_OFFICER`,\n" +
                        "  `ARMED_POLICE_SOLDIER`,\n" +
                        "  `CIVILIAN_OFFICIAL_ID`,\n" +
                        "  `CONSCRIPT_SOLDIER_ID`,\n" +
                        "  `NON_COMMISSIONED_OFFICER_ID`,\n" +
                        "  `HK_MACAO_RESIDENT_PERMIT`,\n" +
                        "  `TAIWAN_RESIDENT_TRAVEL_PERMIT`,\n" +
                        "  `BLIND_PASSENGER`,\n" +
                        "  `DEAF_PASSENGER`,\n" +
                        "  `IS_DIRECT_USER`,\n" +
                        "  `IS_OFFICIAL_WEBSITE_NON_REGISTERED`,\n" +
                        "  `IS_DOUYIN_CARD_PURCHASER`,\n" +
                        "  `DIRECT_USER_STATUS`,\n" +
                        "  `DIRECT_REGISTER_DATE`,\n" +
                        "  `LAST_LOGIN_TIME`,\n" +
                        "  `DIRECT_VERIFIED_FLAG`,\n" +
                        "  `DIRECT_VERIFY_DATE`,\n" +
                        "  `IS_BLACKLIST_USER`,\n" +
                        "  `STUDENT_FLAG`,\n" +
                        "  `TEACHER_FLAG`,\n" +
                        "  `AGENT_FLAG`,\n" +
                        "  `IS_KEY_ACCOUNT`,\n" +
                        "  `KEY_ACCOUNT_NUMBER`,\n" +
                        "  `IS_FREQUENT_TRAVELER`,\n" +
                        "  `FREQUENT_TRAVELER_CARDNO`,\n" +
                        "  `FREQUENT_TRAVELER_LEVEL`,\n" +
                        "  `YJ_CARD_NUMBER`,\n" +
                        "  `YJ_CARD_EXPIREDATE`,\n" +
                        "  `DEV_CHANNEL_ONE`,\n" +
                        "  `DEV_CHANNEL_TWO`,\n" +
                        "  `DEV_CHANNEL_THREE`,\n" +
                        "  `DEV_CHANNEL_FOUR`,\n" +
                        "  `FT_REGISTER_TIME`,\n" +
                        "  `ACCEPT_SMS_MARKETING`,\n" +
                        "  `ACCEPT_EMAIL_MARKETING`,\n" +
                        "  `PARENT_FT_CARDNO`,\n" +
                        "  `IS_LY_USER`,\n" +
                        "  `LY_REGISTER_TIME`,\n" +
                        "  `LY_CARD_NUMBER`,\n" +
                        "  `LY_USER_LEVEL`,\n" +
                        "  `LY_USER_STATUS`,\n" +
                        "  `LY_REGISTER_STATUS`,\n" +
                        "  `LY_VERIFY_STATUS`,\n" +
                        "  `LY_LIFETIME_POINTS`,\n" +
                        "  `LY_AVAILABLE_POINTS`,\n" +
                        "  `IS_HIGH_TRAVELER`,\n" +
                        "  `HIGH_TRAVELER_TYPE`,\n" +
                        "  `HIGH_TRAVELER_TIER`,\n" +
                        "  `TIER_PRESTIGE_EXPIREDATE`,\n" +
                        "  `TIER_HONOR_EXPIREDATE`,\n" +
                        "  `HIGH_TRAVELER_DS`,\n" +
                        "  `IS_YJ_PERSONNEL`,\n" +
                        "  `VIP_FLAG`,\n" +
                        "  `CIP_FLAG`,\n" +
                        "  `VVIP_FLAG`,\n" +
                        "  `FOOD_PREFERENCE`,\n" +
                        "  `SEAT_PREFERENCE`,\n" +
                        "  `TEMP_FOOD_PREFERENCE`,\n" +
                        "  `TEMP_SEAT_PREFERENCE`,\n" +
                        "  `BEVERAGE_PREFERENCE`,\n" +
                        "  `LONG_TERM_SEAT_PREFERENCE`,\n" +
                        "  `FIRST_CLASS_LOUNGE_PREFERENCE`,\n" +
                        "  `MERGED_TO_USERID`,\n" +
                        "  `IS_VALID_USER`,\n" +
                        "  `IS_FREQUENT_FLYER_NUMBER_VERIFIED`,\n" +
                        "  `CREATE_TIME`,\n" +
                        "  `UPDATE_TIME` \n" +
                        " FROM T_DIM_USER_DIM \n"
                        + " WHERE  CAST(CREATE_TIME AS DATE)='" + etlDate + "' OR CAST(UPDATE_TIME AS DATE)='" + etlDate + "'";

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();*/

        //配置增量字段、数据加密等
        //todo 最近12个月平均提前值机时间、直销用户生命周期标签、支付方式偏好、1年内购买舱位偏好 等复杂逻辑待实现
        String addExtractSql =
                "INSERT INTO DIM_TEST.T_DIM_USER_DIM (\n" +
                        "  PK_ID, \n" +
                        "  HAS_REGISTERED_FREQUENT_TRAVELER,\n" +
                        "  LAST_REGISTRATION_TIME_FREQUENT_TRAVELER,\n" +
                        "  MILEAGE_EXCHANGE_COUNT_12M_AIR,\n" +
                        "  MILEAGE_EXCHANGE_AMOUNT_12M_AIR,\n" +
                        "  MILEAGE_EXCHANGE_COUNT_12M_NON_AIR,\n" +
                        "  MILEAGE_EXCHANGE_AMOUNT_12M_NON_AIR,\n" +
                        "  AVERAGE_ADVANCE_CHECK_IN_TIME_LAST_12M,\n" +
                        "  INSURANCE_PURCHASE_COUNT_LAST_12M,\n" +
                        "  PAID_SEAT_SELECTION_PURCHASE_COUNT_LAST_12M,\n" +
                        "  PREPAID_BAGGAGE_PURCHASE_COUNT_LAST_12M,\n" +
                        "  TOTAL_TICKET_ISSUANCE_SEGMENTS_ALL_TIME,\n" +
                        "  TOTAL_INSURANCE_PRODUCT_PURCHASE_AMOUNT_ALL_TIME,\n" +
                        "  TOTAL_PAID_SEAT_SELECTION_PRODUCT_PURCHASE_AMOUNT_ALL_TIME,\n" +
                        "  TOTAL_PREPAID_BAGGAGE_PRODUCT_PURCHASE_AMOUNT_ALL_TIME,\n" +
                        "  TICKET_PURCHASE_SEGMENTS_COUNT_LAST_1_YEAR,\n" +
                        "  INSURANCE_PRODUCT_PURCHASE_AMOUNT_LAST_1_YEAR,\n" +
                        "  PAID_SEAT_SELECTION_PRODUCT_PURCHASE_AMOUNT_LAST_1_YEAR,\n" +
                        "  PREPAID_BAGGAGE_PRODUCT_PURCHASE_AMOUNT_LAST_1_YEAR,\n" +
                        "  FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS,\n" +
                        "  HAS_MULTIPLE_FREQUENT_TRAVELER_CARD_NUMBERS\n" +
                        "  -- ,HAS_MULTIPLE_CUSTOMER_IDS\n" +
                        "  ) \n" +
                        "  (SELECT \n" +
                        "  tdud.pk_id,\n" +
                        "  -- 是否报名过常客产品\n" +
                        "  CASE \n" +
                        "        WHEN (\n" +
                        "            SELECT COUNT(1) \n" +
                        "            FROM DWD_TEST.T_DWD_ACT_SIGNUP_FACT asu \n" +
                        "            WHERE asu.MEMBER_TID = tdud.PK_ID\n" +
                        "        ) >= 1\n" +
                        "        THEN '是'\n" +
                        "        ELSE '否' \n" +
                        "    END,\n" +
                        "    -- 常客产品最近一次报名时间\n" +
                        "    (SELECT MAX(asu.FK_ACTION_DATE) \n" +
                        "     FROM DWD_TEST.T_DWD_ACT_SIGNUP_FACT asu \n" +
                        "     WHERE asu.MEMBER_TID = tdud.PK_ID),\n" +
                        "    -- 最近12个月里程兑换次数-航空\n" +
                        "    (SELECT SUM(mr.EXCH_COUNT) \n" +
                        " from DWD_TEST.T_DWD_MILE_REDEMPTION_FACT mr\n" +
                        "     WHERE mr.CUMULATION_TID = tdud.PK_ID\n" +
                        "     and mr.BIZ_TYPE = 'TKT'\n" +
                        "     AND mr.EXCH_DATE >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)),\n" +
                        "     -- 最近12个月兑换里程数量-航空\n" +
                        "     (SELECT SUM(mr.MILES_ACCUMULATED) \n" +
                        " from DWD_TEST.T_DWD_MILE_REDEMPTION_FACT mr\n" +
                        "     WHERE mr.CUMULATION_TID = tdud.PK_ID\n" +
                        "     and mr.BIZ_TYPE = 'TKT'\n" +
                        "     AND mr.EXCH_DATE >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)),\n" +
                        "     -- 最近12个月里程兑换次数-非航\n" +
                        "     (SELECT SUM(mr.EXCH_COUNT) \n" +
                        " from DWD_TEST.T_DWD_MILE_REDEMPTION_FACT mr\n" +
                        "     WHERE mr.CUMULATION_TID = tdud.PK_ID\n" +
                        "     and mr.BIZ_TYPE != 'TKT'\n" +
                        "     AND mr.EXCH_DATE >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)),\n" +
                        "     -- 最近12个月兑换里程数量-非航\n" +
                        "     (SELECT SUM(mr.MILES_ACCUMULATED) \n" +
                        " from DWD_TEST.T_DWD_MILE_REDEMPTION_FACT mr\n" +
                        "     WHERE mr.CUMULATION_TID = tdud.PK_ID\n" +
                        "     and mr.BIZ_TYPE != 'TKT'\n" +
                        "     AND mr.EXCH_DATE >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)),\n" +
                        "     -- 最近12个月平均提前购票时间\n" +
                        "(SELECT CONCAT(ROUND(SUM(ts.AK_ADVBOOK_DAY)/SUM(ts.TIK_SEGCOUNT), 1), '天')\n" +
                        " from DWD_TEST.T_DWD_TICKING_SEG_FACT ts\n" +
                        "     WHERE ts.FK_BOOKING_USER_TID = tdud.PK_ID\n" +
                        "     AND ts.FK_ISSUE_DATE >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)), \n" +
                        "     -- 最近12个月购买保险次数\n" +
                        "     (SELECT SUM(at.INSURANCE_COUNT) \n" +
                        " from DWD_TEST.T_DWD_AUIS_SEG_FACT at\n" +
                        "     WHERE at.FK_PASSENGER_USER_TID = tdud.PK_ID\n" +
                        "     AND at.FK_BKAUIS_DATE >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)),\n" +
                        "     -- 最近12个月购买付费选座次数\n" +
                        "     (SELECT SUM(st.SEAT_COUNT) \n" +
                        " from DWD_TEST.T_DWD_SEAT_TIK_FACT st\n" +
                        "     WHERE st.FK_PASSENGER_USER_TID = tdud.PK_ID\n" +
                        "     AND st.FK_SEATTIK_DATE >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)),\n" +
                        "     -- 最近12个月购买预付费行李次数\n" +
                        "     (SELECT SUM(bt.BAGGAGE_COUNT) \n" +
                        " from DWD_TEST.T_DWD_BAGGAGE_TIK_FACT bt\n" +
                        "     WHERE bt.FK_PASSENGER_USER_TID = tdud.PK_ID\n" +
                        "     AND bt.FK_BAGGAGETIK_DATE >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)),\n" +
                        "     -- 全期出票航段次数\n" +
                        "     (SELECT SUM(ts.TIK_SEGCOUNT) \n" +
                        " from DWD_TEST.T_DWD_TICKING_SEG_FACT ts\n" +
                        "     WHERE ts.FK_BOOKING_USER_TID = tdud.PK_ID),\n" +
                        "     -- 全期保险产品购买金额\n" +
                        "     (SELECT SUM(at.INSURANCE_AMT) \n" +
                        " from DWD_TEST.T_DWD_AUIS_SEG_FACT at\n" +
                        "     WHERE at.FK_BOOKING_USER_TID = tdud.PK_ID),\n" +
                        "     -- 全期付费选座产品购买金额\n" +
                        "     (SELECT SUM(st.SEAT_AMOUNT) \n" +
                        " from DWD_TEST.T_DWD_SEAT_TIK_FACT st\n" +
                        "     WHERE st.FK_BOOKING_USER_TID = tdud.PK_ID),\n" +
                        "     -- 全期付费行李产品购买金额\n" +
                        "     (SELECT SUM(bt.BAGGAGE_AMOUNT) \n" +
                        " from DWD_TEST.T_DWD_BAGGAGE_TIK_FACT bt\n" +
                        "     WHERE bt.FK_BOOKING_USER_TID = tdud.PK_ID),\n" +
                        "     -- 1年内购票航段数量\n" +
                        "     (SELECT SUM(ts.TIK_SEGCOUNT) \n" +
                        " from DWD_TEST.T_DWD_TICKING_SEG_FACT ts\n" +
                        "     WHERE ts.FK_BOOKING_USER_TID = tdud.PK_ID\n" +
                        "     AND ts.FK_ISSUE_DATE >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)),\n" +
                        "     -- 1年内保险产品购买金额\n" +
                        "     (SELECT SUM(at.INSURANCE_AMT) \n" +
                        " from DWD_TEST.T_DWD_AUIS_SEG_FACT at\n" +
                        "     WHERE at.FK_BOOKING_USER_TID = tdud.PK_ID\n" +
                        "     AND at.FK_BKAUIS_DATE >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)),\n" +
                        "     -- 1年内付费选座产品购买金额\n" +
                        "     (SELECT SUM(st.SEAT_AMOUNT) \n" +
                        " from DWD_TEST.T_DWD_SEAT_TIK_FACT st\n" +
                        "     WHERE st.FK_BOOKING_USER_TID = tdud.PK_ID\n" +
                        "     AND st.FK_SEATTIK_DATE >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)),\n" +
                        "     -- 1年内付费行李产品购买金额\n" +
                        "     (SELECT SUM(bt.BAGGAGE_AMOUNT) \n" +
                        " from DWD_TEST.T_DWD_BAGGAGE_TIK_FACT bt\n" +
                        "     WHERE bt.FK_BOOKING_USER_TID = tdud.PK_ID\n" +
                        "     AND bt.FK_BAGGAGETIK_DATE >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)),\n" +
                        "     -- 近3年乘机航段数\n" +
                        "     (SELECT count(1) \n" +
                        " from DWD_TEST.T_DWD_DEPART_SEG_FACT ds\n" +
                        "     WHERE ds.FK_PASSENGER_USER_TID = tdud.PK_ID\n" +
                        "     AND ds.FK_DEPARTURES_DATE >= DATE_SUB(CURRENT_DATE, INTERVAL 3 YEAR)),\n" +
                        "     -- 是否存在多个常客卡号\n" +
                        "     CASE \n" +
                        "        WHEN (\n" +
                        "            SELECT count(distinct(fd.FFRF)) \n" +
                        " from DIM_TEST.T_DIM_FFP_DIM fd\n" +
                        "     WHERE fd.T_ID = tdud.PK_ID\n" +
                        "        ) > 1\n" +
                        "        THEN '是'\n" +
                        "        ELSE '否' \n" +
                        "     END\n" +
                        "     -- 是否存在多个customerID\n" +
                        "     -- ,CASE \n" +
                        "     --    WHEN (\n" +
                        "     --        SELECT count(distinct(cd.customer_ID)) \n" +
                        " -- from DIM_TEST.T_DIM_CUSTOM_DIM cd\n" +
                        " --     WHERE cd.T_ID = tdud.PK_ID\n" +
                        " --        ) > 1\n" +
                        " --        THEN '是'\n" +
                        " --        ELSE '否' \n" +
                        " --    END\n" +
                        "  FROM DIM_TEST.T_DIM_USER_DIM tdud"
                        + " WHERE  CAST(CREATE_TIME AS DATE)='" + etlDate + "' OR CAST(UPDATE_TIME AS DATE)='" + etlDate + "'";

        TableResult addResult = tEnv.executeSql(addExtractSql
        );

        addResult.print();

        /*String queryExtractSql =
                "SELECT \n" +
                        "tdud.pk_id,\n" +
                        "    -- 最近12个月平均提前值机时间(小时)\n" +
                        "(SELECT ROUND(SUM(cs.EARLY_CHECKINTIME)/COUNT(cs.EARLY_CHECKINTIME), 1)\n" +
                        " from DWD_TEST.T_DWD_CHECKIN_SEG_FACT cs\n" +
                        "     right join DIM_TEST.T_DIM_USER_DIM tdud \n" +
                        "     on cs.FK_PASSENGER_USER_TID = tdud.PK_ID\n" +
                        "     WHERE cs.FK_PASSENGER_USER_TID = tdud.PK_ID) AVERAGE_ADVANCE_CHECK_IN_TIME_LAST_12M_HOUR\n" +
                        "  FROM DIM_TEST.T_DIM_USER_DIM tdud"
                        + " WHERE  CAST(CREATE_TIME AS DATE)='" + etlDate + "' OR CAST(UPDATE_TIME AS DATE)='" + etlDate + "'";

        Table joinedTable = tEnv.sqlQuery(queryExtractSql);
        // 转换为 DataStream<Row>
        DataStream<Map> sourceStream = tEnv.toDataStream(joinedTable).map(row -> {
            Map<String, String> hashMap = new HashMap<>();
            String tid = row.getFieldAs("pk_id");
            String AVERAGE_ADVANCE_CHECK_IN_TIME_LAST_12M_HOUR = row.getFieldAs("AVERAGE_ADVANCE_CHECK_IN_TIME_LAST_12M_HOUR");
            hashMap.put("tid", tid);
            hashMap.put("AVERAGE_ADVANCE_CHECK_IN_TIME_LAST_12M_HOUR", AVERAGE_ADVANCE_CHECK_IN_TIME_LAST_12M_HOUR);
            return hashMap;
        });*/

        // 最近12个月平均提前值机小时
        String beforeCheckInHourSql ="INSERT INTO DIM_TEST.T_DIM_USER_DIM (PK_ID, " +
                "AVERAGE_ADVANCE_CHECK_IN_TIME_LAST_12M)\n" +
                "SELECT \n" +
                "    COALESCE(checkin_stats.FK_PASSENGER_USER_TID, dim.PK_ID) as PK_ID,\n" +
                "    COALESCE(\n" +
                "        CASE \n" +
                "            WHEN checkin_stats.TOTAL_CHECKIN_COUNT > 0 \n" +
                "            THEN checkin_stats.TOTAL_EARLY_CHECKINTIME / checkin_stats.TOTAL_CHECKIN_COUNT\n" +
                "            ELSE dim.AVERAGE_ADVANCE_CHECK_IN_TIME_LAST_12M  -- 保持原值如果没有值机记录\n" +
                "        END, \n" +
                "        dim.AVERAGE_ADVANCE_CHECK_IN_TIME_LAST_12M  -- 保持原值如果计算结果为NULL\n" +
                "    ) as AVERAGE_ADVANCE_CHECK_IN_TIME_LAST_12M\n" +
                "FROM DIM_TEST.T_DIM_USER_DIM dim\n" +
                "LEFT JOIN (\n" +
                "    SELECT \n" +
                "        FK_PASSENGER_USER_TID,\n" +
                "        SUM(EARLY_CHECKINTIME) as TOTAL_EARLY_CHECKINTIME,\n" +
                "        SUM(CHECKIN_COUNT) as TOTAL_CHECKIN_COUNT\n" +
                "    FROM DWD_TEST.T_DWD_CHECKIN_SEG_FACT\n" +
                "    WHERE FK_CHECKIN_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)\n" +
                "    GROUP BY FK_PASSENGER_USER_TID\n" +
                ") checkin_stats ON dim.PK_ID = checkin_stats.FK_PASSENGER_USER_TID"+
                " WHERE CAST(dim.CREATE_TIME AS DATE) = '" + etlDate + "' OR CAST(dim.UPDATE_TIME AS DATE) = '" + etlDate + "'";;
        TableResult beforeCheckInHourResult = tEnv.executeSql(beforeCheckInHourSql);
        beforeCheckInHourResult.print();

        // 支付方式偏好
        String paymentMethodSql ="INSERT INTO DIM_TEST.T_DIM_USER_DIM (PK_ID, PAYMENT_METHOD_PREFERENCE)\n" +
                "SELECT \n" +
                "    user_pay.FK_BOOKING_USER_TID as PK_ID,\n" +
                "    user_pay.AK_PAY_METHOD as PAYMENT_METHOD_PREFERENCE\n" +
                "FROM (\n" +
                "    SELECT \n" +
                "        pay.FK_BOOKING_USER_TID,\n" +
                "        pay.AK_PAY_METHOD,\n" +
                "        pay.pay_count,\n" +
                "        pay.rn\n" +
                "    FROM (\n" +
                "        SELECT \n" +
                "            FK_BOOKING_USER_TID,\n" +
                "            AK_PAY_METHOD,\n" +
                "            COUNT(*) as pay_count,\n" +
                "            ROW_NUMBER() OVER (PARTITION BY FK_BOOKING_USER_TID ORDER BY COUNT(*) DESC) as rn\n" +
                "        FROM DWD_TEST.T_DWD_PAYDETAILS_ORD_FACT\n" +
                "        GROUP BY FK_BOOKING_USER_TID, AK_PAY_METHOD\n" +
                "    ) pay\n" +
                "    INNER JOIN DIM_TEST.T_DIM_USER_DIM dim ON pay.FK_BOOKING_USER_TID = dim.PK_ID\n" +
                "    WHERE CAST(dim.CREATE_TIME AS DATE) = '" + etlDate + "' OR CAST(dim.UPDATE_TIME AS DATE) = '" + etlDate + "'\n" +
                ") user_pay\n" +
                "WHERE user_pay.rn = 1;";
        TableResult paymentMethodResult = tEnv.executeSql(paymentMethodSql);
        paymentMethodResult.print();

        // 1年内购买舱位偏好
        String cabinClassSql ="INSERT INTO DIM_TEST.T_DIM_USER_DIM (PK_ID, CABIN_CLASS_PREFERENCE_LAST_1_YEAR)\n" +
                "SELECT \n" +
                "    COALESCE(user_cabin.FK_BOOKING_USER_TID, dim.PK_ID) as PK_ID,\n" +
                "    COALESCE(user_cabin.AK_SEGCABIN, dim.CABIN_CLASS_PREFERENCE_LAST_1_YEAR) as CABIN_CLASS_PREFERENCE_LAST_1_YEAR\n" +
                "FROM DIM_TEST.T_DIM_USER_DIM dim\n" +
                "LEFT JOIN (\n" +
                "    SELECT \n" +
                "        FK_BOOKING_USER_TID,\n" +
                "        AK_SEGCABIN,\n" +
                "        COUNT(*) as cabin_count,\n" +
                "        ROW_NUMBER() OVER (\n" +
                "            PARTITION BY FK_BOOKING_USER_TID \n" +
                "            ORDER BY COUNT(*) DESC\n" +
                "        ) as rn\n" +
                "    FROM DWD_TEST.T_DWD_TICKING_SEG_FACT\n" +
                "    WHERE FK_ISSUE_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)\n" +
                "    GROUP BY FK_BOOKING_USER_TID, AK_SEGCABIN\n" +
                ") user_cabin ON dim.PK_ID = user_cabin.FK_BOOKING_USER_TID AND user_cabin.rn = 1"+
                " WHERE CAST(dim.CREATE_TIME AS DATE) = '" + etlDate + "' OR CAST(dim.UPDATE_TIME AS DATE) = '" + etlDate + "'";
        TableResult cabinClassResult = tEnv.executeSql(cabinClassSql);
        cabinClassResult.print();


    }
}
