package com.travelsky.dataplatform.main.dim;


import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

/**
 * @author TS.SHA.fuhuazhang
 * @date 2025/9/8  10:44
 */
public class ExtractTDimUserDimDailyTag {
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
        tEnv.executeSql("CREATE TABLE T_DIM_USER_DIM (\n" +
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
                "  BIRTHDAY TIMESTAMP(6),\n" +
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
                "  LAST_LOGIN_TIME TIMESTAMP(6),\n" +
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
                "  FT_REGISTER_TIME TIMESTAMP(6),\n" +
                "  ACCEPT_SMS_MARKETING boolean,\n" +
                "  ACCEPT_EMAIL_MARKETING boolean,\n" +
                "  PARENT_FT_CARDNO varchar(128),\n" +
                "  IS_LY_USER boolean,\n" +
                "  LY_REGISTER_TIME TIMESTAMP(6),\n" +
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
                "  CREATE_TIME TIMESTAMP(6),\n" +
                "  UPDATE_TIME TIMESTAMP(6),\n" +
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
                ")");

        // 直销用户生命周期标签
        String customerLifeCycleLabelSql ="INSERT INTO DIM_TEST.T_DIM_USER_DIM (PK_ID, " +
                "CUSTOMER_LIFE_CYCLE_LABEL_DIRECT_SALES)\n" +
                "SELECT \n" +
                "    user_data.PK_ID,\n" +
                "    CASE \n" +
                "        -- 1. 导入期：今年注册，下单数 < 1\n" +
                "        WHEN user_data.REGISTER_YEAR = YEAR(CURRENT_DATE()) AND booking_count.TOTAL_BOOKINGS < 1 \n" +
                "            THEN 'NUS'\n" +
                "        \n" +
                "        -- 2. 成长期：今年注册，下单数 = 1\n" +
                "        WHEN user_data.REGISTER_YEAR = YEAR(CURRENT_DATE()) AND booking_count.TOTAL_BOOKINGS = 1 \n" +
                "            THEN 'GRO'\n" +
                "        \n" +
                "        -- 3. 活跃期：今年注册且下单超过一次 或 连续两年下单\n" +
                "        WHEN (user_data.REGISTER_YEAR = YEAR(CURRENT_DATE()) AND booking_count.TOTAL_BOOKINGS > 1) \n" +
                "            OR (booking_count.THIS_YEAR_BOOKINGS > 0 AND booking_count.LAST_YEAR_BOOKINGS > 0)\n" +
                "            THEN 'ALV'\n" +
                "        \n" +
                "        -- 4. 沉睡期：去年下单，今年未下单\n" +
                "        WHEN booking_count.LAST_YEAR_BOOKINGS > 0 AND booking_count.THIS_YEAR_BOOKINGS = 0 \n" +
                "            THEN 'SLP'\n" +
                "        \n" +
                "        -- 5. 流失期：两年未下单\n" +
                "        WHEN booking_count.THIS_YEAR_BOOKINGS = 0 AND booking_count.LAST_YEAR_BOOKINGS = 0 \n" +
                "            THEN 'CHN'\n" +
                "        \n" +
                "        -- 6. 回流期：去年未下单，今年下单\n" +
                "        WHEN booking_count.LAST_YEAR_BOOKINGS = 0 AND booking_count.THIS_YEAR_BOOKINGS > 0 \n" +
                "            THEN 'RTN'\n" +
                "        \n" +
                "        -- 默认情况：没有预订记录的用户\n" +
                "        ELSE 'CHN'\n" +
                "    END as CUSTOMER_LIFE_CYCLE_LABEL_DIRECT_SALES\n" +
                "\n" +
                "FROM (\n" +
                "    -- 获取直销用户基本信息\n" +
                "    SELECT \n" +
                "        PK_ID,\n" +
                "        IS_DIRECT_USER,\n" +
                "        DIRECT_REGISTER_DATE,\n" +
                "        YEAR(DIRECT_REGISTER_DATE) as REGISTER_YEAR\n" +
                "    FROM DIM_TEST.T_DIM_USER_DIM\n" +
                "    WHERE IS_DIRECT_USER = true\n" +
                "    AND CAST(CREATE_TIME AS DATE) = '" + etlDate + "' OR CAST(UPDATE_TIME AS DATE) ='" + etlDate + "'\n" +
                ") user_data\n" +
                "\n" +
                "LEFT JOIN (\n" +
                "    -- 统计每个用户的预订情况\n" +
                "    SELECT \n" +
                "        FK_BOOKING_USER_TID,\n" +
                "        -- 总下单数\n" +
                "        COUNT(*) as TOTAL_BOOKINGS,\n" +
                "        -- 今年下单数\n" +
                "        SUM(CASE WHEN YEAR(FK_BOOKING_DATE) = YEAR(CURRENT_DATE()) THEN 1 ELSE 0 END) as THIS_YEAR_BOOKINGS,\n" +
                "        -- 去年下单数\n" +
                "        SUM(CASE WHEN YEAR(FK_BOOKING_DATE) = YEAR(CURRENT_DATE()) - 1 THEN 1 ELSE 0 END) as LAST_YEAR_BOOKINGS\n" +
                "    FROM DWD_TEST.T_DWD_BOOKING_SEG_FACT\n" +
                "    WHERE FK_BOOKING_USER_TID IN (SELECT PK_ID FROM T_DIM_USER_DIM WHERE IS_DIRECT_USER = true)\n" +
                "    GROUP BY FK_BOOKING_USER_TID\n" +
                ") booking_count ON user_data.PK_ID = booking_count.FK_BOOKING_USER_TID ";
        TableResult customerLifeCycleLabelResult = tEnv.executeSql(customerLifeCycleLabelSql);
        customerLifeCycleLabelResult.print();
    }
}
