package com.travelsky.dataplatform.main.dwq;

import com.google.common.collect.Lists;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.trp.utils.DateUtil;
import com.travelsky.dataplatform.module.dwq.CustomTag;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.DmDbUtils;
import com.travelsky.dataplatform.utils.DorisUtils;
import com.travelsky.dataplatform.utils.SM4Utils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * 用户数据同步
 *
 * @author z
 * @since 2025/12/21
 **/
public class UserSync {

    static final Logger logger = LoggerFactory.getLogger(UserSync.class);

    public static void main(String[] args) throws Exception {
        System.out.println("UserSync start...");
        long start = System.currentTimeMillis();
        if (args.length < 1) {
            System.exit(0);
        }
        String etlDate = args[0];
        String startDate = etlDate;
        String endDate = etlDate;
        String db = Constants.DWQ_DB;
        String transtb = "T_DIM_USER_DIM_TRANS";
        String targettb = "T_DIM_USER_DIM";
        String origintb = "T_DIM_USER_DIM_VIEW";
        if (args.length > 2) {
            startDate = args[1];
            endDate = args[2];
        }
        if (args.length > 3){
            db = args[3];
        }
        if (args.length > 4){
            transtb = args[4];
        }
        if (args.length > 5){
            targettb = args[5];
        }
        if (args.length > 6){
            origintb = args[6];
        }
        System.out.println("UserSync start..., etlDate:"+etlDate+",startDate:"+startDate+",endDate:"+endDate+",db:"+db+",transtb:"+transtb+",targettb:"+targettb);

        // 同步用户数据
        syncUser(startDate, endDate, db, transtb, targettb, origintb);
        long end = System.currentTimeMillis();
        System.out.println("UserSync end..., 耗时：" + (end - start) + "ms");
    }

    /**
     * 同步用户数据
     * 根据指定的开始日期和结束日期范围，执行用户数据的同步操作
     *
     * @param startDate 同步数据的开始日期，格式应为字符串类型（如：yyyy-MM-dd）
     * @param endDate   同步数据的结束日期，格式应为字符串类型（如：yyyy-MM-dd）
     */
    private static void syncUser(String startDate, String endDate, String dbSchema, String transTableName, String targetTableName, String originTableName) {
        long start = System.currentTimeMillis();
        try {
            //1.同步表结构，获取DWQ_PROD.T_DIM_USER_DIM_TRANS的字段，增加或删除DWQ_PROD.T_DIM_USER_DIM_TRANS的自定义标签
            List<CustomTag> tags = syncUserFields(dbSchema, transTableName);
            //2.备份DWQ_PROD.T_DIM_USER_DIM到DWQ_PROD.T_DIM_USER_DIM_TRANS
            backupUserData(dbSchema, transTableName, targetTableName);
            //3.DWQ_PROD.T_DIM_USER_DIM_VIEW关联DWQ_PROD.T_DIM_USER_DIM_TRANS，同步数据到DWQ_PROD.T_DIM_USER_DIM
            syncUserData(dbSchema, transTableName, targetTableName, originTableName, tags);
        } catch (Exception e) {
            System.out.println("同步用户数据失败，原因：" + e.getMessage());
        }
        long end = System.currentTimeMillis();
        System.out.println("同步用户数据完成，耗时：" + (end - start) + "ms");
    }

    /**
     * 同步用户表中的自定义标签
     * 该方法负责从达梦数据库获取自定义标签，并同步到DWQ_PROD.T_DIM_USER_DIM_TRANS表中
     *
     * @return boolean 返回同步后的操作结果
     */
    private static List<CustomTag> syncUserFields(String dbSchema, String tableName) {
        //1.获取达梦数据库中自定义标签
        System.out.println("获取自定义标签列表...");
        List<CustomTag> customTags = getCustomTags();
        for (CustomTag customTag : customTags){
            System.out.println("自定义标签customTaglist："+customTag);
        }
        //2.获取DWQ_PROD.T_DIM_USER_DIM_TRANS的字段
        System.out.println("获取T_DIM_USER_DIM_TRANS表中的字段...");
        Map<String,String> userDimFileds = getUserTags(dbSchema, tableName);
        System.out.println("T_DIM_USER_DIM_TRANS表字段列表："+userDimFileds);
        //3.增加或删除DWQ_PROD.T_DIM_USER_DIM_TRANS的自定义标签
        List<String> addsqls = new ArrayList<>();
        for (CustomTag customTag : customTags) {
            if (userDimFileds.containsKey(customTag.getTagName())) {
                //两边都有的情况下，从userDimFileds删除掉，最后剩下的就是需要删除的字段
                userDimFileds.remove(customTag.getTagName());
            } else {
                String sql = "ALTER TABLE "+dbSchema+"."+tableName+" ADD COLUMN " + customTag.getTagName() + " STRING NULL COMMENT '自定义标签-"+customTag.getTagShowName()+"';";
                addsqls.add( sql);
            }
        }
        List<String> dropsqls = new ArrayList<>();
        for (String key : userDimFileds.keySet()) {
            String sql = "ALTER TABLE "+dbSchema+"."+tableName+" DROP COLUMN " + key + ";";
            dropsqls.add(sql);
        }
        //4.执行同步语句
        Connection connection = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
        Statement statement = DorisUtils.getStatement(connection);
        for (String sql : addsqls) {
            System.out.println("增加字段："+sql);
            DorisUtils.excuteDorisInsert(statement, sql);
        }
        for (String sql : dropsqls) {
            System.out.println("删除字段："+sql);
            DorisUtils.excuteDorisInsert(statement, sql);
        }
        DorisUtils.close(connection, statement, null);
        return customTags;
    }

    /**
     * 获取自定义标签列表
     * @return 自定义标签列表，如果获取失败则返回空列表
     */
    private static List<CustomTag> getCustomTags() {
        List<CustomTag> customTags = new ArrayList<>();
        try {
            Connection connection = DmDbUtils.getConnection(Constants.DM_JDBC_URL, Constants.DM_USER, Constants.DM_PWD);
            Statement statement = DmDbUtils.getStatement(connection);
            String querysql = "SELECT cti.TAG_NAME,cti.TAG_SHOW_NAME  FROM UCVPROD.CUSTOMIZE_TAG_INFO AS cti WHERE DATA_RANGE = 'ALL_CHANNEL' ORDER BY cti.TAG_ID";
            System.out.println("自定义标签列表querysql:"+querysql);
            ResultSet rs = DmDbUtils.getDmResult(statement, querysql);
            try {
                if (rs != null) {
                    while (rs.next()) {
                        CustomTag customTag = new CustomTag(
                        rs.getString("TAG_NAME"),
                        rs.getString("TAG_SHOW_NAME"));
                        customTags.add(customTag);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            rs.close();
            statement.close();
            connection.close();
        } catch (Exception e) {
            // 记录异常日志
            System.out.println("获取自定义标签失败，原因：" + e.getMessage());
        }
        return customTags;
    }

    /**
     * 获取用户维度表的字段信息
     * 从Doris数据库的INFORMATION_SCHEMA中查询指定表的所有字段名，
     * 并将字段类型为text的字段名作为key和value构建Map返回
     *
     * @param dbSchema 数据库模式名
     * @param tableName 表名
     * @return Map<String,String> 包含用户表text类型字段名的映射，key为字段名，value为字段名
     */
    private static Map<String,String> getUserTags(String dbSchema, String tableName) {
        Map<String, String> userTableFields = new HashMap<>();
        try {
            // 建立Doris数据库连接
            Connection connection = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
            Statement statement = DorisUtils.getStatement(connection);
            String querysql = "SELECT COLUMN_NAME,COLUMN_TYPE,COLUMN_COMMENT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '"+dbSchema+"' AND TABLE_NAME = '"+tableName+"';";
            System.out.println("用户表获取sql:"+querysql);
            ResultSet rs = DorisUtils.getDorisResult(statement, querysql);
            try {
                // 遍历查询结果，筛选字段类型为text的字段
                if (rs != null) {
                    while (rs.next()) {
                        //if(rs.getString("COLUMN_COMMENT").contains("自定义标签")) {
                        if("string".equalsIgnoreCase(rs.getString("COLUMN_TYPE"))) {
                            userTableFields.put(
                                    rs.getString("COLUMN_NAME"),
                                    rs.getString("COLUMN_NAME"));
                        }
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            // 关闭数据库连接资源
            rs.close();
            statement.close();
            connection.close();
        } catch (Exception e) {
            // 记录异常日志
            System.out.println("获取用户表字段失败，原因：" + e.getMessage());
        }
        return userTableFields;
    }

    /**
     * 备份用户数据到临时表
     * 该方法将目标表的数据备份到转换表中，首先清空转换表，然后将目标表数据插入转换表
     *
     * @param dbSchema 数据库模式名称
     * @param transTableName 转换表名称（用于存储备份数据）
     * @param targetTableName 目标表名称（源数据表）
     */
    private static void backupUserData(String dbSchema,String transTableName,String targetTableName){
        long startTime = System.currentTimeMillis();
        try {
            Connection connection = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
            Statement statement = DorisUtils.getStatement(connection);

            // 清空转换表数据
            String truncatesql = "truncate TABLE "+dbSchema+"."+transTableName+";";
            System.out.println("清空转换表数据sql:"+truncatesql);
            DorisUtils.excuteDorisInsert(statement, truncatesql);

            // 将目标表数据插入到转换表中
            String transsql = "INSERT INTO "+dbSchema+"."+transTableName+" SELECT * FROM "+dbSchema+"."+targetTableName+";";
            System.out.println("将目标表数据插入转换表sql:"+transsql);
            DorisUtils.excuteDorisInsert(statement, transsql);

            DorisUtils.close(connection, statement, null);
        } catch (Exception e) {
            // 记录异常日志
            System.out.println("数据备份失败，原因：" + e.getMessage());
        }
        long endTime = System.currentTimeMillis();
        System.out.println("数据备份完成，耗时：" + (endTime - startTime) + "毫秒");
    }

    private static void syncUserData(String dbSchema,String transTableName, String targetTableName, String orgTableName,List<CustomTag> tags){
        long startTime = System.currentTimeMillis();
        try {
            String transsql = "INSERT OVERWRITE TABLE "+dbSchema+"."+targetTableName+"(PK_ID, PASSENGER_SOURCE_CITY, PREFERENCE_DOMESTIC_TOP_CITY, PREFERENCE_INTL_TOP_CITY, PASSENGER_TOTAL_CONSUME_PERCENTILE_ALL_TIME, COUPON_USAGE_COUNT_LAST_12M, AGE, ID_CARD_REGION, HAS_REGISTERED_FREQUENT_TRAVELER, LAST_REGISTRATION_TIME_FREQUENT_TRAVELER, MILEAGE_EXCHANGE_COUNT_12M_AIR, MILEAGE_EXCHANGE_AMOUNT_12M_AIR, MILEAGE_EXCHANGE_COUNT_12M_NON_AIR, MILEAGE_EXCHANGE_AMOUNT_12M_NON_AIR, AVERAGE_ADVANCE_PURCHASE_TIME_LAST_12M, AVERAGE_ADVANCE_CHECK_IN_TIME_LAST_12M, ANCILLARY_PRODUCT_PREFERENCE_CATEGORY, INSURANCE_PURCHASE_COUNT_LAST_12M, PAID_SEAT_SELECTION_PURCHASE_COUNT_LAST_12M, PREPAID_BAGGAGE_PURCHASE_COUNT_LAST_12M, UPGRADE_VOUCHER_USAGE_COUNT_LAST_12M, IN_FLIGHT_UPGRADE_COUNT_BUSINESS_LAST_12M, IN_FLIGHT_UPGRADE_COUNT_PREMIUM_ECONOMY_LAST_12M, BOARDING_GATE_UPGRADE_COUNT_BUSINESS_LAST_12M, BOARDING_GATE_UPGRADE_COUNT_PREMIUM_ECONOMY_LAST_12M, UPGRADE_VOUCHER_USAGE_AMOUNT_LAST_12M, IN_FLIGHT_UPGRADE_AMOUNT_BUSINESS_LAST_12M, IN_FLIGHT_UPGRADE_AMOUNT_PREMIUM_ECONOMY_LAST_12M, BOARDING_GATE_UPGRADE_AMOUNT_BUSINESS_LAST_12M, BOARDING_GATE_UPGRADE_AMOUNT_PREMIUM_ECONOMY_LAST_12M, TOTAL_UPGRADE_VOUCHER_USAGE_COUNT, TOTAL_IN_FLIGHT_UPGRADE_COUNT_PREMIUM_ECONOMY, TOTAL_IN_FLIGHT_UPGRADE_COUNT_BUSINESS, TOTAL_BOARDING_GATE_UPGRADE_COUNT_PREMIUM_ECONOMY, TOTAL_BOARDING_GATE_UPGRADE_COUNT_BUSINESS, TOTAL_UPGRADE_VOUCHER_USAGE_AMOUNT, TOTAL_IN_FLIGHT_UPGRADE_AMOUNT_PREMIUM_ECONOMY, TOTAL_IN_FLIGHT_UPGRADE_AMOUNT_BUSINESS, TOTAL_UPGRADE_CABIN_PREFERENCE, UPGRADE_CABIN_PREFERENCE_LAST_12M, CALCULATED_SEAT_PREFERENCE, COUNTRY_VISITED_COUNT, CITY_VISITED_COUNT, MOST_COMMON_DOMESTIC_DEPARTURE_LOCATION_ALL_TIME, MOST_COMMON_INTERNATIONAL_DEPARTURE_LOCATION_ALL_TIME, MOST_COMMON_DOMESTIC_ARRIVAL_LOCATION_ALL_TIME, MOST_COMMON_INTERNATIONAL_ARRIVAL_LOCATION_ALL_TIME, TOTAL_TRAVEL_SEGMENTS_ALL_TIME, SUSPECTED_AGENT, IS_HIGH_VALUE_FREQUENT_TRAVELER_MEMBER, CUSTOMER_LIFE_CYCLE_LABEL_DIRECT_SALES, CUSTOMER_CHURN_LABEL, CUSTOMER_FLIGHT_PURCHASE_RATE_REPURCHASE_RATE, PAYMENT_METHOD_PREFERENCE, FLIGHT_BOOKING_COUNT_LAST_30_DAYS, TOTAL_TICKET_ISSUANCE_SEGMENTS_ALL_TIME, TOTAL_TICKET_ISSUANCE_AMOUNT_INTERNATIONAL_ALL_TIME, TOTAL_TICKET_ISSUANCE_AMOUNT_DOMESTIC_ALL_TIME, TOTAL_INSURANCE_PRODUCT_PURCHASE_AMOUNT_ALL_TIME, TOTAL_PAID_SEAT_SELECTION_PRODUCT_PURCHASE_AMOUNT_ALL_TIME, TOTAL_PREPAID_BAGGAGE_PRODUCT_PURCHASE_AMOUNT_ALL_TIME, CABIN_CLASS_PREFERENCE_LAST_1_YEAR, TICKET_PURCHASE_SEGMENTS_COUNT_LAST_1_YEAR, TICKET_PURCHASE_AMOUNT_INTERNATIONAL_LAST_1_YEAR, TICKET_PURCHASE_AMOUNT_DOMESTIC_LAST_1_YEAR, INSURANCE_PRODUCT_PURCHASE_AMOUNT_LAST_1_YEAR, PAID_SEAT_SELECTION_PRODUCT_PURCHASE_AMOUNT_LAST_1_YEAR, PREPAID_BAGGAGE_PRODUCT_PURCHASE_AMOUNT_LAST_1_YEAR, GROUP_TICKET_PURCHASE_ISSUANCE_COUNT_LAST_1_YEAR, GROUP_TICKET_PURCHASE_ISSUANCE_COUNT_ALL_TIME, TOTAL_DOMESTIC_TICKET_FACE_VALUE_CARRIED_BY_SHANDONG_AIRLINES_LAST_3_YEARS, TOTAL_INTERNATIONAL_TICKET_FACE_VALUE_CARRIED_BY_SHANDONG_AIRLINES_LAST_3_YEARS, TOTAL_INSURANCE_PRODUCT_CONSUMPTION_AMOUNT_LAST_3_YEARS, TOTAL_PAID_SEAT_SELECTION_PRODUCT_CONSUMPTION_AMOUNT_LAST_3_YEARS, TOTAL_PREPAID_BAGGAGE_PRODUCT_CONSUMPTION_AMOUNT_LAST_3_YEARS, TOTAL_UNACCOMPANIED_MINOR_EMD_PRODUCT_CONSUMPTION_AMOUNT_LAST_3_YEARS, FLIGHT_CONSUMPTION_RANK_LAST_3_YEARS, TOTAL_CONSUMPTION_AMOUNT_ALL_TIME, TOTAL_CONSUMPTION_RANK_ALL_TIME, FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS, CUSTOMER_CONTRIBUTION, TRAVELER_SOURCE_LOCATION, TRAVELER_PREFERRED_DESTINATION_CITY, TRAVELER_DEPARTURE_UPGRADE_PREFERENCE, TRAVELER_DOMESTIC_TICKET_DISCOUNT_DEVIATION, FREQUENT_TRAVELER_VALUE, TRAVELER_COMPREHENSIVE_VALUE, TRAVELER_CONSUMPTION_VALUE, TRAVELER_FLIGHT_ACTIVITY_LEVEL, HAS_MULTIPLE_FREQUENT_TRAVELER_CARD_NUMBERS, HAS_MULTIPLE_CUSTOMER_IDS, SUMMER_TRAVEL_COUNT_ALL_TIME, SPRING_FESTIVAL_TRAVEL_COUNT_ALL_TIME, HOLIDAY_TRAVEL_COUNT_ALL_TIME, TRAVEL_SEGMENTS_WITH_ELDERLY_ALL_TIME, TRAVEL_SEGMENTS_WITH_CHILDREN_ALL_TIME, SELF_BOOKING_TRAVEL_SEGMENTS_ALL_TIME, COUPON_USAGE_AMOUNT_LAST_12M, LY_VIP_ID, CRM_CUSTOMER_ID, LY_MEMBER_ID, SYS_REGISTER_ID, FFP_REGISTER_ID, HISTORICAL_FFP_ID, ALIPAY_ID, WECHAT_ID, DOUYIN_ID, CN_NAME, EN_NAME, SEX, USER_TYPE, BIRTHDAY, PROVINCE, CITY, NATIONALITY, ETHNICITY, EMPLOYER, MOBILE_PHONE, MOBILE_NUMBER_RELIABILITY, EMAIL, ID_CARD, PASSPORT, SEAMAN_ID, ALIEN_PERMIT, DIPLOMATIC_STAFF_CERTIFICATE, PERMANENT_RESIDENT_ID, CIVILIAN_STAFF_ID, STAFF_ID, OFFICER_ID_CARD, ARMED_POLICE_OFFICER, ARMED_POLICE_SOLDIER, CIVILIAN_OFFICIAL_ID, CONSCRIPT_SOLDIER_ID, NON_COMMISSIONED_OFFICER_ID, HK_MACAO_RESIDENT_PERMIT, TAIWAN_RESIDENT_TRAVEL_PERMIT, BLIND_PASSENGER, DEAF_PASSENGER, IS_DIRECT_USER, IS_OFFICIAL_WEBSITE_NON_REGISTERED, IS_DOUYIN_CARD_PURCHASER, DIRECT_USER_STATUS, DIRECT_REGISTER_DATE, DIRECT_LASTLOGIN_DATE, DIRECT_VERIFIED_FLAG, DIRECT_VERIFY_DATE, IS_BLACKLIST_USER, STUDENT_FLAG, TEACHER_FLAG, AGENT_FLAG, IS_KEY_ACCOUNT, KEY_ACCOUNT_NUMBER, IS_FREQUENT_TRAVELER, FREQUENT_TRAVELER_CARDNO, FREQUENT_TRAVELER_LEVEL, YJ_CARD_NUMBER, YJ_CARD_EXPIREDATE, DEV_CHANNEL_ONE, DEV_CHANNEL_TWO, DEV_CHANNEL_THREE, DEV_CHANNEL_FOUR, FT_REGISTER_TIME, ACCEPT_SMS_MARKETING, ACCEPT_EMAIL_MARKETING, PARENT_FT_CARDNO, IS_LY_USER, LY_REGISTER_TIME, LY_CARD_NUMBER, LY_USER_LEVEL, LY_USER_STATUS, LY_REGISTER_STATUS, LY_VERIFY_STATUS, LY_LIFETIME_POINTS, LY_AVAILABLE_POINTS, IS_HIGH_TRAVELER, HIGH_TRAVELER_TYPE, HIGH_TRAVELER_TIER, TIER_PRESTIGE_EXPIREDATE, TIER_HONOR_EXPIREDATE, HIGH_TRAVELER_DS, IS_YJ_PERSONNEL, VIP_FLAG, CIP_FLAG, VVIP_FLAG, FOOD_PREFERENCE, SEAT_PREFERENCE, TEMP_FOOD_PREFERENCE, TEMP_SEAT_PREFERENCE, BEVERAGE_PREFERENCE, LONG_TERM_SEAT_PREFERENCE, FIRST_CLASS_LOUNGE_PREFERENCE, MERGED_TO_USERID, IS_FREQUENT_FLYER_NUMBER_VERIFIED, CREATE_TIME, UPDATE_TIME, CUSTOMER_GROUP, MANUAL_TAG, HK_MACAO_TRAVEL_PERMIT, MAINLAND_TO_TAIWAN_TRAVEL_PERMIT, HK_MACAO_TAIWAN_RESIDENCE_PERMIT, HK_MACAO_TAIWAN_ID_CARD, IS_CORP_TRAVEL_MILITARY_CARD, PAS_LOCATION, PRICE_DIS_DEV, DEP_CITY, DCS_UP_CLASS, MEMBER_VALUE, PAS_ACTIVITY, PAS_TRAVEL_VALUE, PASSENGER_COMSUM_VALUE, PAS_VALUE_UPDATE, IS_VALID_USER\n";
            String targetfileds = "";
            String selectfileds = "";
            for (CustomTag tag : tags){
                targetfileds += ","+ tag.getTagName();
                selectfileds += ", t2."+tag.getTagName();
            }
            transsql = transsql + targetfileds +") \n";
            transsql = transsql + " SELECT t1.PK_ID, t1.PASSENGER_SOURCE_CITY, t1.PREFERENCE_DOMESTIC_TOP_CITY, t1.PREFERENCE_INTL_TOP_CITY, t1.PASSENGER_TOTAL_CONSUME_PERCENTILE_ALL_TIME, t1.COUPON_USAGE_COUNT_LAST_12M, t1.AGE, t1.ID_CARD_REGION, t1.HAS_REGISTERED_FREQUENT_TRAVELER, t1.LAST_REGISTRATION_TIME_FREQUENT_TRAVELER, t1.MILEAGE_EXCHANGE_COUNT_12M_AIR, t1.MILEAGE_EXCHANGE_AMOUNT_12M_AIR, t1.MILEAGE_EXCHANGE_COUNT_12M_NON_AIR, t1.MILEAGE_EXCHANGE_AMOUNT_12M_NON_AIR, t1.AVERAGE_ADVANCE_PURCHASE_TIME_LAST_12M, t1.AVERAGE_ADVANCE_CHECK_IN_TIME_LAST_12M, t1.ANCILLARY_PRODUCT_PREFERENCE_CATEGORY, t1.INSURANCE_PURCHASE_COUNT_LAST_12M, t1.PAID_SEAT_SELECTION_PURCHASE_COUNT_LAST_12M, t1.PREPAID_BAGGAGE_PURCHASE_COUNT_LAST_12M, t1.UPGRADE_VOUCHER_USAGE_COUNT_LAST_12M, t1.IN_FLIGHT_UPGRADE_COUNT_BUSINESS_LAST_12M, t1.IN_FLIGHT_UPGRADE_COUNT_PREMIUM_ECONOMY_LAST_12M, t1.BOARDING_GATE_UPGRADE_COUNT_BUSINESS_LAST_12M, t1.BOARDING_GATE_UPGRADE_COUNT_PREMIUM_ECONOMY_LAST_12M, t1.UPGRADE_VOUCHER_USAGE_AMOUNT_LAST_12M, t1.IN_FLIGHT_UPGRADE_AMOUNT_BUSINESS_LAST_12M, t1.IN_FLIGHT_UPGRADE_AMOUNT_PREMIUM_ECONOMY_LAST_12M, t1.BOARDING_GATE_UPGRADE_AMOUNT_BUSINESS_LAST_12M, t1.BOARDING_GATE_UPGRADE_AMOUNT_PREMIUM_ECONOMY_LAST_12M, t1.TOTAL_UPGRADE_VOUCHER_USAGE_COUNT, t1.TOTAL_IN_FLIGHT_UPGRADE_COUNT_PREMIUM_ECONOMY, t1.TOTAL_IN_FLIGHT_UPGRADE_COUNT_BUSINESS, t1.TOTAL_BOARDING_GATE_UPGRADE_COUNT_PREMIUM_ECONOMY, t1.TOTAL_BOARDING_GATE_UPGRADE_COUNT_BUSINESS, t1.TOTAL_UPGRADE_VOUCHER_USAGE_AMOUNT, t1.TOTAL_IN_FLIGHT_UPGRADE_AMOUNT_PREMIUM_ECONOMY, t1.TOTAL_IN_FLIGHT_UPGRADE_AMOUNT_BUSINESS, t1.TOTAL_UPGRADE_CABIN_PREFERENCE, t1.UPGRADE_CABIN_PREFERENCE_LAST_12M, t1.CALCULATED_SEAT_PREFERENCE, t1.COUNTRY_VISITED_COUNT, t1.CITY_VISITED_COUNT, t1.MOST_COMMON_DOMESTIC_DEPARTURE_LOCATION_ALL_TIME, t1.MOST_COMMON_INTERNATIONAL_DEPARTURE_LOCATION_ALL_TIME, t1.MOST_COMMON_DOMESTIC_ARRIVAL_LOCATION_ALL_TIME, t1.MOST_COMMON_INTERNATIONAL_ARRIVAL_LOCATION_ALL_TIME, t1.TOTAL_TRAVEL_SEGMENTS_ALL_TIME, t2.SUSPECTED_AGENT, t1.IS_HIGH_VALUE_FREQUENT_TRAVELER_MEMBER, t2.CUSTOMER_LIFE_CYCLE_LABEL_DIRECT_SALES, t2.CUSTOMER_CHURN_LABEL, t2.CUSTOMER_FLIGHT_PURCHASE_RATE_REPURCHASE_RATE, t1.PAYMENT_METHOD_PREFERENCE, t1.FLIGHT_BOOKING_COUNT_LAST_30_DAYS, t1.TOTAL_TICKET_ISSUANCE_SEGMENTS_ALL_TIME, t1.TOTAL_TICKET_ISSUANCE_AMOUNT_INTERNATIONAL_ALL_TIME, t1.TOTAL_TICKET_ISSUANCE_AMOUNT_DOMESTIC_ALL_TIME, t1.TOTAL_INSURANCE_PRODUCT_PURCHASE_AMOUNT_ALL_TIME, t1.TOTAL_PAID_SEAT_SELECTION_PRODUCT_PURCHASE_AMOUNT_ALL_TIME, t1.TOTAL_PREPAID_BAGGAGE_PRODUCT_PURCHASE_AMOUNT_ALL_TIME, t1.CABIN_CLASS_PREFERENCE_LAST_1_YEAR, t1.TICKET_PURCHASE_SEGMENTS_COUNT_LAST_1_YEAR, t1.TICKET_PURCHASE_AMOUNT_INTERNATIONAL_LAST_1_YEAR, t1.TICKET_PURCHASE_AMOUNT_DOMESTIC_LAST_1_YEAR, t1.INSURANCE_PRODUCT_PURCHASE_AMOUNT_LAST_1_YEAR, t1.PAID_SEAT_SELECTION_PRODUCT_PURCHASE_AMOUNT_LAST_1_YEAR, t1.PREPAID_BAGGAGE_PRODUCT_PURCHASE_AMOUNT_LAST_1_YEAR, t1.GROUP_TICKET_PURCHASE_ISSUANCE_COUNT_LAST_1_YEAR, t1.GROUP_TICKET_PURCHASE_ISSUANCE_COUNT_ALL_TIME, t1.TOTAL_DOMESTIC_TICKET_FACE_VALUE_CARRIED_BY_SHANDONG_AIRLINES_LAST_3_YEARS, t1.TOTAL_INTERNATIONAL_TICKET_FACE_VALUE_CARRIED_BY_SHANDONG_AIRLINES_LAST_3_YEARS, t1.TOTAL_INSURANCE_PRODUCT_CONSUMPTION_AMOUNT_LAST_3_YEARS, t1.TOTAL_PAID_SEAT_SELECTION_PRODUCT_CONSUMPTION_AMOUNT_LAST_3_YEARS, t1.TOTAL_PREPAID_BAGGAGE_PRODUCT_CONSUMPTION_AMOUNT_LAST_3_YEARS, t1.TOTAL_UNACCOMPANIED_MINOR_EMD_PRODUCT_CONSUMPTION_AMOUNT_LAST_3_YEARS, t1.FLIGHT_CONSUMPTION_RANK_LAST_3_YEARS, t1.TOTAL_CONSUMPTION_AMOUNT_ALL_TIME, t1.TOTAL_CONSUMPTION_RANK_ALL_TIME, t1.FLIGHT_SEGMENTS_COUNT_LAST_3_YEARS, t1.CUSTOMER_CONTRIBUTION, t1.TRAVELER_SOURCE_LOCATION, t1.TRAVELER_PREFERRED_DESTINATION_CITY, t1.TRAVELER_DEPARTURE_UPGRADE_PREFERENCE, t1.TRAVELER_DOMESTIC_TICKET_DISCOUNT_DEVIATION, t1.FREQUENT_TRAVELER_VALUE, t1.TRAVELER_COMPREHENSIVE_VALUE, t1.TRAVELER_CONSUMPTION_VALUE, t1.TRAVELER_FLIGHT_ACTIVITY_LEVEL, t1.HAS_MULTIPLE_FREQUENT_TRAVELER_CARD_NUMBERS, t1.HAS_MULTIPLE_CUSTOMER_IDS, t1.SUMMER_TRAVEL_COUNT_ALL_TIME, t1.SPRING_FESTIVAL_TRAVEL_COUNT_ALL_TIME, t1.HOLIDAY_TRAVEL_COUNT_ALL_TIME, t1.TRAVEL_SEGMENTS_WITH_ELDERLY_ALL_TIME, t1.TRAVEL_SEGMENTS_WITH_CHILDREN_ALL_TIME, t1.SELF_BOOKING_TRAVEL_SEGMENTS_ALL_TIME, t1.COUPON_USAGE_AMOUNT_LAST_12M, t1.LY_VIP_ID, t1.CRM_CUSTOMER_ID, t1.LY_MEMBER_ID, t1.SYS_REGISTER_ID, t1.FFP_REGISTER_ID, t1.HISTORICAL_FFP_ID, t1.ALIPAY_ID, t1.WECHAT_ID, t1.DOUYIN_ID, t1.CN_NAME, t1.EN_NAME, t1.SEX, t1.USER_TYPE, t1.BIRTHDAY, t1.PROVINCE, t1.CITY, t1.NATIONALITY, t1.ETHNICITY, t1.EMPLOYER, t1.MOBILE_PHONE, t1.MOBILE_NUMBER_RELIABILITY, t1.EMAIL, t1.ID_CARD, t1.PASSPORT, t1.SEAMAN_ID, t1.ALIEN_PERMIT, t1.DIPLOMATIC_STAFF_CERTIFICATE, t1.PERMANENT_RESIDENT_ID, t1.CIVILIAN_STAFF_ID, t1.STAFF_ID, t1.OFFICER_ID_CARD, t1.ARMED_POLICE_OFFICER, t1.ARMED_POLICE_SOLDIER, t1.CIVILIAN_OFFICIAL_ID, t1.CONSCRIPT_SOLDIER_ID, t1.NON_COMMISSIONED_OFFICER_ID, t1.HK_MACAO_RESIDENT_PERMIT, t1.TAIWAN_RESIDENT_TRAVEL_PERMIT, t1.BLIND_PASSENGER, t1.DEAF_PASSENGER, t1.IS_DIRECT_USER, t1.IS_OFFICIAL_WEBSITE_NON_REGISTERED, t1.IS_DOUYIN_CARD_PURCHASER, t1.DIRECT_USER_STATUS, t1.DIRECT_REGISTER_DATE, t1.DIRECT_LASTLOGIN_DATE, t1.DIRECT_VERIFIED_FLAG, t1.DIRECT_VERIFY_DATE, t1.IS_BLACKLIST_USER, t1.STUDENT_FLAG, t1.TEACHER_FLAG, t1.AGENT_FLAG, t1.IS_KEY_ACCOUNT, t1.KEY_ACCOUNT_NUMBER, t1.IS_FREQUENT_TRAVELER, t1.FREQUENT_TRAVELER_CARDNO, t1.FREQUENT_TRAVELER_LEVEL, t1.YJ_CARD_NUMBER, t1.YJ_CARD_EXPIREDATE, t1.DEV_CHANNEL_ONE, t1.DEV_CHANNEL_TWO, t1.DEV_CHANNEL_THREE, t1.DEV_CHANNEL_FOUR, t1.FT_REGISTER_TIME, t1.ACCEPT_SMS_MARKETING, t1.ACCEPT_EMAIL_MARKETING, t1.PARENT_FT_CARDNO, t1.IS_LY_USER, t1.LY_REGISTER_TIME, t1.LY_CARD_NUMBER, t1.LY_USER_LEVEL, t1.LY_USER_STATUS, t1.LY_REGISTER_STATUS, t1.LY_VERIFY_STATUS, t1.LY_LIFETIME_POINTS, t1.LY_AVAILABLE_POINTS, t1.IS_HIGH_TRAVELER, t1.HIGH_TRAVELER_TYPE, t1.HIGH_TRAVELER_TIER, t1.TIER_PRESTIGE_EXPIREDATE, t1.TIER_HONOR_EXPIREDATE, t1.HIGH_TRAVELER_DS, t1.IS_YJ_PERSONNEL, t1.VIP_FLAG, t1.CIP_FLAG, t1.VVIP_FLAG, t1.FOOD_PREFERENCE, t1.SEAT_PREFERENCE, t1.TEMP_FOOD_PREFERENCE, t1.TEMP_SEAT_PREFERENCE, t1.BEVERAGE_PREFERENCE, t1.LONG_TERM_SEAT_PREFERENCE, t1.FIRST_CLASS_LOUNGE_PREFERENCE, t1.MERGED_TO_USERID, t1.IS_FREQUENT_FLYER_NUMBER_VERIFIED, t1.CREATE_TIME, t1.UPDATE_TIME, t1.CUSTOMER_GROUP, t1.MANUAL_TAG, t1.HK_MACAO_TRAVEL_PERMIT, t1.MAINLAND_TO_TAIWAN_TRAVEL_PERMIT, t1.HK_MACAO_TAIWAN_RESIDENCE_PERMIT, t1.HK_MACAO_TAIWAN_ID_CARD, t1.IS_CORP_TRAVEL_MILITARY_CARD, t1.PAS_LOCATION, t1.PRICE_DIS_DEV, t1.DEP_CITY, t1.DCS_UP_CLASS, t1.MEMBER_VALUE, t1.PAS_ACTIVITY, t1.PAS_TRAVEL_VALUE, t1.PASSENGER_COMSUM_VALUE, t1.PAS_VALUE_UPDATE, t1.IS_VALID_USER\n";
            transsql = transsql + selectfileds+" \n";
            transsql = transsql + " FROM " + dbSchema + "." + orgTableName + " t1 \n";
            transsql = transsql + " LEFT JOIN "+dbSchema+"."+transTableName+" t2 ON t1.PK_id = t2.PK_id;";
            System.out.println("数据同步transsql:"+transsql);

            Connection connection = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
            Statement statement = DorisUtils.getStatement(connection);
            DorisUtils.excuteDorisInsert(statement, transsql);

            DorisUtils.close(connection, statement, null);
        } catch (Exception e) {
            // 记录异常日志
            System.out.println("数据传输失败，原因：" + e.getMessage());
        }
        long endTime = System.currentTimeMillis();
        System.out.println("数据同步完成，耗时：" + (endTime - startTime) / 1000 + "秒");
    }
}