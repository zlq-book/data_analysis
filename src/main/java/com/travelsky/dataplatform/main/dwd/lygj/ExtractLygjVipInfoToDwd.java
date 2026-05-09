package com.travelsky.dataplatform.main.dwd.lygj;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.LygjCreateToSql;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.transform.lygj.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


public class ExtractLygjVipInfoToDwd {

    //private static final Logger logger = LoggerFactory.getLogger(ExtractLygjVipInfoToDwd.class);

    public static void main(String[] args) throws Exception {

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
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointUtils.setCheckpoint(env, "ExtractLygjVipInfoToDwd");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        //1. 创建Doris目标表
        tEnv.executeSql(LygjCreateToSql.VIP_INFO);

        String vipInfoQuery = "SELECT " +
                "ETL_DATE, " +
                "ID, " +
                "CARD_TYPE, " +
                "VIP_TYPE_ID, " +
                "ID_CARD, " +
                "CERTIFICATE_TYPE, " +
                "NAME, " +  // 姓名（中英文）
                "GENDER, " +  // 性别
                "BIRTHDAY, " +  // 生日
                "NATIONALITY, " +  // 民族
                "MOBILE, " +  // 手机号
                "COMPANY, " +  // 工作单位
                "LAYER_TYPE_ID, " +  // 分层类型
                "LAYER_VALIDITY, " +  // 分层有效期
                "BL8, " +  // 高端旅客数据来源
                "VIP_FOOD_LOVE_NAME, " +  // 餐食喜好名称
                "VIP_DRINK_LOVE_NAMES, " +  // 饮品喜好名称

                "VIP_FOOD_LOVE_IDS, " +  // 餐食喜好
                "SEAT_LOVE, " +  // 座位喜好
                "FOOD_DESC_TEMP, " + // 临时餐食喜好
                "SEAT_LOVE_TEMP, " + // 临时座位喜好
                "VIP_SEAT_LOVE_NAMES, " + // 长期座位喜好
                "VIP_LOUNGE_LOVE_NAMES, " + // 头等舱休息室喜好
                "VIP_DRINK_LOVE_IDS " +  // 饮品喜好
                "FROM T_ODS_LYGJ_VIP_INFO " +
                "WHERE ETL_DATE >= '" + startDate + "'"  + " AND ETL_DATE <= '" + endDate + "'";
        System.out.println("日期为：" + etlDate + "的VIP_INFO数据开始查询...");
        Table vipInfoTable = tEnv.sqlQuery(vipInfoQuery);
        //logger.info("VIP信息查询SQL：" + vipInfoQuery);
        DataStream<JSONObject> vipInfoStream = tEnv.toChangelogStream(vipInfoTable).map(new MapFunction<Row, JSONObject>() {
            @Override
            public JSONObject map(Row row) throws Exception {
                JSONObject map = new JSONObject();
                String idCard = row.getFieldAs("ID_CARD");
                if (StringUtils.isBlank(idCard)) return null;
                // 1. 获取所有字段值
                String vipInfoEtlDate = DateTimeUtils.localDateToString(row.getFieldAs("ETL_DATE"));
                String id = row.getFieldAs("ID").toString();
                String cardType = row.getFieldAs("CARD_TYPE");
                String vipTypeId = row.getFieldAs("VIP_TYPE_ID").toString();
                String certificateType = row.getFieldAs("CERTIFICATE_TYPE");
                String name = row.getFieldAs("NAME");
                String gender = row.getFieldAs("GENDER");
                String birthday = row.getFieldAs("BIRTHDAY");
                String nationality = row.getFieldAs("NATIONALITY");
                String company = row.getFieldAs("COMPANY");
                String layerTypeId = row.getFieldAs("LAYER_TYPE_ID");
                String layerValidity = null == row.getFieldAs("LAYER_VALIDITY") ? "" : DateTimeUtils.localDateTimeToString(row.getFieldAs("LAYER_VALIDITY"));
                String bl8 = row.getFieldAs("BL8");
                String foodLoveIds = row.getFieldAs("VIP_FOOD_LOVE_IDS");
                String seatLove = row.getFieldAs("SEAT_LOVE");
                String foodDescTemp = row.getFieldAs("FOOD_DESC_TEMP");

                String seatLoveTemp = row.getFieldAs("SEAT_LOVE_TEMP");
                String vipSeatLoveNames = row.getFieldAs("VIP_SEAT_LOVE_NAMES");
                String vipLoungeLoveNames = row.getFieldAs("VIP_LOUNGE_LOVE_NAMES");
                String drinkLoveIds = row.getFieldAs("VIP_DRINK_LOVE_IDS");
                String mobile = row.getFieldAs("MOBILE");
                String vipFoodLoveName = row.getFieldAs("VIP_FOOD_LOVE_NAME");
                String vipDrinkLoveNames = row.getFieldAs("VIP_DRINK_LOVE_NAMES");

                // 2. 标准化处理关键字段
                idCard = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, idCard, DataSource.LUYAN_STEWARD);
                cardType = NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, cardType, DataSource.LUYAN_STEWARD);
                certificateType = NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, certificateType, DataSource.LUYAN_STEWARD);
                gender = NormalizationUtils.standardize(FieldType.GENDER, gender, DataSource.LUYAN_STEWARD);
                mobile = NormalizationUtils.standardize(FieldType.MOBILE_NO, mobile, DataSource.LUYAN_STEWARD);
                foodDescTemp = NormalizationUtils.standardize(FieldType.LYGJ_VIP_FOOD_LOVE_NAME, foodDescTemp, DataSource.LUYAN_STEWARD);
                // 3. 生成TID
                Tid tid = new Tid();
                tid.setTid(idCard);
                Map<String, String> certification = new HashMap<>();
                certification.put(certificateType, idCard);
                tid.setCertification(certification);
                String tidStr = IdMapping.idMappingFunction(tid, "LYGJ");
                if (tidStr == null) return null;
                // 4. 将所有字段放入Map
                // 基础信息
                map.put("TID", tidStr);
                map.put("ETL_DATE", vipInfoEtlDate);
                map.put("ID", id);
                map.put("CARD_TYPE", cardType);
                map.put("VIP_TYPE_ID", vipTypeId);
                map.put("ID_CARD", idCard);
                map.put("CERTIFICATE_TYPE", certificateType);

                // 用户信息
                map.put("NAME", name);
                map.put("GENDER", gender);
                map.put("BIRTHDAY", birthday);
                map.put("NATIONALITY", nationality);
                map.put("COMPANY", company);
                map.put("MOBILE", mobile);

                // 高端旅客信息
                map.put("LAYER_TYPE_ID", layerTypeId);
                map.put("LAYER_VALIDITY", layerValidity);
                map.put("BL8", bl8);
                map.put("VIP_FOOD_LOVE_IDS", foodLoveIds);
                map.put("SEAT_LOVE", seatLove);
                map.put("FOOD_DESC_TEMP", foodDescTemp);
                map.put("SEAT_LOVE_TEMP", seatLoveTemp);
                map.put("VIP_SEAT_LOVE_NAMES", vipSeatLoveNames);
                map.put("VIP_LOUNGE_LOVE_NAMES", vipLoungeLoveNames);
                map.put("VIP_DRINK_LOVE_IDS", drinkLoveIds);
                map.put("VIP_FOOD_LOVE_NAME", vipFoodLoveName);
                map.put("VIP_DRINK_LOVE_NAMES", vipDrinkLoveNames);

                return map;
            }
        }).filter(Objects::nonNull);

        //写高端旅客维表
        LygjVipInfoToGdlkDimTrans.result(vipInfoStream);
        // 写高端旅客类型维表
        LygjVipInfoToGdlkTypeDimTrans.result(vipInfoStream);
        // 写证件信息维表
        LygjVipInfoToCertDimTrans.result(vipInfoStream);
        // vipinfo写用户维表
        LygjVipInfoToUserDimFrans.result(vipInfoStream);
        // vipInfo写手机号维表
        LygjVipInfoToMobileDimTrans.result(vipInfoStream);
        //启动任务
        env.execute("ExtractLygjVipInfoToDwd");


    }
}
