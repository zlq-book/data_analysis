package com.travelsky.trp.usercenter.data.analysis.transform.lygj;


import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.DorisUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.GdlkDimModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.decomposeVipType;

/**
 * 高端旅客维表
 */
public class LygjVipInfoToGdlkDimTrans {
    //private static final Logger logger = LoggerFactory.getLogger(LygjDpiToDwdBookingPnrFactTrans.class);

    public static void result(DataStream<JSONObject> vipInfoStream) {
        //tEnv.executeSql(LygjCreateToSql.VIP_TYPE);
        //tEnv.executeSql(LygjCreateToSql.LAYER_TYPE);

        // 2. 处理VIP信息数据
        SingleOutputStreamOperator<GdlkDimModel> gdlkDimFactStream = vipInfoStream.flatMap(
                        new RichFlatMapFunction<JSONObject, GdlkDimModel>() {
                            Connection conn;
                            Statement stmt;
                            Map<Long, Map<String, String>> vipTypeMap = new HashMap<>();
                            Map<Long, Map<String, String>> layerTypeMap = new HashMap<>();
                            List<Long> typeIds = new ArrayList<>();
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                conn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.ODS_USER, Constants.ODS_PWD);
                                stmt = DorisUtils.getStatement(conn);
                                String vipTypeQuery = "SELECT " +
                                        "ID, " +
                                        "CODE, " +
                                        "NAME " +
                                        "FROM " + Constants.ODS_DB + ".T_ODS_LYGJ_VIP_TYPE ";
                                ResultSet res = DorisUtils.getDorisResult(stmt, vipTypeQuery);
                                if (null == res) {
                                    throw new Exception("T_ODS_LYGJ_VIP_TYPE字典数据查询异常");
                                }
                                while (res.next()) {
                                    Object idObj = res.getString("ID");
                                    Long id = null;
                                    if (idObj instanceof Number) {
                                        id = ((Number) idObj).longValue();
                                    } else if (idObj instanceof String) {
                                        id = Long.parseLong((String) idObj);
                                    }

                                    if (id != null) {
                                        typeIds.add(id);
                                        // 将Row转换为Map存储
                                        Map<String, String> vipType = new HashMap<>();
                                        vipType.put("ID", res.getString("ID").toString());
                                        vipType.put("CODE", res.getString("CODE"));
                                        vipType.put("NAME", res.getString("NAME"));
                                        vipTypeMap.put(id, vipType);
                                    }
                                }
                                String layerTypeQuery = "SELECT " +
                                        "ID, " +
                                        "TYPE_NAME " +
                                        "FROM " + Constants.ODS_DB + ".T_ODS_LYGJ_LAYER_TYPE ";
                                ResultSet layerTypeRs = DorisUtils.getDorisResult(stmt, layerTypeQuery);
                                if (null == layerTypeRs) {
                                    throw new Exception("T_ODS_LYGJ_LAYER_TYPE字典数据查询异常");
                                }
                                while (layerTypeRs.next()) {
                                    Object idObj = layerTypeRs.getString("ID");
                                    Long id = null;
                                    if (idObj instanceof Number) {
                                        id = ((Number) idObj).longValue();
                                    } else if (idObj instanceof String) {
                                        id = Long.parseLong((String) idObj);
                                    }
                                    if (id != null) {
                                        // 将Row转换为Map存储
                                        Map<String, String> layerType = new HashMap<>();
                                        layerType.put("ID", layerTypeRs.getString("ID").toString());
                                        layerType.put("TYPE_NAME", layerTypeRs.getString("TYPE_NAME"));
                                        layerTypeMap.put(id, layerType);
                                    }
                                }
                            }

                            @Override
                            public void close() throws Exception {
                                DorisUtils.close(conn, stmt, null);
                            }

                            @Override
                            public void flatMap(JSONObject json, Collector<GdlkDimModel> out) throws Exception {
                                try {
                                    Map<String, String> map = json.toJavaObject(Map.class);
                                    GdlkDimModel model = new GdlkDimModel();
                                    String layerTypeId = map.get("LAYER_TYPE_ID");
                                    String vipId = map.get("ID");
                                    // 设置TID
                                    model.setFkUserTid(map.get("TID"));
                                    long vipTypeId = StringUtils.isNotBlank(map.get("VIP_TYPE_ID")) ? Long.parseLong(map.get("VIP_TYPE_ID")) : null;
                                    if (StringUtils.isBlank(vipId) || StringUtils.isBlank(map.get("TID"))) {
                                        System.out.println("提取GdlkDimFactStream失败，主键部分组成为空: " + map.toString());
                                        //logger.error("提取GdlkDimFactStream失败，主键部分组成为空: {}", map.toString());
                                        return;
                                    }
                                    // 设置系统主键
                                    model.setSystemKey(map.get("TID") + vipId);
                                    // 高端旅客类型
                                    List<Long> vipInfoTypeIds = decomposeVipType(vipTypeId, typeIds);
                                    String names = vipInfoTypeIds.stream()
                                            .map(vipTypeMap::get)         // 获取对应的 Map
                                            .filter(Objects::nonNull)     // 过滤掉 null 的 Map
                                            .map(vipType -> vipType.get("NAME"))  // 提取 NAME
                                            .filter(Objects::nonNull)     // 过滤掉 null 的 NAME
                                            .collect(Collectors.joining(","));  // 用逗号拼接

                                    model.setHighTravelerType(names);
                                    // 高端旅客分层类型
                                    if (StringUtils.isNotBlank(layerTypeId)) {
                                        long ltId = Long.parseLong(layerTypeId);
                                        String typeName = layerTypeMap.get(ltId).get("TYPE_NAME");
                                        model.setHighTravelerTier(typeName);
                                        String layerValidity = map.get("LAYER_VALIDITY");
                                        if ("至尊".equals(typeName)) {
                                            // 至尊身份分层有效期
                                            model.setTierPrestigeExpiredate(layerValidity);
                                        } else if ("荣耀".equals(typeName)) {
                                            // 荣耀身份分层有效期
                                            model.setTierHonorExpiredate(layerValidity);
                                        }

                                    }

                                    // 高端旅客数据来源
                                    model.setHighTravelerDs(map.get("BL8"));

                                    // VIP标识

                                    // CIP标识

                                    // VVIP标识

                                    // 餐食喜好
                                    String vipFoodLoveName = map.get("VIP_FOOD_LOVE_NAME");
                                    model.setFoodPreference(vipFoodLoveName);

                                    // 座位喜好
                                    String seatLove = map.get("SEAT_LOVE");
                                    model.setSeatPreference(seatLove);

                                    // 临时餐食喜好
                                    String foodDescTemp = map.get("FOOD_DESC_TEMP");
                                    model.setTempFoodPreference(foodDescTemp);

                                    // 临时座位喜好
                                    String seatLoveTemp = map.get("SEAT_LOVE_TEMP");
                                    model.setTempSeatPreference(seatLoveTemp);

                                    // 饮品喜好
                                    String vipDrinkLoveNames = map.get("VIP_DRINK_LOVE_NAMES");
                                    model.setBeveragePreference(vipDrinkLoveNames);

                                    // 长期座位喜好
                                    String vipSeatLoveNames = map.get("VIP_SEAT_LOVE_NAMES");
                                    model.setLongTermSeatPreference(vipSeatLoveNames);

                                    // 头等舱休息室喜好
                                    String vipLoungeLoveNames = map.get("VIP_LOUNGE_LOVE_NAMES");
                                    model.setFirstClassLoungePreference(vipLoungeLoveNames);


                                    // 系统信息
                                    String now = DateTimeUtils.getCurrentDateTime();
                                    model.setCreateTime(now);
                                    model.setUpdateTime(now);

                                    out.collect(model);
                                } catch (Exception e) {
                                    //logger.error("处理VIP信息数据失败: {}", json, e);
                                    System.out.println("处理VIP信息数据失败: " + json);
                                }
                            }
                        })
                .returns(TypeInformation.of(GdlkDimModel.class))
                .name("GdlkDimFactStreamFlatMap");

        // 3. 写入Doris
        DorisSink<GdlkDimModel> dorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_GDLK_DIM");
        gdlkDimFactStream.sinkTo(dorisSink);
    }
}
