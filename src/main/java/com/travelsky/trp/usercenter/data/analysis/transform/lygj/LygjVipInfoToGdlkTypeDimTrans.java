package com.travelsky.trp.usercenter.data.analysis.transform.lygj;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.DorisUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.GdlkTypeDimModel;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.decomposeVipType;
/**
 * 高端旅客类型维表
 */
public class LygjVipInfoToGdlkTypeDimTrans {
    //private static final Logger logger = LoggerFactory.getLogger(LygjVipInfoToGdlkTypeDimTrans.class);
    public static void result(DataStream<JSONObject> vipInfoStream) {

        // 2. 处理VIP信息数据
        SingleOutputStreamOperator<GdlkTypeDimModel> gdlkTypeDimFactStream = vipInfoStream.flatMap(
                new RichFlatMapFunction<JSONObject, GdlkTypeDimModel>() {
                    Connection conn;
                    Statement stmt;
                    List<Long> typeIds = new ArrayList<>();
                    Map<Long, Map<String, String>> vipTypeMap = new HashMap<>();
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
                    }

                    @Override
                    public void close() throws Exception {
                        DorisUtils.close(conn, stmt, null);
                    }

                    @Override
                    public void flatMap(JSONObject json, Collector<GdlkTypeDimModel> out) throws Exception {
                        try {
                            Map<String, String> map = json.toJavaObject(Map.class);
                            long vipTypeId = Long.parseLong(map.get("VIP_TYPE_ID"));
                            List<Long> vipInfoTypeIds = decomposeVipType(vipTypeId, typeIds);

                            for (long vipInfoTypeId : vipInfoTypeIds) {
                                GdlkTypeDimModel model = new GdlkTypeDimModel();
                                // 设置TID
                                model.settId(map.get("TID"));
                                String id = map.get("ID");
                                model.setFkLyVipId(id);

                                // 获取VIP类型信息
                                Map<String, String> vipType = vipTypeMap.get(vipInfoTypeId);
                                if (vipType != null) {
                                    model.setHighTravelerType(vipType.get("NAME"));
                                    model.setHighTravelerCode(vipType.get("CODE"));
                                    model.setHighTravelerTypeId(vipType.get("ID"));
                                }
                                // 设置系统主键
                                model.setSystemKey(map.get("TID") + vipType.get("ID"));
                                // 系统信息
                                String now = DateTimeUtils.getCurrentDateTime();
                                model.setCreateTime(now);
                                model.setUpdateTime(now);

                                out.collect(model);
                            }
                        } catch (Exception e) {
                            //logger.error("处理VIP信息数据失败: {}", json, e);
                            System.out.println("处理VIP信息数据失败: " + json);
                        }
                    }
                })
                .returns(TypeInformation.of(GdlkTypeDimModel.class))
                .name("GdlkTypeDimFactStreamFlatMap");

        // 3. 写入Doris
        DorisSink<GdlkTypeDimModel> dorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_GDLK_TYPE_DIM");
        gdlkTypeDimFactStream.sinkTo(dorisSink);
    }
}
