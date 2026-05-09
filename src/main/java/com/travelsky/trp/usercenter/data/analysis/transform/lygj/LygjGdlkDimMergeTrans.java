package com.travelsky.trp.usercenter.data.analysis.transform.lygj;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


public class LygjGdlkDimMergeTrans {
    public static void result(DataStream<JSONObject> sourceStream) {
        SingleOutputStreamOperator<GdlkDimModel> gdlkDimFactStream = sourceStream.flatMap(
                        new RichFlatMapFunction<JSONObject, GdlkDimModel>() {
                            Connection conn;
                            Statement stmt;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                conn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DIM_USER, Constants.DIM_PWD);
                                stmt = DorisUtils.getStatement(conn);
                            }

                            @Override
                            public void close() throws Exception {
                                DorisUtils.close(conn, stmt, null);
                            }

                            @Override
                            public void flatMap(JSONObject json, Collector<GdlkDimModel> out) throws Exception {
                                try {
                                    Map<String, String> row = json.toJavaObject(Map.class);
                                    GdlkDimModel model = new GdlkDimModel();
                                    String tid = row.get("TID");
                                    if (StringUtils.isBlank(tid)) {
                                        System.out.println("TID为空，跳过处理: " + json);
                                        return;
                                    }
                                    String vip = row.get("IS_VIP");
                                    String cip = row.get("IS_CIP");
                                    String vvip = row.get("IS_VVIP");
                                    // 查询维度表，判断记录是否存在
                                    String selectQuery = "SELECT SYSTEM_KEY FROM " + Constants.DIM_DB + ".T_DIM_GDLK_DIM " +
                                            "WHERE FK_USER_TID = '" + tid + "'";
                                    ResultSet res = DorisUtils.getDorisResult(stmt, selectQuery);

                                    if (res != null && res.next()) {
                                        // 记录存在，使用查询到的 SYSTEM_KEY
                                        String systemKey = res.getString("SYSTEM_KEY");
                                        if (StringUtils.isNotBlank(systemKey)) {
                                            model.setVipFlag(Boolean.valueOf(vip));
                                            model.setCipFlag(Boolean.valueOf(cip));
                                            model.setVvipFlag(Boolean.valueOf(vvip));
                                            model.setFkUserTid(tid);
                                            model.setSystemKey(systemKey);
                                        }
                                        res.close();
                                    } else{
                                        return;
                                    }
                                    out.collect(model);
                                } catch (Exception e) {
                                    System.out.println("处理信息数据失败: " + json);
                                }
                            }
                        }).returns(TypeInformation.of(GdlkDimModel.class))
                .filter(Objects::nonNull)
                .name("GdlkDimFactStreamFlatMap");
        // 3. 写入Doris
        DorisSink<GdlkDimModel> dorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_GDLK_DIM");
        gdlkDimFactStream.sinkTo(dorisSink);
    }
}
