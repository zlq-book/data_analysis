package com.travelsky.trp.usercenter.data.analysis.transform.Kfbp;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.AuisSegFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class KfbpBusOrderToDwdAuisSegFactTrans {
    public static void result(DataStream<JSONObject> source) {
        SingleOutputStreamOperator<AuisSegFactModel> auisSegFactStream = source

                .flatMap(new FlatMapFunction<JSONObject, AuisSegFactModel>() {
                    @Override
                    public void flatMap(JSONObject map, Collector<AuisSegFactModel> out) throws Exception {
                        AuisSegFactModel model = new AuisSegFactModel();

                        // 渠道订单号 呼叫白屏票务系统	BUS_ORDER（基本订单表）	order_no
                        model.setAkOrdernum(map.get("ORDER_SN") == null ? null : map.get("ORDER_SN").toString());
                        //预订渠道	呼叫白屏票务系统	BUS_ORDER（基本订单表）	写死：呼叫白屏票务系统-code:SCWHP
                        model.setAkChannel("SCWHP");

                        // 票号
                        model.setAkTiknum(map.get("TICKET_NO") == null ? null : map.get("TICKET_NO").toString());
                        // 预定日期时间
                        model.setFkBkauisDate(map.get("CREATE_DATE") == null ? null
                                :DateTimeUtils.dateTimeParseToDateFormat(map.get("CREATE_DATE").toString(), "yyyy-MM-dd"));
                        model.setFkBkauisTime(map.get("CREATE_DATE") == null ? null
                                :DateTimeUtils.dateTimeParseToDateFormat(map.get("CREATE_DATE").toString(), "HH:mm:ss"));

                        model.setCnName(map.get("FARE_NAME") == null ? null : map.get("FARE_NAME").toString());
                        model.setAkCertType(map.get("ID_TYPE") == null ? null : map.get("ID_TYPE").toString());
                        model.setCertNumber(map.get("ID_CODE") == null ? null : map.get("ID_CODE").toString());
                        // TID
                        model.setFkPassengerUserTid(map.get("TID") == null ? null : map.get("TID").toString());
                        model.setFkBookingUserTid(map.get("BOOKING_TID") == null ? null : map.get("BOOKING_TID").toString());
                        model.setFkBookingUserOriginId(map.get("CUR_TEL") == null ? null : map.get("CUR_TEL").toString());

                        // 时间 机场
                        model.setFkSegDate(map.get("FLT_DEPARTURE_DAY") == null ? null
                                :DateTimeUtils.dateTimeParseToDateFormat(map.get("FLT_DEPARTURE_DAY").toString(), "yyyy-MM-dd"));
                        model.setFkSegTime(map.get("FLT_DEPARTURE_DAY") == null ? null
                                :DateTimeUtils.dateTimeParseToDateFormat(map.get("FLT_DEPARTURE_DAY").toString(), "HH:mm:ss"));
                        model.setFkDepairport(map.get("FLT_START_CITY") == null ? null : map.get("FLT_START_CITY").toString());
                        model.setFkArriairport(map.get("FLT_ARRIVE_CITY") == null ? null : map.get("FLT_ARRIVE_CITY").toString());

                        LocalDateTime now = LocalDateTime.now();
                        model.setSystemCreatetime(now.toString());
                        model.setSystemLastUpdatetime(now.toString());

                        // 判断3中类型 都要输出 pkId 保单号 类型 状态 金额
                        if (null != map.get("INSURE_EXPECT_POLICY_NO")) {
                            // 组装主键
                            model.setPkId(model.getAkTiknum() + map.get("INSURE_EXPECT_POLICY_NO").toString());
                            model.setInsurancePolicyNo(map.get("INSURE_EXPECT_POLICY_NO").toString());
                            model.setAkInsuranceType("HYX");
                            model.setAkInsurstatus(map.get("INSURE_EXPECT_STATUS") == null ? null : map.get("INSURE_EXPECT_STATUS").toString());
                            model.setInsuranceAmt(map.get("EXPECT_INSURE_FEE") == null ? null : Double.parseDouble(map.get("EXPECT_INSURE_FEE").toString()));
                            model.setInsuranceCount(1l);

                            // 输出记录
                            out.collect(model);
                        }

                        if (null != map.get("INSURE_DELAY_POLICY_NO")) {
                            // 组装主键
                            model.setPkId(model.getAkTiknum() + map.get("INSURE_DELAY_POLICY_NO").toString());
                            model.setInsurancePolicyNo(map.get("INSURE_DELAY_POLICY_NO").toString());
                            model.setAkInsuranceType("HYWX");
                            model.setAkInsurstatus(map.get("INSURE_DELAY_STATUS") == null ? null : map.get("INSURE_DELAY_STATUS").toString());
                            model.setInsuranceAmt(map.get("DELAY_INSURE_FEE") == null ? null : Double.parseDouble(map.get("DELAY_INSURE_FEE").toString()));
                            model.setInsuranceCount(1l);

                            // 输出记录
                            out.collect(model);
                        }

                        if (null != map.get("INSURE_ALL_RISKS_POLICY_NO")) {
                            // 组装主键
                            model.setPkId(model.getAkTiknum() + map.get("INSURE_ALL_RISKS_POLICY_NO").toString());
                            model.setInsurancePolicyNo(map.get("INSURE_ALL_RISKS_POLICY_NO").toString());
                            model.setAkInsuranceType("ZHX");
                            model.setAkInsurstatus(map.get("INSURE_ALL_RISKS_STATUS") == null ? null : map.get("INSURE_ALL_RISKS_STATUS").toString());
                            model.setInsuranceAmt(map.get("ALL_RISKS_INSURE_FEE") == null ? null : Double.parseDouble(map.get("ALL_RISKS_INSURE_FEE").toString()));
                            model.setInsuranceCount(1l);

                            // 输出记录
                            out.collect(model);
                        }


                    }
                });

        // 写入Doris
        DorisSink<AuisSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_AUIS_SEG_FACT");
        auisSegFactStream.sinkTo(dorisSink);
    }
}
