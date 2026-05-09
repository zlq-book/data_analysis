package com.travelsky.trp.usercenter.data.analysis.transform.Kfbp;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.BookingPnrFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class KfbpBusOrderToDwdBookingPnrFactTrans {
    public static void result(DataStream<JSONObject> source) {
        SingleOutputStreamOperator<BookingPnrFactModel> bookingPnrFactStream = source

                .flatMap(new FlatMapFunction<JSONObject, BookingPnrFactModel>() {
                    @Override
                    public void flatMap(JSONObject map, Collector<BookingPnrFactModel> out) throws Exception {
                        String createDateStr = map.getString("CREATE_DATE");
                        // 获取创建日期
                        LocalDateTime createDate = null;
                        if (map.getString("CREATE_DATE") != null) {
                            // String转LocalDateTime
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
                            createDate = LocalDateTime.parse(createDateStr, formatter);
                        }
                        // 获取pnr
                        String pnr = map.getString("PNR");
                        // 判断是否需要生成两个主键（23点的数据）
                        // 生成主键列表
                        List<String> pkIds = new ArrayList<>();
                        if (createDate != null) {
                            int hour = createDate.getHour();
                            if (hour == 23) {
                                // T 日期
                                String dateT = createDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                                pkIds.add(dateT + pnr);
                                // 如果是23点，添加 T+1 日期
                                String dateTPlus1 = createDate.plusDays(1)
                                        .format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                                pkIds.add(pnr + dateTPlus1);
                            }
                        }
                        // 判断是否需要生成两个主键（00：00：00-00：01：00）

                        if (createDate != null) {
                            int hour = createDate.getHour();
                            int minute = createDate.getMinute();
                            if (hour == 00 && minute < 01) {
                                // T 日期
                                String dateT = createDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                                pkIds.add(pnr + dateT);
                                // 如果是00：00：00-00：01：00，添加 T-1 日期
                                String dateTPlus1 = createDate.minusDays(1)
                                        .format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                                pkIds.add(pnr + dateTPlus1);
                            }
                        }
                        // 为每个主键生成一条记录
                        for (String pkId : pkIds) {
                            BookingPnrFactModel model = new BookingPnrFactModel();
                            // 组装主键
                            model.setPkId(pkId);
                            // 渠道订单号 呼叫白屏票务系统	BUS_ORDER（基本订单表）	order_sn
                            model.setChannelOrderId(map.getString("ORDER_SN"));
                            //预订渠道	呼叫白屏票务系统	BUS_ORDER（基本订单表）	写死：呼叫白屏票务系统-code:SCWHP
                            model.setAkChannel("SCWHP");
                            //联系人手机号 呼叫白屏票务系统	BUS_ORDER（基本订单表）	contact_mobile
                            model.setContactMobileNumber(map.getString("CONTACT_MOBILE"));
                            //联系人姓名 呼叫白屏票务系统	BUS_ORDER（基本订单表）	contact_name
                            model.setContactName(map.getString("CONTACT_NAME"));
                            //联系人固定电话 呼叫白屏票务系统	BUS_ORDER（基本订单表）	contact_phone
                            model.setContactLandlineMobileNumber(map.getString("CONTACT_PHONE"));
                            //预订人TID  呼叫白屏票务系统	BUS_ORDER（基本订单表）	根据预订人源ID找TID
                            model.setFkBookingUserTid(map.getString("TID"));
                            //预订人源ID 呼叫白屏票务系统	BUS_ORDER（基本订单表）	cur_tel
                            model.setFkBookingUserOriginId(map.getString("CUR_TEL"));
                            //源系统最后更新时间 呼叫白屏票务系统	BUS_ORDER（基本订单表）	update_date
                            model.setSourceLastUpdatetime(map.getString("UPDATE_DATE"));

                            LocalDateTime now = LocalDateTime.now();
                            model.setSystemCreatetime(now.toString());
                            model.setSystemLastUpdatetime(now.toString());
                            // 输出记录
                            out.collect(model);
                        }
                    }
                });

        // 写入Doris
        DorisSink<BookingPnrFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_BOOKING_PNR_FACT");
        bookingPnrFactStream.sinkTo(dorisSink);
    }
}
