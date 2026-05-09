package com.travelsky.trp.usercenter.data.analysis.transform.Kfbp;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.BookingSegFactModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.LocalDateTime;
import java.util.Objects;

public class KfbpBusOrderToDwdBookingSegFactTrans {
    public static void result(DataStream<JSONObject> source) {
        SingleOutputStreamOperator<BookingSegFactModel> bookingSegFactStream = source
                .map(map -> {

                    BookingSegFactModel model = new BookingSegFactModel();
                    String pnr = map.getString("PNR");
                    String fltStartCity = map.getString("FLT_START_CITY");
                    String fltDepartureDay = StringUtils.isNotBlank(map.getString("FLT_DEPARTURE_DAY")) ? map.getString("FLT_DEPARTURE_DAY").replace("-", "") : "";
                    String fltDepartureTime = StringUtils.isNotBlank(map.getString("FLT_DEPARTURE_TIME")) ? map.getString("FLT_DEPARTURE_TIME") + ":00" : "";
                    String idCode = StringUtils.isNotBlank(map.getString("ID_CODE")) ? map.getString("ID_CODE") : "";
                    String fareName = map.getString("FARE_NAME");

                    if (StringUtils.isBlank(pnr) || StringUtils.isBlank(fltStartCity)
                            || StringUtils.isBlank(fltDepartureDay) || StringUtils.isBlank(fltDepartureTime)
                            || StringUtils.isBlank(idCode) || StringUtils.isBlank(fareName)) {
                        return null;
                    }
                    String pkId = pnr + fltStartCity + fltDepartureDay + fltDepartureTime
                            + idCode + fareName.replace("/", "");
                    model.setPkId(pkId);


                    // 渠道订单号 呼叫白屏票务系统	BUS_ORDER（基本订单表）	order_no
                    model.setChannelOrderId(map.getString("ORDER_NO"));
                    // 预订渠道 写死：呼叫白屏票务系统-code:SCWHP
                    model.setAkChannel("SCWHP");
                    // 呼叫白屏票务系统	BUS_ORDER（基本订单表）	contact_mobile
                    model.setContactMobileNumber(map.getString("CONTACT_MOBILE"));
                    // 呼叫白屏票务系统	BUS_ORDER（基本订单表）	contact_name
                    model.setContactName(map.getString("CONTACT_NAME"));
                    //呼叫白屏票务系统	BUS_ORDER（基本订单表）	contact_phone
                    model.setContactLandlineMobileNumber(map.getString("CONTACT_PHONE"));
                    // 呼叫白屏票务系统	BUS_ORDER（基本订单表）	根据预订人源ID找TID
                    model.setFkBookingUserTid(map.getString("TID"));
                    // 呼叫白屏票务系统	BUS_ORDER（基本订单表）	cur_tel
                    model.setFkBookingUserOriginId(map.getString("CUR_TEL"));




                    // 源系统最后更新时间 呼叫白屏票务系统	BUS_ORDER（基本订单表）	update_date
                    model.setSourceLastUpdatetime(map.getString("UPDATE_DATE"));

                    LocalDateTime now = LocalDateTime.now();
                    model.setSystemCreatetime(now.toString());
                    model.setSystemLastUpdatetime(now.toString());

                    return model;
                })
                .filter(Objects::nonNull);

        // 写入Doris
        DorisSink<BookingSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_BOOKING_SEG_FACT");
        bookingSegFactStream.sinkTo(dorisSink);
    }
}
