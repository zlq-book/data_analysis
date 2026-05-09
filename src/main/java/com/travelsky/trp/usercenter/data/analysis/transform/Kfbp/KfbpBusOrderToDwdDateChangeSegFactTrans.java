package com.travelsky.trp.usercenter.data.analysis.transform.Kfbp;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.DatechangeSegFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Objects;

public class KfbpBusOrderToDwdDateChangeSegFactTrans {
    public static void result(DataStream<JSONObject> source) {
        SingleOutputStreamOperator<DatechangeSegFactModel> dateChangeSegFactStream = source
                .map(map -> {

                    DatechangeSegFactModel model = new DatechangeSegFactModel();

                    return model;
                })
                .filter(Objects::nonNull);

        // 写入Doris
        DorisSink<DatechangeSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_DATECHANGE_SEG_FACT");
        dateChangeSegFactStream.sinkTo(dorisSink);
    }
}
