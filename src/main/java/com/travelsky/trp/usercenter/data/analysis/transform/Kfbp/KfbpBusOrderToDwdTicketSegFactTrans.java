package com.travelsky.trp.usercenter.data.analysis.transform.Kfbp;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingSegFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Objects;

public class KfbpBusOrderToDwdTicketSegFactTrans {
    public static void result(DataStream<JSONObject> source) {
        SingleOutputStreamOperator<TickingSegFactModel> tickingSegFactStream = source
                .map(map -> {

                    TickingSegFactModel model = new TickingSegFactModel();

                    return model;
                })
                .filter(Objects::nonNull);

        // 写入Doris
        DorisSink<TickingSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_SEG_FACT");
        tickingSegFactStream.sinkTo(dorisSink);
    }
}
