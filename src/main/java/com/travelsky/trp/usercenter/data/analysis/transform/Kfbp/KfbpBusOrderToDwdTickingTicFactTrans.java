package com.travelsky.trp.usercenter.data.analysis.transform.Kfbp;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingTicFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Objects;

public class KfbpBusOrderToDwdTickingTicFactTrans {
    public static void result(DataStream<JSONObject> source) {
        SingleOutputStreamOperator<TickingTicFactModel> tickingTicFactStream = source
                .map(map -> {

                    TickingTicFactModel model = new TickingTicFactModel();

                    return model;
                })
                .filter(Objects::nonNull);

        // 写入Doris
        DorisSink<TickingTicFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_TIC_FACT");
        tickingTicFactStream.sinkTo(dorisSink);
    }
}
