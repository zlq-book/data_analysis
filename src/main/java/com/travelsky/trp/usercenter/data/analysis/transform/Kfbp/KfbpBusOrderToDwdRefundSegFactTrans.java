package com.travelsky.trp.usercenter.data.analysis.transform.Kfbp;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.RefundSegFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Objects;

public class KfbpBusOrderToDwdRefundSegFactTrans {
    public static void result(DataStream<JSONObject> source) {
        SingleOutputStreamOperator<RefundSegFactModel> refundSegFactStream = source
                .map(map -> {

                    RefundSegFactModel model = new RefundSegFactModel();

                    return model;
                })
                .filter(Objects::nonNull);

        // 写入Doris
        DorisSink<RefundSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_REFUND_SEG_FACT");
        refundSegFactStream.sinkTo(dorisSink);
    }
}
