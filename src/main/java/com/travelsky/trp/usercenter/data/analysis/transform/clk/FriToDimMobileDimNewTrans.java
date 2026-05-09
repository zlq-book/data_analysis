package com.travelsky.trp.usercenter.data.analysis.transform.clk;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.MobileDimModel;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class FriToDimMobileDimNewTrans {

    static final Logger logger = LoggerFactory.getLogger(FriToDimMobileDimNewTrans.class);
    // 常客注册事实表 24年开始新数据
    public static void output(DataStream<Row> rowDataStream) {

        // 手机号维表
        SingleOutputStreamOperator<MobileDimModel> mobileDimStream = rowDataStream.map(row -> {
            MobileDimModel model = new MobileDimModel();
            //  tid
            model.setSystemKey(row.getField("TID").toString() + row.getField("FPHONE").toString());

            model.setMobileNumber(row.getField("FPHONE").toString());

            model.setFrequentFlyerMobile(true);

            //是否虚拟手机号
            model.setVirtualMobile(TransUtils.isVirtualMobile(row.getField("FPHONE").toString()));

            model.setUpdateTime(LocalDateTime.now().toString());
            model.setCreateTime(LocalDateTime.now().toString());
            return model;
        });

        DorisSink<MobileDimModel> mobileDimSink = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_MOBILE_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);
        mobileDimStream.sinkTo(mobileDimSink);



    }
}
