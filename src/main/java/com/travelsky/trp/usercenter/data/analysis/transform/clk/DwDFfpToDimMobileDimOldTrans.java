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

public class DwDFfpToDimMobileDimOldTrans {
    static final Logger logger = LoggerFactory.getLogger(DwDFfpToDimMobileDimOldTrans.class);

    // 常客注册事实表 24年之前旧数据
    public static void output(DataStream<Row> rowDataStream) {

        // 手机号维表
        SingleOutputStreamOperator<MobileDimModel> mobileDimStream = rowDataStream.filter(row -> {
            boolean flag = row.getField("PR_MOBILE") == null;
            // 没有 PR_MOBILE 无法获得主键
            if (flag) {
                logger.info("手机号维表异常，oriTable：T_ODS_CLK_T_DW_D_FFP，" + row.toString());
            }
            return !flag;
        }).map(row -> {
            MobileDimModel model = new MobileDimModel();
            //  tid
            model.setSystemKey(row.getField("TID").toString() + row.getField("PR_MOBILE").toString());

            model.setMobileNumber(row.getField("PR_MOBILE").toString());

            model.setFrequentFlyerMobile(true);

            //是否虚拟手机号
            model.setVirtualMobile(TransUtils.isVirtualMobile(row.getField("PR_MOBILE").toString()));

            model.setCreateTime(LocalDateTime.now().toString());
            model.setUpdateTime(LocalDateTime.now().toString());
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
