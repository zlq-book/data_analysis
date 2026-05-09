package com.travelsky.trp.usercenter.data.analysis.transform.clk;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CertDimModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class FriToDimCertDimNewTrans {

    static final Logger logger = LoggerFactory.getLogger(FriToDimCertDimNewTrans.class);
    // 常客注册事实表 24年开始新数据
    public static void output(DataStream<Row> rowDataStream) {

        // 证件信息维表
        SingleOutputStreamOperator<CertDimModel> certDimStream = rowDataStream.map(row -> {
            CertDimModel model = new CertDimModel();
            model.setSystemKey(row.getField("ID").toString() + row.getField("FCARDTYPE").toString() +
                    row.getField("FIDCARD").toString());

            //  tid
            model.settId(row.getField("TID").toString());

            // 证件
            model.setCertType(row.getField("FCARDTYPE").toString());
            model.setCertNumber(row.getField("FIDCARD").toString());


            // 常旅客信息
            model.setFrequentFlyerRegistrationCard(true);

            model.setUpdateTime(LocalDateTime.now().toString());
            model.setCreateTime(LocalDateTime.now().toString());
            return model;
        });

        DorisSink<CertDimModel> certDimSink = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_CERT_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);
        certDimStream.sinkTo(certDimSink);




    }
}
