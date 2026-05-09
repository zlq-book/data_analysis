package com.travelsky.trp.usercenter.data.analysis.transform.lyx;

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

public class LmMlsLluToDimCertDimTrans {

    static final Logger logger = LoggerFactory.getLogger(LmMlsLluToDimCertDimTrans.class);

    public static void output(DataStream<Row> rowDataStream) {

        // 证件信息维表
        SingleOutputStreamOperator<CertDimModel> certDimStream = rowDataStream.filter(row -> {
            boolean flag = row.getField("MAIN_ID_TYPE") == null
                    || row.getField("MAIN_ID_NUM") == null || row.getField("TID") == null;
            if (flag) {
                logger.info("入库证件维表异常，oriTable：鲁雁行联表查询，" + row.toString());
            }
            return !flag;
        }).map(row -> {
            CertDimModel model = new CertDimModel();
            model.setSystemKey(row.getField("TID").toString() + row.getField("MAIN_ID_TYPE").toString() +
                    row.getField("MAIN_ID_NUM").toString());

            //  tid
            model.settId(row.getField("TID").toString());

            // 证件
            model.setCertType(row.getField("MAIN_ID_TYPE").toString());
            model.setCertNumber(row.getField("MAIN_ID_NUM").toString());


            // 鲁雁行
            model.setLyRegistCard(true);
            model.setCertIssueDate(row.getField("EFFECTIVE_TIME") == null ? null :
                    LocalDateTime.parse(row.getField("EFFECTIVE_TIME").toString()).toLocalDate());
            model.setCertExpireDate(row.getField("EXPIRES_TIME") == null ? null :
                    LocalDateTime.parse(row.getField("EXPIRES_TIME").toString()).toLocalDate());

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
