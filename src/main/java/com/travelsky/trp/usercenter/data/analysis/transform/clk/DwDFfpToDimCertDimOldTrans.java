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

public class DwDFfpToDimCertDimOldTrans {
    static final Logger logger = LoggerFactory.getLogger(DwDFfpToDimCertDimOldTrans.class);

    // 常客注册事实表 24年之前旧数据
    public static void output(DataStream<Row> rowDataStream) {

        // 证件信息维表
        SingleOutputStreamOperator<CertDimModel> certDimStream = rowDataStream.filter(row -> {
            boolean flag = row.getField("MEM_NUM") == null;
            // 没有 ly card 无法获得主键 没有证件不入
            if (flag) {
                logger.info("入库证件信息维表异常，oriTable：T_ODS_CLK_T_DW_D_FFP，" + row.toString());
            }
            return !flag;
        }).map(row -> {
            CertDimModel model = new CertDimModel();
            String systemTid = row.getField("TID") == null ? "NULL" :row.getField("TID").toString();
            String systemPrCd = row.getField("PR_CATEGORY_CD") == null ? "NULL" :row.getField("PR_CATEGORY_CD").toString();
            String systemPrId = row.getField("PR_ID") == null ? "NULL" :row.getField("PR_ID").toString();
            model.setSystemKey(systemTid + systemPrCd + systemPrId);

            //  tid
            model.settId(row.getField("TID").toString());

            // 证件
           model.setCertType(row.getField("PR_CATEGORY_CD") == null ? null :row.getField("PR_CATEGORY_CD").toString());
           model.setCertNumber(row.getField("PR_ID") == null ? null :row.getField("PR_ID").toString());


            // 常旅客信息
            model.setFrequentFlyerRegistrationCard(true);
            model.setCertIssueDate(row.getField("PR_ISSUE_TS") == null ? null :
                    LocalDateTime.parse(row.getField("PR_ISSUE_TS").toString()).toLocalDate());
            model.setCertExpireDate(row.getField("PR_EXPIRATION_TS") == null ? null :
                    LocalDateTime.parse(row.getField("PR_EXPIRATION_TS").toString()).toLocalDate());
            model.setCertIssuingCountry(row.getField("PR_ISSUING_COUNTRY") == null ? null :
                    row.getField("PR_ISSUING_COUNTRY").toString());

            model.setCreateTime(LocalDateTime.now().toString());
            model.setUpdateTime(LocalDateTime.now().toString());
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
