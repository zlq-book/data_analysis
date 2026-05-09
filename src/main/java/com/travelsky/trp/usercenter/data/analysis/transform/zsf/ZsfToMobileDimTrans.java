package com.travelsky.trp.usercenter.data.analysis.transform.zsf;

import com.mysql.cj.util.StringUtils;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CertDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.MobileDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;

public class ZsfToMobileDimTrans {
    static final Logger logger = LoggerFactory.getLogger(ZsfToMobileDimTrans.class);

    // 将 Object 转为 String，支持 null.
    private static String toStringSafe(Object obj) {
        return obj != null ? obj.toString() : null;
    }

    // 将 dateTime 转为 LocalDateTime 字符串
    private static String formatDateTimeSafe(Object ts) {
        if (ts != null) {
            LocalDateTime s = (LocalDateTime) ts;
            return s.toString().replace("T", " ");
        }
        return null;
    }

    public static void result(DataStream<Row> rowDataStream, String etlDate) throws Exception {

        DataStream<MobileDimModel> MobileDimModel = rowDataStream.map(row -> {
            MobileDimModel model = new MobileDimModel();

            String phoneNum = toStringSafe(row.getField("PHONE_NUM"));
            String certNo = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,SM4Utils.encrypt(toStringSafe(row.getField("CERT_NO")), Constants.SM4_KEY), DataSource.ZHANGSHANGFEI);
            String certType = NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE,toStringSafe(row.getField("CERT_TYPE")), DataSource.ZHANGSHANGFEI);
            phoneNum = NormalizationUtils.standardize(FieldType.MOBILE_NO,
                    phoneNum,DataSource.ZHANGSHANGFEI);

            Tid tid = new Tid();
            tid.setTid(certNo);
            HashMap<String, String> map = new HashMap<>();
            map.put(certType,certNo);
            tid.setCertification(map);
            String Tid = IdMapping.idMappingFunction(tid, "ZSF");

            model.setSystemKey((Tid==null?"":Tid) + (phoneNum==null?"":phoneNum));
            model.settId(Tid);
            model.setMobileNumber(phoneNum);
            model.setUpdateTime(LocalDateTime.now().toString());
            return model;
        });


        logger.info("连接 Doris 参数: DB={}, Table={}, FE={}, BE={}, User={}",
                Constants.DIM_DB, "T_DIM_MOBILE_DIM", Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT, Constants.DIM_USER);
        DorisSink<MobileDimModel> dorisSink_mobileDim = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_MOBILE_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);
        // 数据写入 Doris
        MobileDimModel.sinkTo(dorisSink_mobileDim);
    }
}
