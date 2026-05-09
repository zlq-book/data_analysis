package com.travelsky.trp.usercenter.data.analysis.transform.hytd;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.FfpDimModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kuangaihua
 * @date 2025/8/6 9:08
 */
public class TManageOthercardToFfpDimTrans {
    private static final Logger logger = LoggerFactory.getLogger(TManageOthercardToFfpDimTrans.class);

    public static void result(DataStream<JSONObject> sourceStream) {
        DataStream<FfpDimModel> certDimModelDataStream = sourceStream.map(row -> {
            FfpDimModel ffpDimModel = new FfpDimModel();
            String crmCardno = row.getString("crmCardno" );
            String cardNo = row.getString("cardNo" );
            String certNo = row.getString("certNo" );
            String endTime = row.getString("endTime" );
            String tidStr = row.getString("TID" );
            String systemKey=crmCardno;
            String currentDateTime = DateTimeUtils.getCurrentDateTime();
            ffpDimModel.setSystemKey(systemKey);
            ffpDimModel.setYjCard(cardNo);
            ffpDimModel.setYjCardExpiredate(endTime);
            ffpDimModel.setCreateTime(currentDateTime);
            ffpDimModel.setUpdateTime(currentDateTime);
            return ffpDimModel;
        }).filter(row -> row != null);
        // 创建 Doris Sink
        DorisSink<FfpDimModel> ffpDimModelDorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_FFP_DIM");
        certDimModelDataStream.sinkTo(ffpDimModelDorisSink);
    }

}
