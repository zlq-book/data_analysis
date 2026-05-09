package com.travelsky.trp.usercenter.data.analysis.transform.clk;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.FfpDimModel;
import com.travelsky.trp.usercenter.data.analysis.utils.ChannelInfo;
import com.travelsky.trp.usercenter.data.analysis.utils.CardNumberSegmentMatcher;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class DwDFfpToDimFfpDimOldTrans {
    static final Logger logger = LoggerFactory.getLogger(DwDFfpToDimFfpDimOldTrans.class);

    private static CardNumberSegmentMatcher matcher = CardNumberSegmentMatcher.getInstance();

    // 常客注册事实表 24年之前旧数据
    public static void output(DataStream<Row> rowDataStream) {
        
        // 常客信息维表
        SingleOutputStreamOperator<FfpDimModel> ffpDimStream = rowDataStream.map(row -> {
            FfpDimModel model = new FfpDimModel();
            String prId = row.getField("PR_ID") == null ? "null" :
                    row.getField("PR_ID").toString();
            //  V3  1030修改：主键修改为“常客卡号“的加密值
            model.setSystemKey(row.getField("MEM_NUM").toString());

            //  tid
            model.settId(row.getField("TID").toString());

            // 常旅客信息
            model.setFfrf(row.getField("MEM_NUM").toString());
            model.setFtRegisterTime(row.getField("FFP_CREATED").toString());
//            model.setFfCertType(row.getField("PR_CATEGORY_CD") == null ? null :row.getField("PR_CATEGORY_CD").toString());
//            model.setFfCertNumber(row.getField("PR_ID") == null ? null :row.getField("PR_ID").toString());

            // 渠道逻辑（与 DwDFfpToDwdMileRegisterFactOldTrans 保持一致）
            // 发展渠道级别 23年之前老数据通过号段来进行匹配
            Object orgFCardObj = row.getField("ORG_F_CARD");
            if (orgFCardObj != null) {
                String orgFCard = orgFCardObj.toString();
                ChannelInfo channelInfo = matcher.match(orgFCard);
                if (channelInfo != null) {
                    model.setDevChannelOne(channelInfo.getLevel1Code());
                    model.setDevChannelTwo(channelInfo.getLevel2Code());
                    model.setDevChannelThree(channelInfo.getLevel3Code());
                    model.setDevChannelFour(channelInfo.getLevel4Code());
                }
            }

            // 当前最可信来源
            model.setMostTrustedSource("1");

            model.setCreateTime(LocalDateTime.now().toString());
            model.setUpdateTime(LocalDateTime.now().toString());
            return model;
        });

        DorisSink<FfpDimModel> ffpDimSink = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_FFP_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);
        ffpDimStream.sinkTo(ffpDimSink);


    }

}
