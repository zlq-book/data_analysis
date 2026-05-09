package com.travelsky.trp.usercenter.data.analysis.transform.hytd;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CertDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.FfpDimModel;
import com.travelsky.trp.usercenter.data.analysis.utils.ChannelInfo;
import com.travelsky.trp.usercenter.data.analysis.utils.channel.CompositeChannelProcessor;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author kuangaihua
 * @date 2025/8/6 9:08
 */
public class SysRegisterToFfpDimTrans {
    private static final Logger logger = LoggerFactory.getLogger(SysRegisterToFfpDimTrans.class);

    static final CompositeChannelProcessor channelProcessor = new CompositeChannelProcessor();

    public static void result(DataStream<JSONObject> sourceStream) {
        DataStream<FfpDimModel> certDimModelDataStream = sourceStream.map(row -> {
            FfpDimModel ffpDimModel = new FfpDimModel();
            //V3.1删除这两个字段
            /*
            //证件号码
            String credentialNum = row.getString("CREDENTIAL_NUM");
            //证件类型
            String credentialType = row.getString("CREDENTIAL_TYPE");*/
            //常客卡号
            String memberNumber = row.getString("MEMBER_NUMBER");
            //父母常客卡号
            String parentMemberNum = row.getString("PARENT_MEMBER_NUM");
            //常客注册时间
            String createDate = row.getString("CREATE_DATE");
            String tid = row.getString("TID");
            String currentDateTime = DateTimeUtils.getCurrentDateTime();
            ffpDimModel.setSystemKey(memberNumber);
//            ffpDimModel.setFfCertNumber(credentialNum);
//            ffpDimModel.setFfCertType(credentialType);
            ffpDimModel.settId(tid);
            ffpDimModel.setFfrf(memberNumber);
            if (StringUtils.isNotBlank(parentMemberNum)) {
                ffpDimModel.setParentCardNumber(parentMemberNum);
            }

            // 渠道逻辑（与 SysRegisterToMemberRegisterFactTrans 保持一致）
            String channelId = row.getString("CHANNEL_ID");
            if (channelProcessor.supports(channelId)) {
                ChannelInfo info = channelProcessor.map(channelId);
                if (info != null) {
                    ffpDimModel.setDevChannelOne(info.getLevel1Code());
                    ffpDimModel.setDevChannelTwo(info.getLevel2Code());
                    ffpDimModel.setDevChannelThree(info.getLevel3Code());
                    ffpDimModel.setDevChannelFour(info.getLevel4Code());
                }
            }

            ffpDimModel.setMostTrustedSource("0");
            ffpDimModel.setFtRegisterTime(createDate);
            ffpDimModel.setUpdateTime(currentDateTime);
            ffpDimModel.setCreateTime(currentDateTime);
            return ffpDimModel;
        }).filter(row -> row != null);
        // 创建 Doris Sink
        DorisSink<FfpDimModel> ffpDimModelDorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_FFP_DIM");
        certDimModelDataStream.sinkTo(ffpDimModelDorisSink);
    }

}
