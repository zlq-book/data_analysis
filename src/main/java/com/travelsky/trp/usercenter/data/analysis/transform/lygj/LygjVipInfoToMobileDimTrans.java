package com.travelsky.trp.usercenter.data.analysis.transform.lygj;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.SM4Utils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.MobileDimModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Objects;

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.isVirtualMobile;

public class LygjVipInfoToMobileDimTrans {
    //private static final Logger logger = LoggerFactory.getLogger(LygjVipInfoToMobileDimTrans.class);

    public static void result(DataStream<JSONObject> sourceStream) {
        DataStream<MobileDimModel> mobileDimModelDataStream = sourceStream
                // 仅写入11位手机号
                .filter( row -> {
                    String phoneNum = row.getString("MOBILE");
                    // 手机号解密
                    if (StringUtils.isNotBlank(phoneNum) && StringUtils.isNotBlank(Constants.SM4_KEY)) {
                        String phone = SM4Utils.decrypt(phoneNum, Constants.SM4_KEY);
                        phoneNum = phone == null ? "" : phone.trim();
                        return StringUtils.isNotBlank(phoneNum) && phoneNum.length() == 11;
                    }
                    return false;
                })
                .map(row -> {
            MobileDimModel model = new MobileDimModel();
            String phoneNum = row.getString("MOBILE");
            String tid = row.getString("TID");
            if (StringUtils.isBlank(phoneNum) || StringUtils.isBlank(tid)) {
                System.out.println("提取LygjVipInfoToMobileDimTrans失败，主键组成部分为空: " + row.toString());
                //logger.error("提取LygjVipInfoToMobileDimTrans失败，主键组成部分为空: {}", row.toString());
                return null;
            }
            String systemKey = tid + phoneNum;
            model.setSystemKey(systemKey);
            model.settId(tid);
            model.setHighMobileNumber(true);
            model.setCurrentPhoneHighestPriority(2);
            model.setVirtualMobile(isVirtualMobile(phoneNum));
            String currentDateTime = DateTimeUtils.getCurrentDateTime();
            model.setCreateTime(currentDateTime);
            model.setUpdateTime(currentDateTime);
            return model;
        }).filter(Objects::nonNull);;
        //  创建 Doris Sink 并写入
        DorisSink<MobileDimModel> dorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_MOBILE_DIM");
        //数据写入doris
        mobileDimModelDataStream.sinkTo(dorisSink);
    }
}
