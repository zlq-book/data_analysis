package com.travelsky.trp.usercenter.data.analysis.transform.hytd;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.SM4Utils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.MobileDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.CheckinSegFactModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author kuangaihua
 * @date 2025/8/6 9:08
 */
public class SysRegisterToMobileDimTrans {
    private static final Logger logger = LoggerFactory.getLogger(SysRegisterToMobileDimTrans.class);

    public static void result(DataStream<JSONObject> sourceStream) {
        DataStream<MobileDimModel> mobileDimModelDataStream = sourceStream.map(row -> {
            MobileDimModel model = new MobileDimModel();
            String phoneNum = row.getString("PHONE_NUM");
            String tid = row.getString("TID");
            if (StringUtils.isBlank(phoneNum)) {
                return null;
            }
            String systemKey = tid + phoneNum;
            model.setSystemKey(systemKey);
            model.settId(tid);
//            model.setMobileNumberSource("会员天地");
            model.setFrequentFlyerMobile(true);
            model.setCurrentPhoneHighestPriority(3);
            //解密手机号
            String phoneNumRaw = SM4Utils.decrypt(phoneNum, Constants.SM4_KEY);
            if (phoneNumRaw.startsWith("170") || phoneNumRaw.startsWith("171")
                    || phoneNumRaw.startsWith("162") || phoneNumRaw.startsWith("165") || phoneNumRaw.startsWith("167")) {
                //虚拟手机号
                model.setVirtualMobile(true);
            }else {
                model.setVirtualMobile(false);
            }
            model.setMobileNumber(phoneNum);
            String currentDateTime = DateTimeUtils.getCurrentDateTime();
            model.setCreateTime(currentDateTime);
            model.setUpdateTime(currentDateTime);
            return model;
        });
        //  创建 Doris Sink 并写入
        DorisSink<MobileDimModel> dorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_MOBILE_DIM");
        //数据写入doris
        mobileDimModelDataStream.sinkTo(dorisSink);
    }
}
