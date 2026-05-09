package com.travelsky.trp.usercenter.data.analysis.transform.lygj;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.MobileDimModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Objects;

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.isVirtualMobile;

public class LygjDpiToMobileDimTrans {
    //private static final Logger logger = LoggerFactory.getLogger(LygjDpiToMobileDimTrans.class);

    public static void result(DataStream<JSONObject> sourceStream) {
        DataStream<MobileDimModel> mobileDimModelDataStream = sourceStream
                .map(row -> {
                    MobileDimModel model = new MobileDimModel();
                    String phoneNum = row.getString("MOBILE_PHONE");
                    String tid = row.getString("TID");
                    if (StringUtils.isBlank(phoneNum) || StringUtils.isBlank(tid)) {
                        System.out.println("提取LygjDpiToMobileDimTrans失败，主键组成部分为空: " + row.toString());
                        //logger.error("提取LygjDpiToMobileDimTrans失败，主键组成部分为空: {}", row.toString());
                        return null;
                    }
                    String systemKey = tid + phoneNum;
                    model.setSystemKey(systemKey);
                    model.settId(tid);
                    model.setMobileNumber(phoneNum);
                    //是否直销实名认证手机号
                    model.setDirectSaleMobile(false);
                    //是否鲁雁行实名认证用户手机号
                    model.setLyxRealnameMobile(false);
                    //是否为抖音次卡购买人手机号
                    model.setDouyinCardPurchasersMobile(false);
                    //是否为常客手机号
                    model.setFrequentFlyerMobile(false);
                    //是否高端旅客手机号
                    model.setHighMobileNumber(false);
                    //当前手机号最高优先级
                    model.setCurrentPhoneHighestPriority(2);
                    //是否虚拟手机号
                    model.setVirtualMobile(isVirtualMobile(phoneNum));
                    String currentDateTime = DateTimeUtils.getCurrentDateTime();
                    model.setCreateTime(currentDateTime);
                    model.setUpdateTime(currentDateTime);
                    return model;
                }).setParallelism(4).filter(Objects::nonNull);
        //  创建 Doris Sink 并写入
        DorisSink<MobileDimModel> dorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_MOBILE_DIM");
        //数据写入doris
        mobileDimModelDataStream.sinkTo(dorisSink);
    }
}
