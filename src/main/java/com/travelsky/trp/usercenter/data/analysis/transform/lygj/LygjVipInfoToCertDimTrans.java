package com.travelsky.trp.usercenter.data.analysis.transform.lygj;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CertDimModel;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Objects;

public class LygjVipInfoToCertDimTrans {
    //private static final Logger logger = LoggerFactory.getLogger(LygjVipInfoToCertDimTrans.class);
    public static void result(DataStream<JSONObject> source) {
        // 2. 转换为DataStream
        DataStream<CertDimModel> certDimModelDataStream = source
                .map(map -> {
                    CertDimModel model = new CertDimModel();

                    // 获取证件信息
                    String idCard = map.getString("ID_CARD");
                    String certificateType = map.getString("CERTIFICATE_TYPE");
                    String tid = map.getString("TID");

                    // 检查主键是否完整
                    if (StringUtils.isBlank(idCard) || StringUtils.isBlank(certificateType) || StringUtils.isBlank(tid)) {
                        System.out.println("提取LygjVipInfoToCertDimTrans失败，主键组成部分不全: " + map.toString());
                        //logger.error("提取LygjVipInfoToCertDimTrans失败，主键组成部分不全: {}", map.toString());
                        return null;
                    }

                    // 设置TID（直接从map中获取已生成的TID）
                    model.settId(tid);

                    // 设置系统主键
                    model.setSystemKey(tid + certificateType + idCard);

                    // 证件信息
                    model.setCertType(certificateType);
                    model.setCertNumber(idCard);

                    // 默认值设置
                    // 是否直销实名认证证件
                    model.setDirectSalesRealNameVerifiedCard(false);
                    // 是否鲁雁行实名认证证件
                    model.setLyRegistCard(false);
                    // 是否常客注册证件
                    model.setFrequentFlyerRegistrationCard(false);
                    //是否次卡受益人证件
                    model.setPurchasersBeneficiaryCard(false);
                    //是否抖音次卡受益人证件
                    model.setDouyinPurchasersBeneficiaryCard(false);
                    // 是否符合编码规则
                    model.setCodeRuleCompliant(TransUtils.isCodeRuleCompliant(idCard));
                    // 当前证件最高优先级
                    model.setCurrentCertHighestPriority(2);

                    // 系统时间
                    String now = DateTimeUtils.getCurrentDateTime();
                    model.setCreateTime(now);
                    model.setUpdateTime(now);

                    return model;
                })
                .filter(Objects::nonNull);

        // 3. 写入Doris
        DorisSink<CertDimModel> dorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_CERT_DIM");
        certDimModelDataStream.sinkTo(dorisSink).name("DorisSink-LygjVipInfoToCertDimTrans");
    }
}
