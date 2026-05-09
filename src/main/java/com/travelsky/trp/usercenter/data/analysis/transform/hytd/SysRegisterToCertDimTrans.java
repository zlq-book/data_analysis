package com.travelsky.trp.usercenter.data.analysis.transform.hytd;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CertDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimSummaryModel;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
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
public class SysRegisterToCertDimTrans {
    private static final Logger logger = LoggerFactory.getLogger(SysRegisterToCertDimTrans.class);

    public static void result(DataStream<JSONObject> sourceStream) {
        DataStream<CertDimModel> certDimModelDataStream = sourceStream.map(row -> {
            CertDimModel certDimModel = new CertDimModel();
            //证件号码
            String credentialNum = row.getString("CREDENTIAL_NUM");
            //证件类型
            String credentialType = row.getString("CREDENTIAL_TYPE");
            String tid = row.getString("TID");
            if (StringUtils.isNotBlank(credentialNum) && StringUtils.isNotBlank(credentialType) && StringUtils.isNotBlank(tid)) {

                Boolean codeRuleCompliant = null;
                if ("NI".equals(credentialType)){
                    codeRuleCompliant = TransUtils.isCodeRuleCompliant(credentialNum);
                }
                certDimModel.setCertNumber(credentialNum);
                certDimModel.setCertType(credentialType);
                String systemKey = tid + credentialType + credentialNum;
                certDimModel.setSystemKey(systemKey);
                certDimModel.settId(tid);
                certDimModel.setCurrentCertHighestPriority(1);
                certDimModel.setCodeRuleCompliant(codeRuleCompliant);
                // 是否常客注册证件
                certDimModel.setFrequentFlyerRegistrationCard(true);
                // 默认值设置
                // 是否直销实名认证证件
                certDimModel.setDirectSalesRealNameVerifiedCard(false);
                // 是否鲁雁行实名认证证件
                certDimModel.setLyRegistCard(false);
                //是否次卡受益人证件
                certDimModel.setPurchasersBeneficiaryCard(false);
                //是否抖音次卡受益人证件
                certDimModel.setDouyinPurchasersBeneficiaryCard(false);
                return certDimModel;
            } else {
                return null;
            }
        }).filter(row -> row != null);
        // 创建 Doris Sink
        DorisSink<CertDimModel> certDimModelDorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_CERT_DIM");
        certDimModelDataStream.sinkTo(certDimModelDorisSink);
    }
}
