package com.travelsky.trp.usercenter.data.analysis.transform.zsf;

import com.mysql.cj.util.StringUtils;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CertDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.ZsfMultiCardCancelFactModel;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;

public class ZsfToCertDimTrans {
    static final Logger logger = LoggerFactory.getLogger(ZsfToCertDimTrans.class);

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

        DataStream<CertDimModel> certDimStream = rowDataStream.map(row -> {
            CertDimModel model = new CertDimModel();

            // 证件类型
            String certNo = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, SM4Utils.encrypt(toStringSafe(row.getField("CERT_NO")), Constants.SM4_KEY), DataSource.ZHANGSHANGFEI);
            String certType = NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, toStringSafe(row.getField("CERT_TYPE")), DataSource.ZHANGSHANGFEI);
            // 证件号码
            //1、tid
            Tid tid = new Tid();
            tid.setTid(certNo);
            HashMap<String, String> map = new HashMap<>();
            map.put(certType, certNo);
            tid.setCertification(map);
            String Tid = IdMapping.idMappingFunction(tid, "ZSF");
            model.settId(Tid);

            // 2、主键
            model.setSystemKey((Tid != null ? Tid : "") +
                    (certType != null ? certType : "") +
                    (certNo != null ? certNo : ""));

            //3、证件类型
            model.setCertType(certType);

            //4、证件号码
            model.setCertNumber(certNo);

            //5、是否直销实名认证证件
            model.setDirectSalesRealNameVerifiedCard(false);

            //6、是否鲁雁行实名认证证件
            model.setLyRegistCard(false);

            //7、是否常客注册证件
            model.setFrequentFlyerRegistrationCard(false);

            //8、是否次卡受益人证件
            model.setPurchasersBeneficiaryCard(true);

            // 是否抖音次卡受益人证件
            model.setDouyinPurchasersBeneficiaryCard(false);

            //9、是否符合编码规则
            model.setCodeRuleCompliant(TransUtils.isCodeRuleCompliant(certNo));

            //10、证件签发日期  掌尚飞 没这个字段

            //11、证件过期日期
            String expiryDate = toStringSafe(row.getField("EXPIRATION_DATE"));
            model.setCertExpireDate(StringUtils.isNullOrEmpty(expiryDate) ? null : LocalDate.parse(expiryDate));
            //12、证件签发国
            model.setCertIssuingCountry(toStringSafe(row.getField("PASSPORT_ISSUE_NATION")));

            //13、证件签发机构   掌尚飞 没这个字段

            //14、当前最可信来源
            model.setCurrentCertHighestPriority(1);

            LocalDateTime now = LocalDateTime.now();
            //15、update_time
            model.setUpdateTime(now.toString());

            //16、create_time
            model.setCreateTime(now.toString());

            model.setSystemKey(Tid + certNo + certType);

            return model;
        });


        logger.info("连接 Doris 参数: DB={}, Table={}, FE={}, BE={}, User={}",
                Constants.DIM_DB, "T_DIM_CERT_DIM", Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT, Constants.DIM_USER);
        DorisSink<CertDimModel> dorisSink_cerDim = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_CERT_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);
        // 数据写入 Doris
        certDimStream.sinkTo(dorisSink_cerDim);
    }
}
