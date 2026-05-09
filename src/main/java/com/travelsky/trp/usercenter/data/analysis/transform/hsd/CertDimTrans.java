package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CertDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.HsdProcessDataModel;
import com.travelsky.trp.usercenter.data.analysis.utils.CustomJsonSerializer;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * @author TS.SHA.fuhuazhang
 * @date 2025/8/6  13:31
 */
public class CertDimTrans {

    static final Logger logger = LoggerFactory.getLogger(CertDimTrans.class);

    public static void output(DataStream<String> source) {
        SingleOutputStreamOperator<Void> processedStream = source.process(new ProcessFunction<String, Void>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Void> out) throws Exception {
                Document document;
                try {
                    document = DocumentUtils.string2Document(value);
                } catch (Exception e) {
                    logger.warn("convert document is error, the content is {}, the error is {}", value, e.getMessage());
                    return;
                }
                logger.info("高频数据整合证件信息维表");
                processCertDim(value, ctx, document);
            }
        }).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        // 输出结果查看
        DataStream<HsdProcessDataModel> certDimStream = processedStream.getSideOutput(CommonOutputTags.HSD_PROCESS_DATA_TAG);
//        certDimStream.print("certDimStream");
        // 创建 Doris Sink 并写入
        certDimStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_HSD_PROCESS_DATA")).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
    }

    public static void processCertDim(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 子事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
        // 证件类型
        String documentType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Type");
        // 证件号
        String documentNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Number");
        // 身份证
        String idNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document[@Type='NI']/@Number");
        // 外国人永居证
        String foreignerPermitText = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Segment" +
                "/SpecialServiceRequest[SSRCode = 'DOCS']/Text");
        String foreignerPermit = null;
        if (StringUtils.isNotBlank(foreignerPermitText)) {
            String[] parts = foreignerPermitText.split("/");
            // 确保数组长度足够，避免数组越界异常
            if (parts.length > 2) {
                foreignerPermit = parts[2]; // 获取第三个部分（索引为2）
            }
        }
        // 证件过期日期
        String documentExpireDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@ExpirationDate");
        // 签发国
        String documentIssuingCountry = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@IssueCountry");
        String tid = TransUtils.getTid(documentNumber, documentType, "HSD");
        CertDimModel certDimModel = new CertDimModel();
        // 主键
        certDimModel.setSystemKey(tid + documentType + NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,
                SM4Utils.encrypt(documentNumber, Constants.SM4_KEY)));
        // T_id 证件号加密
        certDimModel.settId(tid);
        // 证件类型
        certDimModel.setCertType(NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, documentType, DataSource.HIGH_FREQUENCY_DATA));
        // 证件号码
        certDimModel.setCertNumber(NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, SM4Utils.encrypt(documentNumber, Constants.SM4_KEY)));
        // 是否符合编码规则
        if(StringUtils.isNotBlank(idNumber)){
            certDimModel.setCodeRuleCompliant(TransUtils.isCodeRuleCompliant(SM4Utils.encrypt(idNumber, Constants.SM4_KEY)));
        }else if(StringUtils.isNotBlank(foreignerPermit)){
            certDimModel.setCodeRuleCompliant(TransUtils.isCodeRuleCompliant(SM4Utils.encrypt(foreignerPermit, Constants.SM4_KEY)));
        }
        // 证件过期日期
        certDimModel.setCertExpireDate(StringUtils.isNotBlank(documentExpireDate) ? LocalDate.parse(documentExpireDate) : null);
        // 证件签发国
        certDimModel.setCertIssuingCountry(documentIssuingCountry);

        HsdProcessDataModel hsdProcessDataModel = new HsdProcessDataModel();
        hsdProcessDataModel.setEvent(event);
        hsdProcessDataModel.setSubEvent(subEvent);
        hsdProcessDataModel.setTableName("T_DIM_CERT_DIM");
        hsdProcessDataModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
        hsdProcessDataModel.setStamp(stamp);
        // 转换为JSON字符串
        hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(certDimModel));
        hsdProcessDataModel.setProcessed(false);
        hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());

        if(StringUtils.isNotEmpty(certDimModel.getSystemKey())) {
            ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
        }else{
            logger.info("高频数据整合证件信息维表[CertDimModel]数据解析异常{}", value);
        }
    }
}
