package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.MobileDimModel;
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
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.time.LocalDateTime;

/**
 * @author TS.SHA.fuhuazhang
 * @date 2025/8/6  13:31
 */
public class MobileDimTrans {

    static final Logger logger = LoggerFactory.getLogger(MobileDimTrans.class);

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
                logger.info("高频数据整合手机号维表");
                processMobileDim(value, ctx, document);
            }
        }).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        // 输出结果查看
        DataStream<HsdProcessDataModel> mobileDimStream = processedStream.getSideOutput(CommonOutputTags.HSD_PROCESS_DATA_TAG);
//        mobileDimStream.print("mobileDimStream");
        // 创建 Doris Sink 并写入
        mobileDimStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_HSD_PROCESS_DATA")).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
    }

    public static void processMobileDim(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 子事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
        // 证件类型
        String documentType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Type");
        // 证件号
        String documentNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Number");

        // 获取所有 Segment 节点
        NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
        for (int a = 0; a < segmentList.getLength(); a++) {
            Node segment = segmentList.item(a);
            // 手机号（CTCM项）
            String mobilePhoneNumber = XpathUtils.getString(segment, "OtherServiceInformation[Text[starts-with(., " + "'CTCM')]]/Text");

            MobileDimModel mobileDimModel = new MobileDimModel();
            // T_ID+手机号的加密值作为主键
            String tid = TransUtils.getTid(documentNumber, documentType, "HSD");
            String phoneNumber = NormalizationUtils.standardize(FieldType.MOBILE_NO,
                    SM4Utils.encrypt(mobilePhoneNumber, Constants.SM4_KEY));
            mobileDimModel.setSystemKey(tid + phoneNumber);
            // T_id 证件号加密
            mobileDimModel.settId(tid);
            // 手机号
            mobileDimModel.setMobileNumber(phoneNumber);

            HsdProcessDataModel hsdProcessDataModel = new HsdProcessDataModel();
            hsdProcessDataModel.setEvent(event);
            hsdProcessDataModel.setSubEvent(subEvent);
            hsdProcessDataModel.setTableName("T_DIM_MOBILE_DIM");
            hsdProcessDataModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
            hsdProcessDataModel.setStamp(stamp);
            // 转换为JSON字符串
            hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(mobileDimModel));
            hsdProcessDataModel.setProcessed(false);
            hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());

            if (StringUtils.isNotEmpty(mobileDimModel.getSystemKey())) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
            }else{
                logger.info("高频-数据整合手机号维表[MobileDimModel]数据解析异常{}", value);
            }
        }
    }
}
