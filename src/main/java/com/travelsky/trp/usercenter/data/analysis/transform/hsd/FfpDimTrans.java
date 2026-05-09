package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.FfpDimModel;
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
 * 日频数据进行处理-根据事实表数据入常客信息维表
 * @author TS.SHA.fuhuazhang
 * @date 2025/8/6  13:31
 */
public class FfpDimTrans {

    static final Logger logger = LoggerFactory.getLogger(FfpDimTrans.class);

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
                // 事件名称
                String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
                if (!Constants.EVENT_FFCHANGE.equals(event)) {
                    logger.info("非旅客常卡变更事件不整合常客信息维表,event:{}", event);
                    return;
                }
                logger.info("高频数据整合常客信息维表");
                // 子事件名称
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
                    // 常客卡号
                    String ffrf = XpathUtils.getString(segment, "FrequentTraveler/@Number");
                    // 常客等级
                    String loyalLevel = XpathUtils.getString(segment, "FrequentTraveler/@LoyalLevel");
                    String tid = TransUtils.getTid(documentNumber, documentType, "HSD");

                    FfpDimModel ffpDimModel = new FfpDimModel();
                    // 主键
                    ffpDimModel.setSystemKey(tid + ffrf + NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,
                            SM4Utils.encrypt(documentNumber, Constants.SM4_KEY)));
                    // T_id 证件号加密
                    ffpDimModel.settId(tid);
                    // 常客卡号
                    ffpDimModel.setFfrf(NormalizationUtils.standardize(FieldType.FREQUENT_FLYER_NUM, SM4Utils.encrypt(ffrf, Constants.SM4_KEY)));
                    // 常客等级
                    ffpDimModel.setFfLevel(loyalLevel);

                    HsdProcessDataModel hsdProcessDataModel = new HsdProcessDataModel();
                    hsdProcessDataModel.setEvent(event);
                    hsdProcessDataModel.setSubEvent(subEvent);
                    hsdProcessDataModel.setTableName("T_DIM_FFP_DIM");
                    hsdProcessDataModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
                    hsdProcessDataModel.setStamp(stamp);
                    // 转换为JSON字符串
                    hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(ffpDimModel));
                    hsdProcessDataModel.setProcessed(false);
                    hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());

                    if(StringUtils.isNotEmpty(ffpDimModel.getSystemKey())) {
                        ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
                    }else{
                        logger.info("高频-数据整合常客信息维表[FfpDimModel]数据解析异常{}",value);
                    }
                }
            }
        }).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        // 输出结果查看
        DataStream<HsdProcessDataModel> dimStream = processedStream.getSideOutput(CommonOutputTags.HSD_PROCESS_DATA_TAG);
//        dimStream.print("dimStream");
        // 创建 Doris Sink 并写入
        dimStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_HSD_PROCESS_DATA")).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
    }
}
