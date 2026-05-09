//package com.travelsky.dataplatform.main.ods;
//
//
//import com.travelsky.dataplatform.constans.Constants;
//import com.travelsky.dataplatform.constans.trp.utils.DocumentUtils;
//import com.travelsky.dataplatform.utils.CheckpointUtils;
//import com.travelsky.dataplatform.utils.XpathUtils;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.connector.base.DeliveryGuarantee;
//import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
//import org.apache.flink.connector.kafka.sink.KafkaSink;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.w3c.dom.Document;
//import org.w3c.dom.Node;
//import org.w3c.dom.NodeList;
//
//import java.time.LocalDate;
//import java.time.LocalDateTime;
//import java.util.ArrayList;
//import java.util.List;
//
//import static com.travelsky.dataplatform.constans.trp.utils.Constants.*;
//import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.DATE_FORMATTER;
//import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.DATE_TIME_FORMATTER;
//
///**
// * 获取400天之内和400天之后的历史数据
// */
//
//public class ExtractTOdsHistoryTRP {
//
//    static final Logger log = LoggerFactory.getLogger(ExtractTOdsHistoryTRP.class);
//    public static void main(String args[]) throws Exception {
//
//        if (args.length < 1) {
//            /*加日志：etl_date参数为空*/
//            System.exit(0);
//        }
//        String etlDate = args[0];
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //checkpoint设置
//        CheckpointUtils.setCheckpoint(env, "ExtractTOdsHistoryTRP");
//        // SASL认证配置（实际应从安全存储获取）
//        String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required " +
//                "username=\"" + Constants.TRP_KAFKA_USERNAME + "\" " +
//                "password=\"" + Constants.TRP_KAFKA_PASSWORD + "\";";
//        // 构建安全Kafka Sink
//        KafkaSink<String> sink = KafkaSink.<String>builder()
//                .setBootstrapServers(Constants.TRP_KAFKA_SERVICES) // SSL端口
//                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
//                        .setTopic(Constants.TRP_KAFKA_TOPIC)
//                        .setValueSerializationSchema(new SimpleStringSchema())
//                        .build())
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                .setProperty("security.protocol", "SASL_PLAINTEXT")
//                .setProperty("sasl.mechanism", "SCRAM-SHA-256")
//                .setProperty("sasl.jaas.config", jaasConfig)
//                .setProperty("transaction.timeout.ms", "600000")
//                .build();
//        List<String> xmlList = new ArrayList<>();
//        // 通过时间段从大数据平台提取数据
//        List<String> spnrIdList = getSpnrXmlFormBigDataPlatform(etlDate);
//        // 获取详情数据
//        setSpnrDetailXml(xmlList, spnrIdList);
//
//        DataStream<String> stream = env.fromCollection(xmlList);
//        stream.sinkTo(sink);
//        env.execute("Secure Kafka Producer");
//
//
//    }
//
//    private static void setSpnrDetailXml(List<String> xmlList, List<String> spnrIdList) throws Exception {
//        for (String spnrId : spnrIdList) {
//            String getSpnrXml = HttpClientUtils.createGetSpnrXml(spnrId);
//            String detailXml = HttpClientUtils.doPost(GET_MESSAGE_URL_BIG, GET_MESSAGE_URL_BIG_TK, null, getSpnrXml);
//            log.info("发送的请求为：" + getSpnrXml);
//            Node node = null;
//            Document document = null;
//            NodeList oj_superPNR = null;
//            if (StringUtils.isNotBlank(detailXml) && Utils.isNotContains(detailXml, SPNR_XML_LOG_004)) {
//                document = DocumentUtils.string2Document(detailXml);
//                oj_superPNR = document.getElementsByTagName(SPNR_XML_LOG_006);
//                node = oj_superPNR.item(0);
//
//                if (null == node) {
//                    node = document.getElementsByTagName(SPNR_XML_LOG_007).item(0);
//                }
//            }
//            if (StringUtils.isBlank(detailXml)) {
//                detailXml = "";
//            }
//            log.info("获取的summary报文大小为：" + detailXml.getBytes().length / 1024 / 1024 + "Mb");
//
//            String xml = DocumentUtils.toStringFromDoc(DocumentUtils.nodeToDocument(node));
//            xmlList.add(xml);
//            log.info(spnrId + "成功获取到了DETAIL报文");
//        }
//    }
//
//    private static List<String> getSpnrXmlFormBigDataPlatform(String etlDate) throws Exception {
//
//        // 1. 将输入字符串解析为 LocalDate 对象
//        LocalDate localDate = LocalDate.parse(etlDate, DATE_FORMATTER);
//
//        // 2. 获取当天的开始时间 (00:00:00)
//        LocalDateTime startDateTime = localDate.atStartOfDay(); // 这是获取一天开始最便捷的方法
//
//        // 计算当天的结束时间 (23:59:59.999)
//        LocalDateTime endDateTime = localDate.atTime(23, 59, 59, 999);
//
//        // 4. 将 LocalDateTime 对象格式化为目标字符串
//        String startTimeStr = startDateTime.format(DATE_TIME_FORMATTER);
//        String endTimeStr = endDateTime.format(DATE_TIME_FORMATTER);
//        // 先去拿概要信息
//        String getSummaryXml1 = HttpClientUtils.createGetSummaryXml(startTimeStr, endTimeStr);
//        String summaryXml = HttpClientUtils.doPost(GET_MESSAGE_URL_BIG, GET_MESSAGE_URL_BIG_TK, null, getSummaryXml1);
//        log.info("发送的请求为：" + getSummaryXml1);
//        Node node = null;
//        Document document = null;
//        List<String> spnrIds = new ArrayList<>();
//        if (StringUtils.isNotBlank(summaryXml) && Utils.isNotContains(summaryXml, SPNR_XML_LOG_004)) {
//            document = DocumentUtils.string2Document(summaryXml);
//            NodeList oj_superPNRs = document.getElementsByTagName(SPNR_XML_LOG_006);
//            NodeList ota_superPNRs = document.getElementsByTagName(SPNR_XML_LOG_007);
//            NodeList spnerPNRs = oj_superPNRs.getLength() > 0 ? oj_superPNRs : ota_superPNRs;
//            // 使用时遍历 NodeList
//            for (int i = 0; i < spnerPNRs.getLength(); i++) {
//                Node superPNRNode = spnerPNRs.item(i);
//                Document superPNRDocument = DocumentUtils.nodeToDocument(superPNRNode);
//                String superPnrId = XpathUtils.getString(superPNRDocument, "OJ_SuperPNR/@SuperPNR_ID");
//                spnrIds.add(superPnrId);
//            }
//        }
//        log.info("获取的summary报文大小为：" + summaryXml.getBytes().length / 1024 / 1024 + "Mb");
//        return spnrIds;
//    }
//}
