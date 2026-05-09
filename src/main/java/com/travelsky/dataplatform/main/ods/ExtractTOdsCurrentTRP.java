//package com.travelsky.dataplatform.main.ods;
//
//
//import com.travelsky.dataplatform.constans.Constants;
//import com.travelsky.dataplatform.constans.trp.utils.DocumentUtils;
//import com.travelsky.dataplatform.utils.CheckpointUtils;
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
//import java.io.*;
//import java.util.ArrayList;
//import java.util.List;
//
//import static com.travelsky.dataplatform.constans.trp.utils.Constants.*;
//
///**
// * 获取400天之内和400天之后的历史数据
// */
//
//public class ExtractTOdsCurrentTRP {
//
//    static final Logger log = LoggerFactory.getLogger(ExtractTOdsCurrentTRP.class);
//    // 文件路径
//    private static final String CURRENT_SPNRID_FILE_BASEPATH = "/usr/bch/3.3.0/test/spnr/current/";
//    public static void main(String args[]) throws Exception {
//
//        if (args.length < 1) {
//            /*加日志：etl_date参数为空*/
//            System.exit(0);
//        }
//        String etlDate = args[0];
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //checkpoint设置
//        CheckpointUtils.setCheckpoint(env, "ExtractTOdsCurrentTRP");
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
//        // 通过spnrId从山航中提取数据
//        getSpnrXmlFormText(xmlList, etlDate);
//
//        DataStream<String> stream = env.fromCollection(xmlList);
//        stream.sinkTo(sink);
//        env.execute("Secure Kafka Producer");
//
//
//    }
//
//    private static void getSpnrXmlFormText(List<String> xmlList, String etlDate) throws Exception {
//        // 从文件中获取spnrId列表，调用接口拿数据
//        List<String> spnrIds = getSpnrIds(etlDate);
//        for (String spnrId : spnrIds) {
//            String getSpnrXml = HttpClientUtils.createGetSpnrXml(spnrId);
//            String summaryXml = HttpClientUtils.call(getSpnrXml, GET_MESSAGE_URL);
//            log.info("发送的请求为：" + getSpnrXml);
//            Node node = null;
//            Document document = null;
//            NodeList oj_superPNR = null;
//            if (StringUtils.isNotBlank(summaryXml) && Utils.isNotContains(summaryXml, SPNR_XML_LOG_004)) {
//                document = DocumentUtils.string2Document(summaryXml);
//                oj_superPNR = document.getElementsByTagName(SPNR_XML_LOG_006);
//                node = oj_superPNR.item(0);
//
//                if (null == node) {
//                    node = document.getElementsByTagName(SPNR_XML_LOG_007).item(0);
//                }
//            }
//            if (StringUtils.isBlank(summaryXml)) {
//                summaryXml = "";
//            }
//            log.info("获取的summary报文大小为：" + summaryXml.getBytes().length / 1024 / 1024 + "Mb");
//
//            String xml = DocumentUtils.toStringFromDoc(DocumentUtils.nodeToDocument(node));
//            if (StringUtils.isNotBlank(xml)) {
//                xmlList.add(xml);
//            } else {
//                log.info(etlDate + "没有获取到SUMMARY报文");
//            }
//            Thread.sleep(10000);
//        }
//    }
//
//    public static List<String> getSpnrIds(String etlDate) {
//        // 组装路径
//        String currentSpnrIdfilePath = CURRENT_SPNRID_FILE_BASEPATH + etlDate.replace("-", "") + ".txt";
//        List<String> spnrIds = new ArrayList<>();
//        try (BufferedReader reader = new BufferedReader(new FileReader(currentSpnrIdfilePath))) {
//            String line;
//            while ((line = reader.readLine()) != null) {
//                // 匹配格式：| 202202060000049115   |
//                if (line.trim().matches("\\|\\s*\\d+\\s*\\|")) {
//                    // 提取数字部分
//                    String spnrId = line.replaceAll("[^0-9]", "").trim();
//                    if (!spnrId.isEmpty()) {
//                        spnrIds.add(spnrId);
//                    }
//                }
//            }
//        } catch (IOException e) {
//            log.error("读取SPNR ID文件失败: " + currentSpnrIdfilePath, e);
//        }
//
//        return spnrIds;
//    }
//}
