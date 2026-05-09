package com.travelsky.trp.usercenter.data.analysis.main.b2fdm.hsd;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.w3c.dom.Document;

import java.io.IOException;
import java.util.Properties;

/**
 * 旅客订座系统预留座位变更
 */
public class B2HSDAsrChange {

    static final String EVENT_AsrChange = "AsrChange";

//    public static void main(String[] args) throws Exception {
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        Utils.setParam();
//
//        String date = Utils.setParameters(args);
//        if (Constants.DEBUG_MODE) {
//            Utils.setHadoopHome();
//        }
//        B2HSDAsrChange.streamConfiguration(env);
//        env.execute("B2HSDAsrChange");
//    }
//    /**
//     * 流处理公用方法
//     * @param env
//     * @throws Exception
//     */
//    public static void streamConfiguration(StreamExecutionEnvironment env) throws Exception {
//
//        //启用checkpoint机制来进行容错处理
//
//        // 每隔10s进行启动一个检查点【设置checkpoint的周期】
//        env.enableCheckpointing(Constants.CHECKPOINTING);
//        // 高级选项：
//        // 设置模式为exactly-once （这是默认值）
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(Constants.MIN_PAUSE_BETWEEN_CHECKPOINTS);
//        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
//        env.getCheckpointConfig().setCheckpointTimeout(Constants.CHECKPOINT_TIMEOUT);
//        // 同一时间只允许进行一个检查点
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
//        env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        /*
//        cancel处理选项：
//        （1）ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:
//        表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定
//                的Checkpoint
//
//       （2）ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION:
//        表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会
//                保存checkpoint
//        */
//
//
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", Constants.KAFKA_BOOTSTRAP_SERVERS);
//        properties.setProperty("group.id", Constants.KAFKA_SPNR_PARQUET_GROUP);
//
//
//        //在没有启用checkpoint机制下，进行自动提交offset,如果启用checkpoint,此设置会被忽略
//        properties.setProperty("auto.commit.interval.ms", "1000");
//        properties.setProperty("auto.offset.reset", "earliest");
//
//        //kafka sasl配置
//        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
//        properties.setProperty("sasl.mechanism", "PLAIN");
//        properties.setProperty("sasl.jaas.config",
//                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
//                        "username=\"" + Constants.KAFKA_USERNAME + "\" " +
//                        // No need to modify - belongs to the variable name.
//                        "password=\"" + Constants.KAFKA_PASSWORD + "\";");
///*
//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
////        每次重启都重头消费
////        kafkaConsumer.setStartFromEarliest();
////        kafkaConsumer.setStartFromLatest();
////        消费者组不变时从上次消费的位置开始消费（更换消费者组时重头开始消费）
//        kafkaConsumer.setStartFromGroupOffsets();
//
//        //启用checkpoint机制下，设#查TID所用到的hbase表置自动提交offset，默认为true
//        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
//
//        DataStreamSource<String> source1 = env.addSource(kafkaConsumer);*/
//        DataStreamSource<String> source1 = env.readTextFile("hdfs://nameservice1/tmp/hsd/AsrChange.xml","utf-8");
//        processAsrChange(source1);
//    }
//
//    public static void processAsrChange(DataStreamSource<String> source1) throws IOException {
//        DataStream<String> source = outputTtf(source1);
//        //      获取Tid,并存parquet到hdfs
//        AsrChangeFactTrans.outputgbsf(source);
//    }
//
//    public static DataStream<String> outputTtf(DataStream<String> source) throws IOException {
//        DataStream<String> transResult = source.map(new MapFunction<String, String>() {
//            @Override
//            public String   map(String s) throws Exception {
//                Document document;
//                try {
//                    document = DocumentUtils.string2Document(s);
//                } catch (Exception e) {
//                    return null;
//                }
//                //校验前序事件、book不需要
//                //非book事件不写入
//                if (!EVENT_AsrChange.equals(XpathUtils.getString(document, "/Msg/Hdr/Event"))) {
//                    return null;
//                }
//                String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptm");
//
//                String pnrKey = HsdTool.getPnrIndexKey(document);
//                //查一下HBase里有没有
//                //这个id代表了订单的版本号，数值越大代表越新
//                String oldUptmTime = HBaseUtils.searchSpecifiedColumn(Constants.HSD_PNR_INDEX, pnrKey, Constants.INDEX_TABLE_FAMILY,"asr_change_time");
//
//                //最新的则解析，不是最新的就返回null
//                if (IdSetUtils.isNull(oldUptmTime) || Long.parseLong(uptm)>=Long.parseLong(oldUptmTime)){
//                    SpnrTool.insertIndex(Constants.HSD_PNR_INDEX, pnrKey, "asr_change_flag", "0");
//                    SpnrTool.insertIndex(Constants.HSD_PNR_INDEX, pnrKey, "asr_change_time", uptm);
//                    SpnrTool.insertIndex(Constants.HSD_PNR_INDEX, pnrKey, "asr_change_data", s);
//                    return s;
//                }
//                else
//                    return null;
//            }
//        });
//        return transResult;
//    }

}
