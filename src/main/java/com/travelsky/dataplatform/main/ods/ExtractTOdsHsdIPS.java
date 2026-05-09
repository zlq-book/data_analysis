package com.travelsky.dataplatform.main.ods;

import com.travelsky.aic.data.common.MessagePackUtil;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.source.WholeFileSource;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.IdMapping;
import com.travelsky.trp.usercenter.data.analysis.transform.hsd.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.api.common.time.Time;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class ExtractTOdsHsdIPS {

    static final Logger logger = LoggerFactory.getLogger(ExtractTOdsHsdIPS.class);
    public static void main(String[] args) throws Exception {
        String startingOffsets;
        OffsetsInitializer offsetsInitializer = OffsetsInitializer.committedOffsets();
        if (args.length > 0) {
            startingOffsets = args[0];
            if(OffsetResetStrategy.EARLIEST.toString().equalsIgnoreCase(startingOffsets)){
                offsetsInitializer = OffsetsInitializer.earliest();
            } else if (OffsetResetStrategy.LATEST.toString().equalsIgnoreCase(startingOffsets)){
                offsetsInitializer = OffsetsInitializer.latest();
            }
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
//        env.enableCheckpointing(Constants.CHECKPOINTING);
        //CheckpointUtils.setCheckpoint(env, "ExtractTOdsHsdIPS");
        env.setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        // 故障率重启策略（推荐）
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                100,  // 每个时间间隔内的最大失败次数
                Time.of(5, TimeUnit.MINUTES),  // 时间间隔
                Time.of(10, TimeUnit.SECONDS)  // 重启延迟
        ));
        // SASL认证配置（实际应从安全存储获取）
        String jaasConfig = Constants.HSD_KAFKA_JAAS_CLAZZ+" required " +
                "username=\""+ Constants.HSD_KAFKA_USERNAME+"\" " +
                "password=\""+ Constants.HSD_KAFKA_PASSWORD +"\";";
        // 构建 Kafka Source
        KafkaSource<ConsumerRecord<String, String>> kafkaSource = KafkaSource.<ConsumerRecord<String, String>>builder()
                .setBootstrapServers(Constants.HSD_KAFKA_SERVICES)
                .setTopics(Constants.HSD_KAFKA_TOPIC)  // 替换为你的 topic
                .setGroupId(Constants.HSD_KAFKA_GROUPID)  //  替换为你的 group id
                .setStartingOffsets(offsetsInitializer)// 从最新的偏移量开始消费
                // 👇 只获取 value，使用 StringDeserializer，如果需要使用key的话需要自定义反序列化器
                .setDeserializer(new KafkaRecordDeserializationSchema<ConsumerRecord<String, String>>() {
                    @Override
                    public TypeInformation<ConsumerRecord<String, String>> getProducedType() {
                        return TypeInformation.of(new TypeHint<ConsumerRecord<String, String>>() {
                        });
                    }

                    @Override
                    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<ConsumerRecord<String, String>> collector) throws IOException {
                        String key = consumerRecord.key() == null ? null : new String(consumerRecord.key());
                        String value = consumerRecord.value() == null ? null : new String(consumerRecord.value());
                        collector.collect(new ConsumerRecord<>(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.timestamp(), consumerRecord.timestampType(), consumerRecord.checksum(), consumerRecord.serializedKeySize(), consumerRecord.serializedValueSize(), key, value));
                    }
                })
                .setProperty("security.protocol", Constants.HSD_KAFKA_SECURITY_PROTOCOL)
                .setProperty("sasl.mechanism", Constants.HSD_KAFKA_SASLME_CHANISM)
                .setProperty("sasl.jaas.config", jaasConfig)
                .setProperty("session.timeout.ms", "1200000")
                .setProperty("fetch.wait.max.ms", "100000")
                //自动提交偏移量
                .setProperty("enable.auto.commit", "true")
                .setProperty("auto.commit.interval.ms", "5000")
                .build();
        // 启动流式任务，打印 value
        SingleOutputStreamOperator<String> kafka_source = env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source"
                ).setParallelism(Constants.HSD_KAFKA_PARALLELISM)
                .map(record -> {
                    try {
                        //System.out.println("HSD Kafka Data: " + MessagePackUtil.unpackingMessage(record.value()));
                        // kafka数据解密
                        return MessagePackUtil.unpackingMessage(record.value());
                    } catch (Exception e) {
                        logger.error("Record processing failed", e);
                        System.out.println("高频Kafka数据解密失败: " + record.value());
                        return null;
                    }
                }).filter(data -> data != null && !data.isEmpty()); // 添加过滤空数据的步骤

        /*WholeFileSource wholeFileSource = new WholeFileSource("E:\\TravelSky\\GitLab-SC\\data_analysis\\src\\test" +
                "\\resources\\hsd\\mockdata\\IDChange.xml");
        DataStreamSource<String> kafka_source = env.addSource(wholeFileSource);*/

        // 数据采集（入ods）
        BaseTrans.output(kafka_source);
        HsdCommonFactTrans.output(kafka_source);

        /*// 数据整合(入dim用户维表)
        UserDimTrans.output(kafka_source);
        // 证件信息维表
        CertDimTrans.output(kafka_source);
        // 手机号维表
        MobileDimTrans.output(kafka_source);

        // 数据整合（入dwd事实表）
        // 航段ActionCode值变更
        ActionCodeChangeFactTrans.output(kafka_source);
        // 旅客订座系统预留座位变更
        AsrChangeFactTrans.output(kafka_source);
        // 行李变更
        BagChangeTrans.output(kafka_source);
        // 旅客客票登机取消通知
        Boarded2OpenFactTrans.output(kafka_source);
        // 旅客登机
        BoardFactTrans.output(kafka_source);
        // 旅客预订座位事件
        BookFactTrans.output(kafka_source);
        // 客票舱位变更
        CabinChangeFactTrans.output(kafka_source);
        // 旅客取消座位预订 V2处理
        //CancelFactTrans.output(kafka_source);
        // 值机
        CheckInTrans.output(kafka_source);
        // 值机拉下
        CheckOutTrans.output(kafka_source);
        // 解挂
        TickingSegFactFactTrans.outputDesuspend(kafka_source);
        // EMD登机
        EMDBoardFactTrans.output(kafka_source);
        // EMD值机
        EMDCheckInFactTrans.output(kafka_source);
        // EMD成行
        EMDFlownFactTrans.output(kafka_source);
        // EMD出票
        EMDIssueFactTrans.output(kafka_source);
        // EMD退票
        EMDRefundFactTrans.output(kafka_source);
        // EMD废票
        EMDVOIDFactTrans.output(kafka_source);
        // 客票取消退票
        ETRFFactTrans.output(kafka_source);
        // 客票取消换开
        Exchange2OpenFactTrans.output(kafka_source);
        // 客票换开
        ExchangeFactTrans.output(kafka_source);
        // 旅客常卡变更
        FFChangeTrans.output(kafka_source);
        // 客票取消临时换开
        FIMExchange2OpenFactTrans.output(kafka_source);
        // 客票使用
        FlownFactTrans.output(kafka_source);
        // 旅客证件信息变更
        IDChangeFactTrans.output(kafka_source);
        // 客票IRR标识变更
        IRRChangeFactTrans.output(kafka_source);
        // 客票出票
        IssueFactTrans.output(kafka_source);
        // 旅客姓名变更事件
        NameChangeFactTrans.output(kafka_source);
        // NOSHOW
        NoshowFactTrans.output(kafka_source);
        // PNR OSI变更
        OSIChangeFactTrans.output(kafka_source);
        // 航段改期
        RevalidationFactTrans.output(kafka_source);
        // 旅客拆分
        SplitFactTrans.output(kafka_source);
        // PNR SSR变更
        SSRChangeFactTrans.output(kafka_source);
        // 旅客特服更改
        SSRChangeFactTrans.output(kafka_source);
        // 挂起
        TickingSegFactFactTrans.outputSuspend(kafka_source);
        // 客票值机
        TicketCheckInFactTrans.output(kafka_source);
        // 客票值机拉下
        TicketCheckOutFactTrans.output(kafka_source);
        // 旅客客票航段有效期变更
        TktSegDurChangeFactTrans.output(kafka_source);
        // 客票退票
        TickingSegFactFactTrans.outputVT(kafka_source);*/
        //启动任务
        env.execute("Flink Kafka Consumer -> Doris HSD IPS ");
        IdMapping.close(false);
    }

}
