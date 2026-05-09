package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class ExtractTOdsHsdIPSTrans {

    public static Logger log = LoggerFactory.getLogger(ExtractTOdsHsdIPSTrans.class);
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
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsHsdIPSTrans");
        env.setParallelism(1);
        // SASL认证配置（实际应从安全存储获取）
        StringBuffer jaasConfigBuffer = new StringBuffer();
        jaasConfigBuffer.append(Constants.PRIMARY_KAFKA_JAAS_CLAZZ).append(" required ");
        jaasConfigBuffer.append("username=\"").append(Constants.PRIMARY_KAFKA_USERNAME).append("\" ");
        jaasConfigBuffer.append("password=\"").append(Constants.PRIMARY_KAFKA_PASSWORD).append("\";");
        String jaasConfig = jaasConfigBuffer.toString();
        
        // 构建 Kafka Source
        KafkaSource<ConsumerRecord<String, String>> kafkaSource = KafkaSource.<ConsumerRecord<String, String>>builder()
                .setBootstrapServers(Constants.PRIMARY_KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(Constants.PRIMARY_KAFKA_TOPICS)
                .setGroupId(Constants.PRIMARY_KAFKA_GROUPID)
                .setStartingOffsets(offsetsInitializer)
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
                .setProperty("security.protocol", Constants.PRIMARY_KAFKA_SECURITY_PROTOCOL)
                .setProperty("sasl.mechanism", Constants.PRIMARY_KAFKA_SASLME_CHANISM)
                .setProperty("sasl.jaas.config", jaasConfig)
                .setProperty("session.timeout.ms", "1200000")
                .setProperty("fetch.wait.max.ms", "100000")
                //自动提交偏移量
                .setProperty("enable.auto.commit", "true")
                .setProperty("auto.commit.interval.ms", "60000")
                .build();
        
        // 启动流式任务，处理Kafka数据
        SingleOutputStreamOperator<String> kafka_source = env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source"
                )
                .map(record -> {
                    try {
                        System.out.println(String.format("%s, Topic: %s, Partition: %s, Offset: %s, Timestamp: %s, Key: %s, Value: %s",
                                DateTimeUtils.getCurrentDateTime(),
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.timestamp(),
                                record.key(),
                                record.value()));
                        // kafka数据
                        return record.value();
                    } catch (Exception e) {
                        System.err.println("Record processing failed" + e);
                        return null;
                    }
                }).filter(data -> data != null && !data.isEmpty()); // 添加过滤空数据的步骤

        // 移动云Kafka Sink配置
        StringBuffer secondaryJaasConfigBuffer = new StringBuffer();
        secondaryJaasConfigBuffer.append(Constants.KAFKA_JAAS_CLAZZ).append(" required ");
        secondaryJaasConfigBuffer.append("username=\"").append(Constants.KAFKA_USERNAME).append("\" ");
        secondaryJaasConfigBuffer.append("password=\"").append(Constants.KAFKA_PASSWORD).append("\";");
        String secondaryJaasConfig = secondaryJaasConfigBuffer.toString();
                
        // 获取多个topic配置，假设通过逗号分隔
        String[] secondaryTopics = Constants.KAFKA_CONSUME_TOPIC.split(",");
        
        // 为每个topic创建KafkaSink并连接到数据流
        for (String topic : secondaryTopics) {
            topic = topic.trim(); // 去除空格
            if (!topic.isEmpty()) {
                KafkaSink<String> secondaryKafkaSink = KafkaSink.<String>builder()
                        .setBootstrapServers(Constants.KAFKA_BOOTSTRAP_SERVERS)
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setProperty("security.protocol", Constants.KAFKA_SECURITY_PROTOCOL)
                        .setProperty("sasl.mechanism", Constants.KAFKA_SASLME_CHANISM)
                        .setProperty("sasl.jaas.config", secondaryJaasConfig)
                        .setProperty("transaction.timeout.ms", "60000")
                        .build();
                        
                // 将处理后的数据写入二级Kafka的每个topic
                kafka_source.sinkTo(secondaryKafkaSink);
            }
        }
        
        //启动任务
        env.execute("Flink Kafka Consumer -> transfer data to Multi-topic secondary Kafka");
    }

}
