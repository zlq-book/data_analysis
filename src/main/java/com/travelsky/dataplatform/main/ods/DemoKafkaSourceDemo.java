package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.io.IOException;
import java.util.Properties;

/**
 * @author kuangaihua
 * @date 2025/7/10 9:51
 */
public class DemoKafkaSourceDemo {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "DemoKafkaSourceDemo");
        env.setParallelism(3);
        // SASL认证配置（实际应从安全存储获取）
        String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                "username=\""+ Constants.KAFKA_USERNAME+"\" " +
                "password=\""+ Constants.KAFKA_PASSWORD +"\";";
        // 构建 Kafka Source
        KafkaSource<ConsumerRecord<String, String>> kafkaSource = KafkaSource.<ConsumerRecord<String, String>>builder()
                .setBootstrapServers(Constants.KAFKA_BOOTSTRAP_SERVERS)
                .setTopics("test")  // 替换为你的 topic
                .setGroupId("test07")  //  替换为你的 group id
                .setStartingOffsets(OffsetsInitializer.earliest())// 从最新的偏移量开始消费
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
                .setProperty("security.protocol", "SASL_PLAINTEXT")
                .setProperty("sasl.mechanism", "SCRAM-SHA-256")
                .setProperty("sasl.jaas.config", jaasConfig)
                .setProperty("session.timeout.ms", "1200000")
                .setProperty("fetch.wait.max.ms", "100000")
                //自动提交偏移量
                .setProperty("enable.auto.commit", "true")
                .setProperty("auto.commit.interval.ms", "5000")
                .build();
        // 启动流式任务，打印 value
        SingleOutputStreamOperator<String> kafka_source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(record -> String.format(
                        "Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s",
                        record.topic(), record.partition(), record.offset(),
                        record.key(), record.value()));
        kafka_source.print();
        // 执行 Flink 作业
        env.execute("Flink Kafka Consumer");
    }
}
