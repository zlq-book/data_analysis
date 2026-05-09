package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;


import java.util.Arrays;
import java.util.List;

/**
 * @author kuangaihua
 * @date 2025/7/10 9:47
 */
public class DemoCreateDataToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "DemoCreateDataToKafka");
        String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                "username=\""+ Constants.KAFKA_USERNAME+"\" " +
                "password=\""+ Constants.KAFKA_PASSWORD +"\";";
        // 构建安全Kafka Sink
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(Constants.KAFKA_BOOTSTRAP_SERVERS) // SSL端口
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("test")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setProperty("security.protocol", "SASL_PLAINTEXT")
                .setProperty("sasl.mechanism", "SCRAM-SHA-256")
                .setProperty("sasl.jaas.config", jaasConfig)
                .setProperty("transaction.timeout.ms", "600000")

                .build();

        // 模拟数据源
        DataStream<String> stream = env.fromCollection(
                Arrays.asList("record1", "record2", "record3"));
        stream.sinkTo(sink);
        env.execute("Secure Kafka Producer");
    }

}
