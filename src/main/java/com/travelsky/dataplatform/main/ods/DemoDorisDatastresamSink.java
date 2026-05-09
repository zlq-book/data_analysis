package com.travelsky.dataplatform.main.ods;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.TOdsHytdTManageOthercard;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.source.DMDBSourceFunction;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;




import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author kuangaihua
 * @date 2025/7/9 14:18
 */
public class DemoDorisDatastresamSink {
    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "DemoDorisDatastresamSink");
 /*       Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", Constants.KAFKA_BOOTSTRAP_SERVERS);
        properties.setProperty("group.id", Constants.KAFKA_SPNR_PARQUET_GROUP);
        //kafka sasl配置
        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"" + Constants.KAFKA_USERNAME + "\" " +
                        // No need to modify - belongs to the variable name.
                        "password=\"" + Constants.KAFKA_PASSWORD + "\";");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(Constants.KAFKA_CONSUME_TOPIC, new SimpleStringSchema(), properties);
        //消费者组不变时从上次消费的位置开始消费（更换消费者组时重头开始消费）
        kafkaConsumer.setStartFromGroupOffsets();
        //启用checkpoint机制下，设#查TID所用到的hbase表置自动提交offset，默认为true
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.print();*/
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String url = "jdbc:dm://" + Constants.HYTD_IP + ":" + Constants.HYTD_PORT + "/" + Constants.HYTD_DB;
        String user = Constants.HYTD_USER;
        String password = Constants.HYTD_PWD;
        String query = "SELECT " +
                "ID\n" +
                ",CRM_CARDNO\n" +
                ",CARD_NO\n" +
                ",NAME\n" +
                ",NAME_EN\n" +
                ",CERT_TYPE\n" +
                ",CERT_NO\n" +
                ",END_TIME\n FROM " + Constants.HYTD_SCHEMA + ".T_MANAGE_OTHERCARD AS sr";
        String columStr = "ID,CRM_CARDNO,CARD_NO,NAME,NAME_EN,CERT_TYPE,CERT_NO,END_TIME";
        String[] columnStrs = columStr.split(",");
        List<String> columns = Arrays.stream(columnStrs).collect(Collectors.toList());
        DMDBSourceFunction function = new DMDBSourceFunction(url, user, password, query, columns);
        DataStreamSource<JSONObject> jsonObjectDataStreamSource = env.addSource(function);
        SingleOutputStreamOperator<TOdsHytdTManageOthercard> tSource = jsonObjectDataStreamSource.map(jsonObject -> {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            TOdsHytdTManageOthercard tOdsHytdTManageOthercard = mapper.readValue(JSONObject.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue), TOdsHytdTManageOthercard.class);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            tOdsHytdTManageOthercard.setETL_DATE(Date.valueOf(LocalDate.parse(etlDate, formatter)));
            return tOdsHytdTManageOthercard;
        });
        DorisSink<TOdsHytdTManageOthercard> dorisSink = FlinkDorisUtils.creatDorisSink(Constants.ODS_DB, "T_ODS_HYTD_T_MANAGE_OTHERCARD", Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT, Constants.ODS_USER, Constants.ODS_PWD);
        tSource.sinkTo(dorisSink);
        env.execute();
    }
}
