package com.travelsky.dataplatform.main.ods;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.TOdsHytdTManageOthercard;
import com.travelsky.dataplatform.source.DMDBSourceFunction;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
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
public class DemoExtractHsdIpsByJdbcDemo {
    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "DemoExtractHsdIpsByJdbcDemo");
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
        SinkFunction<TOdsHytdTManageOthercard> jdbcSink = JdbcSink.sink(
                "INSERT INTO T_ODS_HYTD_T_MANAGE_OTHERCARD(" +
                        "ETL_DATE  " +
                        ",ID\n" +
                        ",CRM_CARDNO\n" +
                        ",CARD_NO\n" +
                        ",NAME\n" +
                        ",NAME_EN\n" +
                        ",CERT_TYPE\n" +
                        ",CERT_NO\n" +
                        ",END_TIME\n)  values (?,?,?,?,?,?,?,?,?)" ,
                (ps, t) -> {
                    ps.setDate(1, t.getETL_DATE());
                    ps.setString(2, t.getID());
                    ps.setString(3, t.getCRM_CARDNO());
                    ps.setString(4, t.getCARD_NO());
                    ps.setString(5, t.getNAME());
                    ps.setString(6, t.getNAME_EN());
                    ps.setString(7, t.getCERT_TYPE());
                    ps.setString(8, t.getCERT_NO());
                    ps.setTimestamp(9,  new java.sql.Timestamp(t.getEND_TIME().longValue()));
                },
                JdbcExecutionOptions.builder().withBatchSize(1000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB)
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername(Constants.ODS_USER)
                        .withPassword(Constants.ODS_PWD)
                        .build()
        );
        tSource.addSink(jdbcSink);
        env.execute();
    }
}
