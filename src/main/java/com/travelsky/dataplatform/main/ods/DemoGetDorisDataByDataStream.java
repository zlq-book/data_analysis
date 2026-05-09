package com.travelsky.dataplatform.main.ods;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.doris.flink.source.DorisSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.List;

/**
 * @author kuangaihua
 * @date 2025/6/27 15:13
 */
public class DemoGetDorisDataByDataStream {
    /*public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DorisOptions option = DorisOptions.builder()
                .setFenodes("10.31.0.3:50081")
                .setTableIdentifier("ODS_TEST.T_ODS_WXAP_CHECKIN_TRP_ORDER")
                .setUsername("root")
                .setPassword("Sdyd@10086#Jk")
                .build();

        DorisReadOptions readOptions = DorisReadOptions.builder().build();
        readOptions.setReadFields("name,id");
        DorisSource.DorisSourceBuilder dorisSourceBuilder = new DorisSource.DorisSourceBuilder();
        DorisSourceBuilder.
        DorisSource<List<?>> dorisSource = DorisSource.
                DorisSourceBuilder
                .
                .setDorisOptions(option)
                .setDorisReadOptions(readOptions)
                .setDeserializer(new SimpleListDeserializationSchema())
                .build();

        DataStreamSource<List<?>> doris_source = env.fromSource(dorisSource, WatermarkStrategy.noWatermarks(), "doris source");
        doris_source.print();
        env.execute("Doris Source Test");
    }*/
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "DemoGetDorisDataByDataStream");

        DorisSource<JSONObject> dorisSource = FlinkDorisUtils.creatQueryDorisSource(Constants.ODS_DB, "T_ODS_WXAP_CHECKIN_TRP_ORDER", new String[]{"ETL_DATE", "ID"}, "", JSONObject.class, Constants.FE_URL, Constants.BE_URL, Constants.ODS_USER, Constants.ODS_PWD);
        DataStreamSource<JSONObject> doris_source = env.fromSource(dorisSource, WatermarkStrategy.noWatermarks(), "doris source");
        doris_source.print();
        env.execute("Doris Source Test");


    }
}
