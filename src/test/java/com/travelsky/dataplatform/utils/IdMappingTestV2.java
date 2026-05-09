package com.travelsky.dataplatform.utils;

import com.alibaba.fastjson.JSON;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimSummaryModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * @author kuangaihua
 * @date 2025/7/28 15:01
 */
public class IdMappingTestV2 {
    private static final Logger logger = LoggerFactory.getLogger(IdMappingTestV2.class);

    public static void main(String[] args) throws Exception {

        IdMappingFunctionTest1();

    }
    private static void IdMappingFunctionTest1() throws Exception {

        Timer timer = IdMapping.autoCommit();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Integer> tidList = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            tidList.add(i);
        }
        DataStreamSource<Integer> integerDataStreamSource = env.fromCollection(tidList);
        SingleOutputStreamOperator<String> map = integerDataStreamSource.map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer integer) throws Exception {
                String tidJson="{\n" +
                        "  \"tid\": \""+integer+"\",\n" +
                        "  \"crmCustomerId\": \""+integer+"\"\n" +
                        "}";
                String dataType = "ZXYH";
                Tid tid = JSON.parseObject(tidJson, Tid.class);
                String s = IdMapping.idMappingFunction(tid, dataType,true,true,true);
                return integer+"";
            }
        }).setParallelism(5);
        map.print();
        env.execute();
        timer.cancel();
        IdMapping.close();
    }

    //测试批量插入
private static void batchInsertTest() throws Exception {
        Timer timer = IdMapping.autoCommit();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Integer> tidList = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            tidList.add(i);
        }
        DataStreamSource<Integer> integerDataStreamSource = env.fromCollection(tidList);
        SingleOutputStreamOperator<String> map = integerDataStreamSource.map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer integer) throws Exception {
                IdMapping.createDataTest("2025-10-20",integer+"");
                return integer+"";
            }
        }).setParallelism(1);
        map.print();
        env.execute();
        timer.cancel();
        IdMapping.close();
    }

}
