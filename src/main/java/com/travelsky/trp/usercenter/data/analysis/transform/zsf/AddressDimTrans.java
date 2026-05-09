package com.travelsky.trp.usercenter.data.analysis.transform.zsf;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.AddressDimModel;
import com.travelsky.trp.usercenter.data.analysis.utils.EasyExcelUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AddressDimTrans {

    private static final Map<String, String> CHINESE_TO_ENGLISH_CONTINENT;

    static {
        Map<String, String> map = new HashMap<>();
        map.put("亚洲", "Asia");
        map.put("欧洲", "Europe");
        map.put("非洲", "Africa");
        map.put("北美洲", "North America");
        map.put("南美洲", "South America");
        map.put("大洋洲", "Oceania");
        map.put("南极洲", "Antarctica");
        // 可根据实际数据扩展
        CHINESE_TO_ENGLISH_CONTINENT = Collections.unmodifiableMap(map);
    }

    public static String toEnglishContinent(String chineseContinent) {
        if (chineseContinent == null) {
            return null;
        }
        String trimmed = chineseContinent.trim();
        return CHINESE_TO_ENGLISH_CONTINENT.getOrDefault(trimmed, "Unknown");
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, " AddressDimTrans");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        // 1. 从本地文件读取 Excel（假设路径为 /data/airport.xlsx）
        String excelPath = "C:\\山航\\AirCode.xlsx"; // 修改为你的真实路径

        List<AddressDimModel> airportList = EasyExcelUtils.readExcel(excelPath, AddressDimModel.class);

        // 2. 创建 DataStream<String>
        DataStream<AddressDimModel> dateStream = env.fromCollection(airportList);

        DataStream<AddressDimModel> dateDimStream = dateStream.map(record -> {


            record.setAddressKey(record.getAirportkey());
            // 转为英文洲名
//            String englishContinent = toEnglishContinent(record.getAirport());
//            record.setAddressState(englishContinent);
            return record;
        });

        DorisSink<AddressDimModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_ADDRESS_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);
        // 数据写入 Doris
        dateDimStream.sinkTo(dorisSink);

        env.execute("Address Dimension Generation Job");

    }






}
