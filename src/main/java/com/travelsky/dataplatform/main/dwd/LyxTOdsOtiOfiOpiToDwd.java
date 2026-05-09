package com.travelsky.dataplatform.main.dwd;

import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.trp.usercenter.data.analysis.transform.lyx.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author kuangaihua
 * @date 2025/7/2 9:31
 */
public class LyxTOdsOtiOfiOpiToDwd {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "LyxTOdsOtiOfiOpiToDwd");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 联表 ORDER_TICKET_INFO                  ORDER_FLIGHT_INFOS ORDER_PASSENGER_INFO
        //  机票预订-PNR级 机票预订-航段级 机票出票-客票级 机票出票-航段级  保险事实表-航段级
        OtiOfiOpiDataStream.result(tEnv, etlDate);
        //启动任务
        env.execute("Flink Consumer -> Doris TO DWD ");


    }
}
