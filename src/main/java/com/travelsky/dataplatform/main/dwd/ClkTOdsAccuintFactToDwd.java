package com.travelsky.dataplatform.main.dwd;

import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.trp.usercenter.data.analysis.transform.clk.AccuFactTrans;
import com.travelsky.trp.usercenter.data.analysis.transform.clk.AccuintFactTrans;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author kuangaihua
 * @date 2025/7/2 9:31
 */
public class ClkTOdsAccuintFactToDwd {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ClkTOdsAccuintFactToDwd");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        // CLK 累积2张
        AccuintFactTrans.result(tEnv, etlDate);

        //启动任务
        env.execute("Flink Consumer ->CLK Doris TO DWD ");


    }
}
