package com.travelsky.dataplatform.main.dwd;

import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.trp.usercenter.data.analysis.transform.clk.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author kuangaihua
 * @date 2025/7/2 9:31
 */
public class ClkTOdsFfpRegisterInfoToDwd {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
//        String etlDate = "2025-05-01";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ClkTOdsFfpRegisterInfoToDwd");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        // FFP_REGISTER_INFO 常客注册实事 用户维表
        // 实时方案：在 FriDataStream 中会加载当天的 ffp_register_scan 数据，并在 FriToDwdMileRegisterFactNewTrans 中匹配处理渠道逻辑
        FriDataStream.result(tEnv, etlDate);

        //启动任务
        env.execute("Flink Consumer ->CLK Doris TO DWD ");


    }
}
