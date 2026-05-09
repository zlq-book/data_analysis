package com.travelsky.dataplatform.main.dwd;

import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.trp.usercenter.data.analysis.transform.zxyh.CoiToDimUserDimTrans;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author kuangaihua
 * @date 2025/7/2 9:31
 */
public class ZxyhTOdsCustomerOpenInfoToDwd {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ZxyhTOdsCustomerOpenInfoToDwd");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        //  CRM_CUSTOMER_OPEN_INFO 用户维表
        CoiToDimUserDimTrans.result(tEnv, etlDate);



        //启动任务
        env.execute("Flink Consumer -> Doris TO DWD ");


    }
}
