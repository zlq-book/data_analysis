package com.travelsky.dataplatform.main.dwd;

import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.trp.usercenter.data.analysis.transform.zxyh.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author kuangaihua
 * @date 2025/7/2 9:31
 */
public class ZxyhTOdsCrmCustomerToDwd {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
//        String etlDate = "2025-04-30";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ZxyhTOdsCrmCustomerToDwd");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 直销用户表
        //  CRM_CUSTOMER
        CrmCustomerDataStream.result(tEnv, etlDate);



        //启动任务
        env.execute("Flink Consumer -> Doris TO DWD ");


    }
}
