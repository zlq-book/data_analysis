package com.travelsky.dataplatform.main.dwd;

import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.trp.usercenter.data.analysis.transform.zxyh.CcToDimUserDimTrans;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author kuangaihua
 * @date 2025/7/2 9:31
 */
public class ZxyhTOdsCustomerCertToDwd {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
//        String etlDate="";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ZxyhTOdsCustomerCertToDwd");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //  CRM_CUSTOMER_CERT 用户维表
        CcToDimUserDimTrans.result(tEnv, etlDate);

        //启动任务
        env.execute("Flink Consumer -> Doris TO DWD ");


    }
}
