package com.travelsky.dataplatform.main.dwd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.trp.usercenter.data.analysis.transform.zsf.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ZsfOdrOrderDetailInsuranceToDwd {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);

        // 执行任务
        OdrOrderDetailInsuranceFactTrans.result( tEnv, etlDate);

        env.execute("Flink Consumer ->ZSF Doris TO DWD");


    }
}
