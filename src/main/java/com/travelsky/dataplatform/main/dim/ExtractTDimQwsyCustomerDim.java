package com.travelsky.dataplatform.main.dim;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.trp.usercenter.data.analysis.transform.qwsy.QwsyCustomerDimTrans;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 企微小山客户信息维表数据转换启动类
 * @date 2025/12/4
 */
public class ExtractTDimQwsyCustomerDim {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        CheckpointUtils.setCheckpoint(env, "ExtractTDimQwsyCustomerDim");
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 企微小山客户信息维表数据转换
        QwsyCustomerDimTrans.result(tEnv, etlDate);

        env.execute("Flink Consumer -> QwsyCustomerDim");

    }
}
