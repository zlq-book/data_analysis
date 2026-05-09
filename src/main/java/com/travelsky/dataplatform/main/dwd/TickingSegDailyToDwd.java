package com.travelsky.dataplatform.main.dwd;

import com.travelsky.trp.usercenter.data.analysis.transform.labels.BookingSegDataStream;
import com.travelsky.trp.usercenter.data.analysis.transform.labels.TickingSegDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TickingSegDailyToDwd {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
//        String etlDate = "2025-04-30";
        String sm4key = "JUzgwCrDIT6v4SMg+BMX4A==";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*// 1. 初始化Kerberos认证
        System.setProperty("java.security.krb5.conf", "D:\\myjava\\SdaData\\src\\main\\resources\\krb5.conf");
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(hadoopConf);
        UserGroupInformation.loginUserFromKeytab("sys_ucv_test_for_mapreduce@BCHKDC", "D:\\myjava\\SdaData\\src\\main\\resources\\sys_ucv_test_for_mapreduce.keytab");
        // 每隔60s进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(Constants.CHECKPOINTING);
        // 高级选项：
        // 设置模式为exactly-once （这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(Constants.MIN_PAUSE_BETWEEN_CHECKPOINTS);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(Constants.CHECKPOINT_TIMEOUT);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 设置可容忍的失败次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);
        //使用hdfs checkpoint
        env.getCheckpointConfig().setCheckpointStorage("hdfs:///data/test/checkpoint");
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        */
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        // 提前出票天数
        TickingSegDataStream.result(tEnv, etlDate);

        //启动任务
        env.execute("Flink Consumer ->order label Doris TO DWD ");

    }
}
