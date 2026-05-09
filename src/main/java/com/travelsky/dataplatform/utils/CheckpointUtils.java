package com.travelsky.dataplatform.utils;

import com.travelsky.dataplatform.constans.Constants;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author kuangaihua
 * @date 2025/9/4 10:19
 */
public class CheckpointUtils {
    public static void setCheckpoint(StreamExecutionEnvironment env,String taskName) {
        // 1. 初始化Kerberos认证
        // KerberosAuthUtils.initKerberosAuth(Constants.KRB5_CONF_PATH, Constants.SYS_UCV_TEST_FOR_MAPREDUCE_PRINCIPAL, Constants.SYS_UCV_TEST_FOR_MAPREDUCE_KEYTAB_PATH);

        // 每隔60s进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(Constants.CHECKPOINTING);
        // 高级选项：
        // 设置模式为exactly-once （这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(Constants.MIN_PAUSE_BETWEEN_CHECKPOINTS);
        // 检查点必须在3分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(Constants.CHECKPOINT_TIMEOUT);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 设置可容忍的失败次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(100);
        //使用hdfs checkpoint
//        env.getCheckpointConfig().setCheckpointStorage(Constants.CHECKPOINT_STORAGE+taskName);
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 启动时自动恢复最新checkpoint
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
    }
    public static void setCheckpoint(StreamExecutionEnvironment env) {
        // 1. 初始化Kerberos认证
//        KerberosAuthUtils.initKerberosAuth(Constants.KRB5_CONF_PATH, Constants.SYS_UCV_TEST_FOR_MAPREDUCE_PRINCIPAL, Constants.SYS_UCV_TEST_FOR_MAPREDUCE_KEYTAB_PATH);

        // 每隔60s进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(Constants.CHECKPOINTING);
        // 高级选项：
        // 设置模式为exactly-once （这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(Constants.MIN_PAUSE_BETWEEN_CHECKPOINTS);
        // 检查点必须在三分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(Constants.CHECKPOINT_TIMEOUT);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 设置可容忍的失败次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);
        //使用hdfs checkpoint
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cmss/data/test/checkpoint");
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    }
}
