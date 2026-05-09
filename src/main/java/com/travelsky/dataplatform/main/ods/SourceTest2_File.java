package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.kafka.

/**
 * @author kuangaihua
 * @date 2025/4/7 16:59
 */
public class SourceTest2_File {
    public static void main(String[] args) throws Exception {
        for (int i = 0; i < args.length; i++) {

        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "SourceTest2_File");
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> dataStream = env.readTextFile("D:\\myjava\\Flink\\src\\main\\resources\\sensor.txt", "utf-8");

        // 打印输出
        dataStream.print();

        env.execute();
    }

    public int[] twoSum(int[] nums, int target) {
        for (int a = 0; a < nums.length; a++
        ) {
            for (int b = 0; b < nums.length; b++
            ) {
                if (nums[a] + nums[b] == target
                ) {
                    return new int[]{a, b};
                }
            }

        }
        return null;
    }
}
