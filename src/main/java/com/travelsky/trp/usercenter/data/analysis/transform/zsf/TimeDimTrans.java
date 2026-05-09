package com.travelsky.trp.usercenter.data.analysis.transform.zsf;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.TimeDimModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TimeDimTrans {
    public static Logger log = LoggerFactory.getLogger(TimeDimTrans.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointUtils.setCheckpoint(env, "TimeDimTrans");
        env.setParallelism(1); // 时间维表数据量小，单并行度即可

        // 1. 生成所有时间点（按秒，00:00:00 ~ 23:59:59）
        List<String> timeList = generateTimeRange();

        // 2. 创建 DataStream
        DataStream<String> timeStream = env.fromCollection(timeList);

        // 3. 转换为 TimeDimModel
        DataStream<TimeDimModel> timeDimStream = timeStream.map(timeStr -> {
            // timeStr 格式: "HHmmss"，如 "000000", "000001", ..., "235959"
            int hour = Integer.parseInt(timeStr.substring(0, 2));
            int minute = Integer.parseInt(timeStr.substring(2, 4));
            int second = Integer.parseInt(timeStr.substring(4, 6));

            String timeValue = String.format("%02d:%02d:%02d", hour, minute, second);
            String timeKey = String.format("%02d%02d%02d", hour, minute, second); // HHMMSS

            TimeDimModel model = new TimeDimModel();
            model.setTimeKey(timeKey);
            model.setTimevalue(timeValue);
            model.setTimeHour(hour);
            model.setTimeMinute(minute);

            return model;
        });

        // 4. 配置 Doris Sink
        log.info("连接 Doris 参数: DB={}, Table={}, FE={}, BE={}, User={}",
                Constants.DIM_DB, "T_DIM_TIME_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER);

        DorisSink<TimeDimModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_TIME_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD
        );

        // 5. 写入 Doris
        timeDimStream.sinkTo(dorisSink);

        env.execute("Time Dimension Generation Job");
    }

    /**
     * 生成 00:00:00 到 23:59:59 的所有秒级时间点（格式：HHmmss）
     */
    public static List<String> generateTimeRange() {
        List<String> times = new ArrayList<>();
        for (int hour = 0; hour < 24; hour++) {
            for (int minute = 0; minute < 60; minute++) {
                for (int second = 0; second < 60; second++) {
                    times.add(String.format("%02d%02d%02d", hour, minute, second)); // e.g., "000000", "000001", ..., "235959"
                }
            }
        }
        return times;
    }
}




//-- 时间维度表初始化脚本
//-- 生成 00:00:00 到 23:59:59 的所有秒级时间点（共86400条记录）
//        -- 目标表：DIM_PROD.T_DIM_TIME_DIM
//
//-- 使用递归CTE生成0-86399的数字序列（代表从00:00:00开始的秒数）
//INSERT INTO DIM_PROD.T_DIM_TIME_DIM
//        (
//                TIME_KEY,
//                TIMEVALUE,
//                TIME_HOUR,
//                TIME_MINUTE
//                )
//WITH RECURSIVE time_seq AS (
//    -- 基础情况：从0开始
//                SELECT 0 AS sec_offset
//                UNION ALL
//                -- 递归：每次加1，直到86399（24小时 * 60分钟 * 60秒 - 1）
//                SELECT sec_offset + 1
//                FROM time_seq
//                WHERE sec_offset < 86399
//)
//SELECT
//    -- TIME_KEY: HHMMSS格式，如 "000000", "235959"
//CONCAT(
//        LPAD(FLOOR(sec_offset / 3600), 2, '0'),           -- 小时部分
//LPAD(FLOOR((sec_offset % 3600) / 60), 2, '0'),    -- 分钟部分
//LPAD(sec_offset % 60, 2, '0')                     -- 秒部分
//    ) AS TIME_KEY,
//
//    -- TIMEVALUE: HH:MM:SS格式，如 "00:00:00", "23:59:59"
//CONCAT(
//        LPAD(FLOOR(sec_offset / 3600), 2, '0'), ':',
//LPAD(FLOOR((sec_offset % 3600) / 60), 2, '0'), ':',
//LPAD(sec_offset % 60, 2, '0')
//    ) AS TIMEVALUE,
//
//    -- TIME_HOUR: 小时数（0-23）
//FLOOR(sec_offset / 3600) AS TIME_HOUR,
//
//    -- TIME_MINUTE: 分钟数（0-59）
//FLOOR((sec_offset % 3600) / 60) AS TIME_MINUTE
//
//FROM time_seq;
//
//COMMIT;
//
//-- 查看结果统计
//-- SELECT COUNT(*) AS total_records FROM DIM_PROD.T_DIM_TIME_DIM;
//-- SELECT * FROM DIM_PROD.T_DIM_TIME_DIM ORDER BY TIME_KEY LIMIT 10;
