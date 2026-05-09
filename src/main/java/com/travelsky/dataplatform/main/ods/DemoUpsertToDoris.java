package com.travelsky.dataplatform.main.ods;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.dim.TDimIdmappingError;
import com.travelsky.dataplatform.module.ods.TOdsHytdTManageOthercard;
import com.travelsky.dataplatform.source.WholeFileSource;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.DocumentUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.XpathUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.w3c.dom.Document;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

/**
 * @author kuangaihua
 * @date 2025/7/14 9:47
 */
public class DemoUpsertToDoris {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "DemoUpsertToDoris");
        // 模拟数据源
        TDimIdmappingError tDimIdmappingError = new TDimIdmappingError();
        tDimIdmappingError.setERROR_TID("2");
        tDimIdmappingError.setDIFF_STRONGID("2");
//        tDimIdmappingError.setSAME_STRONGID("7");
        Timestamp timestamp = new Timestamp(new Date(System.currentTimeMillis()).getTime());
        System.out.println(timestamp);
//        String columns= "ERROR_TID,SAME_STRONGID,DIFF_STRONGID,ETL_DATE";
        tDimIdmappingError.setETL_DATE(etlDate);
        tDimIdmappingError.setCREATE_TIME(timestamp.toString());
        tDimIdmappingError.setUPDATE_TIME(timestamp.toString());
        String s = JSONObject.
                toJSONString(tDimIdmappingError).toUpperCase();
        System.out.println(s);
/*        DataStream<String> stream = env.fromCollection(
                Arrays.asList(s));
        DorisSink<String> dorisSink = FlinkDorisUtils.creatDorisJsonPartialSink(
                Constants.DIM_DB,
                "T_DIM_IDMAPPING_ERROR",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD,
                columns);
        stream.sinkTo(dorisSink);*/
        DataStream<TDimIdmappingError> stream1 = env.fromCollection(
                Arrays.asList(tDimIdmappingError));
        DorisSink<TDimIdmappingError> dorisSink1 = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_IDMAPPING_ERROR",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);
        stream1.sinkTo(dorisSink1);
        env.execute("upsert T_DIM_IDMAPPING_ERROR");

    }
}
