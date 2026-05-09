package com.travelsky.dataplatform.main.dwd.lygj;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.LygjCreateToSql;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.transform.lygj.LygjGdlkDimMergeTrans;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


public class LygjDpiMergeGdlkDim {

    //private static final Logger logger = LoggerFactory.getLogger(LygjDpiMergeGdlkDim.class);

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String startDate = etlDate;
        String endDate = etlDate;
        if (args.length > 2) {
            startDate = args[1];
            endDate = args[2];
        }
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointUtils.setCheckpoint(env, "LygjDpiMergeGdlkDim");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);

        // 1. 创建Doris目标表
        tEnv.executeSql(LygjCreateToSql.DEPART_PASSENGER_INFO);
        // 2. 从Doris表中读取数据
        String query = "SELECT \n" +
                "ETL_DATE,\n" +
                "CERT_NO,\n" +
                "ID_TYPE,\n" +
                "FFP,\n" +
                "IS_VVIP,\n" +
                "IS_CIP,\n" +
                "IS_VIP\n" +
                "FROM T_ODS_LYGJ_DEPART_PASSENGER_INFO " +
                "WHERE ETL_DATE >= '" + startDate + "'"  + " AND ETL_DATE <= '" + endDate + "'";
        System.out.println("日期为：" + etlDate + "的DPI数据开始查询...");
        //logger.info("查询sql：" + query);
        Table table = tEnv.sqlQuery(query);
        DataStream<JSONObject> dpiSource = tEnv.toChangelogStream(table).map(new MapFunction<Row, JSONObject>() {
            @Override
            public JSONObject map(Row row) throws Exception {
                // 证件号
                String certNo = row.getFieldAs("CERT_NO");
                if (StringUtils.isBlank(certNo)) {
                    return null;
                }
                // 证件号标准化
                certNo = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, certNo, DataSource.LUYAN_STEWARD);

                // 证件类型
                String idType = row.getFieldAs("ID_TYPE");
                // 证件类型标准化
                idType = NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, idType, DataSource.LUYAN_STEWARD);
                // 生成TID
                Tid tid = new Tid();
                tid.setTid(certNo);
                Map<String, String> certification = new HashMap<>();
                certification.put(idType, certNo);
                String tidStr = IdMapping.idMappingFunction(tid, "LYGJ", false,true,true);

                // VVIP标识
                String isVvip = row.getFieldAs("IS_VVIP");
                // VVIP标识标准化
                isVvip = NormalizationUtils.standardize(FieldType.VVIP_FLAG, isVvip, DataSource.LUYAN_STEWARD);


                JSONObject map = new JSONObject();
                // 添加所有字段到map
                map.put("TID", tidStr);
                map.put("IS_VVIP", isVvip);
                map.put("IS_CIP", row.getFieldAs("IS_CIP"));
                map.put("IS_VIP", row.getFieldAs("IS_VIP"));
                return map;
            }
        }).filter(Objects::nonNull);
        // 更新高端旅客维表中的vip、cip、vvip数据
        LygjGdlkDimMergeTrans.result(dpiSource);
        //启动任务
        env.execute("LygjDpiMergeGdlkDim");
    }
}
