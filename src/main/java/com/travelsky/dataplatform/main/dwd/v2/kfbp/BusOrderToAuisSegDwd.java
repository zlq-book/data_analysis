package com.travelsky.dataplatform.main.dwd.v2.kfbp;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.KfbpCreateToSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.transform.Kfbp.KfbpBusOrderToDwdAuisSegFactTrans;
import com.travelsky.trp.usercenter.data.analysis.transform.Kfbp.KfbpBusOrderToDwdBookingPnrFactTrans;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 呼叫白屏票务系统-BUS_ORDER（基本订单表）
 */
public class BusOrderToAuisSegDwd {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
//        String etlDate = "2025-11-13";
        String startDate = etlDate;
        String endDate = etlDate;
        if (args.length > 2) {
            startDate = args[1];
            endDate = args[2];
        }
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointUtils.setCheckpoint(env, "BusOrderToDwd");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 1. 创建Doris目标表
        tEnv.executeSql(KfbpCreateToSql.BUS_ORDER);
        tEnv.executeSql(KfbpCreateToSql.BUS_FARE_TICKET_FLT);
        tEnv.executeSql(KfbpCreateToSql.BUS_FARE_TICKET);
        tEnv.executeSql(KfbpCreateToSql.BUS_FARE);
        // 2. 从Doris表中读取数据
        String query = "SELECT a.ORDER_SN ,\n" +
                "c.TICKET_NO ,\n" +
                "c.INSURE_EXPECT_POLICY_NO ,\n" +
                "c.INSURE_DELAY_POLICY_NO ,\n" +
                "c.INSURE_ALL_RISKS_POLICY_NO ,\n" +
                "c.CREATE_DATE ,\n" +
                "d.FARE_NAME ,\n" +
                "d.ID_TYPE ,\n" +
                "d.ID_CODE ,\n" +
                "e.CUR_TEL ,\n" +
                "c.FLT_DEPARTURE_DAY ,\n" +
                "c.FLT_START_CITY ,\n" +
                "c.FLT_ARRIVE_CITY ,\n" +
                "c.INSURE_EXPECT_STATUS ,\n" +
                "c.INSURE_DELAY_STATUS ,\n" +
                "c.INSURE_ALL_RISKS_STATUS ,\n" +
                "a.EXPECT_INSURE_FEE ,\n" +
                "a.DELAY_INSURE_FEE ,\n" +
                "a.ALL_RISKS_INSURE_FEE " +
                "FROM T_ODS_KFBP_BUS_ORDER a\n" +
                "INNER JOIN T_ODS_KFBP_BUS_FARE_TICKET b \n" +
                "ON a.ORDER_TYPE ='6' AND a.FARE_PKID  = b.FARE_PKID   \n" +
                "INNER JOIN T_ODS_KFBP_BUS_FARE_TICKET_FLT c \n" +
                "ON b.PKID =c.FARE_TICKET_PKID \n" +
                "INNER JOIN T_ODS_KFBP_BUS_FARE d\n" +
                "ON d.PKID = a.FARE_PKID \n" +
                "LEFT JOIN T_ODS_KFBP_BUS_ORDER e\n" +
                "ON e.ORDER_TYPE ='2' AND e.UNION_ORDER_PKID = a.UNION_ORDER_PKID    " +
                "WHERE (c.INSURE_EXPECT_POLICY_NO IS NOT NULL \n" +
                "OR c.INSURE_DELAY_POLICY_NO IS NOT NULL \n" +
                "OR c.INSURE_ALL_RISKS_POLICY_NO IS NOT NULL )  " +
                "AND a.ETL_DATE >='" + startDate + "'" + " AND a.ETL_DATE <='" + endDate + "'";
        System.out.println("日期为：" + etlDate + "的KFBP数据开始查询...");
        Table table = tEnv.sqlQuery(query);
        DataStream<JSONObject> busRefundSource = tEnv.toChangelogStream(table).
                filter((row) -> row.getKind() == RowKind.INSERT || row.getKind() == RowKind.UPDATE_AFTER).
                map(new MapFunction<Row, JSONObject>() {
            @Override
            public JSONObject map(Row row) throws Exception {
                JSONObject map = new JSONObject();
                map.put("ORDER_SN", row.getField("ORDER_SN"));
                map.put("TICKET_NO", row.getField("TICKET_NO") == null ? null :
                        NormalizationUtils.standardize(FieldType.TICKET_NO,
                                row.getField("TICKET_NO").toString(), DataSource.CALL_CENTER_TICKETING_SYSTEM));
                map.put("INSURE_EXPECT_POLICY_NO", row.getField("INSURE_EXPECT_POLICY_NO"));
                map.put("INSURE_DELAY_POLICY_NO", row.getField("INSURE_DELAY_POLICY_NO"));
                map.put("INSURE_ALL_RISKS_POLICY_NO", row.getField("INSURE_ALL_RISKS_POLICY_NO"));
                map.put("CREATE_DATE", row.getField("CREATE_DATE"));

                map.put("FARE_NAME", row.getField("FARE_NAME") == null ? null :
                        NormalizationUtils.standardize(FieldType.CN_NAME,
                                row.getField("FARE_NAME").toString(), DataSource.CALL_CENTER_TICKETING_SYSTEM));
                Object idType = row.getField("ID_TYPE");
                Object idCode = row.getField("ID_CODE");

                if (null != idType && null != idCode) {
                    String idTypeStr = NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE,
                            idType.toString(), DataSource.CALL_CENTER_TICKETING_SYSTEM);
                    String idCodeStr = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,
                            idCode.toString(), DataSource.CALL_CENTER_TICKETING_SYSTEM);
                    map.put("ID_TYPE", idTypeStr);
                    map.put("ID_CODE",idCodeStr);
                    Tid tid = new Tid();
                    tid.setTid(idCodeStr);
                    Map<String, String> certification = new HashMap<>();
                    certification.put(idTypeStr, idCodeStr);
                    tid.setCertification(certification);
                    String tidStr = IdMapping.idMappingFunction(tid, "KFBP");
                    map.put("TID", tidStr);
                } else {
                    map.put("ID_TYPE", idType);
                    map.put("ID_CODE",idCode);
                    map.put("TID", null);
                }

                map.put("CUR_TEL", row.getField("CUR_TEL"));
                map.put("BOOKING_TID", row.getField("CUR_TEL"));
                if (null != row.getField("CUR_TEL")) {
                    Tid tid = new Tid();
                    tid.setTid(row.getField("CUR_TEL").toString());
                    String tidStr = IdMapping.idMappingFunction(tid, "KFBP");
                    map.put("BOOKING_TID", tidStr);
                }

                map.put("FLT_DEPARTURE_DAY", row.getField("FLT_DEPARTURE_DAY"));
                map.put("FLT_START_CITY", row.getField("FLT_START_CITY"));
                map.put("FLT_ARRIVE_CITY", row.getField("FLT_ARRIVE_CITY"));
                map.put("INSURE_EXPECT_STATUS", row.getField("INSURE_EXPECT_STATUS") == null ? null :
                        NormalizationUtils.standardize(FieldType.INSURANCE_STATUS,
                                row.getField("INSURE_EXPECT_STATUS").toString(), DataSource.CALL_CENTER_TICKETING_SYSTEM));
                map.put("INSURE_DELAY_STATUS", row.getField("INSURE_DELAY_STATUS") == null ? null :
                        NormalizationUtils.standardize(FieldType.INSURANCE_STATUS,
                                row.getField("INSURE_DELAY_STATUS").toString(), DataSource.CALL_CENTER_TICKETING_SYSTEM));
                map.put("INSURE_ALL_RISKS_STATUS", row.getField("INSURE_ALL_RISKS_STATUS") == null ? null :
                        NormalizationUtils.standardize(FieldType.INSURANCE_STATUS,
                                row.getField("INSURE_ALL_RISKS_STATUS").toString(), DataSource.CALL_CENTER_TICKETING_SYSTEM));

                map.put("EXPECT_INSURE_FEE", row.getField("EXPECT_INSURE_FEE"));
                map.put("DELAY_INSURE_FEE", row.getField("DELAY_INSURE_FEE"));
                map.put("ALL_RISKS_INSURE_FEE", row.getField("ALL_RISKS_INSURE_FEE"));

                return map;
            }
        }).filter(Objects::nonNull);

        // 写入保险事实表
        KfbpBusOrderToDwdAuisSegFactTrans.result(busRefundSource);

        //启动任务
        env.execute("BusOrderToBookingPnrDwd");
    }
}
