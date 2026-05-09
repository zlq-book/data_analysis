//package com.travelsky.dataplatform.main.dwd.v2.kfbp;
//
//import com.alibaba.fastjson.JSONObject;
//import com.travelsky.dataplatform.constans.KfbpCreateToSql;
//import com.travelsky.dataplatform.utils.*;
//import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
//import com.travelsky.trp.usercenter.data.analysis.transform.Kfbp.*;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Objects;
//
///**
// * 呼叫白屏票务系统-BUS_ORDER（基本订单表）
// */
//public class BusOrderToBookingPnrDwd {
//    public static void main(String[] args) throws Exception {
//        if (args.length < 1) {
//            /*加日志：etl_date参数为空*/
//            System.exit(0);
//        }
//        String etlDate = args[0];
//        String startDate = etlDate;
//        String endDate = etlDate;
//        if (args.length > 2) {
//            startDate = args[1];
//            endDate = args[2];
//        }
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        CheckpointUtils.setCheckpoint(env, "BusOrderToDwd");
//        env.setParallelism(1);
//        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        // 1. 创建Doris目标表
//        tEnv.executeSql(KfbpCreateToSql.BUS_ORDER);
//        // 2. 从Doris表中读取数据
//        String query = "SELECT \n" +
//                "ETL_DATE,\n" +
//                "PNR,\n" +
//                "ORDER_SN,\n" +
//                "ORDER_TYPE,\n" +
//                "CONTACT_MOBILE,\n" +
//                "CONTACT_NAME,\n" +
//                "CONTACT_PHONE,\n" +
//                "CUR_TEL,\n" +
//                "CREATE_DATE,\n" +
//                "UPDATE_DATE " +
//                "FROM T_ODS_KFBP_BUS_ORDER " +
//                "WHERE ORDER_TYPE = 2 " +
//                "AND ETL_DATE >='" + startDate + "'" + " AND ETL_DATE <='" + endDate + "'";
//        System.out.println("日期为：" + etlDate + "的KFBP数据开始查询...");
//        Table table = tEnv.sqlQuery(query);
//        DataStream<JSONObject> busRefundSource = tEnv.toChangelogStream(table).map(new MapFunction<Row, JSONObject>() {
//            @Override
//            public JSONObject map(Row row) throws Exception {
//                JSONObject map = new JSONObject();
//                map.put("CREATE_DATE", row.getFieldAs("CREATE_DATE"));
//                map.put("UPDATE_DATE", row.getFieldAs("UPDATE_DATE"));
//                map.put("PNR", row.getFieldAs("PNR"));
//                map.put("ORDER_SN", row.getFieldAs("ORDER_SN"));
//                map.put("ORDER_TYPE", row.getFieldAs("ORDER_TYPE"));
//                map.put("CONTACT_PHONE", row.getFieldAs("CONTACT_PHONE"));
//                String contactMobile = row.getFieldAs("CONTACT_MOBILE");
//                contactMobile = NormalizationUtils.standardize(FieldType.MOBILE_NO, contactMobile, DataSource.CALL_CENTER_TICKETING_SYSTEM);
//                map.put("CONTACT_MOBILE", contactMobile);
//                String contactName = row.getFieldAs("CONTACT_NAME");
//                contactName = NormalizationUtils.standardize(FieldType.CN_NAME, contactName, DataSource.CALL_CENTER_TICKETING_SYSTEM);
//                map.put("CONTACT_NAME", contactName);
//                String curTel = row.getFieldAs("CUR_TEL");
//                map.put("CUR_TEL", curTel);
//                Tid tid = new Tid();
//                tid.setTid(curTel);
//                tid.setMobilePhone(curTel);
//                String tidStr = IdMapping.idMappingFunction(tid, "KFBP");
//                map.put("TID", tidStr);
//
//                return map;
//            }
//        }).filter(Objects::nonNull);
//
//        // 写入机票-预订-PNR级事实表
//        KfbpBusOrderToDwdBookingPnrFactTrans.result(busRefundSource);
//
//        //启动任务
//        env.execute("BusOrderToBookingPnrDwd");
//    }
//}
