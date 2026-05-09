//package com.travelsky.dataplatform.main.dwd.v2.clk;
//
//import com.alibaba.fastjson.JSONObject;
//import com.travelsky.dataplatform.constans.ClkCreateToSql;
//import com.travelsky.dataplatform.constans.TrpCreateToSql;
//import com.travelsky.dataplatform.constans.trp.utils.DateUtil;
//import com.travelsky.dataplatform.utils.*;
//import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
//import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.ActSignupFactModal;
//import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.ActSignupFactModal;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.doris.flink.sink.DorisSink;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//
//import java.time.LocalDateTime;
//import java.time.format.DateTimeFormatter;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Objects;
//
///**
// * *常旅客FTP--FFP_DBRQ_YYYYMMDD（YYYYMMDD为日期）-sheet2（达标人群(无需报名)数据表）
// *  T_DWD_ACT_SIGNUP_FACT
// */
//public class QualifiedNoRegistrationToActSignupDwd {
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
//        CheckpointUtils.setCheckpoint(env, "QualifiedNoRegistrationToActSignupDwd");
//        env.setParallelism(1);
//        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        // 1. 创建Doris目标表
//        tEnv.executeSql(ClkCreateToSql.QUALIFIED_NO_REGISTRATION);
//        // 2. 从Doris表中读取数据
//        String query = "SELECT \n" +
//                // 活动code
//                " ACTIVITY_CODE,\n" +
//                // 常客卡号
//                " MEMBER_CARD_NUMBER,\n" +
//                // 活动中文名称
//                " ACTIVITY_CHINESE_NAME\n" +
//                " FROM T_ODS_CLK_QUALIFIED_NO_REGISTRATION" +
//                " WHERE ETL_DATE >='" + startDate + "'" + " AND ETL_DATE <='" + endDate + "'"
//                ;
//        System.out.println("日期为：" + etlDate + "的DPI数据开始查询...");
//        Table table = tEnv.sqlQuery(query);
//        DataStream<JSONObject> qualifiedNoRegistrationStream = tEnv.toChangelogStream(table).map(new MapFunction<Row, JSONObject>() {
//            @Override
//            public JSONObject map(Row row) throws Exception {
//                JSONObject map = new JSONObject();
//                map.put("ACTIVITY_CODE", row.getFieldAs("ACTIVITY_CODE"));
//                String memberCardNumber = row.getFieldAs("MEMBER_CARD_NUMBER");
//                memberCardNumber = NormalizationUtils.standardize(FieldType.FREQUENT_FLYER_NUM, memberCardNumber, DataSource.FREQUENT_FLYER_FTP);
//                map.put("MEMBER_CARD_NUMBER", memberCardNumber);
//                map.put("ACTIVITY_CHINESE_NAME", row.getFieldAs("ACTIVITY_CHINESE_NAME"));
//                Tid tid = new Tid();
//                tid.setTid(memberCardNumber);
//                tid.setFrequentTravelerCardno(memberCardNumber);
//                String tidStr = IdMapping.idMappingFunction(tid, "CLK",false,true,true);
//                map.put("TID", tidStr);
//
//                return map;
//            }
//        }).filter(Objects::nonNull);
//        SingleOutputStreamOperator<ActSignupFactModal> actSignupFactStream = qualifiedNoRegistrationStream
//                .map(map -> {
//                    ActSignupFactModal model = new ActSignupFactModal();
//                    // 活动code+常客卡号
//                    model.setPkId(map.getString("ACTIVITY_CODE") + map.getString("MEMBER_CARD_NUMBER"));
//                    // 活动名称ID
//                    model.setProductId(map.getString("ACTIVITY_CODE"));
//                    // 活动中文名称
//                    model.setProductCnName(map.getString("ACTIVITY_CHINESE_NAME"));
//                    // 会员卡号
//                    model.setMemberCard(map.getString("MEMBER_CARD_NUMBER"));
//                    // 是否达标
//                    model.setIsQualified("1");
//                    // 报名人TID
//                    model.setMemberTid(map.getString("TID"));
//
//                    // 报名类型
//                    model.setSignupType("FM");
//
//                    // 系统信息
//                    LocalDateTime now = LocalDateTime.now();
//                    model.setSystemCreatetime(now.toString());
//                    model.setSystemLastUpdatetime(now.toString());
//
//                    return model;
//                }).setParallelism(4);
//
//        // 5. 创建Doris Sink并写入数据
//        DorisSink<ActSignupFactModal> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_ACT_SIGNUP_FACT");
//
//        actSignupFactStream.sinkTo(dorisSink);
//
//        //启动任务
//        env.execute("QualifiedNoRegistrationToActSignupDwd");
//    }
//}
