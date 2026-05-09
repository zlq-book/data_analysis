//package com.travelsky.dataplatform.main.dwd.v2.kfbp;
//
//import com.alibaba.fastjson.JSONObject;
//import com.travelsky.dataplatform.constans.KfbpCreateToSql;
//import com.travelsky.dataplatform.utils.*;
//import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
//import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.DatechangeSegFactModel;
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
//import java.util.Objects;
//
///**
// * 呼叫白屏票务系统-BUS_FARE_TICKET（旅客票号表）
// */
//public class BusFareTicketToDateChangeDwd {
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
//        CheckpointUtils.setCheckpoint(env, "BusFareTicketToDwd");
//        env.setParallelism(1);
//        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//
//        // 1. 创建Doris目标表
//        tEnv.executeSql(KfbpCreateToSql.BUS_ORDER);
//        tEnv.executeSql(KfbpCreateToSql.BUS_FLIGHT);
//        tEnv.executeSql(KfbpCreateToSql.BUS_FARE_TICKET);
//
//        // 2. 从Doris表中读取数据
//        String query = "SELECT \n" +
//                " BO.ORDER_SN,\n" +
//                " BO.CONTACT_MOBILE,\n" +
//                " BO.CONTACT_NAME,\n" +
//                " BO.CONTACT_PHONE,\n" +
//                " BO.CUR_TEL,\n" +
//                " BO.REISSUE_TYPE,\n" +
//                " BFT.FLT_START_CITY, " +
//                " BFT.FLT_DEPARTURE_DAY, " +
//                " TK.TICKET_NO, " +
//                " TK.UPDATE_DATE " +
//                " FROM T_ODS_KFBP_BUS_FARE_TICKET TK " +
//                " LEFT JOIN T_ODS_KFBP_BUS_ORDER BO ON TK.ORDER_PKID = BO.PKID " +
//                " LEFT JOIN T_ODS_KFBP_BUS_FLIGHT BFT ON TK.ORDER_PKID = BFT.ORDER_PKID " +
//                " WHERE BO.TICKET_TYPE IN (2, 3, 5, 6) " +
//                " AND TK.ETL_DATE >= '" + startDate + "'" +
//                " AND TK.ETL_DATE <= '" + endDate + "'";
//
//        System.out.println("日期为：" + etlDate + "的改期数据开始查询...");
//        Table table = tEnv.sqlQuery(query);
//
//        DataStream<JSONObject> dateChangeSource = tEnv.toChangelogStream(table).map(new MapFunction<Row, JSONObject>() {
//            @Override
//            public JSONObject map(Row row) throws Exception {
//                JSONObject map = new JSONObject();
//                map.put("UPDATE_DATE", row.getFieldAs("UPDATE_DATE"));
//
//                map.put("ORDER_SN", row.getFieldAs("ORDER_SN"));
//                map.put("TICKET_NO", row.getFieldAs("TICKET_NO"));
//                map.put("CONTACT_PHONE", row.getFieldAs("CONTACT_PHONE"));
//                String contactMobile = row.getFieldAs("CONTACT_MOBILE");
//                contactMobile = NormalizationUtils.standardize(FieldType.MOBILE_NO, contactMobile, DataSource.CALL_CENTER_TICKETING_SYSTEM);
//                map.put("CONTACT_MOBILE", contactMobile);
//                String contactName = row.getFieldAs("CONTACT_NAME");
//                contactName = NormalizationUtils.standardize(FieldType.CN_NAME, contactName, DataSource.CALL_CENTER_TICKETING_SYSTEM);
//                map.put("CONTACT_NAME", contactName);
//                String reissueType = row.getFieldAs("REISSUE_TYPE");
//                reissueType = NormalizationUtils.standardize(FieldType.CHANGE_TYPE, reissueType, DataSource.CALL_CENTER_TICKETING_SYSTEM);
//                map.put("REISSUE_TYPE", reissueType);
//                map.put("FLT_START_CITY", row.getFieldAs("FLT_START_CITY"));
//                map.put("FLT_DEPARTURE_DAY", row.getFieldAs("FLT_DEPARTURE_DAY"));
//
//                String curTel = row.getFieldAs("CUR_TEL");
//                map.put("CUR_TEL", curTel);
//                Tid tid = new Tid();
//                tid.setTid(curTel);
//                String tidStr = IdMapping.idMappingFunction(tid, "KFBP");
//                map.put("TID", tidStr);
//
//                return map;
//            }
//        }).filter(Objects::nonNull);
//        // 机票-改期-航段级事实表
//        sinkToDateChangeFact(dateChangeSource);
//
//        //启动任务
//        env.execute("BusFareTicketToDwd");
//    }
//
//    private static void sinkToDateChangeFact(DataStream<JSONObject> dateChangeSource) {
//        SingleOutputStreamOperator<DatechangeSegFactModel> dateChangeSegFactStream = dateChangeSource
//                .map(map -> {
//                    DatechangeSegFactModel model = new DatechangeSegFactModel();
//
//                    // 主键ID：票号(删除横杠) + 起飞机场 + 起飞日期 + 证件号 + 旅客姓名
//                    String ticketNo = StringUtils.isNotBlank(map.getString("TICKET_NO")) ?
//                            map.getString("TICKET_NO").replace("-", "") : "";
//                    String fltStartCity = map.getString("FLT_START_CITY");
//                    String fltDepartureDay = StringUtils.isNotBlank(map.getString("FLT_DEPARTURE_DAY")) ?
//                            map.getString("FLT_DEPARTURE_DAY").replace("-", "") : "";
//                    String pkId = ticketNo + fltStartCity + fltDepartureDay;
//                    model.setPkId(pkId);
//
//                    // 渠道改期订单号
//                    model.setChangeOrderno(map.getString("ORDER_SN"));
//
//                    // 改升预订渠道 - 写死：SCWHP
//                    model.setAkChannel("SCWHP");
//
//                    // 联系人手机号
//                    model.setContactMobileNumber(map.getString("CONTACT_MOBILE"));
//
//                    // 联系人姓名
//                    model.setContactName(map.getString("CONTACT_NAME"));
//
//                    // 联系人固定电话
//                    model.setContactLandlineMobileNumber(map.getString("CONTACT_PHONE"));
//
//                    // 预订人TID
//                    model.setFkBookingUserTid(map.getString("TID"));
//
//                    // 预订人源ID
//                    model.setFkBookingUserOriginId(map.getString("CUR_TEL"));
//
//                    // 渠道记录的改期类型
//                    model.setChangeType(map.getString("REISSUE_TYPE"));
//
//                    // 源系统最后更新时间 - 使用BUS_FARE_TICKET表的update_date
//                    model.setSourceLastUpdatetime(map.getString("UPDATE_DATE"));
//
//                    LocalDateTime now = LocalDateTime.now();
//                    model.setSystemCreatetime(now.toString());
//                    model.setSystemLastUpdatetime(now.toString());
//
//                    return model;
//                })
//                .filter(Objects::nonNull);
//
//        // 写入Doris
//        DorisSink<DatechangeSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_DATECHANGE_SEG_FACT");
//        dateChangeSegFactStream.sinkTo(dorisSink);
//    }
//
//}
