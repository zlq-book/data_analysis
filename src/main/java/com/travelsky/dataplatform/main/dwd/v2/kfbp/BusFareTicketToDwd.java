//package com.travelsky.dataplatform.main.dwd.v2.kfbp;
//
//import com.alibaba.fastjson.JSONObject;
//import com.travelsky.dataplatform.constans.KfbpCreateToSql;
//import com.travelsky.dataplatform.utils.*;
//import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
//import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingSegFactModel;
//import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingTicFactModel;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.doris.flink.sink.DorisSink;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//import org.apache.flink.types.StringValue;
//import org.apache.flink.util.Collector;
//
//import java.time.LocalDateTime;
//import java.time.format.DateTimeFormatter;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Objects;
//import java.util.Optional;
//
//public class BusFareTicketToDwd {
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
//                " TK.TICKET_NO, " +
//                " TK.CREATE_DATE, " +
//                " TK.UPDATE_DATE, " +
//                " BFT.FLT_START_CITY, " +
//                " BFT.FLT_DEPARTURE_DAY " +
//                " FROM T_ODS_KFBP_BUS_FARE_TICKET TK " +
//                " LEFT JOIN T_ODS_KFBP_BUS_ORDER BO ON TK.ORDER_PKID = BO.PKID " +
//                " LEFT JOIN T_ODS_KFBP_BUS_FLIGHT BFT ON TK.ORDER_PKID = BFT.ORDER_PKID " +
//                " WHERE BO.IS_FICTITIOUS_ORDER = 0 " +
//                " AND TK.ETL_DATE >= '" + startDate + "'" +
//                " AND TK.ETL_DATE <= '" + endDate + "'";
//
//        System.out.println("日期为：" + etlDate + "的改期数据开始查询...");
//        Table table = tEnv.sqlQuery(query);
//
//        DataStream<JSONObject> tickingSource = tEnv.toChangelogStream(table).map(new MapFunction<Row, JSONObject>() {
//            @Override
//            public JSONObject map(Row row) throws Exception {
//                JSONObject map = new JSONObject();
//                map.put("UPDATE_DATE", row.getFieldAs("UPDATE_DATE"));
//                map.put("CREATE_DATE", row.getFieldAs("CREATE_DATE"));
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
//                //String reissueType = Optional.ofNullable(row.getFieldAs("REISSUE_TYPE"))
//                //        .map(Object::toString)
//                //        .orElse("");
//                //reissueType = NormalizationUtils.standardize(FieldType.CHANGE_TYPE, reissueType, DataSource.CALL_CENTER_TICKETING_SYSTEM);
//                //map.put("REISSUE_TYPE", reissueType);
//                map.put("FLT_START_CITY", row.getFieldAs("FLT_START_CITY"));
//                map.put("FLT_DEPARTURE_DAY", row.getFieldAs("FLT_DEPARTURE_DAY"));
//
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
//        });
//        // 机票-出票-客票级事实表
//        sinkToTickingTicFact(tickingSource);
//
//        //启动任务
//        env.execute("BusFareTicketToDwd");
//    }
//
//    private static void sinkToTickingTicFact(DataStream<JSONObject> tickingSource) {
//        SingleOutputStreamOperator<TickingTicFactModel> bookingPnrFactStream = tickingSource
//
//                .flatMap(new FlatMapFunction<JSONObject, TickingTicFactModel>() {
//                    @Override
//                    public void flatMap(JSONObject map, Collector<TickingTicFactModel> out) throws Exception {
//                        String createDateStr = map.getString("CREATE_DATE");
//                        // 获取创建日期
//                        LocalDateTime createDate = null;
//                        if (map.getString("CREATE_DATE") != null) {
//                            // String转LocalDateTime
//                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
//                            createDate = LocalDateTime.parse(createDateStr, formatter);
//                        }
//                        // 获取pnr
//                        String ticketNo = StringUtils.isNotBlank(map.getString("TICKET_NO")) ?
//                                map.getString("TICKET_NO").replace("-", "") : "";
//                        // 判断是否需要生成两个主键（23点的数据）
//                        // 生成主键列表
//                        List<String> pkIds = new ArrayList<>();
//                        if (createDate != null) {
//                            int hour = createDate.getHour();
//                            if (hour == 23) {
//                                // T 日期
//                                String dateT = createDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
//                                pkIds.add(dateT + ticketNo);
//                                // 如果是23点，添加 T+1 日期
//                                String dateTPlus1 = createDate.plusDays(1)
//                                        .format(DateTimeFormatter.ofPattern("yyyyMMdd"));
//                                pkIds.add(dateTPlus1 + ticketNo);
//                            }
//                        }
//                        // 为每个主键生成一条记录
//                        for (String pkId : pkIds) {
//                            TickingTicFactModel model = new TickingTicFactModel();
//                            // 组装主键
//                            model.setPkId(pkId);
//                            model.setAkOrdernum(map.getString("ORDER_SN"));
//                            //预订渠道	呼叫白屏票务系统	BUS_ORDER（基本订单表）	写死：呼叫白屏票务系统-code:SCWHP
//                            model.setAkChannel("SCWHP");
//                            //预订人TID  呼叫白屏票务系统	BUS_ORDER（基本订单表）	根据预订人源ID找TID
//                            model.setFkBookingUserTid(map.getString("TID"));
//                            //预订人源ID 呼叫白屏票务系统	BUS_ORDER（基本订单表）	cur_tel
//                            model.setFkBookingUserOriginId(map.getString("CUR_TEL"));
//                            //联系人手机号 呼叫白屏票务系统	BUS_ORDER（基本订单表）	contact_mobile
//                            model.setContactMobileNumber(map.getString("CONTACT_MOBILE"));
//                            //联系人姓名 呼叫白屏票务系统	BUS_ORDER（基本订单表）	contact_name
//                            model.setContactName(map.getString("CONTACT_NAME"));
//                            //联系人固定电话 呼叫白屏票务系统	BUS_ORDER（基本订单表）	contact_phone
//                            model.setContactLandlineMobileNumber(map.getString("CONTACT_PHONE"));
//
//                            //源系统最后更新时间 呼叫白屏票务系统	BUS_ORDER（基本订单表）	update_date
//                            model.setSourceLastUpdatetime(map.getString("UPDATE_DATE"));
//
//                            LocalDateTime now = LocalDateTime.now();
//                            model.setSystemCreatetime(now.toString());
//                            model.setSystemLastUpdatetime(now.toString());
//                            // 输出记录
//                            out.collect(model);
//                        }
//                    }
//                });
//
//        // 写入Doris
//        DorisSink<TickingTicFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_TIC_FACT");
//        bookingPnrFactStream.sinkTo(dorisSink);
//    }
//
//}
