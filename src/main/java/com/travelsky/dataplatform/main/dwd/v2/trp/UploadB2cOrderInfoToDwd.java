//package com.travelsky.dataplatform.main.dwd.v2.trp;
//
//import com.alibaba.fastjson.JSONObject;
//import com.travelsky.dataplatform.constans.TrpCreateToSql;
//import com.travelsky.dataplatform.utils.*;
//import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
//import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.BookingPnrFactModel;
//import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.BookingSegFactModel;
//import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingSegFactModel;
//import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingTicFactModel;
//import com.travelsky.trp.usercenter.data.analysis.transform.Kfbp.KfbpBusOrderToDwdBookingSegFactTrans;
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
//import org.apache.flink.util.Collector;
//
//import java.math.BigDecimal;
//import java.time.LocalDateTime;
//import java.time.format.DateTimeFormatter;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Objects;
//
///**
// * TRP（数仓表）
// * *TRP的订单：1、排除渠道=‘TSDF、TSIF、SD、全渠道'这四个渠道的，不入事实表，
// * * 2、使用'PNR'+ORDER_DATE生成主键，其中'ORDER_DATE'原数据是北京时间，
// * * 获取日期，比如2025-08-10，因'ORDER_DATE'与实际PNR生成日期可能有偏差，
// * * 为避免跨天问题，每天23：00：00-00：00：00的订单需要T，T+1（订单先生成，PNR后生成的情况），
// * * 00：00：00-00：01：00的订单需要T-1，T（PNR生成，订单后生成的情况），
// * * 需要先用2条查询，如果找到，就更新那一条（另一条丢弃），
// * * 如果找不到，要把两个主键都写进入。如果不在这个时间范围的数据，
// * * 直接按照T生成主键并写入，无需查询。PS：如果渠道数据先写了2条，后续高频来了，会更新其中一条。
// */
//public class UploadB2cOrderInfoToDwd {
//
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
//        CheckpointUtils.setCheckpoint(env, "UploadB2cOrderInfoToDwd");
//        env.setParallelism(1);
//        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        // 1. 创建Doris目标表
//        tEnv.executeSql(TrpCreateToSql.UPLOAD_B2C_ORDERINFO);
//        // 2. 从Doris表中读取数据
//        String query = "SELECT \n" +
//                " PNR,\n" +
//                " TICKET_NO,\n" + /*票号*/
//                " TK_TIME,\n" +  /*出票日期*/
//                " ORDER_DATE,\n" +
//                " ORDER_NO,\n" +
//                " ORDER_CHANNEL,\n" +
//                " CONTACT_NAME,\n" +
//                " ORDER_USER,\n" +
//                " FLIGHT_SEGMENT_CODE,\n" +
//                " TAKEOFF_TIME,\n" +
//                " COUPON_NUM,\n" +
//                " COUPON_NO,\n" +
//                " COUPON_FARE,\n" +
//                " COUPON_NAME,\n" +
//                " PAY_TIME " +
//                " FROM T_ODS_SC_UPLOAD_B2C_ORDERINFO " +
//                " WHERE ORDER_CHANNEL not in ('TSDF','TSIF','SD','全渠道') " +
//                " AND ETL_DATE >='" + startDate + "'" + " AND ETL_DATE <='" + endDate + "'";
//        System.out.println("日期为：" + etlDate + "的DPI数据开始查询...");
//        Table table = tEnv.sqlQuery(query);
//        DataStream<JSONObject> uploadB2cOrderInfoStream = tEnv.toChangelogStream(table).map(new MapFunction<Row, JSONObject>() {
//            @Override
//            public JSONObject map(Row row) throws Exception {
//                JSONObject map = new JSONObject();
//                map.put("PNR", row.getFieldAs("PNR"));
//                map.put("ORDER_DATE", row.getFieldAs("ORDER_DATE"));
//                map.put("ORDER_NO", row.getFieldAs("ORDER_NO"));
//                String orderChannel = row.getFieldAs("ORDER_CHANNEL");
//                orderChannel = NormalizationUtils.standardize(FieldType.ORDER_CHANNEL_AIRTICKET, orderChannel, DataSource.TRP_SC);
//                map.put("ORDER_CHANNEL", orderChannel);
//                String contactName = row.getFieldAs("CONTACT_NAME");
//                contactName = NormalizationUtils.standardize(FieldType.CN_NAME, contactName, DataSource.TRP_SC);
//                map.put("CONTACT_NAME", contactName);
//                String orderUser = row.getFieldAs("ORDER_USER");
//                map.put("ORDER_USER", orderUser);
//                map.put("PAY_TIME", row.getFieldAs("PAY_TIME"));
//                map.put("FLIGHT_SEGMENT_CODE", row.getFieldAs("FLIGHT_SEGMENT_CODE"));
//                map.put("TAKEOFF_TIME", row.getFieldAs("TAKEOFF_TIME"));
//                map.put("COUPON_NUM", row.getFieldAs("COUPON_NUM"));
//
//                Tid tid = new Tid();
//                tid.setTid(orderUser);
//                String tidStr = IdMapping.idMappingFunction(tid, "TRP");
//                map.put("TID", tidStr);
//
//                map.put("TICKET_NO", row.getFieldAs("TICKET_NO"));
//                map.put("TK_TIME", row.getFieldAs("TK_TIME"));
//                map.put("COUPON_NO", row.getFieldAs("COUPON_NO"));
//                map.put("COUPON_FARE", row.getFieldAs("COUPON_FARE"));
//                map.put("COUPON_NAME", row.getFieldAs("COUPON_NAME"));
//
//                return map;
//            }
//        });
//        // 写入机票-预订-PNR级事实表
//        sinkToBookingPnrFact(uploadB2cOrderInfoStream);
//        // 写入机票-出票-客票级事实表
//        sinkToTickingTicFact(uploadB2cOrderInfoStream);
//        // 写入机票-出票-航段级事实表
//        sinkToTickingSegFact(uploadB2cOrderInfoStream);
//
//
//        //启动任务
//        env.execute("UploadB2cOrderInfoToDwd");
//    }
//
//    private static void sinkToTickingSegFact(DataStream<JSONObject> uploadB2cOrderInfoStream) {
//        SingleOutputStreamOperator<TickingSegFactModel> tickingSegFactStream = uploadB2cOrderInfoStream
//                .filter(map -> StringUtils.isNotBlank(map.getString("TICKET_NO")))
//                .map(map -> {
//                    TickingSegFactModel model = new TickingSegFactModel();
//
//                    String ticketNumber = map.getString("TICKET_NO");
//                    ticketNumber = NormalizationUtils.standardize(FieldType.TICKET_NO, ticketNumber, DataSource.TRP_SC);
//                    String takeoffTime = map.getString("TAKEOFF_TIME").replace("-", "").substring(0, 8);
//                    String flightSegmentCode = map.getString("FLIGHT_SEGMENT_CODE");
//                    // TICKET_NO（删除横杠）+’FLIGHT_SEGMENT_CODE’（截取前三位）+’TAKEOFF_TIME’（截取YYYYMMDD）
//                    model.setPkId(ticketNumber + flightSegmentCode.substring(0, 3) + takeoffTime);
//
//                    //渠道订单号
//                    model.setChannelOrderno(map.getString("ORDER_NO"));
//                    // 预订渠道
//                    model.setAkChannel(map.getString("ORDER_CHANNEL"));
//                    // 预订人TID
//                    model.setFkBookingUserTid(map.getString("TID"));
//                    // 预订人源ID
//                    model.setFkBookingUserOriginId(map.getString("ORDER_USER"));
//                    // 是否使用优惠券
//                    model.setAkCoupon(StringUtils.isNotBlank(map.getString("COUPON_NUM")));
//                    //优惠券产品编码
//                    model.setAkCouponcode(map.getString("COUPON_NO"));
//                    // 优惠券优惠金额
//                    BigDecimal couponFare = map.getBigDecimal("COUPON_FARE");
//                    model.setCouponAmount(couponFare);
//                    // 优惠券张数
//                    model.setCouponCount(1);
//                    // 优惠券券码
//                    model.setCouponno(map.getString("COUPON_NUM"));
//                    // 优惠券名称
//                    model.setCouponName(map.getString("COUPON_NAME"));
//                    // 联系人姓名
//                    model.setContactName(map.getString("CONTACT_NAME"));
//
//                    // 源系统最后更新时间
//                    model.setSourceLastUpdatetime(map.getString("PAY_TIME"));
//
//                    // 系统信息
//                    LocalDateTime now = LocalDateTime.now();
//                    model.setSystemCreatetime(now.toString());
//                    model.setSystemLastUpdatetime(now.toString());
//
//                    return model;
//                }).returns(TickingSegFactModel.class);
//        // 写入Doris
//        DorisSink<TickingSegFactModel> bookingSegFactSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_SEG_FACT");
//        tickingSegFactStream.sinkTo(bookingSegFactSink);
//    }
//
//    private static void sinkToTickingTicFact(DataStream<JSONObject> uploadB2cOrderInfoStream) {
//        SingleOutputStreamOperator<TickingTicFactModel> tickingTicFactStream = uploadB2cOrderInfoStream
//                .filter(map -> StringUtils.isNotBlank(map.getString("TICKET_NO")))
//                .map(map -> {
//                    TickingTicFactModel model = new TickingTicFactModel();
//
//                    String ticketNumber = map.getString("TICKET_NO");
//                    ticketNumber = NormalizationUtils.standardize(FieldType.TICKET_NO, ticketNumber, DataSource.TRP_SC);
//                    String tkTime = map.getString("TK_TIME").replace("-", "").substring(0, 8);
//                    // TK_TIME（截取YYYYMMDD）+TICKET_NO（删除横杠）
//                    model.setPkId(tkTime + ticketNumber);
//
//                    //渠道订单号
//                    model.setAkOrdernum(map.getString("ORDER_NO"));
//                    // 预订渠道
//                    model.setAkChannel(map.getString("ORDER_CHANNEL"));
//                    // 预订人TID
//                    model.setFkBookingUserTid(map.getString("TID"));
//                    // 预订人源ID
//                    model.setFkBookingUserOriginId(map.getString("ORDER_USER"));
//
//                    // 联系人姓名
//                    model.setContactName(map.getString("CONTACT_NAME"));
//
//                    // 源系统最后更新时间
//                    model.setSourceLastUpdatetime(map.getString("PAY_TIME"));
//
//                    // 系统信息
//                    LocalDateTime now = LocalDateTime.now();
//                    model.setSystemCreatetime(now.toString());
//                    model.setSystemLastUpdatetime(now.toString());
//
//                    return model;
//                }).returns(TickingTicFactModel.class);
//        // 写入Doris
//        DorisSink<TickingTicFactModel> bookingSegFactSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_TIC_FACT");
//        tickingTicFactStream.sinkTo(bookingSegFactSink);
//    }
//
//    private static void sinkToBookingPnrFact(DataStream<JSONObject> uploadB2cOrderInfoStream) {
//        SingleOutputStreamOperator<BookingPnrFactModel> bookingPnrFactStream = uploadB2cOrderInfoStream
//                .flatMap(new FlatMapFunction<JSONObject, BookingPnrFactModel>() {
//                    @Override
//                    public void flatMap(JSONObject map, Collector<BookingPnrFactModel> out) throws Exception {
//                        String orderDateStr = map.getString("ORDER_DATE");
//                        // 获取创建日期
//                        LocalDateTime orderDate = null;
//                        if (StringUtils.isNotBlank(orderDateStr)) {
//                            // String转LocalDateTime
//                            orderDate = LocalDateTime.parse(orderDateStr);
//                        }
//                        // 获取pnr
//                        String pnr = map.getString("PNR");
//                        // 判断是否需要生成两个主键（23点的数据）
//                        // 生成主键列表
//                        List<String> pkIds = new ArrayList<>();
//                        if (orderDate != null) {
//                            int hour = orderDate.getHour();
//                            if (hour == 23) {
//                                // T 日期
//                                String dateT = orderDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
//                                pkIds.add(pnr + dateT);
//                                // 如果是23点，添加 T+1 日期
//                                String dateTPlus1 = orderDate.plusDays(1)
//                                        .format(DateTimeFormatter.ofPattern("yyyyMMdd"));
//                                pkIds.add(pnr + dateTPlus1);
//                            }
//                        }
//                        // 判断是否需要生成两个主键（00：00：00-00：01：00）
//
//                        if (orderDate != null) {
//                            int hour = orderDate.getHour();
//                            int minute = orderDate.getMinute();
//                            if (hour == 00 && minute < 01) {
//                                // T 日期
//                                String dateT = orderDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
//                                pkIds.add(pnr + dateT);
//                                // 如果是00：00：00-00：01：00，添加 T-1 日期
//                                String dateTPlus1 = orderDate.minusDays(1)
//                                        .format(DateTimeFormatter.ofPattern("yyyyMMdd"));
//                                pkIds.add(pnr + dateTPlus1);
//                            }
//                        }
//                        // 为每个主键生成一条记录
//                        for (String pkId : pkIds) {
//                            BookingPnrFactModel model = new BookingPnrFactModel();
//                            // 组装主键
//                            model.setPkId(pkId);
//                            //TRP（数仓表）	T_ODS_UPLOAD_B2C_ORDERINFO	ORDER_NO
//                            model.setChannelOrderId(map.getString("ORDER_NO"));
//                            model.setAkChannel(map.getString("ORDER_CHANNEL"));
//                            model.setContactName(map.getString("CONTACT_NAME"));
//                            //预订人TID
//                            model.setFkBookingUserTid(map.getString("TID"));
//                            //预订人源ID
//                            model.setFkBookingUserOriginId(map.getString("ORDER_USER"));
//                            //源系统最后更新时间
//                            model.setSourceLastUpdatetime(map.getString("PAY_TIME"));
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
//        DorisSink<BookingPnrFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_BOOKING_PNR_FACT");
//        bookingPnrFactStream.sinkTo(dorisSink);
//    }
//}
