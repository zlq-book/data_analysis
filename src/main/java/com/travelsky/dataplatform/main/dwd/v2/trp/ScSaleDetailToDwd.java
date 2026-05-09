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
// * TRP的订单：1、排除渠道=‘TSDF、TSIF、SD、全渠道'这四个渠道的，不入事实表，
// * * 2、使用'PNR'+'订票日期'生成主键，其中'订票日期'原数据是北京时间，
// * * 获取日期，比如2025-08-10，因'订票日期'与实际PNR生成日期可能有偏差，
// * * 为避免跨天问题，每天23：00：00-00：00：00的订单需要T，T+1（订单先生成，PNR后生成的情况），
// * * 00：00：00-00：01：00的订单需要T-1，T（PNR生成，订单后生成的情况），
// * * 需要先用2条查询，如果找到，就更新那一条（另一条丢弃），如果找不到，
// * * 要把两个主键都写进入。如果不在这个时间范围的数据，直接按照T生成主键并写入，
// * * 无需查询。PS：如果渠道数据先写了2条，后续高频来了，会更新其中一条。
// */
//public class ScSaleDetailToDwd {
//
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
//        CheckpointUtils.setCheckpoint(env, "ScSaleDetailToDwd");
//        env.setParallelism(1);
//        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        // 1. 创建Doris目标表
//        tEnv.executeSql(TrpCreateToSql.SC_SALE_DETAIL);
//        // 2. 从Doris表中读取数据
//        String query = "SELECT \n" +
//                " PNR,\n" +
//                // 航段三字码
//                " SEGMENT_THREE_CODE,\n" +
//                // 起飞时间
//                " DEPARTURE_TIME,\n" +
//                // 证件号
//                " ID_NUMBER,\n" +
//                // 乘机人姓名
//                " PASSENGER_NAME,\n" +
//                // 订单号
//                " ORDER_NUMBER,\n" +
//
//                // 票号
//                " TICKET_NUMBER,\n" +
//                // 券码
//                " VOUCHER_CODE,\n" +
//                // 卡券code
//                " COUPON_CODE,\n" +
//                // 卡券金额
//                " COUPON_AMOUNT,\n" +
//                " COUPON_NAME,\n" +
//
//                " BOOKING_DATE,\n" +
//                " ORDER_CHANNEL,\n" +
//                " CONTACT_PHONE,\n" +
//                " CONTACT_NAME,\n" +
//                " BOOKING_USERNAME,\n" +
//                " PAYMENT_TIME " +
//                " FROM T_ODS_TRP_SC_SALE_DETAIL " +
//                " WHERE ORDER_CHANNEL not in ('TSDF','TSIF','SD','全渠道') " +
//                " AND ETL_DATE >='" + startDate + "'" + " AND ETL_DATE <='" + endDate + "'";
//        System.out.println("日期为：" + etlDate + "的DPI数据开始查询...");
//        Table table = tEnv.sqlQuery(query);
//        DataStream<JSONObject> saleDetailStream = tEnv.toChangelogStream(table).map(new MapFunction<Row, JSONObject>() {
//            @Override
//            public JSONObject map(Row row) throws Exception {
//                JSONObject map = new JSONObject();
//                map.put("PNR", row.getFieldAs("PNR"));
//                map.put("BOOKING_DATE", row.getFieldAs("BOOKING_DATE"));
//                map.put("ORDER_NUMBER", row.getFieldAs("ORDER_NUMBER"));
//                String orderChannel = row.getFieldAs("ORDER_CHANNEL");
//                orderChannel = NormalizationUtils.standardize(FieldType.ORDER_CHANNEL_AIRTICKET, orderChannel, DataSource.TRP_FTP);
//                map.put("ORDER_CHANNEL", orderChannel);
//                String contactPhone = row.getFieldAs("CONTACT_PHONE");
//                contactPhone = NormalizationUtils.standardize(FieldType.MOBILE_NO, contactPhone, DataSource.TRP_FTP);
//                map.put("CONTACT_PHONE", contactPhone);
//                String contactName = row.getFieldAs("CONTACT_NAME");
//                contactName = NormalizationUtils.standardize(FieldType.CN_NAME, contactName, DataSource.TRP_FTP);
//                map.put("CONTACT_NAME", contactName);
//                String ticketNumber = row.getFieldAs("TICKET_NUMBER");
//                ticketNumber = NormalizationUtils.standardize(FieldType.TICKET_NO, ticketNumber, DataSource.TRP_FTP);
//                map.put("TICKET_NUMBER", ticketNumber);
//
//
//                String bookingUsername = row.getFieldAs("BOOKING_USERNAME");
//                map.put("BOOKING_USERNAME", bookingUsername);
//                map.put("PAYMENT_TIME", row.getFieldAs("PAYMENT_TIME"));
//
//                Tid tid = new Tid();
//                tid.setTid(bookingUsername);
//                String tidStr = IdMapping.idMappingFunction(tid, "TRP");
//                map.put("TID", tidStr);
//
//                map.put("SEGMENT_THREE_CODE", row.getFieldAs("SEGMENT_THREE_CODE"));
//                map.put("DEPARTURE_TIME", row.getFieldAs("DEPARTURE_TIME"));
//                String idNumber = row.getFieldAs("ID_NUMBER");
//                idNumber = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, idNumber, DataSource.TRP_FTP);
//                map.put("ID_NUMBER", idNumber);
//                String passengerName = row.getFieldAs("PASSENGER_NAME");
//                passengerName = NormalizationUtils.standardize(FieldType.CN_NAME, passengerName, DataSource.TRP_FTP);
//                map.put("PASSENGER_NAME", passengerName);
//                map.put("VOUCHER_CODE", row.getFieldAs("VOUCHER_CODE"));
//                map.put("COUPON_CODE", row.getFieldAs("COUPON_CODE"));
//                map.put("COUPON_AMOUNT", row.getFieldAs("COUPON_AMOUNT"));
//                map.put("COUPON_NAME", row.getFieldAs("COUPON_NAME"));
//
//                return map;
//            }
//        });
//
//        // 写入机票-预订-PNR级事实表
//        sinkToBookingPnrFact(saleDetailStream);
//        // 写入机票-预订-航段级事实表
//        sinkToBookingSegFact(saleDetailStream);
//        // 写入机票-出票-客票级事实表
//        //sinkToTickingTicFact(saleDetailStream);
//        // 写入机票-出票-航段级事实表
//        //sinkToTickingSegFact(saleDetailStream);
//        //启动任务
//        env.execute("ScSaleDetailToDwd");
//    }
//
//    private static void sinkToTickingTicFact(DataStream<JSONObject> saleDetailStream) {
//        SingleOutputStreamOperator<TickingTicFactModel> tickingTicFactStream = saleDetailStream
//                .filter(map -> StringUtils.isNotBlank(map.getString("TICKET_NUMBER")))
//                .map(map -> {
//                    TickingTicFactModel model = new TickingTicFactModel();
//
//                    String ticketNumber = map.getString("TICKET_NUMBER");
//
//                    String bookingDate = map.getString("BOOKING_DATE").replace("/", "").substring(0, 8);
//                    // 使用：出票日期（截取YYYYMMDD）+票号（删除横杠）
//                    model.setPkId(bookingDate + ticketNumber);
//
//                    //渠道订单号
//                    model.setAkOrdernum(map.getString("ORDER_NUMBER"));
//                    // 预订渠道
//                    model.setAkChannel(map.getString("ORDER_CHANNEL"));
//                    // 预订人TID
//                    model.setFkBookingUserTid(map.getString("TID"));
//                    // 预订人源ID
//                    model.setFkBookingUserOriginId(map.getString("BOOKING_USERNAME"));
//
//                    // 联系人手机号
//                    model.setContactMobileNumber(map.getString("CONTACT_PHONE"));
//                    // 联系人姓名
//                    model.setContactName(map.getString("CONTACT_NAME"));
//
//                    // 源系统最后更新时间
//                    model.setSourceLastUpdatetime(map.getString("PAYMENT_TIME"));
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
//    private static void sinkToTickingSegFact(DataStream<JSONObject> saleDetailStream) {
//        SingleOutputStreamOperator<TickingSegFactModel> tickingSegFactStream = saleDetailStream
//                .filter(map -> StringUtils.isNotBlank(map.getString("TICKET_NUMBER")))
//                .map(map -> {
//                    TickingSegFactModel model = new TickingSegFactModel();
//
//                    String ticketNumber = map.getString("TICKET_NUMBER");
//                    // 截取前三位
//                    String segmentThreeCode = map.getString("SEGMENT_THREE_CODE").substring(0, 3);
//                    String departureTimeStr = map.getString("DEPARTURE_TIME");
//                    String departureDate = StringUtils.isNotBlank(departureTimeStr) ? departureTimeStr.replace("/", "").substring(0, 8) : "";
//
//                    // 使用：票号（删除横杠）+’航段三字码’前三位+‘起飞时间‘（截取YYYYMMDD）
//                    model.setPkId(ticketNumber + segmentThreeCode + departureDate);
//
//                    //渠道订单号
//                    model.setChannelOrderno(map.getString("ORDER_NUMBER"));
//                    // 预订渠道
//                    model.setAkChannel(map.getString("ORDER_CHANNEL"));
//                    // 预订人TID
//                    model.setFkBookingUserTid(map.getString("TID"));
//                    // 预订人源ID
//                    model.setFkBookingUserOriginId(map.getString("BOOKING_USERNAME"));
//                    // 是否使用优惠券
//                    model.setAkCoupon(StringUtils.isNotBlank(map.getString("VOUCHER_CODE")));
//                    //优惠券产品编码
//                    model.setAkCouponcode(map.getString("COUPON_CODE"));
//                    // 优惠券优惠金额
//                    String couponAmount = map.getString("COUPON_AMOUNT");
//                    BigDecimal coupon = StringUtils.isNotBlank(couponAmount) ? new BigDecimal(couponAmount) : null;
//                    model.setCouponAmount(coupon);
//                    // 优惠券张数
//                    model.setCouponCount(1);
//                    // 优惠券券码
//                    model.setCouponno(map.getString("VOUCHER_CODE"));
//                    // 优惠券名称
//                    model.setCouponName(map.getString("COUPON_NAME"));
//
//                    // 联系人手机号
//                    model.setContactMobileNumber(map.getString("CONTACT_PHONE"));
//                    // 联系人姓名
//                    model.setContactName(map.getString("CONTACT_NAME"));
//
//                    // 源系统最后更新时间
//                    model.setSourceLastUpdatetime(map.getString("PAYMENT_TIME"));
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
//    private static void sinkToBookingPnrFact(DataStream<JSONObject> saleDetailStream) {
//        SingleOutputStreamOperator<BookingPnrFactModel> bookingPnrFactStream = saleDetailStream
//
//                .flatMap(new FlatMapFunction<JSONObject, BookingPnrFactModel>() {
//                    @Override
//                    public void flatMap(JSONObject map, Collector<BookingPnrFactModel> out) throws Exception {
//                        String orderDateStr = map.getString("BOOKING_DATE");
//                        if (StringUtils.isBlank(orderDateStr)) {
//                            return;
//                        }
//                        // 获取创建日期
//                        LocalDateTime orderDate = null;
//                        if (StringUtils.isNotBlank(orderDateStr)) {
//                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
//                            orderDate = LocalDateTime.parse(orderDateStr.replace("/", "-"), formatter);
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
//                            //渠道订单号
//                            model.setChannelOrderId(map.getString("ORDER_NUMBER"));
//                            // 渠道
//                            model.setAkChannel(map.getString("ORDER_CHANNEL"));
//
//                            model.setContactName(map.getString("CONTACT_NAME"));
//
//                            model.setContactMobileNumber(map.getString("CONTACT_PHONE"));
//
//                            //预订人TID
//                            model.setFkBookingUserTid(map.getString("TID"));
//                            //预订人源ID
//                            model.setFkBookingUserOriginId(map.getString("BOOKING_USERNAME"));
//                            //源系统最后更新时间
//                            model.setSourceLastUpdatetime(map.getString("PAYMENT_TIME"));
//
//                            LocalDateTime now = LocalDateTime.now();
//                            model.setSystemCreatetime(now.toString());
//                            model.setSystemLastUpdatetime(now.toString());
//                            // 输出记录
//                            out.collect(model);
//                        }
//                    }
//                }).returns(BookingPnrFactModel.class);
//
//        // 写入Doris
//        DorisSink<BookingPnrFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_BOOKING_PNR_FACT");
//        bookingPnrFactStream.sinkTo(dorisSink);
//    }
//
//    private static void sinkToBookingSegFact(DataStream<JSONObject> saleDetailStream) {
//        SingleOutputStreamOperator<BookingSegFactModel> bookingSegFactStream = saleDetailStream
//                .map(map -> {
//                    BookingSegFactModel model = new BookingSegFactModel();
//
//                    String pnr = map.getString("PNR");
//                    String segmentThreeCode = map.getString("SEGMENT_THREE_CODE").substring(0, 3);
//                    String departureTimeStr = map.getString("DEPARTURE_TIME");
//                    String departureDateTime = departureTimeStr.replace("-", "")
//                            .replace("/", "").replace("T", "").replace(" ", "").replace(":", "") + "00";
//                    String idNumber = map.getString("ID_NUMBER");
//                    String passengerName = map.getString("PASSENGER_NAME");
//
//
//                    // 使用：PNR编号+起飞机场('航段三字码'截取前三位)+起飞日期（YYYYMMDD，'起飞时间'截取）
//                    // +起飞时间（hhmmss'起飞时间'截取)+乘机人证件号（加密）+乘机人姓名(姓名不带斜杠不带空格)
//                    model.setPkId(pnr + segmentThreeCode + departureDateTime + idNumber + passengerName);
//
//                    //渠道订单号
//                    model.setChannelOrderId(map.getString("ORDER_NUMBER"));
//                    // 预订渠道
//                    model.setAkChannel(map.getString("ORDER_CHANNEL"));
//                    // 联系人手机号
//                    model.setContactMobileNumber(map.getString("CONTACT_PHONE"));
//                    // 联系人姓名
//                    model.setContactName(map.getString("CONTACT_NAME"));
//                    // 预订人TID
//                    model.setFkBookingUserTid(map.getString("TID"));
//                    // 预订人源ID
//                    model.setFkBookingUserOriginId(map.getString("BOOKING_USERNAME"));
//                    // 是否为返乡段 "以下情况为是，否则为否
//                    //1、如果乘机人证件类型为=身份证，并且 乘机人证件号码对应的省份（通过《身份证号-省份+地级市》）与到达机场对应的省份（航程维表：省）相同。
//                    //2、并且起飞机场（机场维表：省）和到达机场（机场维表：省）不在一个省份。"
//                    //model.setHomecomingSeg();
//
//                    // 源系统最后更新时间
//                    model.setSourceLastUpdatetime(map.getString("PAYMENT_TIME"));
//
//                    // 系统信息
//                    LocalDateTime now = LocalDateTime.now();
//                    model.setSystemCreatetime(now.toString());
//                    model.setSystemLastUpdatetime(now.toString());
//
//                    return model;
//                }).returns(BookingSegFactModel.class);
//        // 写入Doris
//        DorisSink<BookingSegFactModel> bookingSegFactSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_BOOKING_SEG_FACT");
//        bookingSegFactStream.sinkTo(bookingSegFactSink);
//    }
//}
