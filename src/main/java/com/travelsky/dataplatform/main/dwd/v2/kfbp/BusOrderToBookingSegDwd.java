//package com.travelsky.dataplatform.main.dwd.v2.kfbp;
//
//
//import com.alibaba.fastjson.JSONObject;
//import com.travelsky.dataplatform.constans.KfbpCreateToSql;
//import com.travelsky.dataplatform.utils.*;
//import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
//import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.BookingSegFactModel;
//import com.travelsky.trp.usercenter.data.analysis.transform.Kfbp.KfbpBusOrderToDwdBookingPnrFactTrans;
//import com.travelsky.trp.usercenter.data.analysis.transform.Kfbp.KfbpBusOrderToDwdBookingSegFactTrans;
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
// * 白屏的数据T+1入库后，找BUS_ORDER（基本订单表）的update_date是T时间的，
// * * 同时order_type=2（机票）的，连表查询：
// * * BUS_ORDER BO LEFT JOIN BUS_FARE BF ON BO.pkid=BF.order_pkid
// * * LEFT JOIN BUS_FLIGHT BFT ON BF.order_pkid=BFT.ORDER_PKID，
// * * （具体连表SQL需要开发再确认，避免出现连表多对多的情况发生）。
// * * 取BUS_ORDER表的pnr+BUS_FLIGHT的FLT_START_CITY+FLT_DEPARTURE_DAY（截取日期）
// * * +FLT_DEPARTURE_DAY（截取时间）+BUS_FARE的id_code（加密）+BUS_FARE的fare_name（删除/)，
// * * 查找到这一条记录，更新此字段及下面几个渠道字段。
// */
//public class BusOrderToBookingSegDwd {
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
//        CheckpointUtils.setCheckpoint(env, "BusOrderToBookingSegDwd");
//        env.setParallelism(1);
//        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        // 1. 创建Doris目标表
//        tEnv.executeSql(KfbpCreateToSql.BUS_ORDER);
//        tEnv.executeSql(KfbpCreateToSql.BUS_FLIGHT);
//        tEnv.executeSql(KfbpCreateToSql.BUS_FARE);
//        // 2. 从Doris表中读取数据
//        String query = "SELECT \n" +
//                " BO.PNR,\n" +
//                " BO.ORDER_SN,\n" +
//                " BO.ORDER_TYPE,\n" +
//                " BO.CONTACT_MOBILE,\n" +
//                " BO.CONTACT_NAME,\n" +
//                " BO.CONTACT_PHONE,\n" +
//                " BO.CUR_TEL,\n" +
//                " BO.CREATE_DATE,\n" +
//                " BO.UPDATE_DATE, " +
//                " BFT.FLT_START_CITY, " +
//                " BFT.FLT_DEPARTURE_DAY, " +
//                " BFT.FLT_DEPARTURE_TIME, " +
//                " BF.FFP_CARD_NO, " +
//                " BF.ID_CODE, " +
//                " BF.FARE_NAME " +
//                " FROM T_ODS_KFBP_BUS_ORDER BO" +
//                " LEFT JOIN T_ODS_KFBP_BUS_FARE BF ON BO.PKID = BF.ORDER_PKID " +
//                " LEFT JOIN T_ODS_KFBP_BUS_FLIGHT BFT ON BF.ORDER_PKID = BFT.ORDER_PKID " +
//                " WHERE BO.ORDER_TYPE = 2 AND BO.ORDER_STATUS IS NOT NULL " +
//                " AND BO.ETL_DATE >='" + startDate + "'" + " AND BO.ETL_DATE <='" + endDate + "'";
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
//                map.put("FLT_START_CITY", row.getFieldAs("FLT_START_CITY"));
//                map.put("FLT_DEPARTURE_DAY", row.getFieldAs("FLT_DEPARTURE_DAY"));
//                map.put("FLT_DEPARTURE_TIME", row.getFieldAs("FLT_DEPARTURE_TIME"));
//                map.put("ID_CODE", row.getFieldAs("ID_CODE"));
//                map.put("FARE_NAME", row.getFieldAs("FARE_NAME"));
//                map.put("FFP_CARD_NO", row.getFieldAs("FFP_CARD_NO"));
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
//        // 机票-预订-航段级事实表
//        SingleOutputStreamOperator<BookingSegFactModel> bookingSegFactStream = busRefundSource
//                .map(map -> {
//
//                    BookingSegFactModel model = new BookingSegFactModel();
//                    String pnr = map.getString("PNR");
//                    String fltStartCity = map.getString("FLT_START_CITY");
//                    String fltDepartureDay = StringUtils.isNotBlank(map.getString("FLT_DEPARTURE_DAY")) ? map.getString("FLT_DEPARTURE_DAY").replace("-", "") : "";
//                    String fltDepartureTime = StringUtils.isNotBlank(map.getString("FLT_DEPARTURE_TIME")) ? map.getString("FLT_DEPARTURE_TIME") + ":00" : "";
//                    String idCode = StringUtils.isNotBlank(map.getString("ID_CODE")) ? map.getString("ID_CODE") : "";
//                    String fareName = map.getString("FARE_NAME");
//
//                    if (StringUtils.isBlank(pnr) || StringUtils.isBlank(fltStartCity)
//                            || StringUtils.isBlank(fltDepartureDay) || StringUtils.isBlank(fltDepartureTime)
//                            || StringUtils.isBlank(idCode) || StringUtils.isBlank(fareName)) {
//                        return null;
//                    }
//                    String pkId = pnr + fltStartCity + fltDepartureDay + fltDepartureTime
//                            + idCode + fareName.replace("/", "");
//                    model.setPkId(pkId);
//
//
//                    // 渠道订单号 呼叫白屏票务系统	BUS_ORDER（基本订单表）	order_no
//                    model.setChannelOrderId(map.getString("ORDER_NO"));
//                    // 预订渠道 写死：呼叫白屏票务系统-code:SCWHP
//                    model.setAkChannel("SCWHP");
//                    // 呼叫白屏票务系统	BUS_ORDER（基本订单表）	contact_mobile
//                    model.setContactMobileNumber(map.getString("CONTACT_MOBILE"));
//                    // 呼叫白屏票务系统	BUS_ORDER（基本订单表）	contact_name
//                    model.setContactName(map.getString("CONTACT_NAME"));
//                    //呼叫白屏票务系统	BUS_ORDER（基本订单表）	contact_phone
//                    model.setContactLandlineMobileNumber(map.getString("CONTACT_PHONE"));
//                    // 呼叫白屏票务系统	BUS_ORDER（基本订单表）	根据预订人源ID找TID
//                    model.setFkBookingUserTid(map.getString("TID"));
//                    // 呼叫白屏票务系统	BUS_ORDER（基本订单表）	cur_tel
//                    model.setFkBookingUserOriginId(map.getString("CUR_TEL"));
//
//
//
//
//                    // 源系统最后更新时间 呼叫白屏票务系统	BUS_ORDER（基本订单表）	update_date
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
//        DorisSink<BookingSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_BOOKING_SEG_FACT");
//        bookingSegFactStream.sinkTo(dorisSink);
//        //启动任务
//        env.execute("BusOrderToBookingSegDwd");
//    }
//}
