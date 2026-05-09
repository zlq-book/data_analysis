package com.travelsky.trp.usercenter.data.analysis.transform.zsf;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.*;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

// 选座订单附加信息
// 数据库没有满足sql条件的数据
public class OdrOrderDetailSeatFactTrans {

    private static final Logger logger = LoggerFactory.getLogger(OdrOrderDetailSeatFactTrans.class);

    // 将 Object 转为 String，支持 null
    private static String toStringSafe(Object obj) {
        return obj != null ? obj.toString() : null;
    }

    // 将 dateTime 转为 LocalDateTime 字符串
    private static String formatDateTimeSafe(Object ts) {
        if (ts != null) {
            LocalDateTime s = (LocalDateTime) ts;
            return s.toString().replace("T", " ");
        }
        return null;
    }

    public static void result(StreamTableEnvironment tEnv, String etlDate) throws Exception {

        // 注册表
        tEnv.executeSql(CreateTableSql.ZSF_ODR_ORDER_DETAIL_SEAT);
        tEnv.executeSql(CreateTableSql.ZSF_ODR_ORDER_CUSTOMER_RELATION);

        // 查询
        String query = "SELECT \n" +
                "    a.ETL_DATE AS ETL_DATE,\n" +
                "    a.ID AS ID,\n" +
                "    a.ORDER_DETAIL_ID AS ORDER_DETAIL_ID,\n" +
                "    a.ORDER_NO AS ORDER_NO,\n" +
                "    a.MAIN_ORDER_NO AS MAIN_ORDER_NO,\n" +
                "    a.MAIN_ORDER_ID AS MAIN_ORDER_ID,\n" +
                "    a.INSTANCE_ID AS INSTANCE_ID,\n" +
                "    a.INSTANCE_NO AS INSTANCE_NO,\n" +
                "    a.MAIN_FLAG AS MAIN_FLAG,\n" +
                "    a.TICKET_NO AS TICKET_NO,\n" +
                "    a.ADT_TICKET_NO AS ADT_TICKET_NO,\n" +
                "    a.PNR AS PNR,\n" +
                "    a.CARRIER AS CARRIER,\n" +
                "    a.FLIGHT_NO AS FLIGHT_NO,\n" +
                "    a.DEP_DATE_TIME AS DEP_DATE_TIME,\n" +
                "    a.DEP_DATE AS DEP_DATE,\n" +
                "    a.ARR_DATE AS ARR_DATE,\n" +
                "    a.DEP_TIME AS DEP_TIME,\n" +
                "    a.ARR_TIME AS ARR_TIME,\n" +
                "    a.DEP_TERMINAL AS DEP_TERMINAL,\n" +
                "    a.ARR_TERMINAL AS ARR_TERMINAL,\n" +
                "    a.FLIGHT_DURATION AS FLIGHT_DURATION,\n" +
                "    a.ORG_CITY AS ORG_CITY,\n" +
                "    a.STOP_CITY AS STOP_CITY,\n" +
                "    a.DST_CITY AS DST_CITY,\n" +
                "    a.CABIN AS CABIN,\n" +
                "    a.PSGR_NAME AS PSGR_NAME,\n" +
                "    a.CERT_TYPE AS CERT_TYPE,\n" +
                "    a.CERT_NO AS CERT_NO,\n" +
                "    a.OLD_SEAT_NO AS OLD_SEAT_NO,\n" +
                "    a.SEAT_NO AS SEAT_NO,\n" +
                "    a.MOBILE AS MOBILE,\n" +
                "    a.SEAT_STATUS AS SEAT_STATUS,\n" +
                "    a.UPDATE_TIMES AS UPDATE_TIMES,\n" +
                "    a.SEAT_TIME AS SEAT_TIME,\n" +
                "    a.SEAT_SOURCE AS SEAT_SOURCE,\n" +
                "    a.SEAT_PRICE AS SEAT_PRICE,\n" +
                "    a.PSGR_TYPE AS PSGR_TYPE,\n" +
                "    a.PSGR_NAME_EN AS PSGR_NAME_EN,\n" +
                "    a.CHECKIN_STATUS AS CHECKIN_STATUS,\n" +
                "    a.IS_DIFF AS IS_DIFF,\n" +
                "    a.SEAT_TYPE AS SEAT_TYPE,\n" +
                "    a.CHECK_IN_TYPE AS CHECK_IN_TYPE,\n" +
                "    a.APP_CHECK_IN_TYPE AS APP_CHECK_IN_TYPE,\n" +
                "    a.SEG_INDEX AS SEG_INDEX,\n" +
                "    a.ENABLE AS ENABLE,\n" +
                "    a.CREATE_TIME AS CREATE_TIME,\n" +
                "    a.CREATE_USER AS CREATE_USER,\n" +
                "    a.UPDATE_TIME AS UPDATE_TIME,\n" +
                "    a.UPDATE_USER AS UPDATE_USER,\n" +
                "    a.EMD_TICKET_NO AS EMD_TICKET_NO,\n" +
                "    a.IS_PRICE_SEAT AS IS_PRICE_SEAT,\n" +
                "    b.CUSTOMER_ID AS CUSTOMER_ID\n" +
                "FROM T_ODS_ZSF_ODR_ORDER_DETAIL_SEAT a\n" +
                "LEFT JOIN T_ODS_ZSF_ODR_ORDER_CUSTOMER_RELATION b\n" +
                "ON a.ORDER_NO = b.MAIN_ORDER_NO\n" +
//                "WHERE a.ETL_DATE = '" + etlDate + "'";
             " WHERE  a.CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                + " OR  a.UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' ";


        Table dorisTable = tEnv.sqlQuery(query);

        DataStream<Row> rowDataStream = tEnv.toChangelogStream(dorisTable);

        // 后续可以进行 map、filter、sink 操作
        // 机票出票-航段级
        SingleOutputStreamOperator<TickingSegFactModel> mappedStream1 = rowDataStream.map(row -> {
            TickingSegFactModel model = new TickingSegFactModel();

            // 安全获取字段值
            // 去除日期中的 "-"
            String flightDate = toStringSafe(row.getField("DEP_DATE")); // 起飞日期
            String ticketNo = NormalizationUtils.standardize(FieldType.TICKET_NO, toStringSafe(row.getField("TICKET_NO")),DataSource.ZHANGSHANGFEI); // 票号
            String orgCity = NormalizationUtils.standardize(FieldType.CITY, toStringSafe(row.getField("ORG_CITY")),DataSource.ZHANGSHANGFEI); // 起飞机场

            // 拼接主键
            String pkId = (ticketNo != null ? ticketNo : "") + (orgCity != null ? orgCity : "") +
                    (flightDate != null ? flightDate.replace("-", "") : "");
            model.setPkId(pkId);

            // 判断 seatType 并设置渠道
            String seatType = toStringSafe(row.getField("SEAT_TYPE"));
            if ("SEAT".equals(seatType) || "SEAT_CHECKIN".equals(seatType)) {
                model.setSeatChannel("SCZSF");
            } else {
                model.setSeatChannel(null);
            }

            return model;
        });

        // 附加服务_选座出票_航段事实表
        SingleOutputStreamOperator<SeatTikFactModel> mappedStream3 = rowDataStream.map(row -> {
            SeatTikFactModel model = new SeatTikFactModel();

            // 安全获取字段（使用 toStringSafe）
            String emdTicketNo = NormalizationUtils.standardize(FieldType.TICKET_NO, toStringSafe(row.getField("EMD_TICKET_NO")),DataSource.ZHANGSHANGFEI);
            String orgCity = NormalizationUtils.standardize(FieldType.TICKET_NO, toStringSafe(row.getField("ORG_CITY")),DataSource.ZHANGSHANGFEI); // 起飞机场代码
            String dstCity = NormalizationUtils.standardize(FieldType.TICKET_NO, toStringSafe(row.getField("DST_CITY")),DataSource.ZHANGSHANGFEI); // 到达机场代码

            // 拼接主键
            String isPriceSeat = toStringSafe(row.getField("IS_PRICE_SEAT"));
            Integer isPriceSeatInt = null;
            if (isPriceSeat != null) {
                isPriceSeatInt = Integer.parseInt(isPriceSeat);
                if (isPriceSeatInt == 1) {
                    model.setPkId(
                            (emdTicketNo != null ? emdTicketNo : "") +
                                    (orgCity != null ? orgCity : "") +
                                    (dstCity != null ? dstCity : "")
                    );
                }
            } else {
                model.setPkId(null);
            }

            // 设置订单号
            String orderNo = toStringSafe(row.getField("ORDER_NO"));
            model.setAkOrdernum(orderNo); // toStringSafe 已处理 null

            // 预订人信息
            String customerId = toStringSafe(row.getField("CUSTOMER_ID"));
            Tid tid = new Tid();
            tid.setTid(customerId);
            tid.setCrmCustomerId(customerId);
            String bookingUserTid = IdMapping.idMappingFunction(tid, "ZSF");
            model.setFkBookingUserTid(bookingUserTid);
            model.setFkBookingUserOriginId(customerId);

            // 渠道固定
            model.setAkChannel("SCZSF");

            model.setSourceLastUpdatetime(formatDateTimeSafe(row.getField("UPDATE_TIME")));

            LocalDateTime now = LocalDateTime.now();
            model.setSystemCreatetime(now.toString());
            model.setSystemLastUpdatetime(now.toString());

            System.out.println(model);

            return model;
        }).filter(model -> model.getPkId() != null);

        // 附加服务_选座退票_航段事实表
        SingleOutputStreamOperator<SeatRefundFactModel> mappedStream4 = rowDataStream.map(row -> {
            SeatRefundFactModel model = new SeatRefundFactModel();

            String emdTicketNo = NormalizationUtils.standardize(FieldType.TICKET_NO, toStringSafe(row.getField("EMD_TICKET_NO")), DataSource.ZHANGSHANGFEI);
            String orgCity = NormalizationUtils.standardize(FieldType.TICKET_NO, toStringSafe(row.getField("ORG_CITY")),DataSource.ZHANGSHANGFEI); // 起飞机场代码
            String dstCity = NormalizationUtils.standardize(FieldType.TICKET_NO, toStringSafe(row.getField("DST_CITY")),DataSource.ZHANGSHANGFEI); // 到达机场代码

            // 拼接主键
            model.setPkId(
                    (emdTicketNo != null ? emdTicketNo : "") +
                            (orgCity != null ? orgCity : "") +
                            (dstCity != null ? dstCity : "")
            );

            // 设置订单号
            String orderNo = toStringSafe(row.getField("ORDER_NO"));
            model.setAkOrdernum(orderNo);

            // 设置退票渠道信息
            String seatStatus = toStringSafe(row.getField("SEAT_STATUS"));
            if ("6".equals(seatStatus)){
                model.setAkRefundChannel("SCZSF");
            }

            // 预订人信息（TID 和 OriginId）
            String customerId = toStringSafe(row.getField("CUSTOMER_ID"));
            Tid tid = new Tid();
            tid.setTid(customerId);
            tid.setCrmCustomerId(customerId);
            String FkRefundUserTid = IdMapping.idMappingFunction(tid, "ZSF");
            model.setFkRefundUserTid(FkRefundUserTid);
            model.setFkRefundUserOriginId(customerId);

            // 设置系统时间（格式化为 yyyy-MM-dd HH:mm:ss）
            LocalDateTime now = LocalDateTime.now();
            model.setSystemCreatetime(now.toString());
            model.setSystemLastUpdatetime(now.toString());

            System.out.println(model);

            return model;
        });

        // 6. 输出到 Doris
        // 写道  附加服务_选座退票_航段事实表
        logger.info("写入 Doris 表: DB={}, Table={}", Constants.DWD_DB, "T_DWD_TICKING_SEG_FACT");
        DorisSink<TickingSegFactModel> dorisSink1 = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_TICKING_SEG_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        mappedStream1.sinkTo(dorisSink1);

        // 写道 附加服务_选座出票_航段事实表
        logger.info("写入 Doris 表: DB={}, Table={}", Constants.DWD_DB, "T_DWD_SEAT_TIK_FACT");
        DorisSink<SeatTikFactModel> dorisSink3 = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_SEAT_TIK_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        mappedStream3.sinkTo(dorisSink3);

        // 机票退票航段级事实表
        logger.info("写入 Doris 表: DB={}, Table={}", Constants.DWD_DB, "T_DWD_SEAT_REFUND_FACT");
        DorisSink<SeatRefundFactModel> dorisSink4 = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_SEAT_REFUND_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        mappedStream4.sinkTo(dorisSink4);

    }
}
