package com.travelsky.dataplatform.main.dwd.v2.trp;


import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.TrpCreateToSql;
import com.travelsky.dataplatform.constans.trp.utils.DateUtil;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.RefundSegFactModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * T_ODS_TRP_AIR_REFUND_DETAIL机票退款报表明细表
 * 1、排除渠道=‘TSDF、TSIF、SD、全渠道'这四个渠道的，不入事实表，
 * * 2、取TRP退票表的：票号+出发地三字码+起飞日期（截取YYYYMMDD）拼装主键*
 * *
 */

public class AirRefundDetailToRefundDwd {
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
        CheckpointUtils.setCheckpoint(env, "AirRefundDetailToRefundDwd");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 1. 创建Doris目标表
        tEnv.executeSql(TrpCreateToSql.AIR_REFUND_DETAIL);
        // 2. 从Doris表中读取数据
        String query = "SELECT \n" +
                " ETL_DATE,\n" +
                " TICKET_NUMBER,\n" +
                " TICKET_REFUND_APPLICATION_CHANNEL,\n" +
                " DEPARTURE_CITY_CODE,\n" +
                " TICKET_REFUND_NATURE,\n" +
                " ACCOUNT_USERNAME,\n" +
                " SECOND_REVIEW_TIME,\n" +
                " CURRENCY,\n" +
                " REFUND_AMOUNT,\n" +
                " TICKET_REFUND_FEE,\n" +
                " DEPARTURE_DATE\n" +

                " FROM T_ODS_TRP_AIR_REFUND_DETAIL " +
                " WHERE ETL_DATE >='" + startDate + "'" + " AND ETL_DATE <='" + endDate + "'" +
                " AND CHANNEL NOT IN ('TSDF', 'TSIF', 'SD', '全渠道')";
        System.out.println("日期为：" + etlDate + "的DPI数据开始查询...");
        Table table = tEnv.sqlQuery(query);
        DataStream<JSONObject> airRefundDetailStream = tEnv.toChangelogStream(table).map(new MapFunction<Row, JSONObject>() {
            @Override
            public JSONObject map(Row row) throws Exception {
                JSONObject map = new JSONObject();
                String ticketRefundApplicationChannel = row.getFieldAs("TICKET_REFUND_APPLICATION_CHANNEL");
                ticketRefundApplicationChannel = NormalizationUtils.standardize(FieldType.REFUND_CHANNEL_AIRTICKET, ticketRefundApplicationChannel, DataSource.TRP_FTP);
                map.put("TICKET_REFUND_APPLICATION_CHANNEL", ticketRefundApplicationChannel);
                String ticketNumber = row.getFieldAs("TICKET_NUMBER");
                ticketNumber = NormalizationUtils.standardize(FieldType.TICKET_NO, ticketNumber, DataSource.TRP_FTP);
                map.put("TICKET_NUMBER", ticketNumber);
                map.put("DEPARTURE_CITY_CODE", row.getFieldAs("DEPARTURE_CITY_CODE"));
                map.put("DEPARTURE_DATE", row.getFieldAs("DEPARTURE_DATE"));
                String ticketRefundNature = row.getFieldAs("TICKET_REFUND_NATURE");
                ticketRefundNature = NormalizationUtils.standardize(FieldType.REFUND_TYPE, ticketRefundNature, DataSource.TRP_FTP);
                map.put("TICKET_REFUND_NATURE", ticketRefundNature);
                if ("NOV".equals(ticketRefundNature)) {
                    ticketRefundNature = NormalizationUtils.standardize(FieldType.NON_VOLUNTARY_REFUND_REASON, ticketRefundNature, DataSource.TRP_FTP);
                    map.put("NON_VOLUNTARY_REFUND_REASON", ticketRefundNature);
                }
                String accountUsername = row.getFieldAs("ACCOUNT_USERNAME");
                map.put("ACCOUNT_USERNAME", accountUsername);
                Tid tid = new Tid();
                tid.setTid(accountUsername);
                tid.setCrmCustomerId(accountUsername);
                String tidStr = IdMapping.idMappingFunction(tid, "TRP");
                map.put("TID", tidStr);

                map.put("SECOND_REVIEW_TIME", row.getFieldAs("SECOND_REVIEW_TIME"));
                map.put("CURRENCY", row.getFieldAs("CURRENCY"));
                map.put("REFUND_AMOUNT", row.getFieldAs("REFUND_AMOUNT"));
                map.put("TICKET_REFUND_FEE", row.getFieldAs("TICKET_REFUND_FEE"));

                return map;
            }
        }).filter(Objects::nonNull);
        SingleOutputStreamOperator<RefundSegFactModel> refundSegFactStream = airRefundDetailStream
                .map(map -> {
                    RefundSegFactModel model = new RefundSegFactModel();
                    String departureDate = map.getString("DEPARTURE_DATE");
                    departureDate = StringUtils.isNotBlank(departureDate) ? departureDate.replace("-", "").substring(0, 8) : "";
                    // 票号+起飞机场+起飞日期
                    model.setPkId(map.getString("TICKET_NUMBER") + map.getString("DEPARTURE_CITY_CODE") + departureDate);
                    // 退票渠道
                    model.setAkRefundChannel(map.getString("TICKET_REFUND_APPLICATION_CHANNEL"));
                    // 退票类型
                    model.setAkRefundType(map.getString("TICKET_REFUND_NATURE"));
                    // 非自愿退票原因
                    model.setRefundReason(map.getString("NON_VOLUNTARY_REFUND_REASON"));
                    // 退订人TID
                    model.setFkRefundUserTid(map.getString("TID"));
                    // 退订人源ID
                    model.setFkRefundUserOriginId(map.getString("ACCOUNT_USERNAME"));
                    // 退票审核完成日期
                    String secondReviewTimeStr = map.getString("SECOND_REVIEW_TIME");
                    if (StringUtils.isNotBlank(secondReviewTimeStr)) {
                        String secondReviewDate = secondReviewTimeStr.replace("-", "").substring(0, 9);
                        model.setFkCompleteDate(secondReviewDate);
                        String secondReviewTime  = secondReviewTimeStr.substring(11, 16);
                        if (StringUtils.isNotBlank(secondReviewTime)) {
                            // 退票审核完成时间
                            model.setFkCompleteTime(secondReviewTime + ":00");

                        }
                    }
                    // 原币种
                    model.setAkCurrency(map.getString("CURRENCY"));
                    // 原币种应退金额
                    String refundAmountStr = map.getString("REFUND_AMOUNT");
                    Double refundAmount = StringUtils.isNotBlank(refundAmountStr) ? Double.valueOf(refundAmountStr) : null;
                    model.setSegCurrefundamount(refundAmount);
                    // 原币种退票手续费
                    String ticketRefundFeeStr = map.getString("TICKET_REFUND_FEE");
                    Double ticketRefundFee = StringUtils.isNotBlank(ticketRefundFeeStr) ? Double.valueOf(ticketRefundFeeStr) : null;
                    model.setSegCurrefundcharges(map.getDouble("TICKET_REFUND_FEE"));
                    // 应退金额CNY
                    if ("CNY".equals(model.getAkCurrency())) {
                        model.setSegRefundamount(refundAmount);
                        model.setSegRefundcharges(ticketRefundFee);
                    }
                    // 源系统最后更新时间
                    model.setSourceLastUpdatetime(map.getString("SECOND_REVIEW_TIME"));

                    // 系统信息
                    LocalDateTime now = LocalDateTime.now();
                    model.setSystemCreatetime(now.toString());
                    model.setSystemLastUpdatetime(now.toString());

                    return model;
                }).returns(RefundSegFactModel.class);

        // 5. 创建Doris Sink并写入数据
        DorisSink<RefundSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_REFUND_SEG_FACT");

        refundSegFactStream.sinkTo(dorisSink);

        //启动任务
        env.execute("AirRefundDetailToRefundDwd");
    }
}
