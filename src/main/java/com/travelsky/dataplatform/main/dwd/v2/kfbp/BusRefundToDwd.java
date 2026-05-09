//package com.travelsky.dataplatform.main.dwd.v2.kfbp;
//
//
//import com.alibaba.fastjson.JSONObject;
//import com.travelsky.dataplatform.constans.Constants;
//import com.travelsky.dataplatform.constans.KfbpCreateToSql;
//import com.travelsky.dataplatform.constans.trp.utils.DateUtil;
//import com.travelsky.dataplatform.utils.*;
//import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
//import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.RefundSegFactModel;
//import com.travelsky.trp.usercenter.data.analysis.transform.Kfbp.KfbpBusRefundToDwdRefundSegFactTrans;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.doris.flink.sink.DorisSink;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//
//import java.util.Map;
//import java.util.Objects;
//
//import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.normalizeName;
//
///**
// * 呼叫白屏票务系统
// */
//public class BusRefundToDwd {
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
//        CheckpointUtils.setCheckpoint(env, "BusRefundToDwd");
//        env.setParallelism(1);
//        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        // 1. 创建Doris目标表
//        tEnv.executeSql(KfbpCreateToSql.BUS_REFUND);
//        tEnv.executeSql(KfbpCreateToSql.BUS_ORDER);
//        tEnv.executeSql(KfbpCreateToSql.BUS_TK_REFUND_FARE_TICKET_FLT);
//        tEnv.executeSql(KfbpCreateToSql.BUS_FLIGHT);
//        // 2. 从Doris表中读取数据
//        String query = "SELECT DISTINCT\n" +
//                "    RF.PKID,\n" +
//                "    RF.CREATE_DATE,\n" +
//                "    RF.UPDATE_DATE,\n" +
//                "    RF.REFUNDTYPE,\n" +
//                "    RF.REFUND_MARK_STATUS,\n" +
//                "    RF.OFFLINE_TICKET_REFUND,\n" +
//                "    RF.REASON,\n" +
//                "    RF.BUSINESS_TYPE,\n" +
//                "    RF.CUR_TEL,\n" +
//                "    RF.AUDITING_THROUGH_DATE,\n" +
//                "    BO.CASH_CURRENCY_CODE,\n" +
//                "    RFTF.REFUND_TOTAL_FEE,\n" +
//                "    RFTF.REC_TOTAL_FEE,\n" +
//                "    BF.FLT_START_CITY,\n" +
//                "    RFTF.START_CITY AS REFUND_START_CITY\n" +
//                " FROM T_ODS_KFBP_BUS_REFUND RF\n" +
//                " LEFT JOIN T_ODS_KFBP_BUS_TK_REFUND_FARE_TICKET_FLT RFTF ON RF.PKID = RFTF.TK_REFUND_PKID\n" +
//                " LEFT JOIN T_ODS_KFBP_BUS_ORDER BO ON RF.ORDER_PKID = BO.PKID\n" +
//                " LEFT JOIN T_ODS_KFBP_BUS_FLIGHT BF ON BO.PKID = BF.ORDER_PKID\n" +
//                " WHERE RF.ETL_DATE >='" + startDate + "'" + " AND RF.ETL_DATE <='" + endDate + "'\n" +
//                "    AND RF.REFUNDTYPE = 'TkRefund'\n" +
//                "    AND RF.REFUND_MARK_STATUS = 1\n" +
//                "    AND BF.FLT_START_CITY = RFTF.START_CITY";
//        System.out.println("日期为：" + etlDate + "的KFBP数据开始查询...");
//        Table table = tEnv.sqlQuery(query);
//        DataStream<JSONObject> busRefundSource = tEnv.toChangelogStream(table).map(new MapFunction<Row, JSONObject>() {
//            @Override
//            public JSONObject map(Row row) throws Exception {
//                JSONObject map = new JSONObject();
//                String offlineTicketRefund = row.getFieldAs("OFFLINE_TICKET_REFUND");
//                // 标准化退票渠道
//                String refundChannel = NormalizationUtils.standardize(FieldType.REFUND_CHANNEL_AIRTICKET, offlineTicketRefund, DataSource.CALL_CENTER_TICKETING_SYSTEM);
//                map.put("OFFLINE_TICKET_REFUND", refundChannel);
//                String reason = row.getFieldAs("REASON");
//                // 标准化退票原因
//                reason = NormalizationUtils.standardize(FieldType.REFUND_TYPE, reason, DataSource.CALL_CENTER_TICKETING_SYSTEM);
//                map.put("REASON", reason);
//                String businessType = row.getFieldAs("BUSINESS_TYPE");
//                // 标准化非自愿退票原因
//                businessType = NormalizationUtils.standardize(FieldType.NON_VOLUNTARY_REFUND_REASON, businessType, DataSource.CALL_CENTER_TICKETING_SYSTEM);
//                map.put("BUSINESS_TYPE", businessType);
//                String curTel = row.getFieldAs("CUR_TEL");
//                map.put("CUR_TEL", curTel);
//                map.put("AUDITING_THROUGH_DATE", row.getFieldAs("AUDITING_THROUGH_DATE"));
//                map.put("UPDATE_DATE", row.getFieldAs("UPDATE_DATE"));
//                map.put("CASH_CURRENCY_CODE", row.getFieldAs("CASH_CURRENCY_CODE"));
//                map.put("REFUND_TOTAL_FEE", row.getFieldAs("REFUND_TOTAL_FEE"));
//                map.put("REC_TOTAL_FEE", row.getFieldAs("REC_TOTAL_FEE"));
//                map.put("CREATE_DATE", row.getFieldAs("CREATE_DATE"));
//
//                Tid tid = new Tid();
//                tid.setTid(curTel);
//                tid.setMobilePhone(curTel);
//                String tidStr = IdMapping.idMappingFunction(tid, "KFBP");
//                map.put("TID", tidStr);
//                return map;
//            }
//        }).filter(Objects::nonNull);
//
//        // 机票-退票-航段级事实表
//        Map<String, String> map = json.toJavaObject(Map.class);
//
//        RefundSegFactModel model = new RefundSegFactModel();
//        // 票号+起飞机场+起飞日期	起飞日期YYYYMMDD格式拼装，票号（不带横杠，纯数字13位）
//        String ticketNo = map.get("TICKET_NO");
//        String startCity = map.get("START_CITY");
//        String fltDepartureDay = map.get("FLT_DEPARTURE_DAY");
//        if (StringUtils.isBlank(ticketNo) || StringUtils.isBlank(startCity)) {
//            System.out.println("提取KfbpBusRefundToDwdRefundSegFactTrans失败，主键组成部分不全:" + map.toString());
//            return;
//        }
//
//        String pkId = ticketNo.replace("-", "") + startCity;
//        if (StringUtils.isNotBlank(fltDepartureDay)) {
//            pkId = pkId + DateUtil.formatToYmd(fltDepartureDay);
//        }
//        System.out.println("pkId:" + pkId);
//        model.setPkId(pkId);
//        model.setAkRefundChannel(map.get("OFFLINE_TICKET_REFUND"));
//        model.setAkRefundType(map.get("REASON"));
//        model.setRefundReason(map.get("BUSINESS_TYPE"));
//        model.setFkRefundUserOriginId(map.get("CUR_TEL"));
//        String auditingThroughDate = map.get("AUDITING_THROUGH_DATE");
//        if (StringUtils.isNotBlank(auditingThroughDate)) {
//            String[] auditingThroughDateStr = auditingThroughDate.split(" ");
//            if (auditingThroughDateStr.length > 1) {
//                model.setFkCompleteDate(auditingThroughDateStr[0]);
//                model.setFkCompleteTime(auditingThroughDateStr[1]);
//            }
//        }
//        // 退订人TID
//        model.setFkRefundUserTid(map.get("TID"));
//        String cashCurrencyCode = map.get("CASH_CURRENCY_CODE");
//        model.setAkCurrency(cashCurrencyCode);
//
//        // 原币种应退金额
//        String refundTotalFee = map.get("REFUND_TOTAL_FEE");
//        model.setSegCurrefundamount(StringUtils.isNotBlank(refundTotalFee) ? Double.valueOf(refundTotalFee) : null);
//        // 原币种退票手续费
//        String recTotalFee = map.get("REC_TOTAL_FEE");
//        model.setSegCurrefundcharges(StringUtils.isNotBlank(recTotalFee) ? Double.valueOf(recTotalFee) : null);
//        // 应退金额CNY
//        if ("CNY".equals(model.getAkCurrency())) {
//            model.setSegRefundamount(StringUtils.isNotBlank(refundTotalFee) ? Double.valueOf(refundTotalFee) : null);
//            model.setSegRefundcharges(StringUtils.isNotBlank(recTotalFee) ? Double.valueOf(recTotalFee) : null);
//        }
//
//
//        model.setSourceLastUpdatetime(map.get("UPDATE_DATE"));
//
//        String now = DateTimeUtils.getCurrentDateTime();
//        model.setSystemCreatetime(now);
//        model.setSystemLastUpdatetime(now);
//        DorisSink<RefundSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_REFUND_SEG_FACT");
//        refundSegFactStream.sinkTo(dorisSink);
//        //启动任务
//        env.execute("BusRefundToDwd");
//
//
//    }
//
//}
