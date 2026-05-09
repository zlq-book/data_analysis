package com.travelsky.trp.usercenter.data.analysis.transform.zsf;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingTicFactModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.ZsfMultiCardRedeemFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.PaydetailsOrdFactModel;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

import java.math.BigDecimal;

// 在线支付信息表
// 可以跑
public class OdrOrderPayOnlineFactTrans {

    static final Logger logger = LoggerFactory.getLogger(OdrOrderPayOnlineFactTrans.class);

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

//    public static void main(String[] args) throws Exception {
//        OdrOrderPayOnlineFactTrans.result("2023-10-21");
//    }

    public static void result(StreamTableEnvironment tEnv, String etlDate) throws Exception {

        tEnv.executeSql(CreateTableSql.ZSF_ODR_ORDER_PAY_ONLINE);
        tEnv.executeSql(CreateTableSql.ZSF_ODR_ORDER_PAY);

        // 查询
        String query = "SELECT \n" +
                "    a.ETL_DATE AS ETL_DATE,\n" +
                "    a.ID AS ID,\n" +
                "    a.ORDER_PAY_ID AS ORDER_PAY_ID,\n" +
                "    a.PAY_NO AS PAY_NO,\n" +
                "    a.PAY_AMOUNT AS PAY_AMOUNT,\n" +
                "    a.PAY_TIME AS PAY_TIME,\n" +
                "    a.PAY_REASON AS PAY_REASON,\n" +
                "    a.PAY_RETURN_MSG AS PAY_RETURN_MSG,\n" +
                "    a.PAY_RETURN_TIME AS PAY_RETURN_TIME,\n" +
                "    a.BANK_ORDER_NO AS BANK_ORDER_NO,\n" +
                "    a.BANK_TRADE_NO AS BANK_TRADE_NO,\n" +
                "    a.PAY_STATUS AS PAY_STATUS,\n" +
                "    a.PAY_SOURCE AS PAY_SOURCE,\n" +
                "    a.PAY_BANK_CODE AS PAY_BANK_CODE,\n" +
                "    a.PAY_BANK_NAME AS PAY_BANK_NAME,\n" +
                "    a.BUS_PARTER_NO AS BUS_PARTER_NO,\n" +
                "    a.PAY_USER_NAME AS PAY_USER_NAME,\n" +
                "    a.EXTENDS1 AS EXTENDS1,\n" +
                "    a.EXTENDS2 AS EXTENDS2,\n" +
                "    a.EXTENDS3 AS EXTENDS3,\n" +
                "    a.EXTENDS4 AS EXTENDS4,\n" +
                "    a.IS_BINDING AS IS_BINDING,\n" +
                "    a.CUSTOMER_ID AS CUSTOMER_ID,\n" +
                "    a.ENABLE AS ENABLE,\n" +
                "    a.CREATE_TIME AS CREATE_TIME,\n" +
                "    a.CREATE_USER AS CREATE_USER,\n" +
                "    a.UPDATE_TIME AS UPDATE_TIME,\n" +
                "    a.UPDATE_USER AS UPDATE_USER,\n" +
                "    a.CARD_NO AS CARD_NO,\n" +
                "    a.PAY_TYPE AS PAY_TYPE,\n" +
                "    a.PAY_ACTUAL_AMOUNT AS PAY_ACTUAL_AMOUNT,\n" +
                "    a.DEDUCTION_AMOUNT AS DEDUCTION_AMOUNT,\n" +
                "    a.DEDUCTION_TYPE AS DEDUCTION_TYPE,\n" +
                "    a.TRP_PAYMENT_ID AS TRP_PAYMENT_ID,\n" +
                "    b.ORDER_NO AS ORDER_NO\n" +
                "FROM T_ODS_ZSF_ODR_ORDER_PAY_ONLINE a\n" +
                "JOIN T_ODS_ZSF_ODR_ORDER_PAY b\n" +
                "ON a.ORDER_PAY_ID = b.ID\n" +
//                "WHERE a.ETL_DATE = '" + etlDate + "'\n" +
                " WHERE  a.CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                + " OR  a.UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "+
                "  AND a.PAY_STATUS='3'";

        Table dorisTable = tEnv.sqlQuery(query);

        DataStream<Row> rowDataStream = tEnv.toDataStream(dorisTable);

        // 后续可以进行 map、filter、sink 操作
        SingleOutputStreamOperator<PaydetailsOrdFactModel> mappedStream = rowDataStream.map(record -> {

            PaydetailsOrdFactModel model = new PaydetailsOrdFactModel();

            // 主键 pkId = "SCAPP" + ORDER_NO + PAY_NO
            String orderNo = toStringSafe(record.getField("ORDER_NO"));
            String payNo = toStringSafe(record.getField("PAY_NO"));
            model.setPkId("SCAPP" + (orderNo != null ? orderNo : "") + (payNo != null ? payNo : ""));
            model.setBusinessOrderId(orderNo);
            // PAY_NO
            model.setPayNo(toStringSafe(record.getField("PAY_NO")));

            // CUSTOMER_ID
            String customerId = toStringSafe(record.getField("CUSTOMER_ID"));

            Tid tid = new Tid();
            tid.setTid(customerId);
            tid.setCrmCustomerId(customerId);
            String bookingUserTid = IdMapping.idMappingFunction(tid, "ZSF");
            model.setFkBookingUserTid(bookingUserTid);


            model.setFkBookingUserOriginId(customerId);

            // CREATE_TIME   LocalDateTime
            Object createTimeObj = record.getField("CREATE_TIME");
            if (createTimeObj != null) {
                LocalDateTime createTime = (LocalDateTime) createTimeObj;
                model.setFkBookingDate(createTime.toLocalDate().toString());
                model.setFkBookingTime(createTime.toLocalTime().toString());
            } else {
                model.setFkBookingDate(null);
                model.setFkBookingTime(null);
            }

            // PAY_TIME（同 CREATE_TIME）
            Object payTimeObj = record.getField("CREATE_TIME");
            if (payTimeObj != null) {
                LocalDateTime payTime = (LocalDateTime) payTimeObj;
                model.setFkPaymentDate(payTime.toLocalDate().toString());
                model.setFkPaymentTime(payTime.toString());
            } else {
                model.setFkPaymentDate(null);
                model.setFkPaymentTime(null);
            }

            // 支付渠道
            model.setAkPayChannel(NormalizationUtils.standardize(FieldType.PAYMENT_CHANNEL, "掌尚飞", DataSource.ZHANGSHANGFEI));

            // 支付方式
            model.setAkPaymentMethod("CSH");

            // 支付银行代码
            model.setAkCashPlatform(NormalizationUtils.standardize(FieldType.CASH_PAYMENT_PLATFORM, toStringSafe(record.getField("PAY_BANK_CODE")),DataSource.ZHANGSHANGFEI));

            // 支付状态
            model.setAkPaymentStatus(toStringSafe(record.getField("PAY_STATUS")));

            // 币种
            model.setAkCashCurrency("CNY"); // 固定值

            // 金额
            Object payAmountObj = record.getField("PAY_AMOUNT");
            if (payAmountObj instanceof BigDecimal) {
                model.setCashAmount((BigDecimal) payAmountObj);
            } else {
                model.setCashAmount(BigDecimal.ZERO);
            }

            // 系统时间
            LocalDateTime now = LocalDateTime.now();
            model.setSystemCreatetime(now.toString());
            model.setSystemLastUpdatetime(now.toString());

            // 来源最后更新时间
            model.setSourceLastUpdatetime(formatDateTimeSafe(record.getField("UPDATE_TIME")));

            // 订单数量（固定值）
//            model.setOrdCount(1);

            return model;
        });

        // 2. 输出到Doris，调试时先打印
        // 写到 支付流水号事实表
        mappedStream.print("OdrOrderPayOnlineFactTrans");
        logger.info("连接 Doris 参数: DB={}, Table={}, FE={}, BE={}, User={}",
                Constants.DWD_DB, "T_DWD_PAYDETAILS_ORD_FACT", Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT, Constants.DWD_USER);
//        DorisSink<PaydetailsOrdFactModel> dorisSink = FlinkDorisUtils.creatDorisSink(
//                Constants.DWD_DB,
//                "T_DWD_PAYDETAILS_ORD_FACT",
//                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
//                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
//                Constants.DWD_USER,
//                Constants.DWD_PWD);
//        // 数据写入 Doris
//        mappedStream.sinkTo(dorisSink);

        // 写入Doris
        DorisSink<PaydetailsOrdFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_PAYDETAILS_ORD_FACT");
        mappedStream.sinkTo(dorisSink);



    }
}
