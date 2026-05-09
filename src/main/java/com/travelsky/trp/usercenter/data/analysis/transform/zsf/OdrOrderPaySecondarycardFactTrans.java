package com.travelsky.trp.usercenter.data.analysis.transform.zsf;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.ZsfMultiCardRedeemFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;


public class OdrOrderPaySecondarycardFactTrans {

    static final Logger logger = LoggerFactory.getLogger(OdrOrderPaySecondarycardFactTrans.class);

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

        tEnv.executeSql(CreateTableSql.ZSF_ODR_ORDER_PAY_SECONDARYCARD);

        // 查询源表数据
        String query = "SELECT \n" +
                "    ETL_DATE,\n" +
                "    ID,\n" +
                "    ORDER_PAY_ID,\n" +
                "    RESOURCE_ID,\n" +
                "    RESOURCE_NO,\n" +
                "    INSTANCE_ID,\n" +
                "    INSTANCE_NO,\n" +
                "    PRODUCT_ID,\n" +
                "    PRODUCT_NO,\n" +
                "    SALE_ORDER_NO,\n" +
                "    PSGR_NAME,\n" +
                "    CERT_NO,\n" +
                "    ORG_CITY,\n" +
                "    DST_CITY,\n" +
                "    CABIN,\n" +
                "    DEP_DATE,\n" +
                "    DEP_TIME,\n" +
                "    ENABLE,\n" +
                "    CREATE_TIME,\n" +
                "    CREATE_USER,\n" +
                "    UPDATE_TIME,\n" +
                "    UPDATE_USER,\n" +
                "    CARD_NO,\n" +
                "    CARD_VALUE,\n" +
                "    CARD_DESCRIBE,\n" +
                "    CARD_PAY_STATUS\n" +
                "FROM T_ODS_ZSF_ODR_ORDER_PAY_SECONDARYCARD \n" +
//                "WHERE ETL_DATE = '" + etlDate + "'";
               " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                + " OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' ";

        Table dorisTable = tEnv.sqlQuery(query);

        DataStream<Row> rowDataStream = tEnv.toDataStream(dorisTable);

        // 后续可以进行 map、filter、sink 操作
        SingleOutputStreamOperator<ZsfMultiCardRedeemFactModel> mappedStream = rowDataStream.map(row -> {

            ZsfMultiCardRedeemFactModel model = new ZsfMultiCardRedeemFactModel();

            // 基础字段
            model.setPkId(toStringSafe(row.getField("ID")));
            model.setAkOrdernum(toStringSafe(row.getField("SALE_ORDER_NO")));
            model.setProdNo(toStringSafe(row.getField("PRODUCT_NO")));

            model.setDepcity(NormalizationUtils.standardize(FieldType.TICKET_NO,toStringSafe(row.getField("ORG_CITY")), DataSource.ZHANGSHANGFEI));
            model.setArrcity(NormalizationUtils.standardize(FieldType.TICKET_NO,toStringSafe(row.getField("DST_CITY")),DataSource.ZHANGSHANGFEI));

            model.setCabin(toStringSafe(row.getField("CABIN")));

            model.setCnName(NormalizationUtils.standardize(FieldType.CN_NAME,toStringSafe(row.getField("PSGR_NAME")),DataSource.ZHANGSHANGFEI));
            model.setCertNo(toStringSafe(row.getField("CERT_NO")));

            String certNo = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,toStringSafe(row.getField("CERT_NO")),DataSource.ZHANGSHANGFEI);

            Tid tid = new Tid();
            tid.setTid(certNo);
            String passengerUserTid = IdMapping.idMappingFunction(tid, "ZSF");
            model.setPassengerUser(passengerUserTid);

            model.setPsgrName(toStringSafe(row.getField("PSGR_NAME")));

            model.setDepDate(row.getField("DEP_DATE") == null ? null : (LocalDate) row.getField("DEP_DATE"));

            // DEP_TIME
            model.setDepTime(toStringSafe(row.getField("DEP_TIME")));

            // CARD_NO
            model.setCardNo(NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,toStringSafe(row.getField("CARD_NO")),DataSource.ZHANGSHANGFEI));

            // 次卡抵用金额
            Object cardAmountObj = row.getField("CARD_VALUE");
            if (cardAmountObj instanceof BigDecimal) {
                model.setCardAmount((BigDecimal) cardAmountObj);
            } else {
                model.setCardAmount(null);
            }
            // 次卡使用状态
            model.setCardPaystatus(toStringSafe(row.getField("CARD_PAY_STATUS")));
            // 预订时间和预订日期
            Object createTime = row.getField("CREATE_TIME");
            if (createTime != null) {
                LocalDateTime createTime1 = (LocalDateTime) createTime;
                model.setFkBookingDate(createTime1.toLocalDate().toString());
                model.setFkBookingTime(createTime1.toLocalTime().toString());
            } else {
                model.setFkBookingDate(null);
                model.setFkBookingTime(null);
            }
            // 源系统最后更新时间
            model.setSourceLastUpdatetime(formatDateTimeSafe(row.getField("UPDATE_TIME")));

//            model.setExchCount(1);

            // UPDATE_TIME Timestamp
            model.setSourceLastUpdatetime(formatDateTimeSafe(row.getField("UPDATE_TIME")));

            // 系统时间
            LocalDateTime now = LocalDateTime.now();
            model.setSystemCreatetime(now.toString());
//            model.setSystemLastUpdatetime(now.toString());

            return model;
        });


        // 2. 输出到Doris，调试时先打印
        logger.info("连接 Doris 参数: DB={}, Table={}, FE={}, BE={}, User={}",
                Constants.DWD_DB, "T_DWD_MULTI_CARD_REDEEM_FACT", Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT, Constants.DWD_USER);
        DorisSink<ZsfMultiCardRedeemFactModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_MULTI_CARD_REDEEM_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        // 数据写入 Doris
        mappedStream.sinkTo(dorisSink);
    }

}
