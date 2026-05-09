package com.travelsky.trp.usercenter.data.analysis.transform.lyx;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.PaydetailsOrdFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

public class OcriOpiDataStream {

    static final Logger logger = LoggerFactory.getLogger(OcriOpiDataStream.class);

    public static void result(StreamTableEnvironment tEnv, String etlDate) {
        // 创建Doris目标表
        tEnv.executeSql(CreateTableSql.LYX_ORDER_CHANGE_REFUND_INFO);

        // 创建Doris目标表 联表
        tEnv.executeSql(CreateTableSql.LYX_ORDER_PAY_INFO);

        // 从 Doris 表中读取数据
        Table dorisTable = tEnv.sqlQuery( "SELECT \n" +
                "a.ORDER_NO ,\n" +
                "a.LY_CARD ,\n" +
                "a.LY_CARD TID ,\n" +
                "a.CREATE_TIME ,\n" +
                "b.ID ,\n" +
                "b.CASH_PAY_NO,\n" +
                "b.LY_PAY_TIME ,\n" +
                "b.CASH_PAY_TIME ,\n" +
                "b.PAY_WAY ,\n" +
                "b.LY_VALUE_STATUS ,\n" +
                "b.CASH_PAY_STATUS ,\n" +
                "b.CASH_SUM ,\n" +
                "b.LY_SUM, \n" +
                "b.UPDATE_TIME\n" +
                "FROM  T_ODS_LYX_ORDER_PAY_INFO b \n" +
                "INNER JOIN T_ODS_LYX_ORDER_CHANGE_REFUND_INFO a \n" +
                "ON a.PAY_ID = b.ID   \n"
                + " WHERE b.CASH_PAY_STATUS='2' \n"
                + " AND b.ETL_DATE='" + etlDate + "'\n"
        );

        DataStream<Row> rowStream = tEnv.toDataStream(dorisTable);

        // 标准化 IdMapping
        DataStream<Row> rowDataStream = rowStream.map(row -> {
            Row rowModel = Row.copy(row);
            if (null != row.getField("LY_CARD")) {
                // 获取tid
                Tid tid = new Tid();
                tid.setTid( row.getField("LY_CARD").toString());
                tid.setLyCardNumber( row.getField("LY_CARD").toString());

                String tidStr = IdMapping.idMappingFunction(tid, "LYC");
                rowModel.setField("TID", tidStr);
            }

            // 现金支付平台
            Object payWay = row.getField("PAY_WAY");
            if (payWay != null) {
                rowModel.setField("PAY_WAY", NormalizationUtils.standardize(FieldType.CASH_PAYMENT_PLATFORM,
                        payWay.toString(), DataSource.LUYAN_TRIP_CHANGE));
            }
            // TODO 补充 支付状态
            Object lyValueStatus = row.getField("LY_VALUE_STATUS");
            if (lyValueStatus != null) {
                rowModel.setField("LY_VALUE_STATUS", NormalizationUtils.standardize(FieldType.PAYMENT_STATUS,
                        lyValueStatus.toString(), DataSource.LUYAN_TRIP_CHANGE));
            }

            Object cashPayStatus = row.getField("CASH_PAY_STATUS");
            if (cashPayStatus != null) {
                rowModel.setField("CASH_PAY_STATUS", NormalizationUtils.standardize(FieldType.PAYMENT_STATUS,
                        cashPayStatus.toString(), DataSource.LUYAN_TRIP_CHANGE));
            }

            return rowModel;
        });

        // 鲁雁值
        OcriOpiLyzToPayDetailsOrdFactTrans.output(rowDataStream);
        // 现金
        OcriOpiCashToPayDetailsOrdFactTrans.output(rowDataStream);

    }
}
