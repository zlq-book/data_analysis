package com.travelsky.trp.usercenter.data.analysis.transform.lyx;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.AuisSegFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

public class OtiOfiOpiDataStream {

    static final Logger logger = LoggerFactory.getLogger(OtiOfiOpiDataStream.class);

    public static void result(StreamTableEnvironment tEnv, String etlDate) {

        // 创建Doris目标表
        tEnv.executeSql(CreateTableSql.LYX_ORDER_TICKET_INFO);

        // 创建Doris目标表 联表
        tEnv.executeSql(CreateTableSql.LYX_ORDER_FLIGHT_INFOS);
        tEnv.executeSql(CreateTableSql.LYX_ORDER_PASSENGER_INFO);

        // 从 Doris 表中读取数据
        Table dorisTable = tEnv.sqlQuery( "SELECT \n" +
                        "a.ORDER_NO ,\n" +
                        "c.TICKET_NUM ,\n" +
                        "c.INSURE_SERIAL_NUMBER,\n" +
                        "a.CREATE_TIME ,\n" +
                        "c.PASSENGER_TYPE ,\n" +
                        "c.PSG_NAME_CN ,\n" +
                        "c.PSG_NAME_EN ,\n" +
                        "c.CERT_TYPE ,\n" +
                        "c.CERT_NUM ,\n" +
                        "c.CERT_NUM PSG_TID,\n" +
                        "a.LY_CARD ,\n" +
                        "a.LY_CARD TID ,\n" +
                        "b.ORIG ,\n" +
                        "b.DEST ,\n" +
                        "a.ORDER_SOURCE ,\n" +
                        "c.INSURE_STATUS ,\n" +
                        "c.INSURE_TAX ,\n" +
                        "c.CONTACT_NO ,\n" +
                        "c.CONTACT_NAME,\n" +
                        "c.PNR_NO ,\n" +
                        "c.TICKET_TIME ,\n" +
                        "b.DEPATURE_TIME,\n" +
                        "c.OPT_STATUS,\n" +
                        "a.UPDATE_TIME  \n" +
                        "FROM \n" +
                        "T_ODS_LYX_ORDER_TICKET_INFO a\n" +
                        "LEFT JOIN T_ODS_LYX_ORDER_FLIGHT_INFOS b ON\n" +
                        "b.ORDER_NO =a.ORDER_NO\n" +
                        "LEFT JOIN T_ODS_LYX_ORDER_PASSENGER_INFO c ON\n" +
                        "b.ID =c.FLIGHT_ID   \n"
                    + " WHERE  a.ETL_DATE='" + etlDate + "'"
        );

        DataStream<Row> rowStream = tEnv.toChangelogStream(dorisTable)
                .filter((row) -> row.getKind() == RowKind.INSERT || row.getKind() == RowKind.UPDATE_AFTER);


        // 标准化 IdMapping
        DataStream<Row> rowDataStream = rowStream.filter(row -> {
            boolean flag = row.getField("LY_CARD") == null;
            if (flag) {
                logger.info("缺少鲁雁行卡号，oriTable：T_ODS_LYX_ORDER_TICKET_INFO，" + row.toString());
            }
            return !flag;
        }).map(row -> {
            // 获取tid
            Tid tid = new Tid();
            tid.setLyCardNumber( row.getField("LY_CARD").toString());
            tid.setTid( row.getField("LY_CARD").toString());

            String tidStr = IdMapping.idMappingFunction(tid, "LYC");
            Row rowModel = Row.copy(row);
            rowModel.setField("TID", tidStr);


            // 英文姓名
            Object engName = row.getField("PSG_NAME_EN");
            if (engName != null) {
                rowModel.setField("PSG_NAME_EN", NormalizationUtils.standardize(FieldType.EN_NAME,
                        engName.toString(), DataSource.LUYAN_TRIP_CHANGE));
            }
            // 手机号
            Object contactNumber = row.getField("CONTACT_NO");
            if (contactNumber != null) {
                rowModel.setField("CONTACT_NO", NormalizationUtils.standardize(FieldType.MOBILE_NO,
                        contactNumber.toString()));
            }
            // 中文姓名
            Object lastCnName = row.getField("CONTACT_NAME");
            if (lastCnName != null) {
                rowModel.setField("CONTACT_NAME", NormalizationUtils.standardize(FieldType.CN_NAME,
                        lastCnName.toString()));
            }
            Object cnName = row.getField("PSG_NAME_CN");
            if (cnName != null) {
                rowModel.setField("PSG_NAME_CN", NormalizationUtils.standardize(FieldType.CN_NAME,
                        cnName.toString()));
            }

            // 票号
            Object firstCnName = row.getField("TICKET_NUM");
            if (firstCnName != null) {
                rowModel.setField("TICKET_NUM", NormalizationUtils.standardize(FieldType.TICKET_NO,
                        firstCnName.toString()));
            }

            // 渠道
            Object orderSource = row.getField("ORDER_SOURCE");
            if (orderSource != null) {
                rowModel.setField("ORDER_SOURCE", NormalizationUtils.standardize(FieldType.ORDER_PLACEMENT_CHANNEL,
                        orderSource.toString(), DataSource.LUYAN_TRIP_CHANGE));
            }

            // 证件类型
            Object certType = row.getField("CERT_TYPE");
            if (certType != null) {
                rowModel.setField("CERT_TYPE", NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE,
                        certType.toString(), DataSource.LUYAN_TRIP_CHANGE));
            }
            // 证件号
            Object certNumber = row.getField("CERT_NUM");
            if (certNumber != null) {
                rowModel.setField("CERT_NUM", NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,
                        certNumber.toString()));
            }

            if (certType != null && certNumber != null) {
                // 乘机人tid 通过证件号 这里查询的 LYC CERT NO MOBILE 未经认证 无法一起生成tid
                Tid psgTid = new Tid();
                psgTid.setTid(row.getField("CERT_NUM").toString());
                Map<String, String> certMap = new HashMap<>();
                certMap.put(row.getField("CERT_TYPE").toString(), row.getField("CERT_NUM").toString());
                tid.setCertification(certMap);

                String psgTidStr = IdMapping.idMappingFunction(tid, "CERT");
                rowModel.setField("PSG_TID", psgTidStr);
            }

            return rowModel;
        });



        OtiOfiOpiToBookingSegFactTrans.output(rowDataStream);
        OtiOfiOpiToBookingPnrFactTrans.output(rowDataStream);
        OtiOfiOpiToTickingTicFactTrans.output(rowDataStream);
        OtiOfiOpiToTickingSegFactTrans.output(rowDataStream);
        OtiOfiOpiToAuisSegFactTrans.output(rowDataStream);

    }
}
