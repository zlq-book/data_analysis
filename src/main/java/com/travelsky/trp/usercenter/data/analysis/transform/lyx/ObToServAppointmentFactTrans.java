package com.travelsky.trp.usercenter.data.analysis.transform.lyx;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.ServAppointmentFactModal;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ObToServAppointmentFactTrans {

    static final Logger logger = LoggerFactory.getLogger(ObToServAppointmentFactTrans.class);

    public static void result(StreamTableEnvironment tEnv, String etlDate) {
        //创建Doris目标表
        tEnv.executeSql(CreateTableSql.LYX_ORDER_BASE);

        // 从 Doris 表中读取数据
        Table dorisTable = tEnv.sqlQuery( "SELECT " +
                "ORDER_NO,\n" +
                "ORDER_STATUS,\n" +
                "ORDER_TYPE,\n" +
                "PRODUCT_NAME,\n" +
                "ORDER_TIME,\n" +
                "ORDER_ORIGIN,\n" +
                "CURRENT_DEALER,\n" +
                "DEAL_TIME,\n" +
                "PSG_NAME,\n" +
                "VIP_IF,\n" +
                "VIP_LEVEL,\n" +
                "VIP_TYPE,\n" +
                "CERTIFY_TYPE,\n" +
                "CERTIFY_NUM,\n" +
                "CERTIFY_NUM TID,\n" +
                "PHONE_NUM,\n" +
                "TICKET_NUM,\n" +
                "TICKET_STATUS,\n" +
                "ISSUE_OFFICE,\n" +
                "BOOK_OFFICE,\n" +
                "FLT_NUM,\n" +
                "FLT_DATE,\n" +
                "ORIG,\n" +
                "DEST,\n" +
                "LAUNCH_TIME,\n" +
                "ARRIVE_TIME,\n" +
                "FLT_STATUS,\n" +
                "TICKET_PRICE,\n" +
                "CABIN_CLASS,\n" +
                "DISCUSS_STATUS,\n" +
                "DISCUSS_CONTENT,\n" +
                "DISCUSS_LEVEL,\n" +
                "DISCUSS_TIME,\n" +
                "TICKET_FLAG,\n" +
                "ORDER_OLD_NO,\n" +
                "ID,\n" +
                "UPDATE_TIME \n" +
                " FROM T_ODS_LYX_ORDER_BASE "
                + " WHERE ETL_DATE='" + etlDate + "'"
        );

        // 2. 将 Table 转换为 DataStream<Row>
        DataStream<Row> rowStream = tEnv.toDataStream(dorisTable);

        // 标准化 IdMapping
        DataStream<Row> rowDataStream = rowStream.map(row -> {
            Row rowModel = Row.copy(row);

            // 证件类型
            Object certType = row.getField("CERTIFY_TYPE");
            if (certType != null) {
                rowModel.setField("CERTIFY_TYPE", NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE,
                        certType.toString(), DataSource.LUYAN_TRIP));
            }
            // 证件号
            Object certNumber = row.getField("CERTIFY_NUM");
            if (certNumber != null) {
                rowModel.setField("CERTIFY_NUM", NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,
                        certNumber.toString()));
            }
            // 预定渠道
            Object orderChannel = row.getField("ORDER_ORIGIN");
            if (orderChannel != null) {
                rowModel.setField("ORDER_ORIGIN", NormalizationUtils.standardize(FieldType.ORDER_CHANNEL,
                        orderChannel.toString()));
            }

            if (certType != null && certNumber != null) {
                // 乘机人tid 通过证件号 这里查询的 LYC CERT NO MOBILE 未经认证 无法一起生成tid
                Tid psgTid = new Tid();
                psgTid.setTid(row.getField("CERTIFY_NUM").toString());
                Map<String, String> certMap = new HashMap<>();
                certMap.put(row.getField("CERTIFY_TYPE").toString(), row.getField("CERTIFY_NUM").toString());
                psgTid.setCertification(certMap);

                String psgTidStr = IdMapping.idMappingFunction(psgTid, "CERT");
                rowModel.setField("TID", psgTidStr);
            }

            return rowModel;
        });

        // 后续可以进行 map、filter、sink 操作
        SingleOutputStreamOperator<ServAppointmentFactModal> modelStream = rowDataStream.map(row -> {
            ServAppointmentFactModal model = new ServAppointmentFactModal();
            model.setFkBookingDate(row.getField("ORDER_TIME")==null?null:row.getField("ORDER_TIME").toString().substring(0, 10));
            model.setFkBookingTime(row.getField("ORDER_TIME")==null?null:row.getField("ORDER_TIME").toString().substring(11));
            model.setOrderNo(row.getField("ORDER_NO")==null?null:row.getField("ORDER_NO").toString());
            model.setOrderStatus(row.getField("ORDER_STATUS")==null?null:row.getField("ORDER_STATUS").toString());
            model.setPassengerName(row.getField("PSG_NAME")==null?null:row.getField("PSG_NAME").toString());
            model.setCertType(row.getField("CERTIFY_TYPE")==null?null:row.getField("CERTIFY_TYPE").toString());
            model.setCertNumber(row.getField("CERTIFY_NUM")==null?null:row.getField("CERTIFY_NUM").toString());

            // TID T
            model.setPassengerUserTid(row.getField("TID")==null?null:row.getField("TID").toString());
            model.setBookingChannel(row.getField("ORDER_ORIGIN") == null ? null : row.getField("ORDER_ORIGIN").toString());
            model.setServiceItem(row.getField("ORDER_TYPE") == null ? null : row.getField("ORDER_TYPE").toString());
            model.setIssueOffice(row.getField("ISSUE_OFFICE")==null?null:row.getField("ISSUE_OFFICE").toString());
            model.setBookOffice(row.getField("BOOK_OFFICE")==null?null:row.getField("BOOK_OFFICE").toString());
            model.setTicketNumber(row.getField("TICKET_NUM")==null?null:row.getField("TICKET_NUM").toString());

            // 起飞机场
            model.setDepairport(row.getField("ORIG") == null ? null : row.getField("ORIG").toString());
            model.setArriairport(row.getField("DEST") == null ? null : row.getField("DEST").toString());
            model.setFlightNumber(row.getField("FLT_NUM") == null ? null : row.getField("FLT_NUM").toString());
            model.setFlightDate(row.getField("FLT_DATE") == null ? null : row.getField("FLT_DATE").toString());
            model.setDepartureTime(row.getField("LAUNCH_TIME") == null ? null : row.getField("LAUNCH_TIME").toString());
            model.setArrivalTime(row.getField("ARRIVE_TIME") == null ? null : row.getField("ARRIVE_TIME").toString());

            // 舱位
            model.setCabin(row.getField("CABIN_CLASS") == null ? null : row.getField("CABIN_CLASS").toString());
            model.setSegmentPrice(BigDecimal.valueOf(Double.parseDouble(row.getField("TICKET_PRICE").toString())));
            model.setReviewStatus(row.getField("DISCUSS_STATUS") == null ? null : row.getField("DISCUSS_STATUS").toString());
            model.setReviewContent(row.getField("DISCUSS_CONTENT") == null ? null : row.getField("DISCUSS_CONTENT").toString());
            model.setReviewLevel(row.getField("DISCUSS_LEVEL") == null ? null : Integer.parseInt(row.getField("DISCUSS_LEVEL").toString()));
            model.setReviewTime(row.getField("DISCUSS_TIME") == null ? null : row.getField("DISCUSS_TIME").toString());

            model.setTicketFlag(row.getField("TICKET_FLAG") == null ? null : row.getField("TICKET_FLAG").toString());
            model.setOriginalOrderno(row.getField("ORDER_OLD_NO") == null ? null : row.getField("ORDER_OLD_NO").toString());

            // id
            model.setPkId(row.getField("ID").toString());

            model.setBookingCount(1);
            model.setSourceLastUpdatetime(row.getField("UPDATE_TIME") == null ? null :
                    LocalDateTime.parse(row.getField("UPDATE_TIME").toString()).
                            format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")));
            model.setSystemCreatetime(LocalDateTime.now().toString());
            model.setSystemLastUpdatetime(LocalDateTime.now().toString());
            return model;
        });

        DorisSink<ServAppointmentFactModal> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_SERV_APPOINTMENT_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        modelStream.sinkTo(dorisSink);

    }
}
