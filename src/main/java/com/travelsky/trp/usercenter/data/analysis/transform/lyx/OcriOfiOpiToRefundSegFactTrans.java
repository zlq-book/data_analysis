package com.travelsky.trp.usercenter.data.analysis.transform.lyx;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.RefundSegFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class OcriOfiOpiToRefundSegFactTrans {

    static final Logger logger = LoggerFactory.getLogger(OcriOfiOpiToRefundSegFactTrans.class);

    public static void output(DataStream<Row> rowDataStream) {


        SingleOutputStreamOperator<RefundSegFactModel> modelStream = rowDataStream.filter(row -> {
            boolean flag = row.getField("ORDER_TYPE") == null ;
            if (flag) {
                logger.info("入库机票退票-航段级事实表异常，oriTable：T_ODS_LYX_ORDER_CHANGE_REFUND_INFO，" + row.toString());
            }
            return !flag;
        }).flatMap((Row row, Collector<RefundSegFactModel> out) -> {
            if ("2".equals(row.getField("ORDER_TYPE").toString()) && null != row.getField("TICKET_NUM")
                    && null != row.getField("DEPATURE_TIME") && null != row.getField("ORIG")) {
                RefundSegFactModel model = new RefundSegFactModel();
                // 出票时间
                String departureTime = LocalDateTime.parse(row.getField("DEPATURE_TIME").toString()).
                        format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                model.setPkId(row.getField("TICKET_NUM").toString()
                        + row.getField("ORIG").toString() + departureTime);

                model.setAkRefundChannel("SCLYX");
                model.setAkRefundType(row.getField("REFUND_TYPE") == null ? null :
                        row.getField("REFUND_TYPE").toString());
                model.setRefundReason(row.getField("NOVOLUNTEER_REASON") == null ? null :
                        row.getField("NOVOLUNTEER_REASON").toString());
                // TID
                model.setFkRefundUserTid(row.getField("TID").toString());

                model.setFkRefundUserOriginId(row.getField("LY_CARD").toString());

                model.setRefundDate(row.getField("AUDIT_SECOND_TIME") == null? null :row.getField("AUDIT_SECOND_TIME").toString().substring(0, 10));
                model.setRefundTime(row.getField("AUDIT_SECOND_TIME") == null? null :row.getField("AUDIT_SECOND_TIME").toString().substring(11));
                model.setAkCurrency("CNY");
                model.setSegRefundamount(row.getField("REFUND_CASH") == null? null :
                        Double.parseDouble(row.getField("REFUND_CASH") .toString()));

                // 时间
                String updateTime = row.getField("UPDATE_TIME") == null ? row.getField("CREATE_TIME").toString()
                        : row.getField("UPDATE_TIME").toString();
                model.setSourceLastUpdatetime(updateTime.replace("-", "")
                        .replace(" ", "").replace(":", ""));
                model.setSystemCreatetime(LocalDateTime.now().toString());
                model.setSystemLastUpdatetime(LocalDateTime.now().toString());
                out.collect(model);
            } else if ("2".equals(row.getField("ORDER_TYPE").toString())){
                logger.info("入库机票退票-航段级事实表异常，oriTable：T_ODS_LYX_ORDER_CHANGE_REFUND_INFO，" + row.toString());
            }


        }).returns(RefundSegFactModel.class);

        DorisSink<RefundSegFactModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_REFUND_SEG_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        modelStream.sinkTo(dorisSink);


    }
}
