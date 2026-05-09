package com.travelsky.trp.usercenter.data.analysis.transform.lyx;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.PaydetailsOrdFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class OcriOpiCashToPayDetailsOrdFactTrans {

    static final Logger logger = LoggerFactory.getLogger(OcriOpiCashToPayDetailsOrdFactTrans.class);

    public static void output(DataStream<Row> rowDataStream) {

        // 现金支付 CASH_PAY_STATUS IS NOT NULL 不等于0 需要过滤
        SingleOutputStreamOperator<PaydetailsOrdFactModel> modelStream = rowDataStream.filter(row ->
                row.getField("CASH_PAY_STATUS") != null
                        && !"0".equals(row.getField("CASH_PAY_STATUS").toString())
        ).flatMap((Row row, Collector<PaydetailsOrdFactModel> out) -> {
            if (row.getField("CASH_PAY_NO") != null) {
                PaydetailsOrdFactModel model = new PaydetailsOrdFactModel();

                model.setBusinessOrderId(row.getField("ORDER_NO").toString());
                model.setPayNo(row.getField("CASH_PAY_NO").toString());
                model.setPkId("SCLYX" + model.getBusinessOrderId() + model.getPayNo());
                //  tid
                model.setFkBookingUserTid(row.getField("TID") == null ? null
                        :row.getField("TID").toString());
                model.setFkBookingUserOriginId(row.getField("LY_CARD") == null ? null
                        :row.getField("LY_CARD").toString());

                model.setFkBookingDate(row.getField("CREATE_TIME").toString().substring(0, 10));
                model.setFkBookingTime(row.getField("CREATE_TIME").toString().substring(11));
                // 现金 鲁雁值 分情况 set
                model.setFkPaymentDate(row.getField("CASH_PAY_TIME") == null ? null
                        :row.getField("CASH_PAY_TIME").toString().substring(0, 10));
                model.setFkPaymentTime(row.getField("CASH_PAY_TIME") == null ? null
                        :row.getField("CASH_PAY_TIME").toString().substring(11));
                model.setAkPayChannel("SCLYX");
                model.setAkPaymentMethod("现金");

                // 平台
                model.setAkCashPlatform(row.getField("PAY_WAY") == null ? null :row.getField("PAY_WAY").toString());
                model.setAkPaymentStatus(row.getField("CASH_PAY_STATUS").toString());
                // 币种
                model.setAkCashCurrency("CNY");
                model.setCashAmount(row.getField("CASH_SUM") == null ? null :
                        BigDecimal.valueOf(Double.parseDouble(row.getField("CASH_SUM").toString())));
                model.setLyvalueAmount(row.getField("LY_SUM") == null ? null :
                        BigDecimal.valueOf(Double.parseDouble(row.getField("LY_SUM").toString())));


                model.setOrdCount(1);
                String updateTime = row.getField("UPDATE_TIME") == null ? null
                        : row.getField("UPDATE_TIME").toString();
                if (null != updateTime) {
                    model.setSourceLastUpdatetime(updateTime.replace("-", "")
                            .replace(" ", "").replace(":", ""));
                }
                model.setSystemCreatetime(LocalDateTime.now().toString());
                model.setSystemLastUpdatetime(LocalDateTime.now().toString());
                out.collect(model);
            } else {
                logger.info("鲁雁行现金支付缺少流水号异常，oriTable：T_ODS_LYX_ORDER_CHANGE_REFUND_INFO，" + row.toString());
            }

        }).returns(PaydetailsOrdFactModel.class);

        DorisSink<PaydetailsOrdFactModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_PAYDETAILS_ORD_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        modelStream.sinkTo(dorisSink);

    }
}
