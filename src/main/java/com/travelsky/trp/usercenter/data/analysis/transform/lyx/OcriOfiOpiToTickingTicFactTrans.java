package com.travelsky.trp.usercenter.data.analysis.transform.lyx;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.BookingSegFactModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingTicFactModel;
import com.travelsky.trp.usercenter.data.analysis.utils.DorisAsyncFirstIssueFunction;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class OcriOfiOpiToTickingTicFactTrans {

    static final Logger logger = LoggerFactory.getLogger(OcriOfiOpiToTickingTicFactTrans.class);

    public static void output(DataStream<Row> rowDataStream) {

        SingleOutputStreamOperator<TickingTicFactModel> modelStream = rowDataStream.filter(row -> {
            boolean flag = row.getField("ORDER_TYPE") == null ;
            if (flag) {
                logger.info("入库机票出票-客票级事实表异常，oriTable：T_ODS_LYX_ORDER_CHANGE_REFUND_INFO，" + row.toString());
            }
            return !flag;
        }).flatMap((Row row, Collector<TickingTicFactModel> out) -> {
            if ("1".equals(row.getField("ORDER_TYPE").toString())
                    && null != row.getField("TICKET_TIME")) {
                // 鲁雁行改期 ORDER_TYPE='1'
                TickingTicFactModel model = new TickingTicFactModel();
                // 出票时间
                String tkcietTime = LocalDateTime.parse(row.getField("TICKET_TIME").toString()).
                        format(TransUtils.DATE_FORMATTER_TWO);
                model.setPkId(tkcietTime + row.getField("TICKET_NUM").toString());

                model.setAkOrdernum(row.getField("ORDER_NO").toString());
                model.setAkChannel(row.getField("ORDER_SOURCE").toString());

                // TID
                model.setFkBookingUserTid(row.getField("TID").toString());

                model.setFkBookingUserOriginId(row.getField("LY_CARD").toString());
                // 联系人
                model.setContactName(row.getField("CONTACT_NAME").toString());
                model.setContactMobileNumber(row.getField("CONTACT_NO").toString());

                // 时间
                String updateTime = row.getField("UPDATE_TIME") == null ? row.getField("CREATE_TIME").toString()
                        : row.getField("UPDATE_TIME").toString();
                model.setSourceLastUpdatetime(updateTime.replace("-", "")
                        .replace(" ", "").replace(":", ""));
                model.setSystemCreatetime(LocalDateTime.now().toString());
                model.setSystemLastUpdatetime(LocalDateTime.now().toString());
                out.collect(model);
            } else if ("1".equals(row.getField("ORDER_TYPE").toString())){
                logger.info("入库机票出票-客票级事实表异常，oriTable：T_ODS_LYX_ORDER_CHANGE_REFUND_INFO，" + row.toString());
            }


        }).returns(TickingTicFactModel.class);

        //  异步后处理 查询是否首次出票
        DataStream<TickingTicFactModel> modelResultStream = AsyncDataStream
                .unorderedWait(
                        modelStream,                    // 输入流
                        new DorisAsyncFirstIssueFunction(),    // AsyncFunction
                        5000,                           // 超时：5秒
                        java.util.concurrent.TimeUnit.MILLISECONDS,
                        100                             // 并发请求数
                );

        DorisSink<TickingTicFactModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_TICKING_TIC_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        modelResultStream.sinkTo(dorisSink);


    }
}
