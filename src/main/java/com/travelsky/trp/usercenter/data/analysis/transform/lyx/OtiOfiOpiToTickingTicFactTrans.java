package com.travelsky.trp.usercenter.data.analysis.transform.lyx;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingTicFactModel;
import com.travelsky.trp.usercenter.data.analysis.utils.DorisAsyncFirstIssueFunction;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class OtiOfiOpiToTickingTicFactTrans {

    static final Logger logger = LoggerFactory.getLogger(OtiOfiOpiToTickingTicFactTrans.class);

    public static void output(DataStream<Row> rowDataStream) {

        SingleOutputStreamOperator<TickingTicFactModel> modelStream = rowDataStream.filter(row -> {
            boolean flag = row.getField("TICKET_TIME") == null;
            if (flag) {
                logger.info("入库机票出票-客票级事实表异常，oriTable：T_ODS_LYX_ORDER_TICKET_INFO，" + row.toString());
            }
            return !flag;
        }).map(row -> {
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
            return model;

        });

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
