package com.travelsky.trp.usercenter.data.analysis.transform.lyx;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.BookingPnrFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class OcriOfiOpiToBookingPnrFactTrans {

    static final Logger logger = LoggerFactory.getLogger(OcriOfiOpiToBookingPnrFactTrans.class);

    public static void output(DataStream<Row> rowDataStream) {

        //  使用 Async I/O 查询 Doris
        DataStream<Row> resultStream = AsyncDataStream
                .unorderedWait(
                        rowDataStream,                    // 输入流
                        new OcriOfiOpiAsync(),    // AsyncFunction
                        5000,                           // 超时：5秒
                        java.util.concurrent.TimeUnit.MILLISECONDS,
                        100                             // 并发请求数
                );

        // 后续可以进行 map、filter、sink 操作 过滤证件号 是null
        SingleOutputStreamOperator<BookingPnrFactModel> modelStream = resultStream.filter(row -> {
            boolean flag = row.getField("ORDER_TYPE") == null || row.getField("PNR_NO") == null;
            if (flag) {
                logger.info("入库机票预订-PNR级事实表异常，oriTable：T_ODS_LYX_ORDER_CHANGE_REFUND_INFO，" + row.toString());
            }
            return !flag;
        }).flatMap((Row row, Collector<BookingPnrFactModel> out) -> {
            if ("1".equals(row.getField("ORDER_TYPE").toString())) {
                BookingPnrFactModel model = new BookingPnrFactModel();
                // 创建时间
                String createDate = LocalDateTime.parse(row.getField("CREATE_TIME").toString()).
                        format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                // id 取值
                model.setPkId(row.getField("PNR_NO").toString() + createDate);

                model.setChannelOrderId(row.getField("ORDER_NO").toString());
                model.setAkChannel(row.getField("ORDER_SOURCE").toString());
                // TID
                model.setFkBookingUserTid(row.getField("TID") == null ? null : row.getField("TID").toString());

                model.setFkBookingUserOriginId(row.getField("LY_CARD") == null ? null : row.getField("LY_CARD").toString());
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
            }

        }).returns(BookingPnrFactModel.class);

        DorisSink<BookingPnrFactModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_BOOKING_PNR_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        modelStream.sinkTo(dorisSink);

    }
}
