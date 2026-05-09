package com.travelsky.trp.usercenter.data.analysis.transform.lyx;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.BookingSegFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


public class OtiOfiOpiToBookingSegFactTrans {

    static final Logger logger = LoggerFactory.getLogger(OtiOfiOpiToBookingSegFactTrans.class);

    public static void output(DataStream<Row> rowDataStream) {

        // 后续可以进行 map、filter、sink 操作 过滤证件号 是null
        SingleOutputStreamOperator<BookingSegFactModel> modelStream = rowDataStream.filter(row -> {
            boolean flag = row.getField("DEPATURE_TIME") == null || row.getField("PNR_NO") == null
                    || row.getField("ORIG") == null || row.getField("CERT_NUM") == null;
            if (flag) {
                logger.info("入库机票预订-航段级事实表异常，oriTable：T_ODS_LYX_ORDER_TICKET_INFO，" + row.toString());
            }
            return !flag;
        }).flatMap((Row row, Collector<BookingSegFactModel> out) -> {
            BookingSegFactModel model = new BookingSegFactModel();
            // 航班时间
            String flightDate = LocalDateTime.parse(row.getField("DEPATURE_TIME").toString()).
                    format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            // 旅客姓名
            String psgName = row.getField("PSG_NAME_CN") == null ?
                    (row.getField("PSG_NAME_EN") == null ? "" : row.getField("PSG_NAME_EN").toString())
                    : row.getField("PSG_NAME_CN").toString() ;
            // id 取值
            model.setPkId(row.getField("PNR_NO").toString() + row.getField("ORIG").toString()
                    + flightDate + row.getField("CERT_NUM").toString() + psgName);


            model.setAkChannel(row.getField("ORDER_SOURCE").toString());
            model.setChannelOrderId(row.getField("ORDER_NO").toString());
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

        }).returns(BookingSegFactModel.class);

        DorisSink<BookingSegFactModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_BOOKING_SEG_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        modelStream.sinkTo(dorisSink);

    }
}
