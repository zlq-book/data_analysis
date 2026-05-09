package com.travelsky.trp.usercenter.data.analysis.transform.lyx;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingSegFactModel;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class OtiOfiOpiToTickingSegFactTrans {

    static final Logger logger = LoggerFactory.getLogger(OtiOfiOpiToTickingSegFactTrans.class);

    public static void output(DataStream<Row> rowDataStream) {

        SingleOutputStreamOperator<TickingSegFactModel> modelStream = rowDataStream.filter(row -> {
            boolean flag = row.getField("TICKET_NUM") == null || row.getField("DEPATURE_TIME") == null
                    || row.getField("ORIG") == null;
            if (flag) {
                logger.info("入库机票出票-航段级事实表异常，oriTable：T_ODS_LYX_ORDER_TICKET_INFO，" + row.toString());
            }
            return !flag;
        }).map(row -> {
            TickingSegFactModel model = new TickingSegFactModel();
            // 出票时间
            String departureTime = LocalDateTime.parse(row.getField("DEPATURE_TIME").toString()).
                    format(TransUtils.DATE_FORMATTER_TWO);
            model.setPkId(row.getField("TICKET_NUM").toString()
                    + row.getField("ORIG").toString() + departureTime);

            model.setAkChannel(row.getField("ORDER_SOURCE").toString());
            model.setChannelOrderno(row.getField("ORDER_NO").toString());
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
            model.setSystemLastUpdatetime(LocalDateTime.now().toString());
            model.setSystemCreatetime(LocalDateTime.now().toString());
            return model;

        });

        DorisSink<TickingSegFactModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_TICKING_SEG_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        modelStream.sinkTo(dorisSink);


    }
}
