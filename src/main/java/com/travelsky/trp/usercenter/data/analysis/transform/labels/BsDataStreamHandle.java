package com.travelsky.trp.usercenter.data.analysis.transform.labels;

import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.BookingPnrFactModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.BookingSegFactModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingTicFactModel;
import com.travelsky.trp.usercenter.data.analysis.transform.hsd.CommonOutputTags;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class BsDataStreamHandle {
    static final Logger logger = LoggerFactory.getLogger(BsDataStreamHandle.class);

    public static void output(DataStream<Row> rowStream) {
        Map<String,Boolean> peerSeniorMap = new HashMap<>();
        Map<String,Boolean> chdMap = new HashMap<>();
        Map<String,Set<String>> tidMap = new HashMap<>();

        DataStream<Row> rowDataStream = rowStream.filter(row ->
                row.getField("PNR_NUMBER") != null
        ).map(row -> {
            String pnr = row.getField("PNR_NUMBER").toString();
            // 是否老人同行
            boolean isPeerSenior = false;
            if (null != row.getField("PEER_SENIOR")
                    && "1".equals(row.getField("PEER_SENIOR").toString())) {
                isPeerSenior = true;
            }
            if (null == peerSeniorMap.get(pnr)) {
                peerSeniorMap.put(pnr, isPeerSenior);
            } else {
                if (!peerSeniorMap.get(pnr)) {
                    peerSeniorMap.put(pnr, isPeerSenior);
                }
            }
            // 是否儿童同行
            boolean isChd = false;
            if (null != row.getField("AK_PEER_CHD")
                    && "1".equals(row.getField("AK_PEER_CHD").toString())) {
                isChd = true;
            }
            if (null == chdMap.get(pnr)) {
                chdMap.put(pnr, isChd);
            } else {
                if (!chdMap.get(pnr)) {
                    chdMap.put(pnr, isChd);
                }
            }

            // 乘机人列表
            Set<String> tidSet = tidMap.get(pnr);
            if (null != row.getField("FK_PASSENGER_USER_TID")) {
                if (null == tidSet) {
                    tidSet = new HashSet<>();
                }
                tidSet.add(row.getField("FK_PASSENGER_USER_TID").toString());
                tidMap.put(pnr, tidSet);
            }

            return row;
        });

        // 根据pnr 分组处理
        SingleOutputStreamOperator<Void> processedStream = rowDataStream.process(new ProcessFunction<Row, Void>() {
            @Override
            public void processElement(Row row, Context ctx, Collector<Void> out) throws Exception {
                BookingSegFactModel model = new BookingSegFactModel();
                model.setPkId(row.getField("PK_ID").toString());
                model.setSystemLastUpdatetime(LocalDateTime.now().toString());
                String pnr = row.getField("PNR_NUMBER").toString();
                model.setPeerSenior(peerSeniorMap.get(pnr));
                model.setAkPeerChd(chdMap.get(pnr));
                // 是否自己订票
                boolean isSelf = false;
                if (null != row.getField("FK_PASSENGER_USER_TID")
                        && null != row.getField("FK_BOOKING_USER_TID")
                        && row.getField("FK_PASSENGER_USER_TID").toString()
                        .equals(row.getField("FK_BOOKING_USER_TID").toString())) {
                    isSelf = true;
                }
                model.setSelfBooking(isSelf);
                // 乘机人列表
                if (null != row.getField("FK_BOOKING_USER_TID")) {
                    Set<String> tidSet = tidMap.get(pnr);
                    if (null != tidSet && tidSet.contains(row.getField("FK_BOOKING_USER_TID").toString())) {
                        model.setAkBookerIsPassenger(true);
                    }
                }

                // 预定pnr可以拼主键 直接写
                if (null != row.getField("FK_BOOKING_DATE")) {
                    BookingPnrFactModel bookingPnrFactModel = new BookingPnrFactModel();
                    bookingPnrFactModel.setPkId(pnr +
                            row.getField("FK_BOOKING_DATE").toString().replace("-", ""));
                    bookingPnrFactModel.setPeerSenior(model.getPeerSenior());
                    bookingPnrFactModel.setPeerChd(model.getAkPeerChd());
                    bookingPnrFactModel.setAkBookerIsPassenger(model.getAkBookerIsPassenger());
                    bookingPnrFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());
                    ctx.output(CommonOutputTags.BOOKING_PNR_TAG, bookingPnrFactModel);
                }

                ctx.output(CommonOutputTags.BOOKING_SEG_TAG, model);

            }
        });


        // 输出结果查看
        DataStream<BookingSegFactModel> dwdStream = processedStream.getSideOutput(CommonOutputTags.BOOKING_SEG_TAG);
        DataStream<BookingPnrFactModel> bookingPnrStream = processedStream.getSideOutput(CommonOutputTags.BOOKING_PNR_TAG);
        // 创建 Doris Sink 并写入
        dwdStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_BOOKING_SEG_FACT"));
        bookingPnrStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_BOOKING_PNR_FACT"));
    }
}
