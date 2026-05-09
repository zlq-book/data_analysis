package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.DocumentUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.XpathUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.HsdProcessDataModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingSegFactModel;
import com.travelsky.trp.usercenter.data.analysis.utils.CustomJsonSerializer;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.time.LocalDateTime;

/**
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/15  16:43
 */
public class Void2OpenFactTrans {

    static final Logger logger = LoggerFactory.getLogger(Void2OpenFactTrans.class);

    public static void output(DataStream<String> source) {

        SingleOutputStreamOperator<HsdProcessDataModel> void2OpenStream = source.flatMap((String value,
                                                                                          Collector<HsdProcessDataModel> out) -> {
            Document document;
            try {
                document = DocumentUtils.string2Document(value);
            } catch (Exception e) {
                logger.warn("convert document is error, the content is {}, the error is {}", value, e.getMessage());
                return;
            }
            // 事件名称
            String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
            if (!Constants.EVENT_VOID2OPEN.equals(event)) {
                return;
            }
            logger.info("高频-客票取消废票事件数据信息整合");
            // 子事件名称
            String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
            String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
            String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
            // 票号
            String ticketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@TicketNumber");

            NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
            for (int a = 0; a < segmentList.getLength(); a++) {
                Node segment = segmentList.item(a);
                // 起飞机场
                String depairport = XpathUtils.getString(segment, "Departure/@AirportCode");
                // 航班起飞日期
                String departureDate = XpathUtils.getString(segment, "Departure/@Date");
                // 航段状态
                String couponStatus = XpathUtils.getString(segment, "@CouponStatus");

                TickingSegFactModel tickingSegFactModel = new TickingSegFactModel();
                // pkId 票号+起飞机场+起飞日期
                tickingSegFactModel.setPkId(ticketNumber + depairport + departureDate.replace("-",""));
                // 航段状态
                tickingSegFactModel.setSegmentStatus(couponStatus);
                tickingSegFactModel.setDataActive(true);
                tickingSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
                tickingSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
                // 本系统最后更新日期时间
                tickingSegFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

                HsdProcessDataModel hsdProcessDataModel = new HsdProcessDataModel();
                hsdProcessDataModel.setEvent(event);
                hsdProcessDataModel.setSubEvent(subEvent);
                hsdProcessDataModel.setTableName("T_DWD_TICKING_SEG_FACT");
                hsdProcessDataModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
                hsdProcessDataModel.setStamp(stamp);
                // 转换为JSON字符串
                hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(tickingSegFactModel));
                hsdProcessDataModel.setProcessed(false);
                hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());

                // 输出实体对象
                if (StringUtils.isNotEmpty(tickingSegFactModel.getPkId())) {
                    out.collect(hsdProcessDataModel);
                }else{
                    logger.info("高频-客票取消废票事件数据信息整合[TickingSegFactModel]数据解析异常{}",value);
                }
            }

        }).returns(TypeInformation.of(HsdProcessDataModel.class)).name("Void2OpenFlatMap").setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        // 输出结果查看
//        void2OpenStream.print("void2OpenStream");
        // 创建 Doris Sink 并写入
        DorisSink<HsdProcessDataModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_HSD_PROCESS_DATA");
        //数据写入doris
        void2OpenStream.sinkTo(dorisSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
    }

    public static void processVoid2Open(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
        /*if (!Constants.EVENT_VOID2OPEN.equals(event)) {
            return;
        }*/
        logger.info("高频-客票取消废票事件数据信息整合");
        // 子事件名称
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
        // 票号
        String ticketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@TicketNumber");

        NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
        for (int a = 0; a < segmentList.getLength(); a++) {
            Node segment = segmentList.item(a);
            // 起飞机场
            String depairport = XpathUtils.getString(segment, "Departure/@AirportCode");
            // 航班起飞日期
            String departureDate = XpathUtils.getString(segment, "Departure/@Date");
            // 航段状态
            String couponStatus = XpathUtils.getString(segment, "@CouponStatus");

            TickingSegFactModel tickingSegFactModel = new TickingSegFactModel();
            // pkId 票号+起飞机场+起飞日期
            tickingSegFactModel.setPkId(ticketNumber + depairport + departureDate.replace("-",""));
            // 航段状态
            tickingSegFactModel.setSegmentStatus(couponStatus);
            tickingSegFactModel.setDataActive(true);
            tickingSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
            tickingSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
            // 本系统最后更新日期时间
            tickingSegFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

            HsdProcessDataModel hsdProcessDataModel = new HsdProcessDataModel();
            hsdProcessDataModel.setEvent(event);
            hsdProcessDataModel.setSubEvent(subEvent);
            hsdProcessDataModel.setTableName("T_DWD_TICKING_SEG_FACT");
            hsdProcessDataModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
            hsdProcessDataModel.setStamp(stamp);
            // 转换为JSON字符串
            hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(tickingSegFactModel));
            hsdProcessDataModel.setProcessed(false);
            hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());

            // 输出实体对象
            if (StringUtils.isNotEmpty(tickingSegFactModel.getPkId())) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
            }else{
                logger.info("高频-客票取消废票事件数据信息整合[TickingSegFactModel]数据解析异常{}",value);
            }
        }
    }
}
