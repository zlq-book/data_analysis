package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.DocumentUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.XpathUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.CheckinSegFactModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.HsdProcessDataModel;
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
public class BagChangeTrans {

    static final Logger logger = LoggerFactory.getLogger(BagChangeTrans.class);

    public static void output(DataStream<String> source) {

        SingleOutputStreamOperator<HsdProcessDataModel> bagChangeStream = source.flatMap((String value, Collector<HsdProcessDataModel> out) -> {

            Document document;
            try {
                document = DocumentUtils.string2Document(value);
            } catch (Exception e) {
                logger.warn("convert document is error, the content is {}, the error is {}", value, e.getMessage());
                return;
            }
            // 事件名称
            String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
            if (!Constants.EVENT_BAGCHANGE.equals(event)) {
                return;
            }
            logger.info("高频-行李变更事件数据信息整合");
            // 子事件名称
            String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
            String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
            String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
            // 票号
            String ticketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@TicketNumber");

            // 行李总件数
            Integer totalBaggageCount = XpathUtils.getNumber(document,
                    "sum(/Msg/Dat/PassengerSegment/DepartureInfo/BaggageInfo/Baggage/@CheckInCount)").intValue();
            // 行李总重量
            Double totalBaggageWeight = XpathUtils.getNumber(document,
                    "sum(/Msg/Dat/PassengerSegment/DepartureInfo/BaggageInfo/Baggage/@CheckInWeight)");
            NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
            for (int a = 0; a < segmentList.getLength(); a++) {
                Node segment = segmentList.item(a);
                // 航班起飞日期
                String departureDate = XpathUtils.getString(segment, "Departure/@Date").replace("-", "");
                // 起飞机场
                String depAirport = XpathUtils.getString(segment, "Departure/@AirportCode");
                // 降落机场
                String arriAirport = XpathUtils.getString(segment, "Arrival/@AirportCode");

                // 构建每个 Segment 的实体对象
                CheckinSegFactModel checkinSegFactModel = new CheckinSegFactModel();
                // 航班起飞日期 + 票号 + 起飞机场 + 到达机场 9.7改成 票号 + 起飞机场 + 起飞日期
                checkinSegFactModel.setPkId(ticketNumber + depAirport + departureDate);
                // 行李总件数
                checkinSegFactModel.setSegBaggagecount(totalBaggageCount);
                // 行李总重量
                checkinSegFactModel.setSegBaggageweight(totalBaggageWeight);
                checkinSegFactModel.setDataActive(true);
                checkinSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
                checkinSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
                // 本系统最后更新日期时间
                checkinSegFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

                HsdProcessDataModel hsdProcessDataModel = new HsdProcessDataModel();
                hsdProcessDataModel.setEvent(event);
                hsdProcessDataModel.setSubEvent(subEvent);
                hsdProcessDataModel.setTableName("T_DWD_CHECKIN_SEG_FACT");
                hsdProcessDataModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
                hsdProcessDataModel.setStamp(stamp);
                // 转换为JSON字符串
                hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(checkinSegFactModel));
                hsdProcessDataModel.setProcessed(false);
                hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());

                if(StringUtils.isNotEmpty(checkinSegFactModel.getPkId())) {
                    out.collect(hsdProcessDataModel);
                }else{
                    logger.info("高频-行李变更事件数据信息整合[CheckinSegFactModel]数据解析异常{}",value);
                }
            }


        }).returns(TypeInformation.of(HsdProcessDataModel.class)).name("BagChangeFlatMap").setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        // 输出结果查看
//        bagChangeStream.print("bagChangeStream");
        //创建dorisSink
        // 4. 创建 Doris Sink 并写入
        DorisSink<HsdProcessDataModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_HSD_PROCESS_DATA");
        //数据写入doris
        bagChangeStream.sinkTo(dorisSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
    }

    public static void processBagChange(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
        /*if (!Constants.EVENT_BAGCHANGE.equals(event)) {
            return;
        }*/
        logger.info("高频-行李变更事件数据信息整合");
        // 子事件名称
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
        // 票号
        String ticketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@TicketNumber");

        // 行李总件数
        Integer totalBaggageCount = XpathUtils.getNumber(document,
                "sum(/Msg/Dat/PassengerSegment/DepartureInfo/BaggageInfo/Baggage/@CheckInCount)").intValue();
        // 行李总重量
        Double totalBaggageWeight = XpathUtils.getNumber(document,
                "sum(/Msg/Dat/PassengerSegment/DepartureInfo/BaggageInfo/Baggage/@CheckInWeight)");
        NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
        for (int a = 0; a < segmentList.getLength(); a++) {
            Node segment = segmentList.item(a);
            // 航班起飞日期
            String departureDate = XpathUtils.getString(segment, "Departure/@Date").replace("-", "");
            // 起飞机场
            String depAirport = XpathUtils.getString(segment, "Departure/@AirportCode");
            // 降落机场
            String arriAirport = XpathUtils.getString(segment, "Arrival/@AirportCode");

            // 构建每个 Segment 的实体对象
            CheckinSegFactModel checkinSegFactModel = new CheckinSegFactModel();
            // 航班起飞日期 + 票号 + 起飞机场 + 到达机场 9.7改成 票号 + 起飞机场 + 起飞日期
            checkinSegFactModel.setPkId(ticketNumber + depAirport + departureDate);
            // 行李总件数
            checkinSegFactModel.setSegBaggagecount(totalBaggageCount);
            // 行李总重量
            checkinSegFactModel.setSegBaggageweight(totalBaggageWeight);
            checkinSegFactModel.setDataActive(true);
            checkinSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
            checkinSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
            // 本系统最后更新日期时间
            checkinSegFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

            HsdProcessDataModel hsdProcessDataModel = new HsdProcessDataModel();
            hsdProcessDataModel.setEvent(event);
            hsdProcessDataModel.setSubEvent(subEvent);
            hsdProcessDataModel.setTableName("T_DWD_CHECKIN_SEG_FACT");
            hsdProcessDataModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
            hsdProcessDataModel.setStamp(stamp);
            // 转换为JSON字符串
            hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(checkinSegFactModel));
            hsdProcessDataModel.setProcessed(false);
            hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());

            if(StringUtils.isNotEmpty(checkinSegFactModel.getPkId())) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
            }else{
                logger.info("高频-行李变更事件数据信息整合[CheckinSegFactModel]数据解析异常{}",value);
            }
        }
    }
}
