package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.*;
import com.travelsky.trp.usercenter.data.analysis.utils.CustomJsonSerializer;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.commons.lang3.StringUtils;
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
public class FFChangeTrans {

    static final Logger logger = LoggerFactory.getLogger(FFChangeTrans.class);

    public static void output(DataStream<String> source) {

        SingleOutputStreamOperator<Void> processedStream = source.process(new ProcessFunction<String, Void>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Void> out) throws Exception {
                Document document;
                try {
                    document = DocumentUtils.string2Document(value);
                } catch (Exception e) {
                    logger.warn("convert document is error, the content is {}, the error is {}", value, e.getMessage());
                    return;
                }
                processFFChange(value, ctx, document);
            }
        }).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        DataStream<HsdProcessDataModel> hsdProcessDataStream = processedStream.getSideOutput(CommonOutputTags.HSD_PROCESS_DATA_TAG);
        //hsdProcessDataStream.print("hsdProcessDataStream");
        hsdProcessDataStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_HSD_PROCESS_DATA")).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        /*// 获取各个SideOutput流
        DataStream<TickingSegFactModel> tickingSegDataStream = processedStream.getSideOutput(CommonOutputTags.TICKING_SEG_TAG);
//        tickingSegDataStream.print("tickingSegDataStream");
        DataStream<TickingTicFactModel> tickingTicDataStream = processedStream.getSideOutput(CommonOutputTags.TICKING_TIC_TAG);
//        tickingTicDataStream.print("tickingTicDataStream");
        DataStream<BookingSegFactModel> bookingSegDataStream = processedStream.getSideOutput(CommonOutputTags.BOOKING_SEG_TAG);
//        bookingSegDataStream.print("bookingSegDataStream");

        // 创建并配置各模型的Doris Sink
        DorisSink<TickingSegFactModel> tickingSegSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_SEG_FACT");
        DorisSink<TickingTicFactModel> tickingTicSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_TIC_FACT");
        DorisSink<BookingSegFactModel> bookingSegSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_BOOKING_SEG_FACT");

        // 数据分别写入对应的Doris表
        tickingSegDataStream.sinkTo(tickingSegSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        tickingTicDataStream.sinkTo(tickingTicSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        bookingSegDataStream.sinkTo(bookingSegSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);*/
    }

    public static void processFFChange(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
                /*if (!Constants.EVENT_FFCHANGE.equals(event)) {
                    return;
                }*/
        logger.info("高频-旅客常卡变更事件数据信息整合");
        // 子事件名称
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
        // 改升后票号
        String ticketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo" + "/@TicketNumber");
        // 出票日期
        String issueDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@IssueDate");
        // PNR号
        String pnr = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@RecordLocator");
        // PNR创建日期
        String bookDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@BookDate").replace("-", "");
        String bookTime = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@BookTime").replace(":", "");
        // 乘机人英文姓
        String surname = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Surname");
        // 乘机人英文名
        String givenName = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/GivenName");
        String enName = NormalizationUtils.standardize(FieldType.EN_NAME, surname + givenName);
        // 乘机人中文姓名
        String nativeGivenName = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/NativeGivenName");
        nativeGivenName = NormalizationUtils.standardize(FieldType.CN_NAME, nativeGivenName);
        // 乘机人姓名
        String travellerName = StringUtils.isBlank(nativeGivenName) ? enName : nativeGivenName;
        // 乘机人证件号码
        String documentNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Number");

        NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
        for (int a = 0; a < segmentList.getLength(); a++) {
            Node segment = segmentList.item(a);
            // 航班起飞日期
            String departureDate = XpathUtils.getString(segment, "Departure/@Date");
            // 起飞机场
            String depAirport = XpathUtils.getString(segment, "Departure/@AirportCode");
            // 到达机场
            String arrivalAirport = XpathUtils.getString(segment, "Arrival/@AirportCode");
            // 常客卡号
            String ffrf = XpathUtils.getString(segment, "FrequentTraveler/@Number");
            // 常客等级
            String loyalLevel = XpathUtils.getString(segment, "FrequentTraveler/@LoyalLevel");
            // 常客卡航司
            String companyCode = XpathUtils.getString(segment, "FrequentTraveler/@CompanyCode");
            // 常客联盟卡级别
            String allianceLevel = XpathUtils.getString(segment, "FrequentTraveler/@AllianceLevel");

            // 构建每个 Segment 的实体对象
            // 机票预订-航段级
            BookingSegFactModel bookingSegFactModel = new BookingSegFactModel();
            // PNR编号+PNR创建日期+起飞机场+乘机人证件号（加密）+乘机人姓名（姓名，先取乘机人中文姓名，如果没有，取英文姓+英文名）
            bookingSegFactModel.setPkId(pnr + bookDate + bookTime + depAirport + NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,SM4Utils.encrypt(documentNumber, Constants.SM4_KEY)) + travellerName);
            String ffpNumber = NormalizationUtils.standardize(FieldType.FREQUENT_FLYER_NUM,SM4Utils.encrypt(ffrf,
                    Constants.SM4_KEY));
            bookingSegFactModel.setFfrf(ffpNumber);
            bookingSegFactModel.setFfLevel(loyalLevel);
            bookingSegFactModel.setFfAirline(companyCode);
            bookingSegFactModel.setFfAllianceLevel(allianceLevel);
            // 飞行时是否使用常客卡
            bookingSegFactModel.setUsedFfr(StringUtils.isNotBlank(ffrf));
            bookingSegFactModel.setDataActive(true);
            bookingSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
            bookingSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
            bookingSegFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

            HsdProcessDataModel hsdProcessBookingSegFactModel = new HsdProcessDataModel();
            hsdProcessBookingSegFactModel.setEvent(event);
            hsdProcessBookingSegFactModel.setSubEvent(subEvent);
            hsdProcessBookingSegFactModel.setTableName("T_DWD_BOOKING_SEG_FACT");
            hsdProcessBookingSegFactModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
            hsdProcessBookingSegFactModel.setStamp(stamp);
            // 转换为JSON字符串
            hsdProcessBookingSegFactModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(bookingSegFactModel));
            hsdProcessBookingSegFactModel.setProcessed(false);
            hsdProcessBookingSegFactModel.setUpdateTime(LocalDateTime.now().toString());

            if(StringUtils.isNotEmpty(bookingSegFactModel.getPkId())) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessBookingSegFactModel);
            }else{
                logger.info("高频-旅客常卡变更事件数据信息整合[BookingSegFactModel]数据解析异常{}", value);
            }

            // 机票出票-航段级
            TickingSegFactModel tickingSegFactModel = new TickingSegFactModel();
            // pkId 票号+起飞机场+起飞日期
            tickingSegFactModel.setPkId(ticketNumber + depAirport + departureDate.replace("-",""));
            tickingSegFactModel.setFfrf(ffpNumber);
            tickingSegFactModel.setUsedFfr(StringUtils.isNotBlank(ffpNumber));
            tickingSegFactModel.setFfLevel(loyalLevel);
            tickingSegFactModel.setFfAirline(companyCode);
            tickingSegFactModel.setFfAllianceLevel(allianceLevel);
            tickingSegFactModel.setDataActive(true);
            tickingSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
            tickingSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
            tickingSegFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

            HsdProcessDataModel hsdProcessTickingSegFactModel = new HsdProcessDataModel();
            hsdProcessTickingSegFactModel.setEvent(event);
            hsdProcessTickingSegFactModel.setSubEvent(subEvent);
            hsdProcessTickingSegFactModel.setTableName("T_DWD_TICKING_SEG_FACT");
            hsdProcessTickingSegFactModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
            hsdProcessTickingSegFactModel.setStamp(stamp);
            // 转换为JSON字符串
            hsdProcessTickingSegFactModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(tickingSegFactModel));
            hsdProcessTickingSegFactModel.setProcessed(false);
            hsdProcessTickingSegFactModel.setUpdateTime(LocalDateTime.now().toString());

            if(StringUtils.isNotEmpty(tickingSegFactModel.getPkId())) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessTickingSegFactModel);
            }else{
                logger.info("高频-旅客常卡变更事件数据信息整合[TickingSegFactModel]数据解析异常{}", value);
            }

            // 机票出票-客票级
            TickingTicFactModel tickingTicFactModel = new TickingTicFactModel();
            // 出票日期+票号
            tickingTicFactModel.setPkId(issueDate.replace("-","") + ticketNumber);
            tickingTicFactModel.setFfrf(ffpNumber);
            tickingTicFactModel.setFfLevel(loyalLevel);
            tickingTicFactModel.setFfAirline(companyCode);
            tickingTicFactModel.setFfAllianceLevel(allianceLevel);
            tickingTicFactModel.setDataActive(true);
            tickingTicFactModel.setDataActiveTime(LocalDateTime.now().toString());
            tickingTicFactModel.setSystemCreatetime(LocalDateTime.now().toString());
            tickingTicFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

            HsdProcessDataModel hsdProcessTickingTicFactModel = new HsdProcessDataModel();
            hsdProcessTickingTicFactModel.setEvent(event);
            hsdProcessTickingTicFactModel.setSubEvent(subEvent);
            hsdProcessTickingTicFactModel.setTableName("T_DWD_TICKING_TIC_FACT");
            hsdProcessTickingTicFactModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
            hsdProcessTickingTicFactModel.setStamp(stamp);
            // 转换为JSON字符串
            hsdProcessTickingTicFactModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(tickingTicFactModel));
            hsdProcessTickingTicFactModel.setProcessed(false);
            hsdProcessTickingTicFactModel.setUpdateTime(LocalDateTime.now().toString());

            if(StringUtils.isNotEmpty(tickingTicFactModel.getPkId())) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessTickingTicFactModel);
            }else{
                logger.info("高频-旅客常卡变更事件数据信息整合[TickingTicFactModel]数据解析异常{}", value);
            }

        }
    }
}
