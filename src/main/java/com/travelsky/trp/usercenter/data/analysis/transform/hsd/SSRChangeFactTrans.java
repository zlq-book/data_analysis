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

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.isKeyAccount;

/**
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/15  16:43
 */
public class SSRChangeFactTrans {

    static final Logger logger = LoggerFactory.getLogger(SSRChangeFactTrans.class);

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
                processSSRChange(value, ctx, document);
            }
        }).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        DataStream<HsdProcessDataModel> hsdProcessDataStream = processedStream.getSideOutput(CommonOutputTags.HSD_PROCESS_DATA_TAG);
        //hsdProcessDataStream.print("hsdProcessDataStream");
        hsdProcessDataStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_HSD_PROCESS_DATA")).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        /*// 获取各个SideOutput流
        DataStream<BookingPnrFactModel> bookingPnrDataStream = processedStream.getSideOutput(CommonOutputTags.BOOKING_PNR_TAG);
        DataStream<BookingSegFactModel> bookingSegDataStream = processedStream.getSideOutput(CommonOutputTags.BOOKING_SEG_TAG);
        DataStream<TickingSegFactModel> tickingSegDataStream = processedStream.getSideOutput(CommonOutputTags.TICKING_SEG_TAG);
        // 输出结果查看
//        bookingPnrDataStream.print("bookingPnrDataStream");
//        bookingSegDataStream.print("bookingSegDataStream");
//        tickingSegDataStream.print("tickingSegDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<BookingPnrFactModel> bookingPnrSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_BOOKING_PNR_FACT");
        DorisSink<BookingSegFactModel> bookingSegSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_BOOKING_SEG_FACT");
        DorisSink<TickingSegFactModel> tickingSegSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_SEG_FACT");
        // 数据分别写入对应的Doris表
        bookingPnrDataStream.sinkTo(bookingPnrSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        bookingSegDataStream.sinkTo(bookingSegSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        tickingSegDataStream.sinkTo(tickingSegSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);*/

    }

    public static void processSSRChange(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
        if (!event.contains(Constants.EVENT_SSRCHANGE)) {
            return;
        }
        logger.info("高频-{}事件数据信息整合", event);
        // 子事件名称
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
        // PNR号
        String pnr = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@RecordLocator");
        // PNR生成日期
        String bookDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@BookDate").replace("-", "");
        String bookTime = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@BookTime").replace(":", "");
        // 票号
        String ticketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo" + "/@TicketNumber");
        // 乘机人英文姓
        String surname = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Surname");
        // 乘机人英文名
        String givenName = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/GivenName");
        String enName = NormalizationUtils.standardize(FieldType.EN_NAME, surname + givenName);
        // 乘机人中文姓名
        String nativeGivenName = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/NativeGivenName");
        nativeGivenName = NormalizationUtils.standardize(FieldType.CN_NAME, nativeGivenName);
        // 乘机人证件号码
        String documentNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Number");
        // 乘机人姓名
        String travellerName = StringUtils.isBlank(nativeGivenName) ? enName : nativeGivenName;

        NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
        for (int a = 0; a < segmentList.getLength(); a++) {
            Node segment = segmentList.item(a);
            // 起飞机场
            String depAirport = XpathUtils.getString(segment, "Departure/@AirportCode");
            // 航班起飞日期
            String departureDate = XpathUtils.getString(segment, "Departure/@Date");
            // 大客户号
            String largeCustomerNumber = TransUtils.getKeyAccount(segment);

            // 特殊餐食属性（仅限免费的）
            String spmlText = XpathUtils.getString(segment, "SpecialServiceRequest[SSRCode='SPML']/Text");
            String docsText = XpathUtils.getString(segment, "SpecialServiceRequest[SSRCode='DOCS']/Text");
            String spmlCode = null;
            if (StringUtils.isNotBlank(spmlText) && (spmlText.contains("HK") || spmlText.contains("HI"))) {
                spmlCode = spmlText;
            }

            // 构建每个 Segment 的实体对象
            // 机票预订-PNR级
            BookingPnrFactModel bookingPnrFactModel = new BookingPnrFactModel();
            // PNR编号+PNR创建日期
            bookingPnrFactModel.setPkId(pnr + bookDate);
            // 大客户编码取值：PNR 中“SSR CKINXXHKXVICO客户编号”或FP/CASH.CUY/*客户编号”项
            bookingPnrFactModel.setAkKeyAccountCode(largeCustomerNumber);
            // 是否是大客户订单
            bookingPnrFactModel.setKeyAccount(isKeyAccount(largeCustomerNumber));
            bookingPnrFactModel.setDataActive(true);
            bookingPnrFactModel.setDataActiveTime(LocalDateTime.now().toString());
            bookingPnrFactModel.setSystemCreatetime(LocalDateTime.now().toString());
            bookingPnrFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

            HsdProcessDataModel hsdProcessBookingPnrFactModel = new HsdProcessDataModel();
            hsdProcessBookingPnrFactModel.setEvent(event);
            hsdProcessBookingPnrFactModel.setSubEvent(subEvent);
            hsdProcessBookingPnrFactModel.setTableName("T_DWD_BOOKING_PNR_FACT");
            hsdProcessBookingPnrFactModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
            hsdProcessBookingPnrFactModel.setStamp(stamp);
            // 转换为JSON字符串
            hsdProcessBookingPnrFactModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(bookingPnrFactModel));
            hsdProcessBookingPnrFactModel.setProcessed(false);
            hsdProcessBookingPnrFactModel.setUpdateTime(LocalDateTime.now().toString());

            if (StringUtils.isNotEmpty(bookingPnrFactModel.getPkId())) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessBookingPnrFactModel);
            }else{
                logger.info("高频-{}事件数据信息整合[BookingPnrFactModel]数据解析异常{}",event, value);
            }

            // 机票预订-航段级
            BookingSegFactModel bookingSegFactModel = new BookingSegFactModel();
            // PNR编号+PNR创建日期+起飞机场+乘机人证件号（加密）+乘机人姓名（姓名，先取乘机人中文姓名，如果没有，取英文姓+英文名）
            bookingSegFactModel.setPkId(pnr + bookDate + bookTime + depAirport + NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, SM4Utils.encrypt(documentNumber, Constants.SM4_KEY)) + travellerName);
            // 大客户编码取值：PNR 中“SSR CKINXXHKXVICO客户编号”或FP/CASH.CUY/*客户编号”项
            bookingSegFactModel.setAkKeyAccountCode(largeCustomerNumber);
            // 是否是大客户订单
            bookingSegFactModel.setKeyAccount(isKeyAccount(largeCustomerNumber));
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

            if (StringUtils.isNotEmpty(bookingSegFactModel.getPkId())) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessBookingSegFactModel);
            }else{
                logger.info("高频-{}事件数据信息整合[BookingSegFactModel]数据解析异常{}",event, value);
            }


            // 机票出票-航段级
            TickingSegFactModel tickingSegFactModel = new TickingSegFactModel();
            // pkId 票号+起飞机场+起飞日期
            tickingSegFactModel.setPkId(ticketNumber + depAirport + departureDate.replace("-",""));
            tickingSegFactModel.setAkSpcaAtt(StringUtils.isNotBlank(spmlCode) ?
                    spmlCode.split("\\s+")[spmlCode.split("\\s+").length - 1] : null);
            tickingSegFactModel.setDocsCertType(StringUtils.isNotBlank(docsText) ?
                    docsText.chars()
                            .filter(Character::isUpperCase)
                            .mapToObj(c -> String.valueOf((char) c))
                            .findFirst()
                            .orElse(null) : null);
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

            if (StringUtils.isNotEmpty(tickingSegFactModel.getPkId())) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessTickingSegFactModel);
            }else{
                logger.info("高频-{}事件数据信息整合[TickingSegFactModel]数据解析异常{}",event, value);
            }

        }
    }


}
