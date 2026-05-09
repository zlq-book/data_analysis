package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.SegDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.BookingPnrFactModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.BookingSegFactModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.HsdProcessDataModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingTicFactModel;
import com.travelsky.trp.usercenter.data.analysis.utils.CustomJsonSerializer;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.*;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.isKeyAccount;
import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.getCabinClass;

/**
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/15  16:43
 */
public class BookFactTrans {

    static final Logger logger = LoggerFactory.getLogger(BookFactTrans.class);

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
                processBook(value, ctx, document);
            }
        }).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        DataStream<HsdProcessDataModel> hsdProcessDataStream = processedStream.getSideOutput(CommonOutputTags.HSD_PROCESS_DATA_TAG);
//        hsdProcessDataStream.print("hsdProcessDataStream");
        hsdProcessDataStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_HSD_PROCESS_DATA")).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        /*// 获取各个SideOutput流
        DataStream<BookingPnrFactModel> bookingPnrDataStream = processedStream.getSideOutput(CommonOutputTags.BOOKING_PNR_TAG);
        DataStream<BookingSegFactModel> bookingSegDataStream = processedStream.getSideOutput(CommonOutputTags.BOOKING_SEG_TAG);
        DataStream<TickingTicFactModel> tickingTicDataStream = processedStream.getSideOutput(CommonOutputTags.TICKING_TIC_TAG);
        DataStream<SegDimModel> segDimDataStream = processedStream.getSideOutput(CommonOutputTags.SEG_DIM_TAG);
        // 输出结果查看
//        bookingPnrDataStream.print("bookingPnrDataStream");
//        bookingSegDataStream.print("bookingSegDataStream");
//        tickingTicDataStream.print("tickingTicDataStream");
//        segDimDataStream.print("segDimDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<BookingPnrFactModel> bookingPnrSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_BOOKING_PNR_FACT");
        DorisSink<BookingSegFactModel> bookingSegSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_BOOKING_SEG_FACT");
        DorisSink<TickingTicFactModel> tickingTicSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_TIC_FACT");
        DorisSink<SegDimModel> segDimSink =  FlinkDorisUtils.creatDorisDIMSink("T_DIM_SEG_DIM");
        // 数据分别写入对应的Doris表
        bookingPnrDataStream.sinkTo(bookingPnrSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        bookingSegDataStream.sinkTo(bookingSegSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        tickingTicDataStream.sinkTo(tickingTicSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        segDimDataStream.sinkTo(segDimSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);*/
    }

    public static void processBook(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
        /*if (!Constants.EVENT_BOOK.equals(event)) {
            return;
        }*/
        logger.info("高频-旅客预订座位事件数据信息整合");
        // 子事件名称
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
        // 票号
        String ticketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo" + "/@TicketNumber");
        // 出票日期
        String issueDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@IssueDate");
        // PNR号
        String pnr = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@RecordLocator");
        // PNR创建日期
        String bookDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@BookDate").replace("-", "");
        // PNR创建时间
        String bookTime = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@BookTime");
        // 预订本航段使用的GDS系统
        String gdsCode = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@GDSCode");
        // 订座office
        String bookingOffice = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@BookingOffice");
        // 团队标识
        String group = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@Group");
        // 同行人数
        String passengerNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@PassengerNumber");
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
        // 乘机人证件类型
        String documentType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Type");
        // 乘机人证件号码
        String documentNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Number");
        // 出生日期
        String birthDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@DateOfBirth");
        // 乘机人年龄
        Integer age = TransUtils.getAgeByIdCard(documentNumber, birthDate);
        // 运价基础FB
        String fareBasisCode = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@FareBasisCode");
        // 乘机人类型
        String travellerType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/@Type");
        // 是否与老人同行
        boolean isPeerSenior = false;
        if (null != age && age > 65) {
            isPeerSenior = true;
        }

        NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
        for (int a = 0; a < segmentList.getLength(); a++) {
            Node segment = segmentList.item(a);
            // 承运航司
            String operatingAirline = XpathUtils.getString(segment, "OperatingCarrier/@AirlineCode");
            // 承运航班号
            String operatingFlightNum = XpathUtils.getString(segment, "OperatingCarrier/@FlightNumber");
            // 市场方航司
            String marketAirline = XpathUtils.getString(segment, "Carrier/@AirlineCode");
            // 市场方航班号
            String marketFlightNum = XpathUtils.getString(segment, "Carrier/@FlightNumber");
            // 航班起飞日期
            String departureDate = XpathUtils.getString(segment, "Departure/@Date");
            // 航班起飞时间
            String departureTime = XpathUtils.getString(segment, "Departure/@Time");
            // 航班到达日期
            String arriveDate = XpathUtils.getString(segment, "Arrival/@Date");
            // 航班到达时间
            String arriveTime = XpathUtils.getString(segment, "Arrival/@Time");
            // 起飞机场
            String depAirport = XpathUtils.getString(segment, "Departure/@AirportCode");
            // 到达机场
            String arrivalAirport = XpathUtils.getString(segment, "Arrival/@AirportCode");
            // 大客户号
            String largeCustomerNumber = TransUtils.getKeyAccount(segment);
            // PNR 航段行动代码（HK等）
            String actionCode = XpathUtils.getString(segment, "@ActionCode");
            // 舱位
            String clazz = XpathUtils.getString(segment, "Cabin/@Clazz");
            if (clazz != null && !clazz.isEmpty()) {
                clazz = clazz.substring(0, 1);
            }
            // 常客卡号
            String ffrf = XpathUtils.getString(segment, "FrequentTraveler/@Number");
            // 常客等级
            String loyalLevel = XpathUtils.getString(segment, "FrequentTraveler/@LoyalLevel");
            // 常客卡航司
            String companyCode = XpathUtils.getString(segment, "FrequentTraveler/@CompanyCode");
            // 常客联盟卡级别
            String allianceLevel = XpathUtils.getString(segment, "FrequentTraveler/@AllianceLevel");
            String contactPhone = XpathUtils.getString(segment, "OtherServiceInformation/Text[starts-with(., 'CTCT')]");
            if (StringUtils.isNotBlank(contactPhone) && contactPhone.startsWith("CTCT")) {
                contactPhone = contactPhone.substring(4).trim();
            } else {
                contactPhone = null;
            }
            // vvip
            String vvip = XpathUtils.getString(segment, "OtherServiceInformation[Text[starts-with(., " + "'VVIP')]]/Text");
            // 航程主键：起飞日期 + 承运航司 + 承运航班号 +市场航司+市场航班号+ 起飞机场 + 降落机场
            String segmentKey = departureDate + operatingAirline + operatingFlightNum + marketAirline + marketFlightNum + depAirport + arrivalAirport;

            // 航程维表
            SegDimModel segDimModel = new SegDimModel();
            if(StringUtils.isNotEmpty(departureDate) && StringUtils.isNotEmpty(operatingAirline) && StringUtils.isNotEmpty(operatingFlightNum) && StringUtils.isNotEmpty(marketAirline) && StringUtils.isNotEmpty(marketFlightNum) && StringUtils.isNotEmpty(depAirport) && StringUtils.isNotEmpty(arrivalAirport)){
                // 起飞日期 + 承运航司 + 承运航班号 +市场航司+市场航班号+ 起飞机场 + 降落机场
                segDimModel.setSegmentKey(departureDate.replace("-", "") + operatingAirline + operatingFlightNum + marketAirline + marketFlightNum + depAirport + arrivalAirport);
                segDimModel.setAirline(depAirport + "_" + arrivalAirport);
                segDimModel.setOperatAirline(operatingAirline);
                segDimModel.setOperatFlightNum(operatingFlightNum);
                segDimModel.setDepartureDate(departureDate);
                segDimModel.setDepartureTime(departureTime);
                segDimModel.setArrivalDate(arriveDate);
                segDimModel.setArrivalTime(arriveTime);
                segDimModel.setMarketAirline(marketAirline);
                segDimModel.setMarketFlightNum(marketFlightNum);
                segDimModel.setCreateTime(LocalDateTime.now().toString());
                segDimModel.setUpdateTime(LocalDateTime.now().toString());

                HsdProcessDataModel hsdProcessDataModel = new HsdProcessDataModel();
                hsdProcessDataModel.setEvent(event);
                hsdProcessDataModel.setSubEvent(subEvent);
                hsdProcessDataModel.setTableName("T_DIM_SEG_DIM");
                hsdProcessDataModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
                hsdProcessDataModel.setStamp(stamp);
                // 转换为JSON字符串
                hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(segDimModel));
                hsdProcessDataModel.setProcessed(false);
                hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());

                if(StringUtils.isNotEmpty(segDimModel.getSegmentKey())) {
                    ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
                }else{
                    logger.info("高频-旅客预订座位事件数据信息整合[SegDimModel]数据解析异常{}", value);
                }
            }


            // 机票预订-PNR级
            BookingPnrFactModel bookingPnrFactModel = new BookingPnrFactModel();
            // PNR编号+PNR创建日期
            bookingPnrFactModel.setPkId(pnr + bookDate);
            bookingPnrFactModel.setPnrNumber(pnr);
            bookingPnrFactModel.setFkPnrcreateDate(bookDate);
            bookingPnrFactModel.setFkPnrcreateTime(bookTime);
            bookingPnrFactModel.setGdsCode(gdsCode);
            bookingPnrFactModel.setAkBookingOffice(bookingOffice);
            bookingPnrFactModel.setAkTeammark("Y".equals(group));
            bookingPnrFactModel.setAkKeyAccountCode(largeCustomerNumber);
            // 是否是大客户订单
            bookingPnrFactModel.setKeyAccount(isKeyAccount(largeCustomerNumber));
            bookingPnrFactModel.setAkPeerNumber(StringUtils.isNotBlank(passengerNumber) ? Integer.valueOf(passengerNumber) : null);
            // 是否与老人同行
            bookingPnrFactModel.setPeerSenior(isPeerSenior);
            // 是否与儿童同行
            bookingPnrFactModel.setPeerChd("CHD".equals(travellerType));
            // v2-实时高频	旅客预订座位事件	OSI中的CTCT
            bookingPnrFactModel.setContactMobileNumber(NormalizationUtils.standardize(FieldType.MOBILE_NO,
                    SM4Utils.encrypt(contactPhone, Constants.SM4_KEY)));

            bookingPnrFactModel.setDataActive(true);
            bookingPnrFactModel.setDataActiveTime(LocalDateTime.now().toString());
            bookingPnrFactModel.setSystemCreatetime(LocalDateTime.now().toString());
            // 是否单人出行计算 同行人数为1
            if (StringUtils.isNotBlank(passengerNumber)) {
                bookingPnrFactModel.setSingleTravel(Integer.valueOf(passengerNumber) == 1);
            }
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

            if(StringUtils.isNotEmpty(bookingPnrFactModel.getPkId())) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessBookingPnrFactModel);
            }else{
                logger.info("高频-旅客预订座位事件数据信息整合[BookingPnrFactModel]数据解析异常{}", value);
            }

            // 机票预订-航段级
            BookingSegFactModel bookingSegFactModel = new BookingSegFactModel();
            // PNR编号+PNR创建日期+起飞机场+乘机人证件号（加密）+乘机人姓名（姓名，先取乘机人中文姓名，如果没有，取英文姓+英文名）
            bookingSegFactModel.setPkId(pnr + bookDate + bookTime.replace(":","") + depAirport + NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, SM4Utils.encrypt(documentNumber, Constants.SM4_KEY)) + travellerName);
            bookingSegFactModel.setPnrNumber(pnr);
            bookingSegFactModel.setFkBookingDate(bookDate);
            bookingSegFactModel.setFkBookingTime(bookTime);
            bookingSegFactModel.setFkDepairport(depAirport);
            bookingSegFactModel.setFkArriairport(arrivalAirport);
            // 航程维表外键
            bookingSegFactModel.setFkBookingSeg(segmentKey);
            bookingSegFactModel.setFkSegDate(departureDate);
            bookingSegFactModel.setFkSegTime(departureTime);
            bookingSegFactModel.setPassengerType(travellerType);
            bookingSegFactModel.setEnLastName(surname);
            bookingSegFactModel.setEnFirstName(givenName);
            bookingSegFactModel.setCnName(nativeGivenName);
            bookingSegFactModel.setCertType(NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, documentType, DataSource.HIGH_FREQUENCY_DATA));
            bookingSegFactModel.setCertNumber(NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, SM4Utils.encrypt(documentNumber, Constants.SM4_KEY)));
            bookingSegFactModel.setPassengerAge(age);
            // dMapping转换
            bookingSegFactModel.setFkPassengerUserTid(TransUtils.getTid(documentNumber, documentType, "HSD"));
            // VVIP标识
            if(vvip != null && vvip.length() > 4) {
                bookingSegFactModel.setVvip(vvip.substring(4));
            }
            bookingSegFactModel.setAkBookingOfficeNumber(bookingOffice);
            bookingSegFactModel.setAkTeammark("Y".equals(group));
            bookingSegFactModel.setAkSegStatus(actionCode);
            bookingSegFactModel.setAkSegcabin(clazz);
            bookingSegFactModel.setFfrf(NormalizationUtils.standardize(FieldType.FREQUENT_FLYER_NUM, SM4Utils.encrypt(ffrf, Constants.SM4_KEY)));
            bookingSegFactModel.setFfLevel(loyalLevel);
            bookingSegFactModel.setFfAirline(companyCode);
            bookingSegFactModel.setFfAllianceLevel(allianceLevel);
            bookingSegFactModel.setAkKeyAccountCode(largeCustomerNumber);
            // 是否是大客户订单
            bookingSegFactModel.setKeyAccount(isKeyAccount(largeCustomerNumber));
            bookingSegFactModel.setContactMobileNumber(NormalizationUtils.standardize(FieldType.MOBILE_NO,
                    SM4Utils.encrypt(contactPhone, Constants.SM4_KEY)));
            bookingSegFactModel.setAkPeerNumber(StringUtils.isNotBlank(passengerNumber) ? Integer.valueOf(passengerNumber) : null);
            // 是否与老人同行
            bookingSegFactModel.setPeerSenior(isPeerSenior);
            // 是否与儿童同行
            bookingSegFactModel.setAkPeerChd("CHD".equals(bookingSegFactModel.getPassengerType()));
            // 是否单人出行计算 同行人数为1
            if (StringUtils.isNotBlank(passengerNumber)) {
                bookingSegFactModel.setSingleTravel(Integer.valueOf(passengerNumber) == 1);
            }
            // 提前出票天数 起飞日期-出票日期
            Integer daysBetween = null;
            if(StringUtils.isNotBlank(departureDate) && StringUtils.isNotBlank(issueDate)){
                LocalDate depart = LocalDate.parse(departureDate);
                LocalDate issue = LocalDate.parse(issueDate);
                // 计算两个日期之间的天数差
                Period period = Period.between(issue, depart);
                daysBetween = period.getDays();
            }
            bookingSegFactModel.setAkAdvbookDay(daysBetween);
            bookingSegFactModel.setDataActive(true);
            bookingSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
            bookingSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
            bookingSegFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());
            // 舱等
            bookingSegFactModel.setAkCabin(getCabinClass(departureDate, clazz));
            // 飞行时是否使用常客卡
            bookingSegFactModel.setUsedFfr(StringUtils.isNotBlank(ffrf));

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
                logger.info("高频-旅客预订座位事件数据信息整合[BookingSegFactModel]数据解析异常{}", value);
            }


            // 机票出票-客票级
            TickingTicFactModel tickingTicFactModel = new TickingTicFactModel();
            // 出票日期+票号
            tickingTicFactModel.setPkId(issueDate.replace("-","") + ticketNumber);
            tickingTicFactModel.setAkFarebasis(fareBasisCode);
            tickingTicFactModel.setDataActive(true);
            tickingTicFactModel.setAkTeammark("Y".equals(group));
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
                logger.info("高频-旅客预订座位事件数据信息整合[TickingTicFactModel]数据解析异常{}", value);
            }
        }
    }
}
