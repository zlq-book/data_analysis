package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
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

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.calculateEarlyCheckinHours;
import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.getCabinClass;

/**
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/15  16:43
 */
public class CheckInTrans {

    static final Logger logger = LoggerFactory.getLogger(CheckInTrans.class);

    public static void output(DataStream<String> source) {

        SingleOutputStreamOperator<HsdProcessDataModel> checkInStream = source.flatMap((String value, Collector<HsdProcessDataModel> out) -> {
            Document document;
            try {
                document = DocumentUtils.string2Document(value);
            } catch (Exception e) {
                logger.warn("convert document is error, the content is {}, the error is {}", value, e.getMessage());
                return;
            }
            // 事件名称
            String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
            if (!Constants.EVENT_CHECKIN.equals(event)) {
                return;
            }
            logger.info("高频-值机事件数据信息整合");
            // 子事件名称
            String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
            String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
            String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
            // 票号
            String ticketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@TicketNumber");
            // 乘机人类型
            String passengerType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/@Type");
            // 值机日期
            String fkCheckinDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/DepartureInfo/Checkin/@CheckInDate");
            // 值机时间
            String fkCheckinTime = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/DepartureInfo/Checkin/@CheckInTime");
            // 乘机人英文姓
            String surname = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Surname");
            // 乘机人英文名
            String givenName = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/GivenName");
            // 乘机人中文姓名
            String nativeGivenName = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/NativeGivenName");
            nativeGivenName = NormalizationUtils.standardize(FieldType.CN_NAME, nativeGivenName);
            // 乘机人证件类型
            String documentType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Type");
            // 乘机人证件号码
            String documentNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Number");
            // 出生日期
            String birthDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@DateOfBirth");
            // 乘机人年龄
            Integer age = TransUtils.getAgeByIdCard(documentNumber, birthDate);
            // 乘机人TID
            String tid = TransUtils.getTid(documentNumber, documentType, "HSD");
            // 国内国际标识
            String ticketType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@TicketType");
            // 值机终端
            String checkinPid = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/DepartureInfo/Checkin/@CheckInPid");
            // 值机Agent
            String checkinAgent = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/DepartureInfo/Checkin/@CheckInAgent");
            // 座位号
            String seatAssign = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/DepartureInfo/Checkin/Seat/@Location");
            // 值机状态
            String checkinStatus = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/DepartureInfo/Checkin/@Status");
            // 值机方式
            String checkinType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/DepartureInfo/Checkin/@CheckInType");
            // 是否携带行李
            Boolean baggageFlag = XpathUtils.getString(document,
                    "/Msg/Dat/PassengerSegment/DepartureInfo/Checkin/@Baggage").equals("Y");
            // 行李总件数
            Integer totalBaggageCount = XpathUtils.getNumber(document,
                    "sum(/Msg/Dat/PassengerSegment/DepartureInfo/BaggageInfo/Baggage/@CheckInCount)").intValue();
            // 行李总重量
            Double totalBaggageWeight = XpathUtils.getNumber(document,
                    "sum(/Msg/Dat/PassengerSegment/DepartureInfo/BaggageInfo/Baggage/@CheckInWeight)");

            NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
            for (int a = 0; a < segmentList.getLength(); a++) {
                Node segment = segmentList.item(a);
                // 航段状态
                String segmentStatus = XpathUtils.getString(segment, "@CouponStatus");
                // 市场方航司
                String marketAirline = XpathUtils.getString(segment, "Carrier/@AirlineCode");
                // 市场方航班号
                String marketFlightNum = XpathUtils.getString(segment, "Carrier/@FlightNumber");
                // 承运航司
                String operatingAirline = XpathUtils.getString(segment, "OperatingCarrier/@AirlineCode");
                // 承运航班号
                String operatingFlightNum = XpathUtils.getString(segment, "OperatingCarrier/@FlightNumber");
                // 起飞机场
                String depAirport = XpathUtils.getString(segment, "Departure/@AirportCode");
                // 到达机场
                String arriAirport = XpathUtils.getString(segment, "Arrival/@AirportCode");
                // 航班起飞日期
                String departureDate = XpathUtils.getString(segment, "Departure/@Date");
                // 航班起飞时间
                String departureTime = XpathUtils.getString(segment, "Departure/@Time");
                // 常客卡号
                String ffrf = XpathUtils.getString(segment, "FrequentTraveler/@Number");
                // 常客等级
                String loyalLevel = XpathUtils.getString(segment, "FrequentTraveler/@LoyalLevel");
                // 常客卡航司
                String companyCode = XpathUtils.getString(segment, "FrequentTraveler/@CompanyCode");
                // 常客联盟卡级别
                String allianceLevel = XpathUtils.getString(segment, "FrequentTraveler/@AllianceLevel");
                // 航程 航程主键的逻辑为： 起飞日期 + 承运航司 + 承运航班号 +市场航司+市场航班号+ 起飞机场 + 降落机场
                String fkSegSegment =
                        departureDate.replace("-", "") + operatingAirline + operatingFlightNum + marketAirline + marketFlightNum + depAirport + arriAirport;
                // Done by kuangaihua:补充提前值机小时
                String checkInTime = buildCheckinTime(fkCheckinDate, fkCheckinTime);
                long earlyCheckintime = 0L;
                if (checkInTime != null && StringUtils.isNotEmpty(departureDate) && StringUtils.isNotEmpty(departureTime)) {
                    try {
                        earlyCheckintime = (long) calculateEarlyCheckinHours(checkInTime, departureDate, departureTime);
                    } catch (Exception e) {
                        logger.warn("计算提前值机小时异常: checkInTime={}, departureDate={}, departureTime={}, error={}",
                                checkInTime, departureDate, departureTime, e.getMessage());
                        System.out.println("计算提前值机小时异常: checkInTime="+checkInTime+", departureDate="+departureDate+", " +
                                "departureTime="+departureTime);
                    }
                } else {
                    logger.debug("值机时间信息不完整: checkinDate={}, checkInTime={}, departureDate={}, departureTime={}",
                            fkCheckinDate, fkCheckinTime, departureDate, departureTime);
                    System.out.println("值机时间信息不完整: checkinDate="+fkCheckinDate+", checkInTime="+fkCheckinTime+", " +
                            "departureDate="+departureDate+", departureTime="+departureTime);
                }

                // 构建每个 Segment 的实体对象
                CheckinSegFactModel checkinSegFactModel = new CheckinSegFactModel();
                // 航班起飞日期 + 票号 + 起飞机场 + 到达机场 9.7改成 票号 + 起飞机场 + 起飞日期
                checkinSegFactModel.setPkId(ticketNumber + depAirport + departureDate.replace("-", ""));
                checkinSegFactModel.setTikNum(ticketNumber);
                checkinSegFactModel.setFkCheckinDate(fkCheckinDate);
                checkinSegFactModel.setFkCheckinTime(fkCheckinTime);
                checkinSegFactModel.setPassengerType(passengerType);
                checkinSegFactModel.setEnLastName(surname);
                checkinSegFactModel.setEnFirstName(givenName);
                checkinSegFactModel.setCnName(nativeGivenName);
                checkinSegFactModel.setCertType(NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, documentType, DataSource.HIGH_FREQUENCY_DATA));
                checkinSegFactModel.setCertNumber(NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,
                        SM4Utils.encrypt(documentNumber, Constants.SM4_KEY)));
                checkinSegFactModel.setPassengerAge(age);
                checkinSegFactModel.setFkPassengerUserTid(tid);
                checkinSegFactModel.setFfrf(NormalizationUtils.standardize(FieldType.FREQUENT_FLYER_NUM,
                        SM4Utils.encrypt(ffrf, Constants.SM4_KEY)));
                checkinSegFactModel.setFfLevel(loyalLevel);
                checkinSegFactModel.setFfAirline(companyCode);
                checkinSegFactModel.setFfAllianceLevel(allianceLevel);
                checkinSegFactModel.setFkCheckinSeg(fkSegSegment);
                checkinSegFactModel.setFkSegDate(departureDate.replace("-", ""));
                checkinSegFactModel.setFkSegTime(departureTime);
                checkinSegFactModel.setFkDepairport(depAirport);
                checkinSegFactModel.setFkArriairport(arriAirport);
                checkinSegFactModel.setAkCheckinPid(checkinPid);
                checkinSegFactModel.setCheckinAgent(checkinAgent);
                checkinSegFactModel.setCheckinChannel(TransUtils.convertAgentToCode(checkinAgent));
                checkinSegFactModel.setAkSeatAssign(seatAssign);
                checkinSegFactModel.setAkDimark(ticketType);
                checkinSegFactModel.setAkCheckinStatus(checkinStatus);
                checkinSegFactModel.setAkCheckinType(checkinType);
                checkinSegFactModel.setAkBaggageFlag(baggageFlag);
                checkinSegFactModel.setSegBaggagecount(totalBaggageCount);
                checkinSegFactModel.setSegBaggageweight(totalBaggageWeight);
                checkinSegFactModel.setEarlyCheckintime(earlyCheckintime);
                // 数据是否启用
                checkinSegFactModel.setDataActive(true);
                // 数据启用时间
                checkinSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
                checkinSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
                // 本系统最后更新日期时间
                checkinSegFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());
                // 舱等
                String clazz = XpathUtils.getString(segment, "Cabin/@Clazz");
                if (clazz != null && !clazz.isEmpty()) {
                    clazz = clazz.substring(0, 1);
                }
                String departDate = XpathUtils.getString(segment, "Departure/@Date");
                checkinSegFactModel.setAkSegcabin(clazz);
                checkinSegFactModel.setAkCabin(getCabinClass(departDate, clazz));

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

                if (StringUtils.isNotEmpty(checkinSegFactModel.getPkId())) {
                    out.collect(hsdProcessDataModel);
                } else {
                    logger.info("高频-值机事件数据信息整合[CheckinSegFactModel]数据解析异常{}", value);
                }
            }
        }).returns(TypeInformation.of(HsdProcessDataModel.class)).name("CheckInFlatMap").setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        // 输出结果查看
//        checkInStream.print("checkInStream");
        // 创建dorisSink
        DorisSink<HsdProcessDataModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_HSD_PROCESS_DATA");
        // 数据写入doris
        checkInStream.sinkTo(dorisSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
    }

    public static void processCheckIn(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
        /*if (!Constants.EVENT_CHECKIN.equals(event)) {
            return;
        }*/
        logger.info("高频-值机事件数据信息整合");
        // 子事件名称
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
        // 票号
        String ticketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@TicketNumber");
        // 乘机人类型
        String passengerType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/@Type");
        // 值机日期
        String fkCheckinDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/DepartureInfo/Checkin/@CheckInDate");
        // 值机时间
        String fkCheckinTime = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/DepartureInfo/Checkin/@CheckInTime");
        // 乘机人英文姓
        String surname = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Surname");
        // 乘机人英文名
        String givenName = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/GivenName");
        // 乘机人中文姓名
        String nativeGivenName = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/NativeGivenName");
        nativeGivenName = NormalizationUtils.standardize(FieldType.CN_NAME, nativeGivenName);
        // 乘机人证件类型
        String documentType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Type");
        // 乘机人证件号码
        String documentNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Number");
        // 出生日期
        String birthDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@DateOfBirth");
        // 乘机人年龄
        Integer age = TransUtils.getAgeByIdCard(documentNumber, birthDate);
        // 乘机人TID
        String tid = TransUtils.getTid(documentNumber, documentType, "HSD");
        // 国内国际标识
        String ticketType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@TicketType");
        // 值机终端
        String checkinPid = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/DepartureInfo/Checkin/@CheckInPid");
        // 值机Agent
        String checkinAgent = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/DepartureInfo/Checkin/@CheckInAgent");
        // 座位号
        String seatAssign = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/DepartureInfo/Checkin/Seat/@Location");
        // 值机状态
        String checkinStatus = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/DepartureInfo/Checkin/@Status");
        // 值机方式
        String checkinType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/DepartureInfo/Checkin/@CheckInType");
        // 是否携带行李
        Boolean baggageFlag = XpathUtils.getString(document,
                "/Msg/Dat/PassengerSegment/DepartureInfo/Checkin/@Baggage").equals("Y");
        // 行李总件数
        Integer totalBaggageCount = XpathUtils.getNumber(document,
                "sum(/Msg/Dat/PassengerSegment/DepartureInfo/BaggageInfo/Baggage/@CheckInCount)").intValue();
        // 行李总重量
        Double totalBaggageWeight = XpathUtils.getNumber(document,
                "sum(/Msg/Dat/PassengerSegment/DepartureInfo/BaggageInfo/Baggage/@CheckInWeight)");

        NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
        for (int a = 0; a < segmentList.getLength(); a++) {
            Node segment = segmentList.item(a);
            // 航段状态
            String segmentStatus = XpathUtils.getString(segment, "@CouponStatus");
            // 市场方航司
            String marketAirline = XpathUtils.getString(segment, "Carrier/@AirlineCode");
            // 市场方航班号
            String marketFlightNum = XpathUtils.getString(segment, "Carrier/@FlightNumber");
            // 承运航司
            String operatingAirline = XpathUtils.getString(segment, "OperatingCarrier/@AirlineCode");
            // 承运航班号
            String operatingFlightNum = XpathUtils.getString(segment, "OperatingCarrier/@FlightNumber");
            // 起飞机场
            String depAirport = XpathUtils.getString(segment, "Departure/@AirportCode");
            // 到达机场
            String arriAirport = XpathUtils.getString(segment, "Arrival/@AirportCode");
            // 航班起飞日期
            String departureDate = XpathUtils.getString(segment, "Departure/@Date");
            // 航班起飞时间
            String departureTime = XpathUtils.getString(segment, "Departure/@Time");
            // 常客卡号
            String ffrf = XpathUtils.getString(segment, "FrequentTraveler/@Number");
            // 常客等级
            String loyalLevel = XpathUtils.getString(segment, "FrequentTraveler/@LoyalLevel");
            // 常客卡航司
            String companyCode = XpathUtils.getString(segment, "FrequentTraveler/@CompanyCode");
            // 常客联盟卡级别
            String allianceLevel = XpathUtils.getString(segment, "FrequentTraveler/@AllianceLevel");
            // 航程 航程主键的逻辑为： 起飞日期 + 承运航司 + 承运航班号 +市场航司+市场航班号+ 起飞机场 + 降落机场
            String fkSegSegment =
                    departureDate.replace("-", "") + operatingAirline + operatingFlightNum + marketAirline + marketFlightNum + depAirport + arriAirport;
            // Done by kuangaihua:补充提前值机小时
            String checkInTime = buildCheckinTime(fkCheckinDate, fkCheckinTime);
            long earlyCheckintime = 0L;
            if (checkInTime != null && StringUtils.isNotEmpty(departureDate) && StringUtils.isNotEmpty(departureTime)) {
                try {
                    earlyCheckintime = (long) calculateEarlyCheckinHours(checkInTime, departureDate, departureTime);
                } catch (Exception e) {
                    logger.warn("计算提前值机小时异常: checkInTime={}, departureDate={}, departureTime={}, error={}",
                            checkInTime, departureDate, departureTime, e.getMessage());
                    System.out.println("计算提前值机小时异常: checkInTime="+checkInTime+", departureDate="+departureDate+", " +
                            "departureTime="+departureTime);
                }
            } else {
                logger.debug("值机时间信息不完整: checkinDate={}, checkInTime={}, departureDate={}, departureTime={}",
                        fkCheckinDate, fkCheckinTime, departureDate, departureTime);
                System.out.println("值机时间信息不完整: checkinDate="+fkCheckinDate+", checkInTime="+fkCheckinTime+", " +
                        "departureDate="+departureDate+", departureTime="+departureTime);
            }

            // 构建每个 Segment 的实体对象
            CheckinSegFactModel checkinSegFactModel = new CheckinSegFactModel();
            // 航班起飞日期 + 票号 + 起飞机场 + 到达机场 9.7改成 票号 + 起飞机场 + 起飞日期
            checkinSegFactModel.setPkId(ticketNumber + depAirport + departureDate.replace("-", ""));
            checkinSegFactModel.setTikNum(ticketNumber);
            checkinSegFactModel.setFkCheckinDate(fkCheckinDate);
            checkinSegFactModel.setFkCheckinTime(fkCheckinTime);
            checkinSegFactModel.setPassengerType(passengerType);
            checkinSegFactModel.setEnLastName(surname);
            checkinSegFactModel.setEnFirstName(givenName);
            checkinSegFactModel.setCnName(nativeGivenName);
            checkinSegFactModel.setCertType(NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, documentType, DataSource.HIGH_FREQUENCY_DATA));
            checkinSegFactModel.setCertNumber(NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,
                    SM4Utils.encrypt(documentNumber, Constants.SM4_KEY)));
            checkinSegFactModel.setPassengerAge(age);
            checkinSegFactModel.setFkPassengerUserTid(tid);
            checkinSegFactModel.setFfrf(NormalizationUtils.standardize(FieldType.FREQUENT_FLYER_NUM,
                    SM4Utils.encrypt(ffrf, Constants.SM4_KEY)));
            checkinSegFactModel.setFfLevel(loyalLevel);
            checkinSegFactModel.setFfAirline(companyCode);
            checkinSegFactModel.setFfAllianceLevel(allianceLevel);
            checkinSegFactModel.setFkCheckinSeg(fkSegSegment);
            checkinSegFactModel.setFkSegDate(departureDate.replace("-", ""));
            checkinSegFactModel.setFkSegTime(departureTime);
            checkinSegFactModel.setFkDepairport(depAirport);
            checkinSegFactModel.setFkArriairport(arriAirport);
            checkinSegFactModel.setAkCheckinPid(checkinPid);
            checkinSegFactModel.setCheckinAgent(checkinAgent);
            checkinSegFactModel.setCheckinChannel(TransUtils.convertAgentToCode(checkinAgent));
            checkinSegFactModel.setAkSeatAssign(seatAssign);
            checkinSegFactModel.setAkDimark(ticketType);
            checkinSegFactModel.setAkCheckinStatus(checkinStatus);
            checkinSegFactModel.setAkCheckinType(checkinType);
            checkinSegFactModel.setAkBaggageFlag(baggageFlag);
            checkinSegFactModel.setSegBaggagecount(totalBaggageCount);
            checkinSegFactModel.setSegBaggageweight(totalBaggageWeight);
            checkinSegFactModel.setEarlyCheckintime(earlyCheckintime);
            // 数据是否启用
            checkinSegFactModel.setDataActive(true);
            // 数据启用时间
            checkinSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
            checkinSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
            // 本系统最后更新日期时间
            checkinSegFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());
            // 舱等
            String clazz = XpathUtils.getString(segment, "Cabin/@Clazz");
            if (clazz != null && !clazz.isEmpty()) {
                clazz = clazz.substring(0, 1);
            }
            String departDate = XpathUtils.getString(segment, "Departure/@Date");
            checkinSegFactModel.setAkSegcabin(clazz);
            checkinSegFactModel.setAkCabin(getCabinClass(departDate, clazz));

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

            if (StringUtils.isNotEmpty(checkinSegFactModel.getPkId())) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
            } else {
                logger.info("高频-值机事件数据信息整合[CheckinSegFactModel]数据解析异常{}", value);
            }
        }
    }

    /**
     * 构建完整的值机时间字符串，并格式化为标准时间格式
     *
     * @param checkinDate 值机日期
     * @param checkinTime 值机时间
     * @return 标准格式的值机时间字符串(yyyy - MM - dd HH : mm : ss)，如果参数为空则返回null
     */
    private static String buildCheckinTime(String checkinDate, String checkinTime) {
        if (StringUtils.isBlank(checkinDate)) {
            return null;
        }

        String timePart;
        if (StringUtils.isBlank(checkinTime)) {
            timePart = "00:00:00";
        } else {
            timePart = checkinTime;
        }

        String fullDateTime = checkinDate + " " + timePart;

        // 处理时间格式，确保符合计算要求
        if (fullDateTime.length() > 19) {
            return fullDateTime.substring(0, 19);
        }

        return fullDateTime;
    }

}
