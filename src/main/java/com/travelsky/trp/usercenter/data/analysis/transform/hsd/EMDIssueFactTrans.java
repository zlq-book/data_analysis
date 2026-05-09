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

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.getCabinClass;

/**
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/15  16:43
 */
public class EMDIssueFactTrans {

    static final Logger logger = LoggerFactory.getLogger(EMDIssueFactTrans.class);

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
                processEMDIssue(value, ctx, document);
            }
        }).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        DataStream<HsdProcessDataModel> hsdProcessDataStream = processedStream.getSideOutput(CommonOutputTags.HSD_PROCESS_DATA_TAG);
        //hsdProcessDataStream.print("hsdProcessDataStream");
        hsdProcessDataStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_HSD_PROCESS_DATA")).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        /*// 获取各个SideOutput流
        DataStream<SeatTikFactModel > seatTikDataStream = processedStream.getSideOutput(CommonOutputTags.SEAT_TIK_TAG);
        // 输出结果查看
        seatTikDataStream.print("seatTikDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<SeatTikFactModel> seatTikSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_SEAT_TIK_FACT");
        seatTikDataStream.sinkTo(seatTikSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        // 获取各个SideOutput流
        DataStream<BaggageTikFactModel> baggageTikDataStream = processedStream.getSideOutput(CommonOutputTags.BAGGAGE_TIK_TAG);
        // 输出结果查看
        baggageTikDataStream.print("baggageTikDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<BaggageTikFactModel> baggageTikSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_BAGGAGE_TIK_FACT");
        baggageTikDataStream.sinkTo(baggageTikSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        // 获取各个SideOutput流
        DataStream<MealTikFactModel> mealTikDataStream = processedStream.getSideOutput(CommonOutputTags.MEAL_TIK_TAG);
        // 输出结果查看
        mealTikDataStream.print("mealTikDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<MealTikFactModel> mealTikSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_MEAL_TIK_FACT");
        mealTikDataStream.sinkTo(mealTikSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        // 获取各个SideOutput流
        DataStream<UpgrTikFactModel> upgrTikDataStream = processedStream.getSideOutput(CommonOutputTags.UPGR_TIK_TAG);
        // 输出结果查看
        upgrTikDataStream.print("upgrTikDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<UpgrTikFactModel> upgrTikSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_UPGR_TIK_FACT");
        upgrTikDataStream.sinkTo(upgrTikSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        // 获取各个SideOutput流
        DataStream<ExcessBaggageTikFactModel> excessBaggageTikDataStream =
                processedStream.getSideOutput(CommonOutputTags.EXCESS_BAGGAGE_TIK_TAG);
        // 输出结果查看
        excessBaggageTikDataStream.print("excessBaggageTikDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<ExcessBaggageTikFactModel> excessBaggageTikSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_EXCESS_BAGGAGE_TIK_FACT");
        excessBaggageTikDataStream.sinkTo(excessBaggageTikSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);*/
    }

    public static void processEMDIssue(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
                /*if (!Constants.EVENT_EMDISSUE.equals(event)) {
                    return ;
                }*/
        logger.info("高频-EMD出票事件数据信息整合");
        // 子事件名称
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
        // 代码
        String code = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/ReasonForIssuance/@Code");
        // 子代码
        String subCode = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/ReasonForIssuance/@SubCode");
        // EMD票状态
        String newVal = XpathUtils.getString(document, "/Msg/Hdr/Newval");
        // EMD票号
        String emdTicketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/@EMDTicketNumber");
        // EMD出票日期
        String issueDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/@IssueDate");
        // EMD出票时间
        String issueTime = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/@IssueTime");
        // 乘机人英文姓
        String surname = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Surname");
        // 乘机人英文名
        String givenName = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/GivenName");
        // 乘机人中文姓名
        String nativeGivenName = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/NativeGivenName");
        nativeGivenName = NormalizationUtils.standardize(FieldType.CN_NAME, nativeGivenName);
        // 乘机人类型
        String passengerType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/@Type");
        // 乘机人证件类型
        String certiType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Type");
        // 乘机人证件号
        String documentNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Number");
        // 乘机人出生日期
        String birthDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@DateOfBirth");
        // 关联机票票号
        String ticketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@TicketNumber");
        // 国内国际标识
        String ticketType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/@TicketType");
        // EMD类型
        String emdType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/@Type");
        // EMD出票office
        String issueOffice = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/@IssueOffice");
        // EMD出票Office对应的IataCode
        String iataCode = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/@IataCode");
        // 币种
        String currencyCode = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/PaymentDetail/PaymentAmount/@CurrencyCode");
        // 附加服务金额
        Double amount = XpathUtils.getNumber(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/PaymentDetail/PaymentAmount/@Amount");
        // 附加服务详情
        String description = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/ReasonForIssuance/@Description");
        // 升舱后舱位
        String clazz = TransUtils.getOriginalCabin(description);
        if (clazz != null && !clazz.isEmpty()) {
            clazz = clazz.substring(0, 1);
        }
        // 逾重行李计量单位数
        Integer excessBaggageQuantity = XpathUtils.getInteger(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/ExcessBaggage/@UnitOfMeasureQuantity");
        // 行李数量及单位
        String unitOfMeasureCode = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/ExcessBaggage/@UnitOfMeasureCode");
        String tid = TransUtils.getTid(documentNumber, certiType, "HSD");

        NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
        for (int a = 0; a < segmentList.getLength(); a++) {
            Node segment = segmentList.item(a);
            // 起飞机场
            String depAirport = XpathUtils.getString(segment, "Departure/@AirportCode");
            // 到达机场
            String arrivalAirport = XpathUtils.getString(segment, "Arrival/@AirportCode");
            // 航班日期
            String departureDate = XpathUtils.getString(segment, "Departure/@Date");
            // 航班时间
            String departureTime = XpathUtils.getString(segment, "Departure/@Time");
            // 升舱前舱位
            String oldval = XpathUtils.getString(segment, "Cabin/@Clazz");
            if (oldval != null && !oldval.isEmpty()) {
                oldval = oldval.substring(0, 1);
            }
            // 市场航司二字码
            String marketingAirlineCode = XpathUtils.getString(segment, "Carrier/@AirlineCode");
            // 市场航司航班号
            String marketingFlightNumber = XpathUtils.getString(document, "Carrier/@FlightNumber");
            // 承运航司
            String operatingAirlineCode = XpathUtils.getString(segment, "OperatingCarrier/@AirlineCode");
            // 承运航班号
            String operatingFlightNum = XpathUtils.getString(segment, "OperatingCarrier/@FlightNumber");

            // 根据航程维表主键生成逻辑生成。航程主键的逻辑为：起飞日期 + 承运航司 + 承运航班号 +市场航司+市场航班号+ 起飞机场 + 降落机场
            String segmentPkId =
                    departureDate.replace("-", "") + operatingAirlineCode + operatingFlightNum + marketingAirlineCode + marketingFlightNumber + depAirport + arrivalAirport;

            String docNumber = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,
                    SM4Utils.encrypt(documentNumber, Constants.SM4_KEY));
            if ("A".equals(code) && "0B5".equals(subCode)) {
                // 选座出票-航段级
                SeatTikFactModel seatTikFactModel = new SeatTikFactModel();
                //EMD票号+起飞机场+到达机场
                seatTikFactModel.setPkId(emdTicketNumber + depAirport + arrivalAirport);
                seatTikFactModel.setFkSeattikDate(issueDate);
                seatTikFactModel.setFkSeattikTime(issueTime);
                seatTikFactModel.setDepairport(depAirport);
                seatTikFactModel.setArriairport(arrivalAirport);
                seatTikFactModel.setFkSeatstartDate(departureDate);
                seatTikFactModel.setFkSeatstartTime(departureTime);
                seatTikFactModel.setEnLastName(surname);
                seatTikFactModel.setEnFirstName(givenName);
                seatTikFactModel.setCnName(nativeGivenName);
                seatTikFactModel.setPassengerType(passengerType);
                seatTikFactModel.setCertType(NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, certiType,
                        DataSource.HIGH_FREQUENCY_DATA));
                seatTikFactModel.setCertNumber(docNumber);
                seatTikFactModel.setPassengerAge(TransUtils.getAgeByIdCard(documentNumber,birthDate));
                // IdMapping转换

                seatTikFactModel.setFkPassengerUserTid(tid);
                seatTikFactModel.setFkSeatSeg(segmentPkId);
                seatTikFactModel.setAkDimark(ticketType);
                seatTikFactModel.setAkTiknum(ticketNumber);
                seatTikFactModel.setAkEmdnum(emdTicketNumber);
                seatTikFactModel.setAkEmdStatus(newVal);
                seatTikFactModel.setAkSegcabin(clazz);
                seatTikFactModel.setAkCurrency(currencyCode);
                seatTikFactModel.setSeatAmount(amount);
                if ("CNY".equalsIgnoreCase(currencyCode)) {
                    seatTikFactModel.setSeatAmountCny(amount);
                }
                seatTikFactModel.setSeatDetail(description);
                // 解析选座详情：座位区域代码和座位号
                String[] seatInfo = TransUtils.parseSeatInfo(description);
                if (seatInfo != null && seatInfo.length >= 2) {
                    seatTikFactModel.setSeatAtt(seatInfo[0]);
                    seatTikFactModel.setSeatNumber(seatInfo[1]);
                }
                seatTikFactModel.setAkEmdtikchannel(issueOffice);
                seatTikFactModel.setAkEmdiatacode(iataCode);
                seatTikFactModel.setAkEmdtype(emdType);
                seatTikFactModel.setPurchasedWithTicket(false);
                seatTikFactModel.setDataActive(true);
                seatTikFactModel.setDataActiveTime(LocalDateTime.now().toString());
                seatTikFactModel.setSystemCreatetime(LocalDateTime.now().toString());
                seatTikFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());
                // 舱等
                seatTikFactModel.setAkCabin(getCabinClass(departureDate,clazz));

                HsdProcessDataModel hsdProcessDataModel = new HsdProcessDataModel();
                hsdProcessDataModel.setEvent(event);
                hsdProcessDataModel.setSubEvent(subEvent);
                hsdProcessDataModel.setTableName("T_DWD_SEAT_TIK_FACT");
                hsdProcessDataModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
                hsdProcessDataModel.setStamp(stamp);
                // 转换为JSON字符串
                hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(seatTikFactModel));
                hsdProcessDataModel.setProcessed(false);
                hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());

                if(StringUtils.isNotEmpty(seatTikFactModel.getPkId())) {
                    ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
                }else{
                    logger.info("高频-EMD出票事件数据信息整合[SeatTikFactModel]数据解析异常{}", value);
                }
            }

            if ("C".equals(code) && "0AA".equals(subCode)) {
                // 预付费行李出票-航段级
                BaggageTikFactModel baggageTikFactModel = new BaggageTikFactModel();
                //EMD票号+起飞机场+到达机场
                baggageTikFactModel.setPkId(emdTicketNumber + depAirport + arrivalAirport);
                baggageTikFactModel.setFkBaggagetikDate(issueDate);
                baggageTikFactModel.setFkBaggagetikTime(issueTime);
                baggageTikFactModel.setDepairport(depAirport);
                baggageTikFactModel.setArriairport(arrivalAirport);
                baggageTikFactModel.setFkBaggagestartDate(departureDate);
                baggageTikFactModel.setFkBaggagestartTime(departureTime);
                baggageTikFactModel.setEnLastName(surname);
                baggageTikFactModel.setEnFirstName(givenName);
                baggageTikFactModel.setCnName(nativeGivenName);
                baggageTikFactModel.setPassengerType(passengerType);
                baggageTikFactModel.setCertType(NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, certiType, DataSource.HIGH_FREQUENCY_DATA));
                baggageTikFactModel.setCertNumber(docNumber);
                baggageTikFactModel.setPassengerAge(TransUtils.getAgeByIdCard(documentNumber,birthDate));
                // IdMapping转换
                baggageTikFactModel.setFkPassengerUserTid(tid);
                baggageTikFactModel.setFkBaggageSeg(segmentPkId);
                baggageTikFactModel.setAkDimark(ticketType);
                baggageTikFactModel.setAkTiknum(ticketNumber);
                baggageTikFactModel.setAkEmdnum(emdTicketNumber);
                baggageTikFactModel.setAkEmdStatus(newVal);
                baggageTikFactModel.setAkSegcabin(clazz);
                baggageTikFactModel.setAkCurrency(currencyCode);
                baggageTikFactModel.setBaggageAmount(amount);
                if ("CNY".equalsIgnoreCase(currencyCode)) {
                    baggageTikFactModel.setBaggageAmountCny(amount);
                }
                baggageTikFactModel.setBaggageDetail(description);
                baggageTikFactModel.setAkEmdtikchannel(issueOffice);
                baggageTikFactModel.setAkEmdiatacode(iataCode);
                baggageTikFactModel.setAkEmdtype(emdType);
                // 预付费行李属性 TODO 可能有问题
                baggageTikFactModel.setAkBaggageAtt(TransUtils.getBaggageAttr(unitOfMeasureCode));
                if("N".equalsIgnoreCase(unitOfMeasureCode)){
                    baggageTikFactModel.setBaggageBags(TransUtils.getQuantity(description));
                }
                if("700".equals(unitOfMeasureCode)){
                    baggageTikFactModel.setBaggageQuantity(TransUtils.getQuantity(description));
                }
                baggageTikFactModel.setPurchasedWithTicket(false);
                baggageTikFactModel.setDataActive(true);
                baggageTikFactModel.setDataActiveTime(LocalDateTime.now().toString());
                baggageTikFactModel.setSystemCreatetime(LocalDateTime.now().toString());
                baggageTikFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());
                // 舱等
                baggageTikFactModel.setAkCabin(getCabinClass(departureDate,clazz));

                HsdProcessDataModel hsdProcessDataModel = new HsdProcessDataModel();
                hsdProcessDataModel.setEvent(event);
                hsdProcessDataModel.setSubEvent(subEvent);
                hsdProcessDataModel.setTableName("T_DWD_BAGGAGE_TIK_FACT");
                hsdProcessDataModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
                hsdProcessDataModel.setStamp(stamp);
                // 转换为JSON字符串
                hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(baggageTikFactModel));
                hsdProcessDataModel.setProcessed(false);
                hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());

                if(StringUtils.isNotEmpty(baggageTikFactModel.getPkId())) {
                    ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
                }else{
                    logger.info("高频-EMD出票事件数据信息整合[BaggageTikFactModel]数据解析异常{}", value);
                }
            }

            if ("G".equals(code) && ("0AG".equals(subCode) || "01G".equals(subCode) || "0B3".equals(subCode))) {
                // 选餐出票-航段级
                MealTikFactModel mealTikFactModel = new MealTikFactModel();
                //EMD票号+起飞机场+到达机场
                mealTikFactModel.setPkId(emdTicketNumber + depAirport + arrivalAirport);
                mealTikFactModel.setFkMealtikDate(issueDate);
                mealTikFactModel.setFkMealtikTime(issueTime);
                mealTikFactModel.setDepairport(depAirport);
                mealTikFactModel.setArriairport(arrivalAirport);
                mealTikFactModel.setFkMealstartDate(departureDate);
                mealTikFactModel.setFkMealstartTime(departureTime);
                mealTikFactModel.setEnLastName(surname);
                mealTikFactModel.setEnFirstName(givenName);
                mealTikFactModel.setCnName(nativeGivenName);
                mealTikFactModel.setPassengerType(passengerType);
                mealTikFactModel.setCertType(NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, certiType, DataSource.HIGH_FREQUENCY_DATA));
                mealTikFactModel.setCertNumber(docNumber);
                mealTikFactModel.setPassengerAge(TransUtils.getAgeByIdCard(documentNumber,birthDate));
                // IdMapping转换
                mealTikFactModel.setFkPassengerUserTid(tid);
                mealTikFactModel.setFkMealSeg(segmentPkId);
                mealTikFactModel.setAkDimark(ticketType);
                mealTikFactModel.setAkTiknum(ticketNumber);
                mealTikFactModel.setAkEmdnum(emdTicketNumber);
                mealTikFactModel.setAkEmdStatus(newVal);
                mealTikFactModel.setAkSegcabin(clazz);
                mealTikFactModel.setAkCurrency(currencyCode);
                mealTikFactModel.setMealAmount(amount);
                mealTikFactModel.setMealDeatail(description);
                mealTikFactModel.setAkEmdtikchannel(issueOffice);
                mealTikFactModel.setAkEmdiataCode(iataCode);
                mealTikFactModel.setAkEmdtype(emdType);
                mealTikFactModel.setDataActive(true);
                mealTikFactModel.setDataActiveTime(LocalDateTime.now().toString());
                mealTikFactModel.setSystemCreatetime(LocalDateTime.now().toString());
                mealTikFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());
                // 舱等
                mealTikFactModel.setAkCabin(getCabinClass(departureDate,clazz));

                HsdProcessDataModel hsdProcessDataModel = new HsdProcessDataModel();
                hsdProcessDataModel.setEvent(event);
                hsdProcessDataModel.setSubEvent(subEvent);
                hsdProcessDataModel.setTableName("T_DWD_MEAL_TIK_FACT");
                hsdProcessDataModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
                hsdProcessDataModel.setStamp(stamp);
                // 转换为JSON字符串
                hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(mealTikFactModel));
                hsdProcessDataModel.setProcessed(false);
                hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());

                if(StringUtils.isNotEmpty(mealTikFactModel.getPkId())) {
                    ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
                }else{
                    logger.info("高频-EMD出票事件数据信息整合[MealTikFactModel]数据解析异常{}", value);
                }
            }

            if ("A".equals(code) && "04E".equals(subCode)) {
                // 升舱出票-航段级
                UpgrTikFactModel updgrTikFactModel = new UpgrTikFactModel();
                //EMD票号+起飞机场+到达机场
                updgrTikFactModel.setPkId(emdTicketNumber + depAirport + arrivalAirport);
                updgrTikFactModel.setFkUpgrTikDate(issueDate);
                updgrTikFactModel.setFkUpgrTikTime(issueTime);
                updgrTikFactModel.setDepairport(depAirport);
                updgrTikFactModel.setArriairport(arrivalAirport);
                updgrTikFactModel.setFkSegDate(departureDate);
                updgrTikFactModel.setFkSegTime(departureTime);
                updgrTikFactModel.setEnLastName(surname);
                updgrTikFactModel.setEnFirstName(givenName);
                updgrTikFactModel.setCnName(nativeGivenName);
                updgrTikFactModel.setPassengerType(passengerType);
                updgrTikFactModel.setCertType(NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, certiType, DataSource.HIGH_FREQUENCY_DATA));
                updgrTikFactModel.setCertNumber(docNumber);
                updgrTikFactModel.setPassengerAge(TransUtils.getAgeByIdCard(documentNumber,birthDate));
                // IdMapping转换
                updgrTikFactModel.setFkPassengerUserTid(tid);
                updgrTikFactModel.setFkCabinupgrinvSeg(segmentPkId);
                updgrTikFactModel.setAkDimark(ticketType);
                updgrTikFactModel.setAkTiknum(ticketNumber);
                updgrTikFactModel.setAkEmdnum(emdTicketNumber);
                updgrTikFactModel.setAkEmdStatus(newVal);
                updgrTikFactModel.setAkUpgrdoldCabin(oldval);
                updgrTikFactModel.setAkUpgrdnewCabin(clazz);
                updgrTikFactModel.setAkCurrency(currencyCode);
                updgrTikFactModel.setUpgrdoldAmount(amount);
                if ("CNY".equalsIgnoreCase(currencyCode)) {
                    updgrTikFactModel.setUpgrdoldAmountCny(amount);
                }
                updgrTikFactModel.setUpgrdoldDettail(description);
                updgrTikFactModel.setAkEmdtikchannel(issueOffice);
                updgrTikFactModel.setAkEmdiataCode(iataCode);
                updgrTikFactModel.setAkEmdtype(emdType);
                updgrTikFactModel.setDataActive(true);
                updgrTikFactModel.setDataActiveTime(LocalDateTime.now().toString());
                updgrTikFactModel.setSystemCreatetime(LocalDateTime.now().toString());
                updgrTikFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());
                // 升舱前舱等 和 升舱后舱等
                updgrTikFactModel.setUpgrdoldClass(getCabinClass(departureDate,oldval));
                updgrTikFactModel.setUpgrdnewClass(getCabinClass(departureDate,clazz));

                HsdProcessDataModel hsdProcessDataModel = new HsdProcessDataModel();
                hsdProcessDataModel.setEvent(event);
                hsdProcessDataModel.setSubEvent(subEvent);
                hsdProcessDataModel.setTableName("T_DWD_UPGR_TIK_FACT");
                hsdProcessDataModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
                hsdProcessDataModel.setStamp(stamp);
                // 转换为JSON字符串
                hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(updgrTikFactModel));
                hsdProcessDataModel.setProcessed(false);
                hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());

                if(StringUtils.isNotEmpty(updgrTikFactModel.getPkId())) {
                    ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
                }else{
                    logger.info("高频-EMD出票事件数据信息整合[UpgrTikFactModel]数据解析异常{}", value);
                }
            }

            if ("C".equals(code) && ("0GO".equals(subCode) || "0DG".equals(subCode) || "0IJ".equals(subCode))) {
                // 逾重行李出票-航段级
                ExcessBaggageTikFactModel excessBaggageTikFactModel = new ExcessBaggageTikFactModel();
                //EMD票号+起飞机场+到达机场
                excessBaggageTikFactModel.setPkId(emdTicketNumber + depAirport + arrivalAirport);
                excessBaggageTikFactModel.setFkBaggagetikDate(issueDate);
                excessBaggageTikFactModel.setFkBaggagetikTime(issueTime);
                excessBaggageTikFactModel.setDepairport(depAirport);
                excessBaggageTikFactModel.setArriairport(arrivalAirport);
                excessBaggageTikFactModel.setFkBaggagestartDate(departureDate);
                excessBaggageTikFactModel.setFkBaggagestartTime(departureTime);
                excessBaggageTikFactModel.setEnLastName(surname);
                excessBaggageTikFactModel.setEnFirstName(givenName);
                excessBaggageTikFactModel.setCnName(nativeGivenName);
                excessBaggageTikFactModel.setPassengerType(passengerType);
                excessBaggageTikFactModel.setCertType(NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, certiType, DataSource.HIGH_FREQUENCY_DATA));
                excessBaggageTikFactModel.setCertNumber(docNumber);
                excessBaggageTikFactModel.setPassengerAge(TransUtils.getAgeByIdCard(documentNumber,birthDate));
                // IdMapping转换
                excessBaggageTikFactModel.setFkPassengerUserTid(tid);
                excessBaggageTikFactModel.setFkBaggageSeg(segmentPkId);
                excessBaggageTikFactModel.setAkDimark(ticketType);
                excessBaggageTikFactModel.setAkTiknum(ticketNumber);
                excessBaggageTikFactModel.setAkEmdnum(emdTicketNumber);
                excessBaggageTikFactModel.setAkEmdStatus(newVal);
                excessBaggageTikFactModel.setAkEmdtype(emdType);
                // 逾重行李属性 TODO解析行李详情字段：付费分：计件则写入PC/计重则写入KG。混合行李：MIX（既有计件也有计重的emd行李）
                excessBaggageTikFactModel.setAkBaggageAtt(TransUtils.getBaggageAttr(unitOfMeasureCode));
                excessBaggageTikFactModel.setAkExcessBaggageDesc(description);
                if("N".equalsIgnoreCase(unitOfMeasureCode)){
                    excessBaggageTikFactModel.setAkExcessBaggageNumber(TransUtils.getQuantity(description));
                }
                if("700".equals(unitOfMeasureCode)){
                    excessBaggageTikFactModel.setAkExcessBaggageWeight(TransUtils.getQuantity(description));
                }
                excessBaggageTikFactModel.setAkExcessBaggageOldCurrencyCode(currencyCode);
                excessBaggageTikFactModel.setAkExcessBaggageOldAmount(amount);
                if ("CNY".equalsIgnoreCase(currencyCode)) {
                    excessBaggageTikFactModel.setAkExcessBaggageOldAmountCny(amount);
                }
                /**
                  * TODO 与其他EMD不同，可以结合事情数据样例看下
                  * 币种
                  * 逾重行李金额
                  * 逾重行李单价
                  */
                excessBaggageTikFactModel.setAkEmdtikchannel(issueOffice);
                excessBaggageTikFactModel.setAkEmdiataCode(iataCode);
                excessBaggageTikFactModel.setAkEmdtype(emdType);
                excessBaggageTikFactModel.setExcessbaggageQuantity(excessBaggageQuantity);
                excessBaggageTikFactModel.setDataActive(true);
                excessBaggageTikFactModel.setDataActiveTime(LocalDateTime.now().toString());
                excessBaggageTikFactModel.setSystemCreatetime(LocalDateTime.now().toString());
                excessBaggageTikFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

                HsdProcessDataModel hsdProcessDataModel = new HsdProcessDataModel();
                hsdProcessDataModel.setEvent(event);
                hsdProcessDataModel.setSubEvent(subEvent);
                hsdProcessDataModel.setTableName("T_DWD_EXCESS_BAGGAGE_TIK_FACT");
                hsdProcessDataModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
                hsdProcessDataModel.setStamp(stamp);
                // 转换为JSON字符串
                hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(excessBaggageTikFactModel));
                hsdProcessDataModel.setProcessed(false);
                hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());

                if(StringUtils.isNotEmpty(excessBaggageTikFactModel.getPkId())) {
                    ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
                }else{
                    logger.info("高频-EMD出票事件数据信息整合[ExcessBaggageTikFactModel]数据解析异常{}", value);
                }
            }
        }
    }
}
