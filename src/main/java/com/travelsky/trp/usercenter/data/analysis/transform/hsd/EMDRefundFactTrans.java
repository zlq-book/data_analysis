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
public class EMDRefundFactTrans {

    static final Logger logger = LoggerFactory.getLogger(EMDRefundFactTrans.class);
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
                processEMDRefund(value, ctx, document);
            }
        }).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        DataStream<HsdProcessDataModel> hsdProcessDataStream = processedStream.getSideOutput(CommonOutputTags.HSD_PROCESS_DATA_TAG);
        //hsdProcessDataStream.print("hsdProcessDataStream");
        hsdProcessDataStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_HSD_PROCESS_DATA")).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        /*// 获取各个SideOutput流
        DataStream<SeatTikFactModel > seatTikDataStream = processedStream.getSideOutput(CommonOutputTags.SEAT_TIK_TAG);
        // 输出结果查看
//        seatTikDataStream.print("seatTikDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<SeatTikFactModel> seatTikSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_SEAT_TIK_FACT");
        seatTikDataStream.sinkTo(seatTikSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        // 获取各个SideOutput流
        DataStream<SeatRefundFactModel > seatRefundDataStream = processedStream.getSideOutput(CommonOutputTags.SEAT_REFUND_TAG);
        // 输出结果查看
//        seatRefundDataStream.print("seatRefundDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<SeatRefundFactModel> seatRefundSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_SEAT_REFUND_FACT");
        seatRefundDataStream.sinkTo(seatRefundSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        // 获取各个SideOutput流
        DataStream<BaggageTikFactModel> baggageTikDataStream = processedStream.getSideOutput(CommonOutputTags.BAGGAGE_TIK_TAG);
        // 输出结果查看
//        baggageTikDataStream.print("baggageTikDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<BaggageTikFactModel> baggageTikSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_BAGGAGE_TIK_FACT");
        baggageTikDataStream.sinkTo(baggageTikSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        // 获取各个SideOutput流
        DataStream<BaggageRefundFactModel> baggageRefundDataStream = processedStream.getSideOutput(CommonOutputTags.BAGGAGE_REFUND_TAG);
        // 输出结果查看
//        baggageRefundDataStream.print("baggageRefundDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<BaggageRefundFactModel> baggageRefundSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_BAGGAGE_REFUND_FACT");
        baggageRefundDataStream.sinkTo(baggageRefundSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        // 获取各个SideOutput流
        DataStream<MealTikFactModel> mealTikDataStream = processedStream.getSideOutput(CommonOutputTags.MEAL_TIK_TAG);
        // 输出结果查看
//        mealTikDataStream.print("mealTikDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<MealTikFactModel> mealTikSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_MEAL_TIK_FACT");
        mealTikDataStream.sinkTo(mealTikSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        // 获取各个SideOutput流
        DataStream<MealRefundFactModel> mealRefundDataStream = processedStream.getSideOutput(CommonOutputTags.MEAL_REFUND_TAG);
        // 输出结果查看
//        mealRefundDataStream.print("mealRefundDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<MealRefundFactModel> mealRefundSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_MEAL_REFUND_FACT");
        mealRefundDataStream.sinkTo(mealRefundSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        // 获取各个SideOutput流
        DataStream<UpgrTikFactModel> upgrTikDataStream = processedStream.getSideOutput(CommonOutputTags.UPGR_TIK_TAG);
        // 输出结果查看
//        upgrTikDataStream.print("upgrTikDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<UpgrTikFactModel> upgrTikSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_UPGR_TIK_FACT");
        upgrTikDataStream.sinkTo(upgrTikSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        // 获取各个SideOutput流
        DataStream<UpgrRefundFactModel> upgrRefundDataStream = processedStream.getSideOutput(CommonOutputTags.UPGR_REFUND_TAG);
        // 输出结果查看
        upgrRefundDataStream.print("upgrRefundDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<UpgrRefundFactModel> upgrRefundSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_UPGR_REFUND_FACT");
        upgrRefundDataStream.sinkTo(upgrRefundSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        // 获取各个SideOutput流
        DataStream<ExcessBaggageTikFactModel> excessBaggageTikDataStream =
                processedStream.getSideOutput(CommonOutputTags.EXCESS_BAGGAGE_TIK_TAG);
        // 输出结果查看
        excessBaggageTikDataStream.print("excessBaggageTikDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<ExcessBaggageTikFactModel> excessBaggageTikSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_EXCESS_BAGGAGE_TIK_FACT");
        excessBaggageTikDataStream.sinkTo(excessBaggageTikSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);*/
    }

    public static void processEMDRefund(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
                /*if (!Constants.EVENT_EMDREFUND.equals(event)) {
                    return;
                }*/
        logger.info("高频-EMD退票事件数据信息整合");
        // 子事件名称
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
        // 代码
        String code = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/ReasonForIssuance/@Code");
        // 子代码
        String subCode = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/ReasonForIssuance/@SubCode");
        // EMD票状态
        String newVal = XpathUtils.getString(document, "/Msg/Hdr/Newval");
        // 操作时间
        String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
        // 提取日期部分（前8位）
        String refundDate = uptm.length() >= 8 ? uptm.substring(0, 8) : uptm;
        // 提取时间部分（后6位）
        String refundTimeRaw = uptm.length() >= 14 ? uptm.substring(8, 14) : (uptm.length() >= 8 ? uptm.substring(8) : "");

        String refundTime = "";
        if (refundTimeRaw.length() == 6) {
            refundTime = refundTimeRaw.replaceFirst("(\\d{2})(\\d{2})(\\d{2})", "$1:$2:$3");
        } else {
            refundTime = refundTimeRaw; // 或者设置默认值或日志警告
        }

        // EMD票号
        String emdTicketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/@EMDTicketNumber");
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
        // 金额
        Double amount = XpathUtils.getNumber(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/PaymentDetail/PaymentAmount/@Amount");
        // 详情
        String description = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/ReasonForIssuance/@Description");
        // 升舱后舱位
        String clazz = TransUtils.getOriginalCabin(description);
        if (clazz != null && !clazz.isEmpty()) {
            clazz = clazz.substring(0, 1);
        }
        // 行李数量及单位
        String unitOfMeasureCode = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/ExcessBaggage/@UnitOfMeasureCode");
        String tid = TransUtils.getTid(documentNumber, certiType, "HSD");
        String docNumber = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,
                SM4Utils.encrypt(documentNumber, Constants.SM4_KEY));

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
            String segmentPkId = departureDate.replace("-", "") + operatingAirlineCode + operatingFlightNum + marketingAirlineCode + marketingFlightNumber + depAirport + arrivalAirport;

            if ("A".equals(code) && "0B5".equals(subCode)) {
                // 选座出票-航段级
                SeatTikFactModel seatTikFactModel = new SeatTikFactModel();
                //EMD票号+起飞机场+到达机场
                seatTikFactModel.setPkId(emdTicketNumber + depAirport + arrivalAirport);
                seatTikFactModel.setAkEmdStatus(newVal);
                seatTikFactModel.setDataActive(true);
                seatTikFactModel.setDataActiveTime(LocalDateTime.now().toString());
                seatTikFactModel.setSystemCreatetime(LocalDateTime.now().toString());
                seatTikFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

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
                    logger.info("高频-EMD退票事件数据信息整合[SeatTikFactModel]数据解析异常{}", value);
                }

                // 选座退票-航段级
                SeatRefundFactModel seatRefundFactModel = new SeatRefundFactModel();
                //EMD票号+起飞机场+到达机场
                seatRefundFactModel.setPkId(emdTicketNumber + depAirport + arrivalAirport);
                seatRefundFactModel.setFkRefundDate(refundDate);
                seatRefundFactModel.setFkRefundTime(refundTime);
                seatRefundFactModel.setDepairport(depAirport);
                seatRefundFactModel.setArriairport(arrivalAirport);
                seatRefundFactModel.setFkSeatstartDate(departureDate);
                seatRefundFactModel.setFkSeatstartTime(departureTime);
                seatRefundFactModel.setEnLastName(surname);
                seatRefundFactModel.setEnFirstName(givenName);
                seatRefundFactModel.setCnName(nativeGivenName);
                seatRefundFactModel.setPassengerType(passengerType);
                seatRefundFactModel.setCertType(NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, certiType,
                        DataSource.HIGH_FREQUENCY_DATA));
                seatRefundFactModel.setCertNumber(docNumber);
                seatRefundFactModel.setPassengerAge(TransUtils.getAgeByIdCard(documentNumber, birthDate));
                // IdMapping转换
                seatRefundFactModel.setFkPassengerUserTid(tid);
                seatRefundFactModel.setFkSeatSeg(segmentPkId);
                seatRefundFactModel.setAkDimark(ticketType);
                seatRefundFactModel.setAkTiknum(ticketNumber);
                seatRefundFactModel.setAkEmdnum(emdTicketNumber);
                seatRefundFactModel.setAkEmdStatus(newVal);
                seatRefundFactModel.setAkSegcabin(clazz);
                seatRefundFactModel.setAkCurrency(currencyCode);
                seatRefundFactModel.setSeatAmount(amount);
                if ("CNY".equalsIgnoreCase(currencyCode)) {
                    seatTikFactModel.setSeatAmountCny(amount);
                }
                seatRefundFactModel.setSeatDetail(description);
                // 解析选座详情：座位区域代码和座位号
                String[] seatInfo = TransUtils.parseSeatInfo(description);
                if (seatInfo != null && seatInfo.length >= 2) {
                    seatRefundFactModel.setSeatZone(seatInfo[0]);
                    seatRefundFactModel.setSeatNumber(seatInfo[1]);
                }
                // 选座退票次数计数
                seatRefundFactModel.setSeatCount(1);
                seatRefundFactModel.setDataActive(true);
                // 舱等
                seatRefundFactModel.setAkCabin(getCabinClass(departureDate, clazz));
                seatRefundFactModel.setDataActiveTime(LocalDateTime.now().toString());
                seatRefundFactModel.setSystemCreatetime(LocalDateTime.now().toString());
                seatRefundFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

                hsdProcessDataModel.setTableName("T_DWD_SEAT_REFUND_FACT");
                // 转换为JSON字符串
                hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(seatRefundFactModel));
                hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());

                if(StringUtils.isNotEmpty(seatRefundFactModel.getPkId())) {
                    ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
                }else{
                    logger.info("高频-EMD退票事件数据信息整合[SeatRefundFactModel]数据解析异常{}", value);
                }
            }


            if ("C".equals(code) && "0AA".equals(subCode)) {
                // 预付费行李出票-航段级
                BaggageTikFactModel baggageTikFactModel = new BaggageTikFactModel();
                //EMD票号+起飞机场+到达机场
                baggageTikFactModel.setPkId(emdTicketNumber + depAirport + arrivalAirport);
                baggageTikFactModel.setAkEmdStatus(newVal);
                baggageTikFactModel.setDataActive(true);
                baggageTikFactModel.setDataActiveTime(LocalDateTime.now().toString());
                baggageTikFactModel.setSystemCreatetime(LocalDateTime.now().toString());
                baggageTikFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

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
                    logger.info("高频-EMD退票事件数据信息整合[BaggageTikFactModel]数据解析异常{}", value);
                }

                // 预付费行李退票-航段级
                BaggageRefundFactModel baggageRefundFactModel = new BaggageRefundFactModel();
                //EMD票号+起飞机场+到达机场
                baggageRefundFactModel.setPkId(emdTicketNumber + depAirport + arrivalAirport);
                baggageRefundFactModel.setFkRefundDate(refundDate);
                baggageRefundFactModel.setFkRefundTime(refundTime);
                baggageRefundFactModel.setFkBaggagestartDate(departureDate);
                baggageRefundFactModel.setFkBaggagestartTime(departureTime);
                baggageRefundFactModel.setEnLastName(surname);
                baggageRefundFactModel.setEnFirstName(givenName);
                baggageRefundFactModel.setCnName(nativeGivenName);
                baggageRefundFactModel.setPassengerType(passengerType);
                baggageRefundFactModel.setCertType(NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, certiType,
                        DataSource.HIGH_FREQUENCY_DATA));
                baggageRefundFactModel.setCertNumber(docNumber);
                baggageRefundFactModel.setPassengerAge(TransUtils.getAgeByIdCard(documentNumber,birthDate));
                // IdMapping转换
                baggageRefundFactModel.setFkPassengerUserTid(tid);
                baggageRefundFactModel.setFkBaggageSeg(segmentPkId);
                baggageRefundFactModel.setAkDimark(ticketType);
                baggageRefundFactModel.setAkTiknum(ticketNumber);
                baggageRefundFactModel.setAkEmdnum(emdTicketNumber);
                baggageRefundFactModel.setAkEmdStatus(newVal);
                baggageRefundFactModel.setAkSegcabin(clazz);
                baggageRefundFactModel.setAkCurrency(currencyCode);
                // 预付费行李金额
                baggageRefundFactModel.setBaggageAmount(amount);
                if ("CNY".equalsIgnoreCase(currencyCode)) {
                    baggageRefundFactModel.setBaggageAmountCny(amount);
                }
                baggageRefundFactModel.setBaggageDetail(description);
                // 预付费行李属性 TODO 可能有问题
                baggageRefundFactModel.setAkBaggageAtt(TransUtils.getBaggageAttr(unitOfMeasureCode));
                if("N".equalsIgnoreCase(unitOfMeasureCode)){
                    baggageRefundFactModel.setBaggageBags(TransUtils.getQuantity(description));
                }
                if("700".equals(unitOfMeasureCode)){
                    baggageRefundFactModel.setBaggageQuantity(TransUtils.getQuantity(description));
                }
                baggageRefundFactModel.setDataActive(true);
                baggageRefundFactModel.setDataActiveTime(LocalDateTime.now().toString());
                baggageRefundFactModel.setSystemCreatetime(LocalDateTime.now().toString());
                baggageRefundFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());
                // 舱等
                baggageRefundFactModel.setAkCabin(getCabinClass(departureDate,clazz));

                hsdProcessDataModel.setTableName("T_DWD_BAGGAGE_REFUND_FACT");
                // 转换为JSON字符串
                hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(baggageRefundFactModel));
                hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());

                if(StringUtils.isNotEmpty(baggageRefundFactModel.getPkId())) {
                    ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
                }else{
                    logger.info("高频-EMD退票事件数据信息整合[BaggageRefundFactModel]数据解析异常{}", value);
                }
            }

            if ("G".equals(code) && ("0AG".equals(subCode) || "01G".equals(subCode) || "0B3".equals(subCode))) {
                // 选餐出票-航段级
                MealTikFactModel mealTikFactModel = new MealTikFactModel();
                //EMD票号+起飞机场+到达机场
                mealTikFactModel.setPkId(emdTicketNumber + depAirport + arrivalAirport);
                mealTikFactModel.setAkEmdStatus(newVal);
                mealTikFactModel.setDataActive(true);
                mealTikFactModel.setDataActiveTime(LocalDateTime.now().toString());
                mealTikFactModel.setSystemCreatetime(LocalDateTime.now().toString());
                mealTikFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

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
                    logger.info("高频-EMD退票事件数据信息整合[MealTikFactModel]数据解析异常{}", value);
                }

                // 选餐退票-航段级
                MealRefundFactModel mealRefundFactModel = new MealRefundFactModel();
                //EMD票号+起飞机场+到达机场
                mealRefundFactModel.setPkId(emdTicketNumber + depAirport + arrivalAirport);
                mealRefundFactModel.setFkRefundDate(refundDate);
                mealRefundFactModel.setFkRefundTime(refundTime);
                mealRefundFactModel.setFkMealstartDate(departureDate);
                mealRefundFactModel.setFkMealstartTime(departureTime);
                mealRefundFactModel.setEnLastName(surname);
                mealRefundFactModel.setEnFirstName(givenName);
                mealRefundFactModel.setCnName(nativeGivenName);
                mealRefundFactModel.setPassengerType(passengerType);
                mealRefundFactModel.setCertType(NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, certiType,
                        DataSource.HIGH_FREQUENCY_DATA));
                mealRefundFactModel.setCertNumber(docNumber);
                mealRefundFactModel.setPassengerAge(TransUtils.getAgeByIdCard(documentNumber,birthDate));
                // IdMapping转换
//                    mealRefundFactModel.setFkBookingUserTid(tid);
                mealRefundFactModel.setFkMealSeg(segmentPkId);
                mealRefundFactModel.setAkDimark(ticketType);
                mealRefundFactModel.setAkTiknum(ticketNumber);
                mealRefundFactModel.setAkEmdnum(emdTicketNumber);
                mealRefundFactModel.setAkEmdStatus(newVal);
                mealRefundFactModel.setAkSegcabin(clazz);
                mealRefundFactModel.setAkCurrency(currencyCode);
                mealRefundFactModel.setMealAmount(amount);
                mealRefundFactModel.setDepairport(description);
                mealRefundFactModel.setAkEmdtikchannel(issueOffice);
                mealRefundFactModel.setAkEmdiataCode(iataCode);
                mealRefundFactModel.setAkEmdtype(emdType);
                mealRefundFactModel.setDataActive(true);
                mealRefundFactModel.setDataActiveTime(LocalDateTime.now().toString());
                mealRefundFactModel.setSystemCreatetime(LocalDateTime.now().toString());
                mealRefundFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());
                // 舱等
                mealRefundFactModel.setAkCabin(getCabinClass(departureDate,clazz));

                hsdProcessDataModel.setTableName("T_DWD_MEAL_REFUND_FACT");
                // 转换为JSON字符串
                hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(mealRefundFactModel));
                hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());

                if(StringUtils.isNotEmpty(mealRefundFactModel.getPkId())) {
                    ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
                }else{
                    logger.info("高频-EMD退票事件数据信息整合[MealRefundFactModel]数据解析异常{}", value);
                }
            }

            if ("A".equals(code) && "04E".equals(subCode)) {
                // 升舱出票-航段级
                UpgrTikFactModel updgrTikFactModel = new UpgrTikFactModel();
                //EMD票号+起飞机场+到达机场
                updgrTikFactModel.setPkId(emdTicketNumber + depAirport + arrivalAirport);
                updgrTikFactModel.setAkEmdStatus(newVal);
                updgrTikFactModel.setDataActive(true);
                updgrTikFactModel.setDataActiveTime(LocalDateTime.now().toString());
                updgrTikFactModel.setSystemCreatetime(LocalDateTime.now().toString());
                updgrTikFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

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
                    logger.info("高频-EMD退票事件数据信息整合[UpgrTikFactModel]数据解析异常{}", value);
                }

                // 升舱退票-航段级
                UpgrRefundFactModel upgrRefundFactModel = new UpgrRefundFactModel();
                //EMD票号+起飞机场+到达机场
                upgrRefundFactModel.setPkId(emdTicketNumber + depAirport + arrivalAirport);
                upgrRefundFactModel.setFkUpgrrefundDate(refundDate);
                upgrRefundFactModel.setFkUpgrrefundTime(refundTime);
                upgrRefundFactModel.setDepairport(depAirport);
                upgrRefundFactModel.setArriairport(arrivalAirport);
                upgrRefundFactModel.setFkSegDate(departureDate);
                upgrRefundFactModel.setFkSegTime(departureTime);
                upgrRefundFactModel.setEnLastName(surname);
                upgrRefundFactModel.setEnFirstName(givenName);
                upgrRefundFactModel.setCnName(nativeGivenName);
                upgrRefundFactModel.setPassengerType(passengerType);
                upgrRefundFactModel.setCertType(NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, certiType,
                        DataSource.HIGH_FREQUENCY_DATA));
                upgrRefundFactModel.setCertNumber(docNumber);
                upgrRefundFactModel.setPassengerAge(TransUtils.getAgeByIdCard(documentNumber,birthDate));
                // IdMapping转换
                upgrRefundFactModel.setFkPassengerUserTid(tid);
                upgrRefundFactModel.setFkCabinupgrinvSeg(segmentPkId);
                upgrRefundFactModel.setAkDimark(ticketType);
                upgrRefundFactModel.setAkTiknum(ticketNumber);
                upgrRefundFactModel.setAkEmdnum(emdTicketNumber);
                upgrRefundFactModel.setAkEmdStatus(newVal);
                upgrRefundFactModel.setAkUpgrdoldCabin(oldval);
                upgrRefundFactModel.setAkUpgrdnewCabin(clazz);
                upgrRefundFactModel.setAkCurrency(currencyCode);
                upgrRefundFactModel.setUpgrdoldAmount(amount);
                if ("CNY".equalsIgnoreCase(currencyCode)) {
                    upgrRefundFactModel.setUpgrdoldAmountCny(amount);
                }
                upgrRefundFactModel.setUpgrdoldDettail(description);
                upgrRefundFactModel.setAkEmdtikchannel(issueOffice);
                upgrRefundFactModel.setAkEmdiataCode(iataCode);
                upgrRefundFactModel.setAkEmdtype(emdType);
                upgrRefundFactModel.setDataActive(true);
                upgrRefundFactModel.setDataActiveTime(LocalDateTime.now().toString());
                upgrRefundFactModel.setSystemCreatetime(LocalDateTime.now().toString());
                upgrRefundFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());
                // 升舱前舱等 和 升舱后舱等
                upgrRefundFactModel.setUpgrdnewClass(getCabinClass(departureDate,clazz));
                upgrRefundFactModel.setUpgrdoldClass(getCabinClass(departureDate,oldval));

                hsdProcessDataModel.setTableName("T_DWD_UPGR_REFUND_FACT");
                // 转换为JSON字符串
                hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(upgrRefundFactModel));
                hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());

                if(StringUtils.isNotEmpty(upgrRefundFactModel.getPkId())) {
                    ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
                }else{
                    logger.info("高频-EMD退票事件数据信息整合[UpgrRefundFactModel]数据解析异常{}", value);
                }
            }

            if ("C".equals(code) && ("0GO".equals(subCode) || "0DG".equals(subCode) || "0IJ".equals(subCode))) {
                // 逾重行李出票-航段级
                ExcessBaggageTikFactModel excessBaggageTikFactModel = new ExcessBaggageTikFactModel();
                //EMD票号+起飞机场+到达机场
                excessBaggageTikFactModel.setPkId(emdTicketNumber + depAirport + arrivalAirport);
                excessBaggageTikFactModel.setPassengerAge(TransUtils.getAgeByIdCard(documentNumber,birthDate));
                excessBaggageTikFactModel.setAkEmdStatus(newVal);
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
                    logger.info("高频-EMD退票事件数据信息整合[ExcessBaggageTikFactModel]数据解析异常{}", value);
                }
            }
        }
    }
}
