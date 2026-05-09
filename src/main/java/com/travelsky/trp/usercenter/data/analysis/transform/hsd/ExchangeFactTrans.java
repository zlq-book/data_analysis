package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.DatechangeSegFactModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.HsdProcessDataModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingSegFactModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingTicFactModel;
import com.travelsky.trp.usercenter.data.analysis.utils.CustomJsonSerializer;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.getCabinClass;
import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.isKeyAccount;

/**
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/15  16:43
 */
public class ExchangeFactTrans {

    static final Logger logger = LoggerFactory.getLogger(ExchangeFactTrans.class);

    public static void output(DataStream<String> source) {

        SingleOutputStreamOperator<Void> processedStream = source.process(new ProcessFunction<String, Void>() {
            private transient Connection connDwd;
            private transient Statement stmtDwd;

            @Override
            public void open(Configuration parameters) throws Exception {
                connDwd = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
                stmtDwd = DorisUtils.getStatement(connDwd);
            }

            @Override
            public void close() throws Exception {
                DorisUtils.close(connDwd, stmtDwd, null);
            }

            @Override
            public void processElement(String value, Context ctx, Collector<Void> out) throws Exception {
                Document document;
                try {
                    document = DocumentUtils.string2Document(value);
                } catch (Exception e) {
                    logger.warn("convert document is error, the content is {}, the error is {}", value, e.getMessage());
                    return;
                }
                processExchange(value, ctx, document);
            }


        }).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        DataStream<HsdProcessDataModel> hsdProcessDataStream = processedStream.getSideOutput(CommonOutputTags.HSD_PROCESS_DATA_TAG);
        //hsdProcessDataStream.print("hsdProcessDataStream");
        hsdProcessDataStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_HSD_PROCESS_DATA")).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        /*// 获取各个SideOutput流
        DataStream<TickingSegFactModel> tickingSegDataStream = processedStream.getSideOutput(CommonOutputTags.TICKING_SEG_TAG);
        // 输出结果查看
//        tickingSegDataStream.print("tickingSegDataStream");
        // 获取各个SideOutput流
        DataStream<TickingTicFactModel> tickingTicDataStream = processedStream.getSideOutput(CommonOutputTags.TICKING_TIC_TAG);
        // 输出结果查看
//        tickingTicDataStream.print("tickingTicDataStream");
        DataStream<DatechangeSegFactModel> datechangeSegDataStream = processedStream.getSideOutput(CommonOutputTags.DATECHANGE_SEG_TAG);

        // 输出结果查看
//        datechangeSegDataStream.print("datechangeSegDataStream");

        // 创建并配置各模型的Doris Sink
        DorisSink<TickingSegFactModel> tickingSegSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_SEG_FACT");
        DorisSink<TickingTicFactModel> tickingTicSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_TIC_FACT");
        DorisSink<DatechangeSegFactModel> datechangeSegSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_DATECHANGE_SEG_FACT");

        // 数据分别写入对应的Doris表
        tickingSegDataStream.sinkTo(tickingSegSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        tickingTicDataStream.sinkTo(tickingTicSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        datechangeSegDataStream.sinkTo(datechangeSegSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);*/
    }

    public static void processExchange(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
                /*if (!Constants.EVENT_EXCHANGE.equals(event)) {
                    return;
                }*/
        logger.info("高频-换开事件数据信息整合");
        // 子事件名称
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
        // PNR号
        String pnr = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@RecordLocator");
        // 改升后票号
        String newTicketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo" + "/@TicketNumber");
        // 改升前票号
        String oldTicketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo" + "/OriginalTicket/@TicketNumber");
        // 出票office号
        String office = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@IssueOffice");
        // 换开后出票日期
        String issueDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@IssueDate");
        // 换开后出票时间
        String issueTime = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@IssueTime");
        // 乘机人类型（成人等）
        String passengerType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/@Type");
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
        // 国内国际标识
        String ticketType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@TicketType");
        // 销售币种
        String currency = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/FareGroup/Price/Total/@CurrencyCode");
        // 改升支付金额
        String dataChangePayAmount = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/FareGroup/Price/Total/@Amount");
        // 改升手续费
        String dataChangeFee = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/FareGroup" + "/Taxes[@Nature=OC]/@Amount");
        // 票款金额不含税
        Double fareCuramountDouble = XpathUtils.getNumber(document, "/Msg/Dat/PassengerSegment/TicketInfo/FareGroup/Price/Fare/@Amount");
        // 票款金额不含税CNY
        String fareCuramountCNY = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/FareGroup/Price/Fare/@CurrencyCode");
        BigDecimal fareCuramount = (fareCuramountDouble != null && fareCuramountDouble >= 0) ? BigDecimal.valueOf(fareCuramountDouble) : null;
        // 税费
        Double totalTaxDouble = XpathUtils.getNumber(document, "sum(/Msg/Dat/PassengerSegment/TicketInfo/FareGroup/Taxes/Tax/@Amount)");
        // 税费CNY
        String taxCNY = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/FareGroup/Taxes/Tax/@CurrencyCode");
        BigDecimal totalTax = (totalTaxDouble != null && totalTaxDouble >= 0) ? BigDecimal.valueOf(totalTaxDouble) : null;
        // 基建税费
        Double infrastructureTaxDouble = XpathUtils.getNumber(document, "/Msg/Dat/PassengerSegment/TicketInfo/FareGroup/Taxes/Tax[@Nature='CN']/@Amount");
        // 基建税费CNY
        String infrastructureTaxCNY = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/FareGroup/Taxes/Tax[@Nature='CN']/@CurrencyCode");
        BigDecimal infrastructureTax = (infrastructureTaxDouble != null && infrastructureTaxDouble >= 0) ? BigDecimal.valueOf(infrastructureTaxDouble) : null;
        // 燃油税费
        Double fuelTaxDouble = XpathUtils.getNumber(document, "/Msg/Dat/PassengerSegment/TicketInfo/FareGroup/Taxes/Tax[@Nature='YQ']/@Amount");
        // 燃油税费CNY
        String fuelTaxCNY = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/FareGroup/Taxes/Tax[@Nature='YQ']/@CurrencyCode");
        BigDecimal fuelTax = (fuelTaxDouble != null && fuelTaxDouble >= 0) ? BigDecimal.valueOf(fuelTaxDouble) : null;
        // 其他税费
        Double otherTaxDouble = XpathUtils.getNumber(document, "/Msg/Dat/PassengerSegment/TicketInfo/FareGroup/Taxes/Tax[@Nature!='YQ' and @Nature!='CN']/@Amount");
        // 其他税费CNY
        String otherTaxCNY = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/FareGroup/Taxes/Tax[@Nature!='YQ' and @Nature!='CN']/@CurrencyCode");
        BigDecimal otherTax = (otherTaxDouble != null && otherTaxDouble >= 0) ? BigDecimal.valueOf(otherTaxDouble) : null;
        // 票款总额含税
        Double curAmountDouble = XpathUtils.getNumber(document, "/Msg/Dat/PassengerSegment/TicketInfo/FareGroup/Price/Total/@Amount");
        // 票款金额含税CNY
        String curAmountCNY = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/FareGroup/Price/Total/@CurrencyCode");
        BigDecimal curAmount = (curAmountDouble != null && curAmountDouble >= 0) ? BigDecimal.valueOf(curAmountDouble) : null;
        // 是否联票,根据PriorTicket和NextTicket判断
        boolean conjuction = false;
        // 联票上一张票票号
        String priorTicket = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@PriorTicket");
        // 联票下一张票票号
        String nextTicket = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@NextTicket");
        //只要有一个值不为空就是联票
        if (StringUtils.isNotEmpty(priorTicket) || StringUtils.isNotEmpty(nextTicket)) {
            conjuction = true;
        }
        // 运价基础FB
        String fareBasisCode = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@FareBasisCode");
        // 是否政府采购票 /Msg/Dat/PassengerSegment/TicketInfo@GP
        boolean governmentPurchase = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@GP").equals("1");
        // 同行人数
        String passengerNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@PassengerNumber");
        String tid = TransUtils.getTid(documentNumber,documentType,"HSD");

        NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
        // 是否与老人同行
        boolean isPeerSenior = false;
        if (null != age && age > 65) {
            isPeerSenior = true;
        }
        for (int a = 0; a < segmentList.getLength(); a++) {
            Node segment = segmentList.item(a);
            // 航班起飞日期
            String departureDate = XpathUtils.getString(segment, "Departure/@Date");
            // 航班起飞时间
            String departureTime = XpathUtils.getString(segment, "Departure/@Time");
            // 起飞机场
            String depAirport = XpathUtils.getString(segment, "Departure/@AirportCode");
            // 到达机场
            String arrivalAirport = XpathUtils.getString(segment, "Arrival/@AirportCode");
            // 市场航司二字码
            String marketingAirlineCode = XpathUtils.getString(segment, "Carrier/@AirlineCode");
            // 市场航司航班号
            String marketingFlightNumber = XpathUtils.getString(document, "Carrier/@FlightNumber");
            // 承运航司
            String operatingAirline = XpathUtils.getString(segment, "OperatingCarrier/@AirlineCode");
            // 承运航班号
            String operatingFlightNum = XpathUtils.getString(segment, "OperatingCarrier/@FlightNumber");
            // 航程 起飞日期 + 承运航司 + 承运航班号 +市场航司+市场航班号+ 起飞机场 + 降落机场
            String segmentKey = departureDate.replace("-", "") + operatingAirline + operatingFlightNum + marketingAirlineCode + marketingFlightNumber + depAirport + arrivalAirport;
            // 舱位
            String clazz = XpathUtils.getString(segment, "Cabin/@Clazz");
            if (clazz != null && !clazz.isEmpty()) {
                clazz = clazz.substring(0, 1);
            }
            // 航段状态
            String segmentStatus = XpathUtils.getString(segment, "@CouponStatus");
            // 大客户号
            String largeCustomerNumber = TransUtils.getKeyAccount(segment);
            // 常客卡号
            String ffrf = XpathUtils.getString(segment, "FrequentTraveler/@Number");
            // 常客等级
            String loyalLevel = XpathUtils.getString(segment, "FrequentTraveler/@LoyalLevel");
            // 常客卡航司
            String companyCode = XpathUtils.getString(segment, "FrequentTraveler/@CompanyCode");
            // 常客联盟卡级别
            String allianceLevel = XpathUtils.getString(segment, "FrequentTraveler/@AllianceLevel");
            // 联系人手机号
            String contactPhone = XpathUtils.getString(segment, "OtherServiceInformation/Text[starts-with(., 'CTCT')]");
            if (StringUtils.isNotBlank(contactPhone) && contactPhone.startsWith("CTCT")) {
                contactPhone = contactPhone.substring(4).trim();
            } else {
                contactPhone = null;
            }
            // 获取SSRCode为UMNR的特殊服务代码
            String umnrSSRCode = XpathUtils.getString(segment, "SpecialServiceRequest[SSRCode='UMNR']/@SSRCode");
            // 特殊餐食属性（仅限免费的） TODO 处理
            String spmlCode = XpathUtils.getString(segment, "SpecialServiceRequest[SSRCode='SPML']/Text");
            // 是否为无陪儿童票
            boolean unaccompaniedMinor = false;
            if ("UM".equals(passengerType) || StringUtils.isNotBlank(umnrSSRCode)) {
                unaccompaniedMinor = true;
            }
            // 航段有效期起始日期
            String notValidBefore = XpathUtils.getString(segment, "@NotValidBefore");
            // 航段有效期截止日期
            String notValidAfter = XpathUtils.getString(segment, "@NotValidAfter");
            // 航段序号
            String segmentNumber = XpathUtils.getString(segment, "@CouponNumber");


            // 构建每个 Segment 的实体对象
            // 机票出票-航段级
            TickingSegFactModel tickingSegFactModel = new TickingSegFactModel();
            // 机票出票-客票级
            TickingTicFactModel tickingTicFactModel = new TickingTicFactModel();
            // 机票改升换开出票-航段级
            DatechangeSegFactModel datechangeSegFactModel = new DatechangeSegFactModel();

            if (Constants.SUBEVENT_REISSUE.equals(subEvent)) {
                tickingSegFactModel.setAkPnrNumber(pnr);
                tickingSegFactModel.setAkTicketType(subEvent);
                tickingSegFactModel.setAkTicketNumber(newTicketNumber);
                tickingSegFactModel.setAkBookingOfficeNumber(office);
                tickingSegFactModel.setFkIssueDate(issueDate);
                tickingSegFactModel.setFkIssueTime(issueTime);
                tickingSegFactModel.setPassengerType(passengerType);
                tickingSegFactModel.setEnLastName(surname);
                tickingSegFactModel.setEnFirstName(givenName);
                tickingSegFactModel.setCnName(nativeGivenName);
                tickingSegFactModel.setCertType(NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, documentType, DataSource.HIGH_FREQUENCY_DATA));
                String docNumber = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,
                        SM4Utils.encrypt(documentNumber, Constants.SM4_KEY));
                tickingSegFactModel.setCertNumber(docNumber);
                tickingSegFactModel.setPassengerAge(age);
                // IdMapping转换
                tickingSegFactModel.setFkPassengerUserTid(tid);
                String ffpNumber = NormalizationUtils.standardize(FieldType.FREQUENT_FLYER_NUM,
                        SM4Utils.encrypt(ffrf, Constants.SM4_KEY));
                tickingSegFactModel.setFfrf(ffpNumber);
                tickingSegFactModel.setUsedFfr(StringUtils.isNotBlank(ffpNumber));
                tickingSegFactModel.setFfLevel(loyalLevel);
                tickingSegFactModel.setFfAirline(companyCode);
                tickingSegFactModel.setFfAllianceLevel(allianceLevel);
                tickingSegFactModel.setAkDimark(ticketType);
                tickingSegFactModel.setAkConjuction(conjuction);
                tickingSegFactModel.setPriorTicket(priorTicket);
                tickingSegFactModel.setNextTicket(nextTicket);
                tickingSegFactModel.setAkFarebasis(fareBasisCode);
                tickingSegFactModel.setUnaccompaniedMinor(unaccompaniedMinor);
                tickingSegFactModel.setGovernmentPurchase(governmentPurchase);
                tickingSegFactModel.setAkPeerNumber(StringUtils.isNotBlank(passengerNumber) ? Integer.valueOf(passengerNumber) : null);
                // 提前出票天数 起飞日期-出票日期
                Integer daysBetween = null;
                if(StringUtils.isNotBlank(departureDate) && StringUtils.isNotBlank(issueDate)){
                    LocalDate depart = LocalDate.parse(departureDate);
                    LocalDate issue = LocalDate.parse(issueDate);
                    // 计算两个日期之间的天数差
                    Period period = Period.between(issue, depart);
                    daysBetween = period.getDays();
                }

                tickingSegFactModel.setAkAdvbookDay(daysBetween);
                // 是否与老人同行
                tickingSegFactModel.setPeerSenior(isPeerSenior);
                // 是否与儿童同行
                tickingSegFactModel.setAkPeerChd("CHD".equals(tickingSegFactModel.getPassengerType()));
                // 是否单人出行
                tickingSegFactModel.setSingleTraveler(StringUtils.isNotBlank(passengerNumber) && Integer.parseInt(passengerNumber) == 1);
                // 是否大客户
                tickingSegFactModel.setAkKeyAccountCode(largeCustomerNumber);
                tickingSegFactModel.setKeyAccount(isKeyAccount(largeCustomerNumber));
                tickingSegFactModel.setFkDepairport(depAirport);
                tickingSegFactModel.setFkArriairport(arrivalAirport);
                tickingSegFactModel.setFkSegDate(departureDate);
                tickingSegFactModel.setFkSegTime(departureTime);
                tickingSegFactModel.setFkSegSegment(segmentKey);
                tickingSegFactModel.setAkSegcabin(clazz);
                tickingSegFactModel.setSegmentStatus(segmentStatus);
                tickingSegFactModel.setAkSpcaAtt(StringUtils.isNotBlank(spmlCode) ? spmlCode.split("\\s+")[spmlCode.split("\\s+").length - 1] : null);
                tickingSegFactModel.setTikSegstartdate(notValidBefore);
                tickingSegFactModel.setTikSegenddate(notValidAfter);
                tickingSegFactModel.setSegmentSeq(segmentNumber);
                tickingSegFactModel.setRescheduled(false);
                tickingSegFactModel.setDataActive(true);
                tickingSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
                tickingSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
                // 舱等
                tickingSegFactModel.setAkCabin(getCabinClass(departureDate,clazz));

                tickingTicFactModel.setPnrNumber(pnr);
                tickingTicFactModel.setAkTicketType(subEvent);
                tickingTicFactModel.setTicketNumber(newTicketNumber);
                tickingTicFactModel.setFkTicketingDate(issueDate);
                tickingTicFactModel.setFkTicketingTime(issueTime);
                tickingTicFactModel.setAkBookingOfficeNumber(office);
                tickingTicFactModel.setPassengerType(passengerType);
                tickingTicFactModel.setEnLastName(surname);
                tickingTicFactModel.setEnFirstName(givenName);
                tickingTicFactModel.setCnName(nativeGivenName);
                tickingTicFactModel.setCertType(NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, documentType, DataSource.HIGH_FREQUENCY_DATA));
                tickingTicFactModel.setCertNumber(docNumber);
                tickingTicFactModel.setPassengerAge(age);
                // IdMapping转换
                tickingTicFactModel.setFkPassengerUserTid(tid);
                tickingTicFactModel.setFfrf(ffpNumber);
                tickingTicFactModel.setFfLevel(loyalLevel);
                tickingTicFactModel.setFfAirline(companyCode);
                tickingTicFactModel.setFfAllianceLevel(allianceLevel);
                tickingTicFactModel.setAkDimark(ticketType);
                tickingTicFactModel.setAkConjuction(conjuction);
                tickingTicFactModel.setUnaccompaniedMinor(unaccompaniedMinor);
                tickingTicFactModel.setGovernmentPurchase(governmentPurchase);
                tickingTicFactModel.setAkPeerNumber(StringUtils.isNotBlank(passengerNumber) ? Integer.valueOf(passengerNumber) : null);
                // 是否与老人同行
                tickingTicFactModel.setPeerSenior(isPeerSenior);
                // 是否与儿童同行
                tickingTicFactModel.setAkPeerChd("CHD".equals(tickingTicFactModel.getPassengerType()));
                // 是否单人出行
                tickingTicFactModel.setSingleTraveler(StringUtils.isNotBlank(passengerNumber) && Integer.parseInt(passengerNumber) == 1);
                // 是否大客户
                tickingTicFactModel.setAkKeyAccountCode(largeCustomerNumber);
                tickingTicFactModel.setKeyAccount(isKeyAccount(largeCustomerNumber));
                tickingTicFactModel.setAkCurrency(currency);
                tickingTicFactModel.setFareCuramount(fareCuramount);
                //tickingTicFactModel.setTaxCuramount(totalTax);
                //tickingTicFactModel.setInfrastructureTaxCny(infrastructureTax);
                //tickingTicFactModel.setFuelTaxCny(fuelTax);
                //tickingTicFactModel.setOtherTaxCny(otherTax);
                //tickingTicFactModel.setCuramount(curAmount);
                // 票款金额不含税CNY
                tickingTicFactModel.setFareAmount("CNY".equals(fareCuramountCNY) ? fareCuramount : null);
                // 税费CNY
                //tickingTicFactModel.setTaxAmount("CNY".equals(taxCNY) ? totalTax : null);
                // 基建税费CNY
                //tickingTicFactModel.setInfrastructureTaxCny("CNY".equals(infrastructureTaxCNY) ? infrastructureTax : null);
                // 燃油税费CNY
                //tickingTicFactModel.setFuelTaxCny("CNY".equals(fuelTaxCNY) ? fuelTax : null);
                // 其他税费CNY
                //tickingTicFactModel.setOtherTaxCny("CNY".equals(otherTaxCNY) ? otherTax : null);
                // 票款金额含税CNY
                //tickingTicFactModel.setAmountCny("CNY".equals(curAmountCNY) ? curAmount : null);
                tickingTicFactModel.setDataActive(true);
                tickingTicFactModel.setDataActiveTime(LocalDateTime.now().toString());
                tickingTicFactModel.setSystemCreatetime(LocalDateTime.now().toString());

                datechangeSegFactModel.setAkPnrNumber(pnr);
                datechangeSegFactModel.setNewTikNum(newTicketNumber);
                datechangeSegFactModel.setOriTikNum(oldTicketNumber);
                datechangeSegFactModel.setAkBookingOfficeNumber(office);
                datechangeSegFactModel.setFkExchangeDate(issueDate);
                datechangeSegFactModel.setFkExchangeTime(issueTime);
                datechangeSegFactModel.setFkDeparturesDate(departureDate);
                datechangeSegFactModel.setFkDeparturesTime(departureTime);
                datechangeSegFactModel.setFkDepairport(depAirport);
                datechangeSegFactModel.setFkArriairport(arrivalAirport);
                datechangeSegFactModel.setFkDcSeg(segmentKey);
                datechangeSegFactModel.setPassengerType(passengerType);
                datechangeSegFactModel.setEnLastName(surname);
                datechangeSegFactModel.setEnFirstName(givenName);
                datechangeSegFactModel.setCnName(nativeGivenName);
                datechangeSegFactModel.setCertType(NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, documentType, DataSource.HIGH_FREQUENCY_DATA));
                datechangeSegFactModel.setCertNumber(docNumber);
                datechangeSegFactModel.setPassengerAge(age);
                datechangeSegFactModel.setAkSegcabin(clazz);
                datechangeSegFactModel.setAkCabin(TransUtils.convertCabinClass(clazz));

                // 换开前舱位
                /*// 查询出票-航段事实表
                String segCabinQuerySql = "SELECT AK_SEGCABIN, FK_SEG_DATE, FK_SEG_TIME  FROM "
                        + Constants.DWD_DB
                        + ".T_DWD_TICKING_SEG_FACT "
                        +" WHERE AK_TICKET_NUMBER = '" + oldTicketNumber + "'"
                        + " AND FK_DEPAIRPORT = '" + depAirport + "'"
                        + " AND FK_ARRIAIRPORT = '" + arrivalAirport + "'"
                        + " AND DATA_ACTIVE = 1"
                        + " ORDER BY FK_SEG_DATE DESC"
                        + " LIMIT 1";
                logger.info("HSD ExchangeFactTrans mealQuerySql:{}", segCabinQuerySql);
                String segCabin = null;
                String oldDate = null;
                String oldTime = null;
                try (Connection localConn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
                     Statement localStmt = localConn.createStatement();
                     ResultSet mealResult = localStmt.executeQuery(segCabinQuerySql);) {
                    try {
                        if (mealResult != null && mealResult.next()) {
                            segCabin = mealResult.getString("AK_SEGCABIN");
                            oldDate = mealResult.getString("FK_SEG_DATE");
                            oldTime = mealResult.getString("FK_SEG_TIME");
                        }
                    } catch (SQLException e) {
                        try {
                            mealResult.close();
                        } catch (SQLException ex) {
                            throw new RuntimeException(ex);
                        }
                        logger.error("Error processing meal result: {}", e.getMessage(), e);
                    }
                } catch (SQLException e) {
                    logger.error("Error processing meal result: {}", e.getMessage(), e);
                }
                //换开前起飞日期
                datechangeSegFactModel.setOldDeparturesDate(oldDate);
                // 换开前起飞时间
                datechangeSegFactModel.setOldDeparturesTime(oldTime);
                // 换开前舱位
                datechangeSegFactModel.setOldAkSegcabin(segCabin);
                // 换开前舱等
                datechangeSegFactModel.setOldAkCabin(TransUtils.getCabinClass(oldDate, segCabin));
                // 换开类型
                String dateChangeType = Objects.equals(segCabin, clazz) ? "EX" : "UP";
                datechangeSegFactModel.setDateChangeType(dateChangeType);*/

                datechangeSegFactModel.setAkDimark(ticketType);
                datechangeSegFactModel.setSegmentStatus(segmentStatus);
                //datechangeSegFactModel.setRescheduled(false);
                datechangeSegFactModel.setFfrf(ffpNumber);
                datechangeSegFactModel.setFfLevel(loyalLevel);
                datechangeSegFactModel.setFfAirline(companyCode);
                datechangeSegFactModel.setFfAllianceLevel(allianceLevel);
                //datechangeSegFactModel.setAkCurrency(currency);
                //datechangeSegFactModel.setDcCurfee(StringUtils.isNotBlank(dataChangeFee) ? new BigDecimal(dataChangeFee) : null);
                //datechangeSegFactModel.setCabinbalanceCur(StringUtils.isNotBlank(dataChangePayAmount) &&
                // StringUtils.isNotBlank(dataChangeFee) ? new BigDecimal(dataChangePayAmount).subtract(new BigDecimal(dataChangeFee)) : null);
                // 是否大客户
                datechangeSegFactModel.setAkKeyAccountCode(largeCustomerNumber);
                datechangeSegFactModel.setKeyAccount(isKeyAccount(largeCustomerNumber));
                datechangeSegFactModel.setContactMobileNumber(NormalizationUtils.standardize(FieldType.MOBILE_NO,
                        SM4Utils.encrypt(contactPhone, Constants.SM4_KEY)));
                datechangeSegFactModel.setDataActive(true);
                datechangeSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
                datechangeSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
                // 舱等
                datechangeSegFactModel.setAkCabin(getCabinClass(departureDate,clazz));

            }
            if (Constants.SUBEVENT_EXCHANGED.equals(subEvent)) {
                tickingSegFactModel.setSegmentStatus(segmentStatus);
                tickingSegFactModel.setRescheduled(true);
            }

            // 票号+起飞机场三字码
            tickingSegFactModel.setPkId(newTicketNumber + depAirport);
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
                logger.info("高频-换开事件数据信息整合[TickingSegFactModel]数据解析异常{}", value);
            }

            // 出票日期+票号
            tickingTicFactModel.setPkId(issueDate.replace("-","") + newTicketNumber);
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
                logger.info("高频-换开事件数据信息整合[TickingTicFactModel]数据解析异常{}", value);
            }

            // 改升后票号+起飞机场
            datechangeSegFactModel.setPkId(newTicketNumber + depAirport + departureDate.replace("-", ""));
            datechangeSegFactModel.setDataActive(true);
            datechangeSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
            datechangeSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
            datechangeSegFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

            HsdProcessDataModel hsdProcessDatechangeSegFactModel = new HsdProcessDataModel();
            hsdProcessDatechangeSegFactModel.setEvent(event);
            hsdProcessDatechangeSegFactModel.setSubEvent(subEvent);
            hsdProcessDatechangeSegFactModel.setTableName("T_DWD_DATECHANGE_SEG_FACT");
            hsdProcessDatechangeSegFactModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
            hsdProcessDatechangeSegFactModel.setStamp(stamp);
            // 转换为JSON字符串
            hsdProcessDatechangeSegFactModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(datechangeSegFactModel));
            hsdProcessDatechangeSegFactModel.setProcessed(false);
            hsdProcessDatechangeSegFactModel.setUpdateTime(LocalDateTime.now().toString());

            if(StringUtils.isNotEmpty(datechangeSegFactModel.getPkId())) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDatechangeSegFactModel);
            }else{
                logger.info("高频-换开事件数据信息整合[DatechangeSegFactModel]数据解析异常{}", value);
            }
        }
    }
}
