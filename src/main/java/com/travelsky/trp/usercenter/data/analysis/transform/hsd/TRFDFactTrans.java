package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.HsdProcessDataModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.RefundSegFactModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingSegFactModel;
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
import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.isKeyAccount;

/**
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/15  16:43
 */
public class TRFDFactTrans {

    static final Logger logger = LoggerFactory.getLogger(TRFDFactTrans.class);

    public static void output(DataStream<String> source) {

        // 使用Void类型，因为所有数据都通过SideOutput输出
        SingleOutputStreamOperator<Void> processedStream = source.process(
                new ProcessFunction<String, Void>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<Void> out) throws Exception {
                        Document document;
                        try {
                            document = DocumentUtils.string2Document(value);
                        } catch (Exception e) {
                            logger.warn("convert document is error, the content is {}, the error is {}", value, e.getMessage());
                            return;
                        }
                        processTRFD(value, ctx, document);
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
        DataStream<RefundSegFactModel> refundSegDataStream = processedStream.getSideOutput(CommonOutputTags.REFUND_SEG_TAG);

        // 输出结果查看
//        refundSegDataStream.print("refundSegDataStream");

        // 创建并配置各模型的Doris Sink
        DorisSink<TickingSegFactModel> tickingSegSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_SEG_FACT");
        DorisSink<RefundSegFactModel> refundSegSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_REFUND_SEG_FACT");

        // 数据分别写入对应的Doris表
        tickingSegDataStream.sinkTo(tickingSegSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        refundSegDataStream.sinkTo(refundSegSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);*/
    }

    public static void processTRFD(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
        /*if (!Constants.EVENT_TRFD.equals(event)) {
            return;
        }*/
        logger.info("高频-退票事件数据信息整合");
        // 子事件名称
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
        // ICS系统PNR记录编号
        String icsPnr = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@RecordLocator");
        // PNR生成日期
        String bookDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@BookDate");
        // 出票日期
        String issueDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@IssueDate");
        // 出票时间
        String issueTime = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@IssueTime");
        // 报文处理的时间
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
        // 票号
        String ticketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@TicketNumber");
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
            // 航班时间
            String departureTime = XpathUtils.getString(segment, "Departure/@Time");
            // 航段状态
            String segmentStatus = XpathUtils.getString(segment, "@CouponStatus");
            // 航段序号
            String couponNumber = XpathUtils.getString(segment, "@CouponNumber");
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
            // 大客户号
            String largeCustomerNumber = TransUtils.getKeyAccount(segment);
            // 舱位
            String clazz = XpathUtils.getString(segment, "Cabin/@Clazz");
            if (clazz != null && !clazz.isEmpty()) {
                clazz = clazz.substring(0, 1);
            }

            // 构建每个 Segment 的实体对象
            // 机票出票-航段级
            TickingSegFactModel tickingSegFactModel = new TickingSegFactModel();
            // pkId 票号+起飞机场+起飞日期
            tickingSegFactModel.setPkId(ticketNumber + depAirport + departureDate.replace("-",""));
            tickingSegFactModel.setSegmentStatus(segmentStatus);
            // 本系统最后更新日期时间
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
                logger.info("高频-退票事件数据信息整合[TickingSegFactModel]数据解析异常{}", value);
            }

            // 机票退票-航段级
            RefundSegFactModel refundSegFactModel = new RefundSegFactModel();
            refundSegFactModel.setPkId(ticketNumber + depAirport + departureDate.replace("-",""));
            refundSegFactModel.setTicketingDate(issueDate);
            refundSegFactModel.setTicketingTime(issueTime);
            refundSegFactModel.setRefundDate(refundDate);
            refundSegFactModel.setRefundTime(refundTime);
            refundSegFactModel.setAkTikNumber(ticketNumber);
            refundSegFactModel.setFkDepairport(depAirport);
            refundSegFactModel.setFkArriairport(arrivalAirport);
            refundSegFactModel.setSegNo(StringUtils.isNotBlank(couponNumber) ? Integer.valueOf(couponNumber) : null);
            // 航程外键：起飞日期 + 承运航司 + 承运航班号 +市场航司+市场航班号+ 起飞机场 + 降落机场
            refundSegFactModel.setFkSegSegment(departureDate.replace("-", "") + operatingAirline + operatingFlightNum + marketingAirlineCode + marketingFlightNumber + depAirport + arrivalAirport);
            refundSegFactModel.setFkSegDate(departureDate);
            refundSegFactModel.setFkSegTime(departureTime);
            refundSegFactModel.setAkPasstype(passengerType);
            refundSegFactModel.setEnLastName(surname);
            refundSegFactModel.setEnFirstName(givenName);
            refundSegFactModel.setCnName(nativeGivenName);
            refundSegFactModel.setCertType(NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, documentType, DataSource.HIGH_FREQUENCY_DATA));
            refundSegFactModel.setCertNumber(NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,
                    SM4Utils.encrypt(documentNumber, Constants.SM4_KEY)));
            refundSegFactModel.setPassengerAge(age);
            refundSegFactModel.setAkSegcabin(clazz);
            refundSegFactModel.setAkDimark(ticketType);
//                            // 提前出票天数 起飞日期-出票日期 v2.0
//                            LocalDate depart = LocalDate.parse(departureDate);
//                            LocalDate issue = LocalDate.parse(issueDate);
//                            // 计算两个日期之间的天数差
//                            Period period = Period.between(issue, depart);
//                            int daysBetween = period.getDays();
//                            refundSegFactModel.setAkAdvbookDay(daysBetween);
            // 是否与老人同行 v2.0
//                            refundSegFactModel.(isPeerSenior);
            // 是否与儿童同行 v2.0
            // 数据有效性 1为有效，0为无效 默认有效
            refundSegFactModel.setIsValid("1");
            refundSegFactModel.setDataActive(true);
            // 是否大客户
            refundSegFactModel.setKeyAccount(isKeyAccount(largeCustomerNumber));
            refundSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
            refundSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
            // 舱等
            refundSegFactModel.setAkCabin(getCabinClass(departureDate, clazz));
            // 本系统最后更新日期时间
            refundSegFactModel.setDataActive(true);
            refundSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
            refundSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
            refundSegFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

            HsdProcessDataModel hsdProcessRefundSegFactModel = new HsdProcessDataModel();
            hsdProcessRefundSegFactModel.setEvent(event);
            hsdProcessRefundSegFactModel.setSubEvent(subEvent);
            hsdProcessRefundSegFactModel.setTableName("T_DWD_REFUND_SEG_FACT");
            hsdProcessRefundSegFactModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
            hsdProcessRefundSegFactModel.setStamp(stamp);
            // 转换为JSON字符串
            hsdProcessRefundSegFactModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(refundSegFactModel));
            hsdProcessRefundSegFactModel.setProcessed(false);
            hsdProcessRefundSegFactModel.setUpdateTime(LocalDateTime.now().toString());

            if (StringUtils.isNotEmpty(refundSegFactModel.getPkId())) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessRefundSegFactModel);
            }else{
                logger.info("高频-退票事件数据信息整合[RefundSegFactModel]数据解析异常{}", value);
            }
        }
    }
}
