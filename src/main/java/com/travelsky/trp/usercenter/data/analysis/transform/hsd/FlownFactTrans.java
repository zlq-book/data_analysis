package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.DepartSegFactModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.HsdProcessDataModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingSegFactModel;
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

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.getCabinClass;
import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.isKeyAccount;

/**
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/15  16:43
 */
public class FlownFactTrans {

    static final Logger logger = LoggerFactory.getLogger(FlownFactTrans.class);

    public static void output(DataStream<String> source) {
        SingleOutputStreamOperator<Void> processedStream = source.process(
                new ProcessFunction<String, Void>() {
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
                        processFlown(value, ctx, document);
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
        DataStream<DepartSegFactModel> departSegDataStream = processedStream.getSideOutput(CommonOutputTags.DEPART_SEG_TAG);

        // 创建并配置各模型的Doris Sink
        DorisSink<TickingSegFactModel> tickingSegSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_SEG_FACT");
        DorisSink<DepartSegFactModel> departSegSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_DEPART_SEG_FACT");

        // 数据分别写入对应的Doris表
        tickingSegDataStream.sinkTo(tickingSegSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        departSegDataStream.sinkTo(departSegSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
*/
    }

    public static void processFlown(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
                        /*if (!Constants.EVENT_FLOWN.equals(event)) {
                            return;
                        }*/
        logger.info("高频-客票使用事件数据信息整合");
        // 子事件名称
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
        // 票号
        String ticketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@TicketNumber");
        // 出票日期
        String issueDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@IssueDate");
        // 出票时间
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
        // 团队标识
        String group = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@Group");


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
            // 航班到达日期
            String arriveDate = XpathUtils.getString(segment, "Arrival/@Date").replace("-", "");
            // 航班到达时间
            String arriveTime = XpathUtils.getString(segment, "Arrival/@Time");
            // 航段状态
            String segmentStatus = XpathUtils.getString(segment, "@CouponStatus");
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
            // 离港舱位
            String compartment = XpathUtils.getString(segment, "Cabin/@Compartment");
            // 常客卡号
            String ffrf = XpathUtils.getString(segment, "FrequentTraveler/@Number");
            // 常客等级
            String loyalLevel = XpathUtils.getString(segment, "FrequentTraveler/@LoyalLevel");
            // 常客卡航司
            String companyCode = XpathUtils.getString(segment, "FrequentTraveler/@CompanyCode");
            if (compartment != null && !compartment.isEmpty()) {
                compartment = compartment.substring(0, 1);
            }
            // 大客户号
            String largeCustomerNumber = TransUtils.getKeyAccount(segment);
            // 航程主键 起飞日期 + 承运航司 + 承运航班号 +市场航司+市场航班号+ 起飞机场 + 降落机场
            String segmentKey = departureDate.replace("-", "") + operatingAirline + operatingFlightNum + marketingAirlineCode + marketingFlightNumber + depAirport + arrivalAirport;

            // 构建每个 Segment 的实体对象
            // 机票出票-航段级
            TickingSegFactModel tickingSegFactModel = new TickingSegFactModel();
            // pkId 票号+起飞机场+起飞日期
            tickingSegFactModel.setPkId(ticketNumber + depAirport + departureDate.replace("-", ""));
            tickingSegFactModel.setSegmentStatus(segmentStatus);
            tickingSegFactModel.setDataActive(true);
            tickingSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
            tickingSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
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
            // 本系统最后更新日期时间
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
                logger.info("高频-客票使用事件数据信息整合[TickingSegFactModel]数据解析异常{}", value);
            }

            // 成行-航段级
            DepartSegFactModel departSegFactModel = new DepartSegFactModel();
            // 航班起飞日期 + 票号 + 起飞机场 + 到达机场 9.7改成 票号 + 起飞机场 + 起飞日期
            departSegFactModel.setPkId(ticketNumber + depAirport + departureDate.replace("-", ""));
            // 是否是团队票
            departSegFactModel.setAkTeammark("Y".equals(group));
            // 是否大客户
            departSegFactModel.setKeyAccount(isKeyAccount(largeCustomerNumber));
            departSegFactModel.setTikNum(ticketNumber);
            departSegFactModel.setFkDeparturesDate(departureDate.replace("-", ""));
            departSegFactModel.setFkDeparturesTime(departureTime);
            departSegFactModel.setFkArriveDate(arriveDate);
            departSegFactModel.setFkArriveTime(arriveTime);
            departSegFactModel.setFkPnrdSeg(segmentKey);
            departSegFactModel.setFkDepairport(depAirport);
            departSegFactModel.setFkArriairport(arrivalAirport);
            departSegFactModel.setPassengerType(passengerType);
            departSegFactModel.setEnLastName(surname);
            departSegFactModel.setEnFirstName(givenName);
            departSegFactModel.setCnName(nativeGivenName);
            departSegFactModel.setCertType(NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, documentType, DataSource.HIGH_FREQUENCY_DATA));
            departSegFactModel.setCertNumber(NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,
                    SM4Utils.encrypt(documentNumber, Constants.SM4_KEY)));
            departSegFactModel.setPassengerAge(age);

            departSegFactModel.setAkAdvbookDay(daysBetween);
            // 是否与老人同行
            departSegFactModel.setPeerSenior(isPeerSenior);
            // 是否与儿童同行
            departSegFactModel.setPeerChd("CHD".equals(departSegFactModel.getPassengerType()));
            // IdMapping转换
            departSegFactModel.setFkPassengerUserTid(TransUtils.getTid(documentNumber,documentType,"HSD"));
            departSegFactModel.setAkSegcabin(compartment);
            // 舱等
            departSegFactModel.setAkCabin(getCabinClass(departureDate, compartment));
            departSegFactModel.setAkDimark(ticketType);
            // 数据有效性 1为有效，0为无效 默认有效
//                            departSegFactModel.setIsValid("1");
            departSegFactModel.setDataActive(true);
            departSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
            departSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());

            // V2新增
            departSegFactModel.setFkIssueDate(issueDate);
            departSegFactModel.setFkIssueTime(issueTime);

            // V2新增 常客相关
            departSegFactModel.setFfrf(NormalizationUtils.standardize(FieldType.FREQUENT_FLYER_NUM, SM4Utils.encrypt(ffrf, Constants.SM4_KEY)));
            departSegFactModel.setFfLevel(loyalLevel);
            departSegFactModel.setFfAirline(companyCode);
            departSegFactModel.setUsedFfr(StringUtils.isNotBlank(ffrf));

            // 查询是否首次乘机
            /*String sql = "SELECT COUNT(*) TID_COUNT FROM " + Constants.DWD_DB +
                    ".T_DWD_DEPART_SEG_FACT \n" +
                    "WHERE FK_PASSENGER_USER_TID = ";

            // Step 1: 获取原始字段
            String tid = departSegFactModel.getFkPassengerUserTid();
            if (null != tid) {
                // 鲁雁行有卡号 继续
                // Step 2: 处理字段
                sql = sql + "'" + tid + "'";
                try (Connection localConn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
                     Statement localStmt = localConn.createStatement();
                     ResultSet result = localStmt.executeQuery(sql)) {
                    try {
                        if (result != null && result.next()) {
                            // 查询成功：补全维度信息
                            int count = result.getInt("TID_COUNT");
                            // 存在：不是首次
                            departSegFactModel.setFirstTraveler(count <= 0);
                        }
                    } catch (Exception e) {
                        try {
                            result.close();
                        } catch (SQLException ex) {
                            throw new RuntimeException(ex);
                        }
                        logger.error("HSD IDChangeFactTrans sql error:{}", e.getMessage());
                    }

                } catch (SQLException s) {
                    logger.error("HSD IDChangeFactTrans getConnection error:{}", s.getMessage());
                }
            }*/

            // 本系统最后更新日期时间
            departSegFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

            HsdProcessDataModel hsdProcessDepartSegFactModel = new HsdProcessDataModel();
            hsdProcessDepartSegFactModel.setEvent(event);
            hsdProcessDepartSegFactModel.setSubEvent(subEvent);
            hsdProcessDepartSegFactModel.setTableName("T_DWD_DEPART_SEG_FACT");
            hsdProcessDepartSegFactModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
            hsdProcessDepartSegFactModel.setStamp(stamp);
            // 转换为JSON字符串
            hsdProcessDepartSegFactModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(departSegFactModel));
            hsdProcessDepartSegFactModel.setProcessed(false);
            hsdProcessDepartSegFactModel.setUpdateTime(LocalDateTime.now().toString());

            if (StringUtils.isNotEmpty(departSegFactModel.getPkId())) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDepartSegFactModel);
            }else{
                logger.info("高频-客票使用事件数据信息整合[DepartSegFactModel]数据解析异常{}", value);
            }
        }
    }

}
