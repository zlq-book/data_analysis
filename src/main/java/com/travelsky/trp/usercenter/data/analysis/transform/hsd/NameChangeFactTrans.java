package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.*;
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

import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDateTime;

/**
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/15  16:43
 */
public class NameChangeFactTrans {

    static final Logger logger = LoggerFactory.getLogger(NameChangeFactTrans.class);

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
                processNameChange(value, ctx, document);
            }
        }).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        DataStream<HsdProcessDataModel> hsdProcessDataStream = processedStream.getSideOutput(CommonOutputTags.HSD_PROCESS_DATA_TAG);
        //hsdProcessDataStream.print("hsdProcessDataStream");
        hsdProcessDataStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_HSD_PROCESS_DATA")).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        /*// 1.处理数据
        DataStream<BookingSegFactModel> bookingSegDataStream = processedStream.getSideOutput(CommonOutputTags.BOOKING_SEG_TAG);
//        bookingSegDataStream.print("bookingSegDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<BookingSegFactModel> bookingSegSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_BOOKING_SEG_FACT");
        bookingSegDataStream.sinkTo(bookingSegSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        DataStream<TickingSegFactModel> tickingSegDataStream = processedStream.getSideOutput(CommonOutputTags.TICKING_SEG_TAG);
//        tickingSegDataStream.print("tickingSegDataStream");
        DataStream<TickingTicFactModel> tickingTicDataStream = processedStream.getSideOutput(CommonOutputTags.TICKING_TIC_TAG);
//        tickingTicDataStream.print("tickingTicDataStream");
        // 数据分别写入对应的Doris表
        tickingSegDataStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_SEG_FACT"));
        tickingTicDataStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_TIC_FACT"));

        // 获取各个SideOutput流
        DataStream<SeatTikFactModel > seatTikDataStream = processedStream.getSideOutput(CommonOutputTags.SEAT_TIK_TAG);
        // 输出结果查看
//        seatTikDataStream.print("seatTikDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<SeatTikFactModel> seatTikSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_SEAT_TIK_FACT");
        seatTikDataStream.sinkTo(seatTikSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        // 获取各个SideOutput流
        DataStream<BaggageTikFactModel> baggageTikDataStream = processedStream.getSideOutput(CommonOutputTags.BAGGAGE_TIK_TAG);
        // 输出结果查看
//        baggageTikDataStream.print("baggageTikDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<BaggageTikFactModel> baggageTikSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_BAGGAGE_TIK_FACT");
        baggageTikDataStream.sinkTo(baggageTikSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        // 获取各个SideOutput流
        DataStream<MealTikFactModel> mealTikDataStream = processedStream.getSideOutput(CommonOutputTags.MEAL_TIK_TAG);
        // 输出结果查看
//        mealTikDataStream.print("mealTikDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<MealTikFactModel> mealTikSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_MEAL_TIK_FACT");
        mealTikDataStream.sinkTo(mealTikSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        // 获取各个SideOutput流
        DataStream<UpgrTikFactModel> upgrTikDataStream = processedStream.getSideOutput(CommonOutputTags.UPGR_TIK_TAG);
        // 输出结果查看
//        upgrTikDataStream.print("upgrTikDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<UpgrTikFactModel> upgrTikSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_UPGR_TIK_FACT");
        upgrTikDataStream.sinkTo(upgrTikSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        // 获取各个SideOutput流
        DataStream<ExcessBaggageTikFactModel> excessBaggageTikDataStream =
                processedStream.getSideOutput(CommonOutputTags.EXCESS_BAGGAGE_TIK_TAG);
        // 输出结果查看
//        excessBaggageTikDataStream.print("excessBaggageTikDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<ExcessBaggageTikFactModel> excessBaggageTikSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_EXCESS_BAGGAGE_TIK_FACT");
        excessBaggageTikDataStream.sinkTo(excessBaggageTikSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);*/
    }

    public static void processNameChange(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
        if (!Constants.EVENT_NAMECHANGE.equals(event)) {
            return;
        }
        logger.info("高频-旅客姓名变更事件数据信息整合");
        // 子事件名称
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
        // 原姓名
        String oldName = XpathUtils.getString(document, "/Msg/Hdr/Oldval").replace("/", "");
        // 票号
        String ticketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@TicketNumber");
        // 出票日期
        String issueDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@IssueDate");
        // PNR号
        String pnr = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@RecordLocator");
        // PNR生成日期
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
        // 乘机人证件号码
        String documentNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Number");
        // 乘机人姓名
        String travellerName = StringUtils.isBlank(nativeGivenName) ? enName : nativeGivenName;

        NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
        for (int a = 0; a < segmentList.getLength(); a++) {
            Node segment = segmentList.item(a);
            // 航班起飞日期
            String departureDate = XpathUtils.getString(segment, "Departure/@Date");
            // 起飞机场
            String depAirport = XpathUtils.getString(segment, "Departure/@AirportCode");
            String docNumber = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,SM4Utils.encrypt(documentNumber,
                    Constants.SM4_KEY));
            // PNR编号+PNR创建日期+起飞机场+乘机人证件号（加密）+乘机人姓名（姓名，先取乘机人中文姓名，如果没有，取英文姓+英文名）
            String bookingSegPkId = pnr + bookDate + bookTime + depAirport + docNumber + travellerName;
            String oldPkid = pnr + bookDate + bookTime + depAirport + docNumber + oldName;

            // 构建每个 Segment 的实体对象
            // 机票预订-航段级
            BookingSegFactModel bookingSegFactModel = new BookingSegFactModel();
            /*// 查询机票预订航段级事实表数据
            String bookingSegQuerySql = "SELECT * FROM "
                    + Constants.DWD_DB
                    + ".T_DWD_BOOKING_SEG_FACT WHERE PK_ID='" + oldPkid + "' AND DATA_ACTIVE = 1";
            logger.info("HSD NameChangeFactTrans bookingSegQuerySql:{}", bookingSegQuerySql);
            // 查询的数据转换
            try (Connection localConn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
                 Statement localStmt = localConn.createStatement();
                 ResultSet bookingSegResult = localStmt.executeQuery(bookingSegQuerySql)) {
                try {
                    if (bookingSegResult != null && bookingSegResult.next()) {
                        bookingSegFactModel.setPnrNumber(bookingSegResult.getString("PNR_NUMBER"));
                        bookingSegFactModel.setFkBookingDate(bookingSegResult.getString("FK_BOOKING_DATE"));
                        bookingSegFactModel.setFkBookingTime(bookingSegResult.getString("FK_BOOKING_TIME"));
                        bookingSegFactModel.setFkDepairport(bookingSegResult.getString("FK_DEPAIRPORT"));
                        bookingSegFactModel.setFkArriairport(bookingSegResult.getString("FK_ARRIAIRPORT"));
                        bookingSegFactModel.setFkBookingSeg(bookingSegResult.getString("FK_BOOKING_SEG"));
                        bookingSegFactModel.setFkSegDate(bookingSegResult.getString("FK_SEG_DATE"));
                        bookingSegFactModel.setFkSegTime(bookingSegResult.getString("FK_SEG_TIME"));
                        bookingSegFactModel.setPassengerType(bookingSegResult.getString("PASSENGER_TYPE"));
                        bookingSegFactModel.setCertType(bookingSegResult.getString("CERT_TYPE"));
                        bookingSegFactModel.setCertNumber(bookingSegResult.getString("CERT_NUMBER"));
                        bookingSegFactModel.setPassengerAge(bookingSegResult.getInt("PASSENGER_AGE"));
                        bookingSegFactModel.setFkPassengerUserTid(bookingSegResult.getString("FK_PASSENGER_USER_TID"));
                        bookingSegFactModel.setVvip(bookingSegResult.getString("VVIP"));
                        bookingSegFactModel.setAkBookingOfficeNumber(bookingSegResult.getString("AK_BOOKING_OFFICE_NUMBER"));
                        bookingSegFactModel.setAkTeammark(bookingSegResult.getBoolean("AK_TEAMMARK"));
                        bookingSegFactModel.setAkSegStatus(bookingSegResult.getString("AK_SEG_STATUS"));
                        bookingSegFactModel.setAkCabin(bookingSegResult.getString("AK_CABIN"));
                        bookingSegFactModel.setAkSegcabin(bookingSegResult.getString("AK_SEGCABIN"));
                        //bookingSegFactModel.setPriceType(bookingSegResult.getString("PRICE_TYPE"));
                        bookingSegFactModel.setFfrf(bookingSegResult.getString("FFRF"));
                        bookingSegFactModel.setFfLevel(bookingSegResult.getString("FF_LEVEL"));
                        bookingSegFactModel.setFfAirline(bookingSegResult.getString("FF_AIRLINE"));
                        bookingSegFactModel.setFfAllianceLevel(bookingSegResult.getString("FF_ALLIANCE_LEVEL"));
                        bookingSegFactModel.setAkAdvbookDay(bookingSegResult.getInt("AK_ADVBOOK_DAY"));
                        bookingSegFactModel.setAkKeyAccountCode(bookingSegResult.getString("AK_KEY_ACCOUNT_CODE"));
                        bookingSegFactModel.setKeyAccount(bookingSegResult.getBoolean("IS_KEY_ACCOUNT"));
                        bookingSegFactModel.setAkPeerNumber(bookingSegResult.getInt("AK_PEER_NUMBER"));
                        bookingSegFactModel.setAkPeerChd(bookingSegResult.getBoolean("AK_PEER_CHD"));
                        bookingSegFactModel.setPeerSenior(bookingSegResult.getBoolean("PEER_SENIOR"));
                        //bookingSegFactModel.setPeerInfant(bookingSegResult.getBoolean("PEER_INFANT"));
                        //bookingSegFactModel.setSeasonTag(bookingSegResult.getString("SEASON_TAG"));
                        bookingSegFactModel.setSelfBooking(bookingSegResult.getBoolean("SELF_BOOKING"));
                        bookingSegFactModel.setHomecomingSeg(bookingSegResult.getBoolean("HOMECOMING_SEG"));
                        bookingSegFactModel.setSingleTravel(bookingSegResult.getBoolean("SINGLE_TRAVEL"));
                        bookingSegFactModel.setSegCount(bookingSegResult.getInt("SEG_COUNT"));
                        bookingSegFactModel.setChannelOrderId(bookingSegResult.getString("CHANNEL_ORDER_ID"));
                        bookingSegFactModel.setAkChannel(bookingSegResult.getString("AK_CHANNEL"));
                        bookingSegFactModel.setContactMobileNumber(bookingSegResult.getString("CONTACT_MOBILE_NUMBER"));
                        bookingSegFactModel.setContactName(bookingSegResult.getString("CONTACT_NAME"));
                        //bookingSegFactModel.setContactInterMobileCode(bookingSegResult.getString("CONTACT_INTER_MOBILE_CODE"));
                        bookingSegFactModel.setContactLandlineMobileNumber(bookingSegResult.getString("CONTACT_LANDLINE_MOBILE_NUMBER"));
                        //bookingSegFactModel.setContactEmail(bookingSegResult.getString("CONTACT_EMAIL"));
                        bookingSegFactModel.setFkBookingUserTid(bookingSegResult.getString("FK_BOOKING_USER_TID"));
                        bookingSegFactModel.setFkBookingUserOriginId(bookingSegResult.getString("FK_BOOKING_USER_ORIGIN_ID"));
                        //bookingSegFactModel.setAkBookingBrand(bookingSegResult.getString("AK_BOOKING_BRAND"));
                        bookingSegFactModel.setAkBookerIsPassenger(bookingSegResult.getBoolean("AK_BOOKER_IS_PASSENGER"));
                        //bookingSegFactModel.setAkIsseckill(bookingSegResult.getBoolean("AK_ISSECKILL"));
                        bookingSegFactModel.setDiscount(bookingSegResult.getDouble("DISCOUNT"));
                        bookingSegFactModel.setDataActive(bookingSegResult.getBoolean("DATA_ACTIVE"));
                        bookingSegFactModel.setDataActiveTime(bookingSegResult.getString("DATA_ACTIVE_TIME"));
                        bookingSegFactModel.setSourceLastUpdatetime(bookingSegResult.getString("SOURCE_LAST_UPDATETIME"));
                        bookingSegFactModel.setSystemCreatetime(bookingSegResult.getString("SYSTEM_CREATETIME"));
                    }
                } catch (Exception e) {
                    try {
                        bookingSegResult.close();
                    } catch (SQLException ex) {
                        throw new RuntimeException(ex);
                    }
                    logger.error("HSD NameChangeFactTrans bookingSegQuerySql error:{}", e.getMessage());
                }

                String bookingSegDeleteSql = "DELETE FROM " + Constants.DWD_DB + ".T_DWD_BOOKING_SEG_FACT WHERE " +
                        "PK_ID = '" + oldPkid + "'";
                // 删除老数据
                logger.info("HSD NameChangeFactTrans bookingSegDeleteSql:{}", bookingSegDeleteSql);
                localStmt.executeUpdate(bookingSegDeleteSql);
            } catch (Exception e) {
                logger.error("HSD NameChangeFactTrans error:{}", e.getMessage());
            }*/





            // 更新pk_id
            bookingSegFactModel.setPkId(bookingSegPkId);
            bookingSegFactModel.setPnrNumber(oldPkid); // TODO 临时存放数据 sql处理查询更新和删除操作
            // 更新姓名相关字段
            bookingSegFactModel.setEnLastName(surname);
            bookingSegFactModel.setEnFirstName(givenName);
            bookingSegFactModel.setCnName(nativeGivenName);
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
                logger.info("高频-旅客姓名变更事件数据信息整合[BookingSegFactModel]数据解析异常{}",value);
            }

            // 机票出票-航段级
            TickingSegFactModel tickingSegFactModel = new TickingSegFactModel();
            // pkId 票号+起飞机场+起飞日期
            tickingSegFactModel.setPkId(ticketNumber + depAirport + departureDate.replace("-",""));
            tickingSegFactModel.setEnLastName(surname);
            tickingSegFactModel.setEnFirstName(givenName);
            tickingSegFactModel.setCnName(nativeGivenName);
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
                logger.info("高频-旅客姓名变更事件数据信息整合[TickingSegFactModel]数据解析异常{}",value);
            }

            // 机票出票-客票级
            TickingTicFactModel tickingTicFactModel = new TickingTicFactModel();
            // 出票日期+票号
            tickingTicFactModel.setPkId(issueDate.replace("-","") + ticketNumber);
            tickingTicFactModel.setEnLastName(surname);
            tickingTicFactModel.setEnFirstName(givenName);
            tickingTicFactModel.setCnName(nativeGivenName);
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

            if (StringUtils.isNotEmpty(tickingTicFactModel.getPkId())) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessTickingTicFactModel);
            }else{
                logger.info("高频-旅客姓名变更事件数据信息整合[TickingTicFactModel]数据解析异常{}",value);
            }

        }
        // 查询选座出EMD票航段级表
        /*String seatQuerySql = "SELECT * FROM "
                + Constants.DWD_DB
                + ".T_DWD_SEAT_TIK_FACT WHERE AK_TIKNUM='" + ticketNumber + "' AND DATA_ACTIVE = 1";
        logger.info("HSD NameChangeFactTrans seatQuerySql:{}", seatQuerySql);
        String seatPkId = null;
        try (Connection localConn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
             Statement localStmt = localConn.createStatement();
             ResultSet seatResult = localStmt.executeQuery(seatQuerySql)) {
            try {
                if (seatResult != null && seatResult.next()) {
                    seatPkId = seatResult.getString("PK_ID");
                }
            } catch (SQLException e) {
                logger.error("Error processing seat result: {}", e.getMessage(), e);
                try {
                    seatResult.close();
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
            }
        } catch (Exception e) {
            logger.error("Error processing seat query: {}", e.getMessage(), e);
        }*/

        //if (StringUtils.isNotBlank(seatPkId)) {
        // 选座出票-航段级
        SeatTikFactModel seatTikFactModel = new SeatTikFactModel();
        //seatTikFactModel.setPkId(seatPkId);
        seatTikFactModel.setAkTiknum(ticketNumber); // TODO 临时存放数据 sql处理查询更新
        seatTikFactModel.setEnLastName(surname);
        seatTikFactModel.setEnFirstName(givenName);
        seatTikFactModel.setCnName(nativeGivenName);
        seatTikFactModel.setDataActive(true);
        seatTikFactModel.setDataActiveTime(LocalDateTime.now().toString());
        seatTikFactModel.setSystemCreatetime(LocalDateTime.now().toString());
        seatTikFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

        HsdProcessDataModel hsdProcessSeatTikFactModel = new HsdProcessDataModel();
        hsdProcessSeatTikFactModel.setEvent(event);
        hsdProcessSeatTikFactModel.setSubEvent(subEvent);
        hsdProcessSeatTikFactModel.setTableName("T_DWD_SEAT_TIK_FACT");
        hsdProcessSeatTikFactModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
        hsdProcessSeatTikFactModel.setStamp(stamp);
        // 转换为JSON字符串
        hsdProcessSeatTikFactModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(seatTikFactModel));
        hsdProcessSeatTikFactModel.setProcessed(false);
        hsdProcessSeatTikFactModel.setUpdateTime(LocalDateTime.now().toString());

        if (StringUtils.isNotEmpty(seatTikFactModel.getPkId())) {
            ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessSeatTikFactModel);
        }else{
            logger.info("高频-旅客姓名变更事件数据信息整合[SeatTikFactModel]数据解析异常{}",value);
        }
        //}

        // 查询行李出EMD票航段级表
        /*String baggageQuerySql = "SELECT * FROM "
                + Constants.DWD_DB
                + ".T_DWD_BAGGAGE_TIK_FACT WHERE AK_ORDERNUM='" + ticketNumber + "' AND DATA_ACTIVE = 1";
        logger.info("HSD NameChangeFactTrans baggageQuerySql:{}", baggageQuerySql);
        String baggagePkId = null;
        try (Connection localConn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
             Statement localStmt = localConn.createStatement();
             ResultSet baggageResult = localStmt.executeQuery(baggageQuerySql)) {
            try {
                if (baggageResult != null && baggageResult.next()) {
                    baggagePkId = baggageResult.getString("PK_ID");
                }
            } catch (SQLException e) {
                logger.error("Error processing baggage result: {}", e.getMessage(), e);
                try {
                    baggageResult.close();
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
            }
        } catch (Exception e) {
            logger.error("Error processing baggage query: {}", e.getMessage(), e);
        }*/

        //if (StringUtils.isNotBlank(baggagePkId)) {
        // 预付费行李出票-航段级
        BaggageTikFactModel baggageTikFactModel = new BaggageTikFactModel();
        //baggageTikFactModel.setPkId(baggagePkId);
        baggageTikFactModel.setAkOrdernum(ticketNumber); // TODO 临时存放数据 sql处理查询更新
        baggageTikFactModel.setEnLastName(surname);
        baggageTikFactModel.setEnFirstName(givenName);
        baggageTikFactModel.setCnName(nativeGivenName);
        baggageTikFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

        HsdProcessDataModel hsdProcessBaggageTikFactModel = new HsdProcessDataModel();
        hsdProcessBaggageTikFactModel.setEvent(event);
        hsdProcessBaggageTikFactModel.setSubEvent(subEvent);
        hsdProcessBaggageTikFactModel.setTableName("T_DWD_BAGGAGE_TIK_FACT");
        hsdProcessBaggageTikFactModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
        hsdProcessBaggageTikFactModel.setStamp(stamp);
        // 转换为JSON字符串
        hsdProcessBaggageTikFactModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(baggageTikFactModel));
        hsdProcessBaggageTikFactModel.setProcessed(false);
        hsdProcessBaggageTikFactModel.setUpdateTime(LocalDateTime.now().toString());

        if (StringUtils.isNotEmpty(baggageTikFactModel.getPkId())) {
            ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessBaggageTikFactModel);
        }else{
            logger.info("高频-旅客姓名变更事件数据信息整合[BaggageTikFactModel]数据解析异常{}",value);
        }
        //}

        // 查询行李出EMD票航段级表
        /*String mealQuerySql = "SELECT * FROM "
                + Constants.DWD_DB
                + ".T_DWD_MEAL_TIK_FACT WHERE AK_TIKNUM='" + ticketNumber + "' AND DATA_ACTIVE = 1";
        logger.info("HSD NameChangeFactTrans mealQuerySql:{}", mealQuerySql);
        String mealPkId = null;
        try (Connection localConn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
             Statement localStmt = localConn.createStatement();
             ResultSet mealResult = localStmt.executeQuery(mealQuerySql)) {
            try {
                if (mealResult != null && mealResult.next()) {
                    mealPkId = mealResult.getString("PK_ID");
                }
            } catch (SQLException e) {
                logger.error("Error processing meal result: {}", e.getMessage(), e);
                try {
                    mealResult.close();
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
            }
        } catch (Exception e) {
            logger.error("Error processing meal query: {}", e.getMessage(), e);
        }*/

        //if (StringUtils.isNotBlank(mealPkId)) {
        // 选餐出票-航段级
        MealTikFactModel mealTikFactModel = new MealTikFactModel();
        //mealTikFactModel.setPkId(mealPkId);
        mealTikFactModel.setAkTiknum(ticketNumber); // TODO 临时存放数据 sql处理查询更新
        mealTikFactModel.setEnLastName(surname);
        mealTikFactModel.setEnFirstName(givenName);
        mealTikFactModel.setCnName(nativeGivenName);
        mealTikFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

        HsdProcessDataModel hsdProcessMealTikFactModel = new HsdProcessDataModel();
        hsdProcessMealTikFactModel.setEvent(event);
        hsdProcessMealTikFactModel.setSubEvent(subEvent);
        hsdProcessMealTikFactModel.setTableName("T_DWD_MEAL_TIK_FACT");
        hsdProcessMealTikFactModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
        hsdProcessMealTikFactModel.setStamp(stamp);
        // 转换为JSON字符串
        hsdProcessMealTikFactModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(mealTikFactModel));
        hsdProcessMealTikFactModel.setProcessed(false);
        hsdProcessMealTikFactModel.setUpdateTime(LocalDateTime.now().toString());

        if (StringUtils.isNotEmpty(mealTikFactModel.getPkId())) {
            ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessMealTikFactModel);
        }else{
            logger.info("高频-旅客姓名变更事件数据信息整合[MealTikFactModel]数据解析异常{}",value);
        }
        //}

        // 查询行李出EMD票航段级表
        /*String updgrQuerySql = "SELECT * FROM "
                + Constants.DWD_DB
                + ".T_DWD_UPGR_TIK_FACT WHERE AK_TIKNUM='" + ticketNumber + "' AND DATA_ACTIVE = 1";
        logger.info("HSD NameChangeFactTrans updgrQuerySql:{}", updgrQuerySql);
        String updgrPkId = null;
        try (Connection localConn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
             Statement localStmt = localConn.createStatement();
             ResultSet updgrResult = localStmt.executeQuery(updgrQuerySql)) {
            try {
                if (updgrResult != null && updgrResult.next()) {
                    updgrPkId = updgrResult.getString("PK_ID");
                }
            } catch (SQLException e) {
                logger.error("Error processing updgr result: {}", e.getMessage(), e);
                try {
                    updgrResult.close();
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }catch (Exception e){
            logger.error("Error processing updgr query: {}", e.getMessage(), e);
        }*/

        //if (StringUtils.isNotBlank(updgrPkId)) {
        // 升舱出票-航段级
        UpgrTikFactModel updgrTikFactModel = new UpgrTikFactModel();
        //updgrTikFactModel.setPkId(updgrPkId);
        updgrTikFactModel.setAkTiknum(ticketNumber); // TODO 临时存放数据 sql处理查询更新
        updgrTikFactModel.setEnLastName(surname);
        updgrTikFactModel.setEnFirstName(givenName);
        updgrTikFactModel.setCnName(nativeGivenName);
        updgrTikFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

        HsdProcessDataModel hsdProcessUpgrTikFactModel = new HsdProcessDataModel();
        hsdProcessUpgrTikFactModel.setEvent(event);
        hsdProcessUpgrTikFactModel.setSubEvent(subEvent);
        hsdProcessUpgrTikFactModel.setTableName("T_DWD_UPGR_TIK_FACT");
        hsdProcessUpgrTikFactModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
        hsdProcessUpgrTikFactModel.setStamp(stamp);
        // 转换为JSON字符串
        hsdProcessUpgrTikFactModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(updgrTikFactModel));
        hsdProcessUpgrTikFactModel.setProcessed(false);
        hsdProcessUpgrTikFactModel.setUpdateTime(LocalDateTime.now().toString());

        if (StringUtils.isNotEmpty(updgrTikFactModel.getPkId())) {
            ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessUpgrTikFactModel);
        }else{
            logger.info("高频-旅客姓名变更事件数据信息整合[UpgrTikFactModel]数据解析异常{}",value);
        }
        //}

        // 查询行李出EMD票航段级表
        /*String excessBaggageQuerySql = "SELECT * FROM "
                + Constants.DWD_DB
                + ".T_DWD_EXCESS_BAGGAGE_TIK_FACT WHERE AK_TIKNUM='" + ticketNumber + "' AND DATA_ACTIVE = 1";
        logger.info("HSD NameChangeFactTrans excessBaggageQuerySql:{}", excessBaggageQuerySql);
        String excessBaggagePkId = null;
        try (Connection localConn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
             Statement localStmt = localConn.createStatement();
             ResultSet excessBaggageResult = localStmt.executeQuery(excessBaggageQuerySql)) {
            try {
                if (excessBaggageResult != null && excessBaggageResult.next()) {
                    excessBaggagePkId = excessBaggageResult.getString("PK_ID");
                }
            } catch (SQLException e) {
                logger.error("Error processing excessBaggage result: {}", e.getMessage(), e);
                try {
                    excessBaggageResult.close();
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
            }
        } catch (Exception e) {
            logger.error("Error processing excessBaggage query: {}", e.getMessage(), e);
        }*/

        //if (StringUtils.isNotBlank(excessBaggagePkId)) {
        // 逾重行李出票-航段级
        ExcessBaggageTikFactModel excessBaggageTikFactModel = new ExcessBaggageTikFactModel();
        //excessBaggageTikFactModel.setPkId(excessBaggagePkId);
        excessBaggageTikFactModel.setAkTiknum(ticketNumber); // TODO 临时存放数据 sql处理查询更新
        excessBaggageTikFactModel.setEnLastName(surname);
        excessBaggageTikFactModel.setEnFirstName(givenName);
        excessBaggageTikFactModel.setCnName(nativeGivenName);
        excessBaggageTikFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

        HsdProcessDataModel hsdProcessExcessBaggageTikFactModel = new HsdProcessDataModel();
        hsdProcessExcessBaggageTikFactModel.setEvent(event);
        hsdProcessExcessBaggageTikFactModel.setSubEvent(subEvent);
        hsdProcessExcessBaggageTikFactModel.setTableName("T_DWD_EXCESS_BAGGAGE_TIK_FACT");
        hsdProcessExcessBaggageTikFactModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
        hsdProcessExcessBaggageTikFactModel.setStamp(stamp);
        // 转换为JSON字符串
        hsdProcessExcessBaggageTikFactModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(excessBaggageTikFactModel));
        hsdProcessExcessBaggageTikFactModel.setProcessed(false);
        hsdProcessExcessBaggageTikFactModel.setUpdateTime(LocalDateTime.now().toString());

        if (StringUtils.isNotEmpty(excessBaggageTikFactModel.getPkId())) {
            ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessExcessBaggageTikFactModel);
        }else{
            logger.info("高频-旅客姓名变更事件数据信息整合[ExcessBaggageTikFactModel]数据解析异常{}",value);
        }
        //}
    }

}
