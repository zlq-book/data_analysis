package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.SegDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.HsdProcessDataModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingSegFactModel;
import com.travelsky.trp.usercenter.data.analysis.utils.CustomJsonSerializer;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.getCabinClass;

/**
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/15  16:43
 */
public class RevalidationFactTrans {

    static final Logger logger = LoggerFactory.getLogger(RevalidationFactTrans.class);

    public static void output(DataStream<String> source) {

        // 使用Void类型，因为所有数据都通过SideOutput输出
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
                processRevalidation(value, ctx, document);
            }
        }).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        DataStream<HsdProcessDataModel> hsdProcessDataStream = processedStream.getSideOutput(CommonOutputTags.HSD_PROCESS_DATA_TAG);
        //hsdProcessDataStream.print("hsdProcessDataStream");
        hsdProcessDataStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_HSD_PROCESS_DATA")).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        /*// 获取各个SideOutput流
        DataStream<TickingSegFactModel> tickingSegDataStream = processedStream.getSideOutput(CommonOutputTags.TICKING_SEG_TAG);
        // 输出结果查看
//        tickingSegDataStream.print("tickingSegDataStream");
        DataStream<SegDimModel> segDimDataStream = processedStream.getSideOutput(CommonOutputTags.SEG_DIM_TAG);
//        segDimDataStream.print("segDimDataStream");

        // 创建并配置各模型的Doris Sink
        DorisSink<TickingSegFactModel> tickingSegSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_SEG_FACT");
        DorisSink<SegDimModel> segDimSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_SEG_DIM");
        // 数据分别写入对应的Doris表
        tickingSegDataStream.sinkTo(tickingSegSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        segDimDataStream.sinkTo(segDimSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);*/
    }

    public static void processRevalidation(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
        // 子事件名称
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
        if (!Constants.EVENT_REVALIDATION.equals(event)) {
            return;
        }
        logger.info("高频-航段改期事件数据信息整合");
        // 票号
        String newTicketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@TicketNumber");
        // 改升前票号
        String oldTicketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/OriginalTicket/@TicketNumber");
        // NewVal
        String newVal = XpathUtils.getString(document, "/Msg/Hdr/Newval");
        // OldVal
        String oldVal = XpathUtils.getString(document, "/Msg/Hdr/Oldval");

        // 从NewVal中解析起飞机场和起飞时间
        String newDepAirport = null;
        String newDepartureDate = null;
        if (newVal != null && !newVal.isEmpty()) {
            String[] newValParts = newVal.split("/");
            if (newValParts.length >= 5) {
                newDepAirport = newValParts[1].substring(0, 3); // 起飞机场是前3位
                newDepartureDate = newValParts[3].replace("-", ""); // 起飞日期是第4个元素
            }
        }

        // 从OldVal中解析起飞机场和起飞时间
        String oldDepAirport = null;
        String oldDepartureDate = null;
        if (oldVal != null && !oldVal.isEmpty()) {
            String[] oldValParts = oldVal.split("/");
            if (oldValParts.length >= 5) {
                oldDepAirport = oldValParts[1].substring(0, 3); // 起飞机场是前3位
                oldDepartureDate = oldValParts[3].replace("-", ""); // 起飞日期是第4个元素
            }
        }


        NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
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
            // 航班到达日期
            String arriveDate = XpathUtils.getString(segment, "Arrival/@Date");
            // 航班到达时间
            String arriveTime = XpathUtils.getString(segment, "Arrival/@Time");
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

            // 构建每个 Segment 的实体对象
            // 机票出票-航段级
            TickingSegFactModel tickingSegFactModel = new TickingSegFactModel();
            // pkId 票号+起飞机场+起飞日期
            tickingSegFactModel.setPkId(newTicketNumber + newDepAirport + newDepartureDate);
            String oldPkId = newTicketNumber + oldDepAirport + oldDepartureDate;
            //updateTicketSeg(oldPkId, tickingSegFactModel);
            tickingSegFactModel.setAkPnrNumber(oldPkId);// TODO 临时存放数据 sql处理查询更新和删除操作
            if (Constants.SUBEVENT_AIRPORTCHANGE.equals(subEvent)) {
                tickingSegFactModel.setFkDepairport(depAirport);
                tickingSegFactModel.setFkArriairport(arrivalAirport);
            }
            tickingSegFactModel.setFkSegSegment(segmentKey);
            tickingSegFactModel.setAkSegcabin(clazz);
            tickingSegFactModel.setFkSegDate(departureDate);
            tickingSegFactModel.setFkSegTime(departureTime);
            tickingSegFactModel.setDataActive(true);
            tickingSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
            tickingSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
            // 本系统最后更新日期时间
            tickingSegFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());
            // 舱等
            tickingSegFactModel.setAkCabin(getCabinClass(departureDate, clazz));

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
                logger.info("高频-航段改期事件数据信息整合[TickingSegFactModel]数据解析异常{}",value);
            }

            // 改期事件情况下主键的每个组成字段非空（非OPEN票）
            if (StringUtils.isNotBlank(departureDate) && StringUtils.isNotBlank(operatingAirline) && StringUtils.isNotBlank(operatingFlightNum) && StringUtils.isNotBlank(marketingAirlineCode) && StringUtils.isNotBlank(marketingFlightNumber) && StringUtils.isNotBlank(depAirport) && StringUtils.isNotBlank(arrivalAirport)) {
                // 航程维表
                SegDimModel segDimModel = new SegDimModel();
                // 起飞日期 + 承运航司 + 承运航班号 +市场航司+市场航班号+ 起飞机场 + 降落机场
                segDimModel.setSegmentKey(segmentKey);
                segDimModel.setAirline(depAirport + "_" + arrivalAirport);
                segDimModel.setOperatAirline(operatingAirline);
                segDimModel.setOperatFlightNum(operatingFlightNum);
                segDimModel.setDepartureDate(departureDate);
                segDimModel.setDepartureTime(departureTime);
                segDimModel.setArrivalDate(arriveDate);
                segDimModel.setArrivalTime(arriveTime);
                segDimModel.setMarketAirline(marketingAirlineCode);
                segDimModel.setMarketFlightNum(marketingFlightNumber);
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

                if (StringUtils.isNotEmpty(segDimModel.getSegmentKey())) {
                    ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
                }else{
                    logger.info("高频-航段改期事件数据信息整合[SegDimModel]数据解析异常{}",value);
                }
            }
        }
    }

    private static void updateTicketSeg(String oldPkId, TickingSegFactModel tickingSegFactModel) {
        // 查询机票出票航段级表
        String tickingSegQuerySql = "SELECT * FROM "
                + Constants.DWD_DB
                + ".T_DWD_TICKING_SEG_FACT WHERE PK_ID='" + oldPkId + "' AND DATA_ACTIVE = 1";
        logger.info("HSD RevalidationFactTrans tickingSegQuerySql:{}", tickingSegQuerySql);
        try (Connection localConn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
             Statement localStmt = localConn.createStatement();
             ResultSet resultSet = localStmt.executeQuery(tickingSegQuerySql)) {
            try {
                if (resultSet != null && resultSet.next()) {
                    tickingSegFactModel.setAkPnrNumber(resultSet.getString("AK_PNR_NUMBER"));
                    tickingSegFactModel.setAkTicketType(resultSet.getString("AK_TICKET_TYPE"));
                    tickingSegFactModel.setAkTicketNumber(resultSet.getString("AK_TICKET_NUMBER"));
                    tickingSegFactModel.setAkBookingOfficeNumber(resultSet.getString("AK_BOOKING_OFFICE_NUMBER"));
                    tickingSegFactModel.setFkIssueDate(resultSet.getString("FK_ISSUE_DATE"));
                    tickingSegFactModel.setFkIssueTime(resultSet.getString("FK_ISSUE_TIME"));
                    tickingSegFactModel.setPassengerType(resultSet.getString("PASSENGER_TYPE"));
                    tickingSegFactModel.setEnLastName(resultSet.getString("EN_LAST_NAME"));
                    tickingSegFactModel.setEnFirstName(resultSet.getString("EN_FIRST_NAME"));
                    tickingSegFactModel.setCnName(resultSet.getString("CN_NAME"));
                    tickingSegFactModel.setCertType(resultSet.getString("CERT_TYPE"));
                    tickingSegFactModel.setCertNumber(resultSet.getString("CERT_NUMBER"));
                    tickingSegFactModel.setPassengerAge(resultSet.getInt("PASSENGER_AGE"));
                    tickingSegFactModel.setFkPassengerUserTid(resultSet.getString("FK_PASSENGER_USER_TID"));
                    tickingSegFactModel.setFfrf(resultSet.getString("FFRF"));
                    tickingSegFactModel.setFfLevel(resultSet.getString("FF_LEVEL"));
                    tickingSegFactModel.setFfAirline(resultSet.getString("FF_AIRLINE"));
                    tickingSegFactModel.setFfAllianceLevel(resultSet.getString("FF_ALLIANCE_LEVEL"));
                    tickingSegFactModel.setAkDimark(resultSet.getString("AK_DIMARK"));
                    tickingSegFactModel.setAkConjuction(resultSet.getBoolean("AK_CONJUCTION"));
                    tickingSegFactModel.setPriorTicket(resultSet.getString("PRIOR_TICKET"));
                    tickingSegFactModel.setNextTicket(resultSet.getString("NEXT_TICKET"));
                    tickingSegFactModel.setAkFarebasis(resultSet.getString("AK_FAREBASIS"));
                    tickingSegFactModel.setUnaccompaniedMinor(resultSet.getBoolean("UNACCOMPANIED_MINOR"));
                    tickingSegFactModel.setGovernmentPurchase(resultSet.getBoolean("IS_GOVERNMENT_PURCHASE"));
                    tickingSegFactModel.setAkKeyAccountCode(resultSet.getString("AK_KEY_ACCOUNT_CODE"));
                    tickingSegFactModel.setKeyAccount(resultSet.getBoolean("IS_KEY_ACCOUNT"));
                    tickingSegFactModel.setTeamTicket(resultSet.getBoolean("IS_TEAM_TICKET"));
                    tickingSegFactModel.setAkPeerNumber(resultSet.getInt("AK_PEER_NUMBER"));
                    tickingSegFactModel.setSingleTraveler(resultSet.getBoolean("SINGLE_TRAVELER"));
                    tickingSegFactModel.setAkPeerChd(resultSet.getBoolean("AK_PEER_CHD"));
                    tickingSegFactModel.setPeerSenior(resultSet.getBoolean("PEER_SENIOR"));
                    //tickingSegFactModel.setPeerInfant(resultSet.getBoolean("PEER_INFANT"));
                    tickingSegFactModel.setSelfBooking(resultSet.getBoolean("SELF_BOOKING"));
                    tickingSegFactModel.setAkFirstIssue(resultSet.getBoolean("AK_FIRST_ISSUE"));
                    tickingSegFactModel.setAkBookingPassenger(resultSet.getBoolean("AK_BOOKING_PASSENGER"));
                    tickingSegFactModel.setFkDepairport(resultSet.getString("FK_DEPAIRPORT"));
                    tickingSegFactModel.setFkArriairport(resultSet.getString("FK_ARRIAIRPORT"));
                    tickingSegFactModel.setFkSegSegment(resultSet.getString("FK_SEG_SEGMENT"));
                    tickingSegFactModel.setFkSegDate(resultSet.getString("FK_SEG_DATE"));
                    tickingSegFactModel.setFkSegTime(resultSet.getString("FK_SEG_TIME"));
                    tickingSegFactModel.setAkCabin(resultSet.getString("AK_CABIN"));
                    tickingSegFactModel.setAkSegcabin(resultSet.getString("AK_SEGCABIN"));
                    tickingSegFactModel.setSegmentStatus(resultSet.getString("SEGMENT_STATUS"));
                    tickingSegFactModel.setAkIrrflag(resultSet.getString("AK_IRRFLAG"));
                    tickingSegFactModel.setAkSpcaAtt(resultSet.getString("AK_SPCA_ATT"));
                    tickingSegFactModel.setTikSegstartdate(resultSet.getString("TIK_SEGSTARTDATE"));
                    tickingSegFactModel.setTikSegenddate(resultSet.getString("TIK_SEGENDDATE"));
                    tickingSegFactModel.setRescheduled(resultSet.getBoolean("IS_RESCHEDULED"));
                    tickingSegFactModel.setAkAdvbookDay(resultSet.getInt("AK_ADVBOOK_DAY"));
                    tickingSegFactModel.setSeatNumber(resultSet.getString("SEAT_NUMBER"));
                    //tickingSegFactModel.setSeasonTag(resultSet.getString("SEASON_TAG"));
                    tickingSegFactModel.setSegmentSeq(resultSet.getString("SEGMENT_SEQ"));
                    tickingSegFactModel.setHomecomingSeg(resultSet.getBoolean("HOMECOMING_SEG"));
                    tickingSegFactModel.setDestinationStop(resultSet.getBoolean("IS_DESTINATION_STOP"));
                    tickingSegFactModel.setStopoverDay(resultSet.getInt("STOPOVER_DAY"));
                    tickingSegFactModel.setTikSegcount(resultSet.getInt("TIK_SEGCOUNT"));
                    tickingSegFactModel.setSeatChannel(resultSet.getString("SEAT_CHANNEL"));
                    tickingSegFactModel.setPriceType(resultSet.getString("PRICE_TYPE"));
                    //tickingSegFactModel.setAkBookingBrand(resultSet.getString("AK_BOOKING_BRAND"));
                    tickingSegFactModel.setChannelOrderno(resultSet.getString("CHANNEL_ORDERNO"));
                    tickingSegFactModel.setAkChannel(resultSet.getString("AK_CHANNEL"));
                    tickingSegFactModel.setFkBookingUserTid(resultSet.getString("FK_BOOKING_USER_TID"));
                    tickingSegFactModel.setFkBookingUserOriginId(resultSet.getString("FK_BOOKING_USER_ORIGIN_ID"));
                    tickingSegFactModel.setAkCoupon(resultSet.getBoolean("AK_COUPON"));
                    tickingSegFactModel.setAkCouponcode(resultSet.getString("AK_COUPONCODE"));
                    //tickingSegFactModel.setAkCoupontype(resultSet.getString("AK_COUPONTYPE"));
                    String couponAmount = resultSet.getString("COUPON_AMOUNT");
                    BigDecimal coupon = StringUtils.isNotBlank(couponAmount) ? new BigDecimal(couponAmount) : null;
                    tickingSegFactModel.setCouponAmount(coupon);
                    tickingSegFactModel.setCouponCount(resultSet.getInt("COUPON_COUNT"));
                    tickingSegFactModel.setCouponno(resultSet.getString("COUPONNO"));
                    tickingSegFactModel.setCouponName(resultSet.getString("COUPON_NAME"));
                    tickingSegFactModel.setContactMobileNumber(resultSet.getString("CONTACT_MOBILE_NUMBER"));
                    tickingSegFactModel.setContactName(resultSet.getString("CONTACT_NAME"));
                    //tickingSegFactModel.setContactInterMobileCode(resultSet.getString("CONTACT_INTER_MOBILE_CODE"));
                    tickingSegFactModel.setContactLandlineMobileNumber(resultSet.getString("CONTACT_LANDLINE_MOBILE_NUMBER"));
                    //tickingSegFactModel.setContactEmail(resultSet.getString("CONTACT_EMAIL"));
                    tickingSegFactModel.setDataActive(resultSet.getBoolean("DATA_ACTIVE"));
                    tickingSegFactModel.setDataActiveTime(resultSet.getString("DATA_ACTIVE_TIME"));
                    tickingSegFactModel.setSourceLastUpdatetime(resultSet.getString("SOURCE_LAST_UPDATETIME"));
                    tickingSegFactModel.setSystemCreatetime(resultSet.getString("SYSTEM_CREATETIME"));
                    tickingSegFactModel.setSystemLastUpdatetime(resultSet.getString("SYSTEM_LAST_UPDATETIME"));
                }
            } catch (Exception e) {
                try {
                    resultSet.close();
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
                logger.error("HSD RevalidationFactTrans tickingSegQuerySql error:{}", e.getMessage());
            }
            String tickingSegDeleteSql = "DELETE FROM " + Constants.DWD_DB + ".T_DWD_TICKING_SEG_FACT WHERE " +
                    "PK_ID = '" + oldPkId + "'";
            logger.info("HSD RevalidationFactTrans tickingSegDeleteSql:{}", tickingSegDeleteSql);
            localStmt.executeUpdate(tickingSegDeleteSql);
        }catch (Exception e) {
            logger.error("HSD RevalidationFactTrans error:{}", e.getMessage());
        }


    }

}
