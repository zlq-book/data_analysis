package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.BookingSegFactModel;
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

/**
 * 航段ActionCode值变更事件解析
 *
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/15  16:43
 */
public class ActionCodeChangeFactTrans {

    static final Logger logger = LoggerFactory.getLogger(ActionCodeChangeFactTrans.class);

    public static void output(DataStream<String> source) {

        SingleOutputStreamOperator<HsdProcessDataModel> actionCodeChangeStream = source.flatMap((String value,
                                                                                                 Collector<HsdProcessDataModel> out) -> {
            Document document;
            try {
                document = DocumentUtils.string2Document(value);
            } catch (Exception e) {
                logger.warn("convert document is error, the content is {}, the error is {}", value, e.getMessage());
                return;
            }
            // 事件名称
            String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
            if (!Constants.EVENT_ACTIONCODECHANGE.equals(event)) {
                return;
            }
            logger.info("高频-航段ActionCode值变更事件数据信息整合");
            // 子事件名称
            String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
            String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
            String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");

            // ICS系统PNR记录编号
            String icsPnr = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@RecordLocator");
            // PNR生成日期
            String bookDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@BookDate");
            String bookTime = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@BookTime");
            Node passengerSegment = XpathUtils.getNode(document, "/Msg/Dat/PassengerSegment");
            // 获取乘机人节点
            Node traveller = XpathUtils.getNode(passengerSegment, "Traveller");
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
            // 获取所有 Segment 节点
            NodeList segmentList = XpathUtils.getNodeList(passengerSegment, "Segment");
            for (int a = 0; a < segmentList.getLength(); a++) {
                Node segment = segmentList.item(a);
                // 乘机人证件号码
                String documentNumber = XpathUtils.getString(traveller, "Document/@Number");
                // 起飞机场
                String depAirport = XpathUtils.getString(segment, "Departure/@AirportCode");
                // 格式化 PNR 创建日期（去除横线）
                String formattedBookDate = StringUtils.isBlank(bookDate) ? "" : bookDate.replace("-", "");
                String formattedBookTime = StringUtils.isBlank(bookTime) ? "" : bookTime.replace(":", "");

                // 构建每个 Segment 的实体对象
                BookingSegFactModel bookingSegFactModel = new BookingSegFactModel();
                // pk_id  主键：PNR编号+PNR创建日期+起飞机场+乘机人证件号（加密）+乘机人姓名（姓名，先取乘机人中文姓名，如果没有，取英文姓+英文名）
                bookingSegFactModel.setPkId(icsPnr + formattedBookDate + formattedBookTime + depAirport + NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, SM4Utils.encrypt(documentNumber, Constants.SM4_KEY)) + travellerName);
                // PNR 航段行动代码（HK等）
                bookingSegFactModel.setAkSegStatus(XpathUtils.getString(segment, "@ActionCode"));
                bookingSegFactModel.setDataActive(true);
                bookingSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
                bookingSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
                // 本系统最后更新日期时间 修改老数据 只更新 本系统最后更新日期时间
                bookingSegFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

                HsdProcessDataModel hsdProcessDataModel = new HsdProcessDataModel();
                hsdProcessDataModel.setEvent(event);
                hsdProcessDataModel.setSubEvent(subEvent);
                hsdProcessDataModel.setTableName("T_DWD_BOOKING_SEG_FACT");
                hsdProcessDataModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
                hsdProcessDataModel.setStamp(stamp);
                // 转换为JSON字符串
                hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(bookingSegFactModel));
                hsdProcessDataModel.setProcessed(false);
                hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());

                // 输出实体对象
                if(StringUtils.isNotEmpty(bookingSegFactModel.getPkId())){
                    out.collect(hsdProcessDataModel);
                }else{
                    logger.info("高频-航段ActionCode值变更事件数据信息整合[BookingSegFactModel]数据解析异常{}",value);
                }
            }
        }).returns(TypeInformation.of(HsdProcessDataModel.class)).name("ActionCodeChangeFlatMap").setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        // 输出结果查看（调试用）
//        actionCodeChangeStream.print("actionCodeChangeStream");
        // 创建 Doris Sink
        DorisSink<HsdProcessDataModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_HSD_PROCESS_DATA");
        // 数据写入 Doris
        actionCodeChangeStream.sinkTo(dorisSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
    }

    public static void processActionCodeChange(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
        /*if (!Constants.EVENT_ACTIONCODECHANGE.equals(event)) {
            return;
        }*/
        logger.info("高频-航段ActionCode值变更事件数据信息整合");
        // 子事件名称
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");

        // ICS系统PNR记录编号
        String icsPnr = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@RecordLocator");
        // PNR生成日期
        String bookDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@BookDate");
        String bookTime = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@BookTime");
        Node passengerSegment = XpathUtils.getNode(document, "/Msg/Dat/PassengerSegment");
        // 获取乘机人节点
        Node traveller = XpathUtils.getNode(passengerSegment, "Traveller");
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
        // 获取所有 Segment 节点
        NodeList segmentList = XpathUtils.getNodeList(passengerSegment, "Segment");
        for (int a = 0; a < segmentList.getLength(); a++) {
            Node segment = segmentList.item(a);
            // 乘机人证件号码
            String documentNumber = XpathUtils.getString(traveller, "Document/@Number");
            // 起飞机场
            String depAirport = XpathUtils.getString(segment, "Departure/@AirportCode");
            // 格式化 PNR 创建日期（去除横线）
            String formattedBookDate = StringUtils.isBlank(bookDate) ? "" : bookDate.replace("-", "");
            String formattedBookTime = StringUtils.isBlank(bookTime) ? "" : bookTime.replace(":", "");

            // 构建每个 Segment 的实体对象
            BookingSegFactModel bookingSegFactModel = new BookingSegFactModel();
            // pk_id  主键：PNR编号+PNR创建日期+起飞机场+乘机人证件号（加密）+乘机人姓名（姓名，先取乘机人中文姓名，如果没有，取英文姓+英文名）
            bookingSegFactModel.setPkId(icsPnr + formattedBookDate + formattedBookTime + depAirport + NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, SM4Utils.encrypt(documentNumber, Constants.SM4_KEY)) + travellerName);
            // PNR 航段行动代码（HK等）
            bookingSegFactModel.setAkSegStatus(XpathUtils.getString(segment, "@ActionCode"));
            bookingSegFactModel.setDataActive(true);
            bookingSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
            bookingSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
            // 本系统最后更新日期时间 修改老数据 只更新 本系统最后更新日期时间
            bookingSegFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

            HsdProcessDataModel hsdProcessDataModel = new HsdProcessDataModel();
            hsdProcessDataModel.setEvent(event);
            hsdProcessDataModel.setSubEvent(subEvent);
            hsdProcessDataModel.setTableName("T_DWD_BOOKING_SEG_FACT");
            hsdProcessDataModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
            hsdProcessDataModel.setStamp(stamp);
            // 转换为JSON字符串
            hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(bookingSegFactModel));
            hsdProcessDataModel.setProcessed(false);
            hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());

            // 输出实体对象
            if(StringUtils.isNotEmpty(bookingSegFactModel.getPkId())){
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
            }else{
                logger.info("高频-航段ActionCode值变更事件数据信息整合[BookingSegFactModel]数据解析异常{}",value);
            }
        }
    }
}

