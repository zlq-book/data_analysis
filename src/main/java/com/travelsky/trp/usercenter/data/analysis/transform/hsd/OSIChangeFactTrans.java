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
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/15  16:43
 */
public class OSIChangeFactTrans {

    static final Logger logger = LoggerFactory.getLogger(OSIChangeFactTrans.class);

    public static void output(DataStream<String> source) {

        SingleOutputStreamOperator<HsdProcessDataModel> osiChangeStream = source.flatMap((String value,
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
            if (!Constants.EVENT_OSICHANGE.equals(event)) {
                return;
            }
            logger.info("高频-PNR OSI变更事件数据信息整合");
            // 子事件名称
            String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
            String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
            String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
            // ICS系统PNR记录编号
            String icsPnr = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@RecordLocator");
            // PNR生成日期
            String bookDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@BookDate");

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
            //乘机人证件号码（未加密）
            String documentNumber = XpathUtils.getString(traveller, "Document/@Number");
            // 获取所有 Segment 节点
            NodeList segmentList = XpathUtils.getNodeList(passengerSegment, "Segment");
            for (int a = 0; a < segmentList.getLength(); a++) {
                Node segment = segmentList.item(a);

                // 起飞机场
                String depAirport = XpathUtils.getString(segment, "Departure/@AirportCode");
                // 降落机场
                String arriAirport = XpathUtils.getString(segment, "Arrival/@AirportCode");
                // 格式化 PNR 创建日期（去除横线）
                String formattedBookDate = StringUtils.isBlank(bookDate) ? "" : bookDate.replace("-", "");
                //  vvip  订座 PNR 中使用RMK指令备注重保旅客职务，“人大”、“政协”“党代表”“公司指定要客”等，或其他需要备注的字样。要客姓名后输入VVIP。用OSI 指令建立其他服务情况组，在
                //  OSI 组中输入要客最高职务。输入格式例如：
                //  山东省省长：OSI SC VVIP IS SHANDONGSHENGSHENGZHANG
                //  人大代表：OSI SC VVIP IS RENDADAIBIAO
                String vvip = XpathUtils.getString(segment, "OtherServiceInformation[Text[starts-with(., " + "'VVIP')]]/Text");

                // 构建每个 Segment 的实体对象
                BookingSegFactModel bookingSegFactModel = new BookingSegFactModel();
                // pk_id  主键：PNR编号+PNR创建日期+起飞机场+乘机人证件号（加密）+乘机人姓名（姓名，先取乘机人中文姓名，如果没有，取英文姓+英文名）
                bookingSegFactModel.setPkId(icsPnr + formattedBookDate + depAirport + NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,SM4Utils.encrypt(documentNumber, Constants.SM4_KEY)) + travellerName);
                //  vvip标识
                if(vvip != null && vvip.length() > 4){
                    bookingSegFactModel.setVvip(vvip.substring(4));
                }
                bookingSegFactModel.setDataActive(true);
                bookingSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
                bookingSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());

                // 本系统最后更新日期时间 修改老数据 只更新 本系统最后更新日期时间
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

                // 输出实体对象
                if (StringUtils.isNotEmpty(bookingSegFactModel.getPkId())) {
                    out.collect(hsdProcessBookingSegFactModel);
                }else{
                    logger.info("高频-PNR OSI变更事件数据信息整合[BookingSegFactModel]数据解析异常{}",value);
                }
            }
        }).returns(TypeInformation.of(HsdProcessDataModel.class)).name("OSIChangeFlatMap").setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        // 输出结果查看
//        osiChangeStream.print("osiChangeStream");
        //创建dorisSink
        // 4. 创建 Doris Sink 并写入
        DorisSink<HsdProcessDataModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_HSD_PROCESS_DATA");
        //数据写入doris
        osiChangeStream.sinkTo(dorisSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
    }

    public static void processOSIChange(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
        /*if (!Constants.EVENT_OSICHANGE.equals(event)) {
            return;
        }*/
        logger.info("高频-PNR OSI变更事件数据信息整合");
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
        //乘机人证件号码（未加密）
        String documentNumber = XpathUtils.getString(traveller, "Document/@Number");
        // 获取所有 Segment 节点
        NodeList segmentList = XpathUtils.getNodeList(passengerSegment, "Segment");
        for (int a = 0; a < segmentList.getLength(); a++) {
            Node segment = segmentList.item(a);

            // 起飞机场
            String depAirport = XpathUtils.getString(segment, "Departure/@AirportCode");
            // 降落机场
            String arriAirport = XpathUtils.getString(segment, "Arrival/@AirportCode");
            // 格式化 PNR 创建日期（去除横线）
            String formattedBookDate = StringUtils.isBlank(bookDate) ? "" : bookDate.replace("-", "");
            String formattedBookTime = StringUtils.isBlank(bookTime) ? "" : bookTime.replace("-", "");

            //  vvip  订座 PNR 中使用RMK指令备注重保旅客职务，“人大”、“政协”“党代表”“公司指定要客”等，或其他需要备注的字样。要客姓名后输入VVIP。用OSI 指令建立其他服务情况组，在
            //  OSI 组中输入要客最高职务。输入格式例如：
            //  山东省省长：OSI SC VVIP IS SHANDONGSHENGSHENGZHANG
            //  人大代表：OSI SC VVIP IS RENDADAIBIAO
            String vvip = XpathUtils.getString(segment, "OtherServiceInformation[Text[starts-with(., " + "'VVIP')]]/Text");

            // 构建每个 Segment 的实体对象
            BookingSegFactModel bookingSegFactModel = new BookingSegFactModel();
            // pk_id  主键：PNR编号+PNR创建日期+起飞机场+乘机人证件号（加密）+乘机人姓名（姓名，先取乘机人中文姓名，如果没有，取英文姓+英文名）
            bookingSegFactModel.setPkId(icsPnr + formattedBookDate + formattedBookTime + depAirport + NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,SM4Utils.encrypt(documentNumber, Constants.SM4_KEY)) + travellerName);
            //  vvip标识
            if(vvip != null && vvip.length() > 4){
                bookingSegFactModel.setVvip(vvip.substring(4));
            }
            bookingSegFactModel.setDataActive(true);
            bookingSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
            bookingSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());

            // 本系统最后更新日期时间 修改老数据 只更新 本系统最后更新日期时间
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

            // 输出实体对象
            if (StringUtils.isNotEmpty(bookingSegFactModel.getPkId())) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessBookingSegFactModel);
            }else{
                logger.info("高频-PNR OSI变更事件数据信息整合[BookingSegFactModel]数据解析异常{}",value);
            }
        }
    }
}
