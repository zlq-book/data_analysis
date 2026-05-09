package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.DocumentUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.XpathUtils;
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

/**
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/15  16:43
 */
public class EMDFlownFactTrans {

    static final Logger logger = LoggerFactory.getLogger(EMDFlownFactTrans.class);
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
                processEMDFlown(value, ctx, document);
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
        DataStream<UpgrTikFactModel> updaterTikDataStream = processedStream.getSideOutput(CommonOutputTags.UPGR_TIK_TAG);
        // 输出结果查看
//        updaterTikDataStream.print("updaterTikDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<UpgrTikFactModel> updaterTikSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_UPGR_TIK_FACT");
        updaterTikDataStream.sinkTo(updaterTikSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        // 获取各个SideOutput流
        DataStream<ExcessBaggageTikFactModel> excessBaggageTikDataStream =
                processedStream.getSideOutput(CommonOutputTags.EXCESS_BAGGAGE_TIK_TAG);
        // 输出结果查看
//        excessBaggageTikDataStream.print("excessBaggageTikDataStream");
        // 创建并配置各模型的Doris Sink
        DorisSink<ExcessBaggageTikFactModel> excessBaggageTikSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_EXCESS_BAGGAGE_TIK_FACT");
        excessBaggageTikDataStream.sinkTo(excessBaggageTikSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);*/
    }

    public static void processEMDFlown(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
        /*if (!Constants.EVENT_EMDFLOWN.equals(event)) {
            return;
        }*/
        logger.info("高频-EMD成行事件数据信息整合");
        // 子事件名称
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
        // 代码
        String code = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/ReasonForIssuance/@Code");
        // 子代码
        String subCode = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/ReasonForIssuance/@SubCode");
        String newVal = XpathUtils.getString(document, "/Msg/Hdr/Newval");
        // EMD票号
        String emdTicketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/@EMDTicketNumber");

        NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
        for (int a = 0; a < segmentList.getLength(); a++) {
            Node segment = segmentList.item(a);
            // 起飞机场
            String depAirport = XpathUtils.getString(segment, "Departure/@AirportCode");
            // 到达机场
            String arrivalAirport = XpathUtils.getString(segment, "Arrival/@AirportCode");

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
                    logger.info("高频-EMD成行事件数据信息整合[SeatTikFactModel]数据解析异常{}", value);
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
                    logger.info("高频-EMD成行事件数据信息整合[BaggageTikFactModel]数据解析异常{}", value);
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
                    logger.info("高频-EMD成行事件数据信息整合[MealTikFactModel]数据解析异常{}", value);
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
                    logger.info("高频-EMD成行事件数据信息整合[UpgrTikFactModel]数据解析异常{}", value);
                }
            }

            if ("C".equals(code) && ("0GO".equals(subCode) || "0DG".equals(subCode) || "0IJ".equals(subCode))) {
                // 逾重行李出票-航段级
                ExcessBaggageTikFactModel excessBaggageTikFactModel = new ExcessBaggageTikFactModel();
                //EMD票号+起飞机场+到达机场
                excessBaggageTikFactModel.setPkId(emdTicketNumber + depAirport + arrivalAirport);
                excessBaggageTikFactModel.setAkEmdStatus(newVal);
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
                    logger.info("高频-EMD成行事件数据信息整合[ExcessBaggageTikFactModel]数据解析异常{}", value);
                }
            }
        }
    }
}
