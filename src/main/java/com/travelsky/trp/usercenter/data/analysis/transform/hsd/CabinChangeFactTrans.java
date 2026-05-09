package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.DocumentUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.XpathUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.HsdProcessDataModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingSegFactModel;
import com.travelsky.trp.usercenter.data.analysis.utils.CustomJsonSerializer;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
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
public class CabinChangeFactTrans {

    static final Logger logger = LoggerFactory.getLogger(CabinChangeFactTrans.class);

    public static void output(DataStream<String> source) {

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
                        processCabinChange(value, ctx, document);
                    }
                }).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        // 获取各个SideOutput流
        DataStream<HsdProcessDataModel> tickingSegDataStream = processedStream.getSideOutput(CommonOutputTags.HSD_PROCESS_DATA_TAG);
        // 输出结果查看
//        tickingSegDataStream.print("tickingSegDataStream");

        // 创建并配置各模型的Doris Sink
        DorisSink<HsdProcessDataModel> tickingSegSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_HSD_PROCESS_DATA");

        // 数据分别写入对应的Doris表
        tickingSegDataStream.sinkTo(tickingSegSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
    }

    public static void processCabinChange(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
        /*if (!Constants.EVENT_CABINCHANGE.equals(event)) {
            return;
        }*/
        logger.info("高频-客票舱位变更事件数据信息整合");
        // 子事件名称
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
        String newVal = XpathUtils.getString(document, "/Msg/Hdr/Newval");
        String cabin = null;
        if(newVal != null && newVal.contains("/")){
            cabin = newVal.substring(newVal.lastIndexOf('/') + 1);
        }
        // 票号
        String ticketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@TicketNumber");

        NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
        for (int a = 0; a < segmentList.getLength(); a++) {
            Node segment = segmentList.item(a);
            // 起飞机场
            String depAirport = XpathUtils.getString(segment, "Departure/@AirportCode");
            // 航班起飞日期
            String departureDate = XpathUtils.getString(segment, "Departure/@Date");

            // 构建每个 Segment 的实体对象
            // 机票出票-航段级
            TickingSegFactModel tickingSegFactModel = new TickingSegFactModel();
            // pkId 票号+起飞机场+起飞日期
            tickingSegFactModel.setPkId(ticketNumber + depAirport + departureDate.replace("-",""));
            tickingSegFactModel.setAkSegcabin(cabin);
            tickingSegFactModel.setDataActive(true);
            tickingSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
            tickingSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
            // 本系统最后更新日期时间
            tickingSegFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());
            // 舱等
            tickingSegFactModel.setAkCabin(getCabinClass(departureDate,cabin));

            HsdProcessDataModel hsdProcessDataModel = new HsdProcessDataModel();
            hsdProcessDataModel.setEvent(event);
            hsdProcessDataModel.setSubEvent(subEvent);
            hsdProcessDataModel.setTableName("T_DWD_TICKING_SEG_FACT");
            hsdProcessDataModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
            hsdProcessDataModel.setStamp(stamp);
            // 转换为JSON字符串
            hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(tickingSegFactModel));
            hsdProcessDataModel.setProcessed(false);
            hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());

            if(StringUtils.isNotEmpty(tickingSegFactModel.getPkId())) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
            }else{
                logger.info("高频-客票舱位变更事件数据信息整合[TickingSegFactModel]数据解析异常{}", value);
            }
        }
    }
}
