package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.DocumentUtils;
import com.travelsky.dataplatform.utils.DorisUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.XpathUtils;
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

import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDateTime;

/**
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/15  16:43
 */
public class Used2OpenFactTrans {

    static final Logger logger = LoggerFactory.getLogger(Used2OpenFactTrans.class);

    public static void output(DataStream<String> source) {

//        SingleOutputStreamOperator<TickingSegFactModel> used2OpenStream = source.flatMap((String value,
//                                                                                          Collector<TickingSegFactModel> out) -> {
//            Document document;
//            try {
//                document = DocumentUtils.string2Document(value);
//            } catch (Exception e) {
//                logger.warn("convert document is error, the content is {}, the error is {}", value, e.getMessage());
//                return;
//            }
//            // 事件名称
//            String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
//            if (!Constants.EVENT_USED2OPEN.equals(event)) {
//                return;
//            }
//            logger.info("高频-客票取消成行事件数据信息整合");
//            String newVal = XpathUtils.getString(document, "/Msg/Hdr/Newval");
//
//            // 票号
//            String ticketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@TicketNumber");
//
//            NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
//            for (int a = 0; a < segmentList.getLength(); a++) {
//                Node segment = segmentList.item(a);
//                // 起飞机场
//                String depairport = XpathUtils.getString(segment, "Departure/@AirportCode");
//                // 航班起飞日期
//                String departureDate = XpathUtils.getString(segment, "Departure/@Date");
//                String couponStatus = null;
//                // 航段状态  解析<Newval>中的状态
//                if (newVal != null && newVal.contains("/")) {
//                    couponStatus = newVal.substring(newVal.lastIndexOf('/') + 1);
//                }
//                TickingSegFactModel tickingSegFactModel = new TickingSegFactModel();
//                // pkId 票号+起飞机场+起飞日期
//                tickingSegFactModel.setPkId(ticketNumber + depairport + departureDate.replace("-",""));
//                // 航段状态
//                tickingSegFactModel.setSegmentStatus(couponStatus);
//                tickingSegFactModel.setDataActive(true);
//                tickingSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
//                tickingSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
//                // 本系统最后更新日期时间
//                tickingSegFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());
//
//                // 输出实体对象
//                if (StringUtils.isNotEmpty(tickingSegFactModel.getPkId())) {
//                    out.collect(tickingSegFactModel);
//                }else{
//                    logger.info("高频-客票取消成行事件数据信息整合[TickingSegFactModel]数据解析异常{}",value);
//                }
//            }
//
//        }).returns(TypeInformation.of(TickingSegFactModel.class)).name("Used2OpenFlatMap");
//        // 输出结果查看
////        used2OpenStream.print("used2OpenStream");
//        // 创建 Doris Sink 并写入
//        DorisSink<TickingSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_SEG_FACT");
//        //数据写入doris
//        used2OpenStream.sinkTo(dorisSink);


        // V2 新增 成行取消改为根据id删除数据
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
                processUsed2Open(value, ctx, document);
            }
        }).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        DataStream<HsdProcessDataModel> hsdProcessDataStream = processedStream.getSideOutput(CommonOutputTags.HSD_PROCESS_DATA_TAG);
        //hsdProcessDataStream.print("hsdProcessDataStream");
        hsdProcessDataStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_HSD_PROCESS_DATA")).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        /*// 获取各个SideOutput流
        DataStream<TickingSegFactModel> used2OpenStream = processedStream.getSideOutput(CommonOutputTags.TICKING_SEG_TAG);
        // 输出结果查看
//        used2OpenStream.print("used2OpenStream");
        // 创建 Doris Sink 并写入
        DorisSink<TickingSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_SEG_FACT");
        //数据写入doris
        used2OpenStream.sinkTo(dorisSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);*/
    }

    public static void processUsed2Open(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
        /*if (!Constants.EVENT_USED2OPEN.equals(event)) {
            return;
        }*/
        logger.info("高频-客票取消成行事件数据信息整合");
        String newVal = XpathUtils.getString(document, "/Msg/Hdr/Newval");
        // 子事件名称
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");

        // 票号
        String ticketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@TicketNumber");

        NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
        for (int a = 0; a < segmentList.getLength(); a++) {
            Node segment = segmentList.item(a);
            // 起飞机场
            String depairport = XpathUtils.getString(segment, "Departure/@AirportCode");
            // 航班起飞日期
            String departureDate = XpathUtils.getString(segment, "Departure/@Date");
            String couponStatus = null;
            // 航段状态  解析<Newval>中的状态
            if (newVal != null && newVal.contains("/")) {
                couponStatus = newVal.substring(newVal.lastIndexOf('/') + 1);
            }
            TickingSegFactModel tickingSegFactModel = new TickingSegFactModel();
            // pkId 票号+起飞机场+起飞日期
            String pkId = ticketNumber + depairport + departureDate.replace("-", "");
            tickingSegFactModel.setPkId(pkId);
            // 航段状态
            tickingSegFactModel.setSegmentStatus(couponStatus);
            tickingSegFactModel.setDataActive(true);
            tickingSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
            tickingSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
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

            // 输出实体对象 删除表
            if (StringUtils.isNotEmpty(tickingSegFactModel.getPkId())) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessTickingSegFactModel);
                // TODO sql处理删除表
                /*//V2 新增 删除成型表
                String departSegDeleteSql = "DELETE FROM " + Constants.DWD_DB + ".T_DWD_DEPART_SEG_FACT WHERE " +
                        "PK_ID = '" + pkId + "'";
                logger.info("HSD Used2OpenFactTrans departSegDeleteSql:{}", departSegDeleteSql);
                try (Connection localConn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
                     Statement localStmt = localConn.createStatement()){
                    localStmt.executeUpdate(departSegDeleteSql);
                } catch (SQLException e) {
                    logger.error("HSD Used2OpenFactTrans departSegDeleteSql error:{}", e.getMessage());
                }*/
            } else {
                logger.info("高频-客票取消成行事件数据信息整合[TickingSegFactModel]数据解析异常{}", value);
            }
        }
    }

}
