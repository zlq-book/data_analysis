package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.DocumentUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.XpathUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingSegFactModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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
public class VTFactTrans {

    static final Logger logger = LoggerFactory.getLogger(VTFactTrans.class);

    public static void output(DataStream<String> source) {

        SingleOutputStreamOperator<TickingSegFactModel> vtStream = source.flatMap((String value,
                                                                                   Collector<TickingSegFactModel> out) -> {
            Document document;
            try {
                document = DocumentUtils.string2Document(value);
            } catch (Exception e) {
                logger.warn("convert document is error, the content is {}, the error is {}", value, e.getMessage());
                return;
            }
            // 事件名称
            String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
            if (!Constants.EVENT_VT.equals(event)) {
                return;
            }
            logger.info("高频-废票事件数据信息整合");
            // 票号
            String ticketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@TicketNumber");
            NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
            for (int a = 0; a < segmentList.getLength(); a++) {
                Node segment = segmentList.item(a);
                // 航段状态
                String segmentStatus = XpathUtils.getString(segment, "@CouponStatus");
                // 起飞机场
                String depairport = XpathUtils.getString(segment, "Departure/@AirportCode");
                // 航班起飞日期
                String departureDate = XpathUtils.getString(segment, "Departure/@Date");

                TickingSegFactModel tickingSegFactModel = new TickingSegFactModel();
                // pkId 票号+起飞机场+起飞日期
                tickingSegFactModel.setPkId(ticketNumber + depairport + departureDate.replace("-",""));
                // 航段状态
                tickingSegFactModel.setSegmentStatus(segmentStatus);
                tickingSegFactModel.setDataActive(true);
                tickingSegFactModel.setDataActiveTime(LocalDateTime.now().toString());
                tickingSegFactModel.setSystemCreatetime(LocalDateTime.now().toString());
                // 本系统最后更新日期时间
                tickingSegFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());

                // 输出实体对象
                if (StringUtils.isNotEmpty(tickingSegFactModel.getPkId())) {
                    out.collect(tickingSegFactModel);
                }else{
                    logger.info("高频-废票事件数据信息整合[TickingSegFactModel]数据解析异常{}",value);
                }
            }

        }).returns(TypeInformation.of(TickingSegFactModel.class)).name("VTFlatMap").setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        // 输出结果查看
//        vtStream.print("vtStream");
        //创建dorisSink
        // 4. 创建 Doris Sink 并写入
        DorisSink<TickingSegFactModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_TICKING_SEG_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        //数据写入doris
        vtStream.sinkTo(dorisSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
    }
}
