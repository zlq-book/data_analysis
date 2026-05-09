package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.TOdsHsdIPSEntity;
import com.travelsky.dataplatform.utils.DocumentUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.SM4Utils;
import com.travelsky.dataplatform.utils.XpathUtils;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import java.sql.Date;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/15  16:43
 */
public class BaseTrans {

    static final Logger logger = LoggerFactory.getLogger(BaseTrans.class);

    public static void output(DataStream<String> source) {

        SingleOutputStreamOperator<TOdsHsdIPSEntity> ipsStream = source.map(new MapFunction<String,
                TOdsHsdIPSEntity>() {
            @Override
            public TOdsHsdIPSEntity map(String value) throws Exception {
                Document document;
                try {
                    document = DocumentUtils.string2Document(value);
                } catch (Exception e) {
                    logger.warn("convert document is error, the content is {}, the error is {}", value, e.getMessage());
                    return null;
                }
                logger.info("高频旅客信息表数据采集");
                // 报文处理的时间
                String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
                // 事件名称
                String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
                if(!Constants.HSD_EVENTS.contains(event)){
                    return null;
                }
                // 子事件名称
                String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
                // ICS系统PNR记录编号
                String icsPnr = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@RecordLocator");
                // CRS系统PNR记录编号
                String crsPnr = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@GDSRecordLocator");
                // PNR生成日期
                String bookDate = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/BookingInfo/@BookDate");
                // 票号
                String ticketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/TicketInfo/@TicketNumber");
                // EMD票号
                String emdTicketNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/EMDTicketInfo/@EMDTicketNumber");

                // 将原 flatMap 内部逻辑优化如下
                TOdsHsdIPSEntity entity = new TOdsHsdIPSEntity();

                entity.setEVENT(event);
                entity.setSUBEVENT(subEvent);

                // 时间字符串 "20251009050538383" 转换为 Timestamp（包含毫秒）
                entity.setUPTM(TransUtils.parseUptmToTimestamp(uptm));


                // 使用统一系统时间
                LocalDateTime now = LocalDateTime.now();
                entity.setCREATETIME(String.valueOf(now));
                entity.setUPTATETIME(String.valueOf(now));
                entity.setCREATEDATE(Date.valueOf(now.toLocalDate()));

                // 设置业务字段
                entity.setGDSRECORDLOCATOR(crsPnr);
                entity.setRECORDLOCATOR(icsPnr);

                // bookDate 可能为 null 或格式错误，加 try-catch 安全处理
                try {
                    entity.setBOOKDATE(Date.valueOf(bookDate));
                } catch (Exception e) {
                    logger.warn("bookDate 格式错误: {}", bookDate);
                    entity.setBOOKDATE(null);
                }

                entity.setTICKETNUMBER(ticketNumber);
                entity.setEMDTICKETNUMBER(emdTicketNumber); // 允许为空字符串

                // 加密内容
                entity.setCONTENT(SM4Utils.encrypt(value, Constants.SM4_KEY));
                return entity;
            }
        }).setParallelism(Constants.HSD_KAFKA_PARALLELISM).filter(Objects::nonNull);
        // 输出结果查看
//        ipsStream.print("ipsStream");
        // 创建 Doris Sink 并写入
        DorisSink<TOdsHsdIPSEntity> dorisSink = FlinkDorisUtils.creatDorisODSSink("T_ODS_HSD_IPS");
        //数据写入doris
        ipsStream.sinkTo(dorisSink).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
    }
}
