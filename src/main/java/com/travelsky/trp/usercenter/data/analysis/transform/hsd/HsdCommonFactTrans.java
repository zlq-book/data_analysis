package com.travelsky.trp.usercenter.data.analysis.transform.hsd;


import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.DocumentUtils;
import com.travelsky.dataplatform.utils.DorisUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.XpathUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author TS.SHA.fuhuazhang
 * @date 2025/12/6  15:43
 */
public class HsdCommonFactTrans {

    static final Logger logger = LoggerFactory.getLogger(HsdCommonFactTrans.class);
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
                // 数据整合(入dim用户维表)
                UserDimTrans.processUserDim(value, ctx, document); // 查表
                // 证件信息维表
                CertDimTrans.processCertDim(value, ctx, document);
                // 手机号维表
                MobileDimTrans.processMobileDim(value, ctx, document);
                // 事件名称
                String eventType = XpathUtils.getString(document, "/Msg/Hdr/Event");
                // 根据事件类型调用对应的output方法
                switch (eventType) {
                    case "ActionCodeChange":
                        ActionCodeChangeFactTrans.processActionCodeChange(value, ctx, document);
                        break;
                    case "AsrChange":
                        AsrChangeFactTrans.processAsrChange(value, ctx, document);
                        break;
                    case "BagChange":
                        BagChangeTrans.processBagChange(value, ctx, document);
                        break;
                    case "Boarded2Open":
                        Boarded2OpenFactTrans.processBoarded2Open(value, ctx, document);
                        break;
                    case "Boarded":
                        BoardFactTrans.processBoarded(value, ctx, document);
                        break;
                    case "Book":
                        BookFactTrans.processBook(value, ctx, document);
                        break;
                    case "CabinChange":
                        CabinChangeFactTrans.processCabinChange(value, ctx, document);
                        break;
                    case "CheckIn":
                        CheckInTrans.processCheckIn(value, ctx, document);
                        break;
                    case "CheckOut":
                        CheckOutTrans.processCheckOut(value, ctx, document);
                        break;
                    case "DeSuspend":
                        TickingSegFactFactTrans.process(value, ctx, document, "DeSuspend");
                        break;
                    case "EMDBoard":
                        EMDBoardFactTrans.processEMDBoard(value, ctx, document);
                        break;
                    case "EMDCheckIn":
                        EMDCheckInFactTrans.processEMDCheckIn(value, ctx, document);
                        break;
                    case "EMDFlown":
                        EMDFlownFactTrans.processEMDFlown(value, ctx, document);
                        break;
                    case "EMDIssue":
                        EMDIssueFactTrans.processEMDIssue(value, ctx, document);
                        break;
                    case "EMDRefund":
                        EMDRefundFactTrans.processEMDRefund(value, ctx, document);
                        break;
                    case "EMDVOID":
                        EMDVOIDFactTrans.processEMDVOID(value, ctx, document);
                        break;
                    case "ETRF":
                        ETRFFactTrans.processETRF(value, ctx, document);
                        break;
                    case "Exchange2Open":
                        Exchange2OpenFactTrans.processExchange2Open(value, ctx, document);
                        break;
                    case "Exchange":
                        ExchangeFactTrans.processExchange(value, ctx, document); // 查表
                        break;
                    case "FFChange":
                        FFChangeTrans.processFFChange(value, ctx, document);
                        break;
                    case "FIMExchange2Open":
                        FIMExchange2OpenFactTrans.processFimExchange2Open(value, ctx, document);
                        break;
                    case "Flown":
                        FlownFactTrans.processFlown(value, ctx, document); // 查表
                        break;
                    case "IDChange":
                        IDChangeFactTrans.processIDChange(value, ctx, document); // 查询更新和删除表
                        break;
                    case "IRRChange":
                        IRRChangeFactTrans.processIRRChange(value, ctx, document);
                        break;
                    case "Issue":
                        IssueFactTrans.processIssue(value, ctx, document);
                        break;
                    case "NameChange":
                        NameChangeFactTrans.processNameChange(value, ctx, document); // 查询更新和删除表
                        break;
                    case "Noshow":
                        NoshowFactTrans.processNoShow(value, ctx, document); // 查询更新和删除表
                        break;
                    case "OSIChange":
                        OSIChangeFactTrans.processOSIChange(value, ctx, document);
                        break;
                    case "Revalidation":
                        RevalidationFactTrans.processRevalidation(value, ctx, document); // 查询更新和删除表
                        break;
                    case "Split":
                        SplitFactTrans.processSplit(value, ctx, document);
                        break;
                    case "SSRChange":
                    case "CKISSRChange":
                        SSRChangeFactTrans.processSSRChange(value, ctx, document);
                        break;
                    case "Suspend":
                        TickingSegFactFactTrans.process(value, ctx, document, "Suspend");
                        break;
                    case "TicketCheckIn":
                        TicketCheckInFactTrans.processTicketCheckIn(value, ctx, document); // 查询更新和删除表
                        break;
                    case "TicketCheckOut":
                        TicketCheckOutFactTrans.processTicketCheckOut(value, ctx, document);
                        break;
                    case "TktSegDurChange":
                        TktSegDurChangeFactTrans.processTktSegDurChange(value, ctx, document);
                        break;
                    case "TRFD":
                        TRFDFactTrans.processTRFD(value, ctx, document);
                        break;
                    case "Used2Open":
                        Used2OpenFactTrans.processUsed2Open(value, ctx, document); // 删表
                        break;
                    case "Void2Open":
                        Void2OpenFactTrans.processVoid2Open(value, ctx, document);
                        break;
                    case "VT":
                        TickingSegFactFactTrans.process(value, ctx, document, "VT");
                        break;
                    default:
                        System.err.println("非高频处理事件类型: " + eventType);
                }
            }
        }).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        DataStream<HsdProcessDataModel> hsdProcessDataStream = processedStream.getSideOutput(CommonOutputTags.HSD_PROCESS_DATA_TAG);
        //hsdProcessDataStream.print("hsdProcessDataStream");
        hsdProcessDataStream.sinkTo(FlinkDorisUtils.creatHsdDorisDWDSink("T_DWD_HSD_PROCESS_DATA", 120)).setParallelism(3);
    }
}
