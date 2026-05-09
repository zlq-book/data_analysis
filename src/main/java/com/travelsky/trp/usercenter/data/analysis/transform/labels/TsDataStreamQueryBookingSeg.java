package com.travelsky.trp.usercenter.data.analysis.transform.labels;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.DorisUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.DepartSegFactModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingSegFactModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingTicFactModel;
import com.travelsky.trp.usercenter.data.analysis.transform.hsd.CommonOutputTags;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;


public class TsDataStreamQueryBookingSeg {
    static final Logger logger = LoggerFactory.getLogger(TsDataStreamQueryBookingSeg.class);

    public static void output(DataStream<Row> rowDataStream) {

        // 后续可以进行 map、filter、sink 操作 过滤证件号 是null
        SingleOutputStreamOperator<Void> processedStream = rowDataStream.process(new ProcessFunction<Row, Void>() {
            private transient Connection connDim;
            private transient Statement stmtDim;

            @Override
            public void open(Configuration parameters) throws Exception {
                connDim = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
                stmtDim = DorisUtils.getStatement(connDim);
            }

            @Override
            public void close() throws Exception {
                DorisUtils.close(connDim, stmtDim, null);
            }

            @Override
            public void processElement(Row row, Context ctx, Collector<Void> out) throws Exception {
                TickingSegFactModel model = new TickingSegFactModel();
                model.setPkId(row.getField("PK_ID").toString());
                model.setSystemLastUpdatetime(LocalDateTime.now().toString());
                // 是否自己订票
                if (null != row.getField("FK_PASSENGER_USER_TID")
                        && null != row.getField("FK_BOOKING_USER_TID")
                        && row.getField("FK_PASSENGER_USER_TID").toString()
                        .equals(row.getField("FK_BOOKING_USER_TID").toString())) {
                    model.setSelfBooking(true);
                }
                // 查询 预定航段级
                if (null != row.getField("AK_PNR_NUMBER") && null != row.getField("FK_DEPAIRPORT")
                        && null != row.getField("FK_SEG_DATE")
                        && null != row.getField("FK_SEG_TIME")
                        && null != row.getField("CERT_NUMBER")
                        && null != row.getField("PSG_NAME")) {
                    // 查询sql
                    String pkId = row.getField("AK_PNR_NUMBER").toString()
                            + row.getField("FK_DEPAIRPORT").toString()
                            + row.getField("FK_SEG_DATE").toString().replace("-", "")
                            + row.getField("FK_SEG_TIME").toString().replace(":", "")
                            + row.getField("CERT_NUMBER").toString()
                            + row.getField("PSG_NAME").toString();
                    String firstSql = "SELECT \n" +
                            "PEER_SENIOR ,\n" +
                            "AK_PEER_CHD ,\n" +
                            "AK_BOOKER_IS_PASSENGER\n" +
                            "FROM "
                            + Constants.DWD_DB
                            + ".T_DWD_BOOKING_SEG_FACT WHERE PK_ID='" + pkId + "'";
                    ResultSet firstSet = DorisUtils.getDorisResult(stmtDim, firstSql);
                    Integer advDay = 0;
                    LocalDateTime localDateTime = null;
                    try {
                        if (null != firstSet && firstSet.next()) {
                            Boolean peerSenior = firstSet.getBoolean("PEER_SENIOR");
                            Boolean akPeerChd = firstSet.getBoolean("AK_PEER_CHD");
                            Boolean ak_booker_is_passenger = firstSet.getBoolean("AK_BOOKER_IS_PASSENGER");
                            model.setAkBookingPassenger(ak_booker_is_passenger);
                            model.setPeerSenior(peerSenior);
                            model.setAkPeerChd(akPeerChd);

                        }
                    } catch (Exception e) {
                        firstSet.close();
                        logger.error("异步执行异常", e);
                    }
                    firstSet.close();

                    ctx.output(CommonOutputTags.TICKING_SEG_TAG, model);
                    // 出票-客票级、（退票-航段级 v2 才有字段）、 成行-航段级 拼主键 写入
                    if (null != row.getField("FK_ISSUE_DATE") && null != row.getField("AK_TICKET_NUMBER")) {
                        TickingTicFactModel tickingTicFactModel = new TickingTicFactModel();
                        tickingTicFactModel.setPkId(row.getField("FK_ISSUE_DATE").toString().replace("-", "")
                                + row.getField("AK_TICKET_NUMBER").toString());
                        tickingTicFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());
                        tickingTicFactModel.setSelfBooking(model.getSelfBooking());
                        tickingTicFactModel.setPeerSenior(model.getPeerSenior());
                        tickingTicFactModel.setAkPeerChd(model.getAkPeerChd());
                        tickingTicFactModel.setAkBookingPassenger(model.getAkBookingPassenger());
                        ctx.output(CommonOutputTags.TICKING_TIC_TAG, tickingTicFactModel);
                    }

                    if (null != row.getField("FK_ISSUE_DATE")
                            && null != row.getField("AK_TICKET_NUMBER")
                            && null != row.getField("FK_DEPAIRPORT")
                            && null != row.getField("FK_ARRIAIRPORT")) {
                        DepartSegFactModel departSegFactModel = new DepartSegFactModel();
                        departSegFactModel.setPkId(row.getField("FK_ISSUE_DATE").toString().replace("-", "")
                                + row.getField("AK_TICKET_NUMBER").toString()
                                + row.getField("FK_DEPAIRPORT").toString()
                                + row.getField("FK_ARRIAIRPORT").toString());
                        departSegFactModel.setSystemLastUpdatetime(LocalDateTime.now().toString());
                        departSegFactModel.setPeerSenior(model.getPeerSenior());
                        departSegFactModel.setPeerChd(model.getAkPeerChd());
                        departSegFactModel.setSelfBooking(model.getSelfBooking());
                        ctx.output(CommonOutputTags.DEPART_SEG_TAG, departSegFactModel);
                    }

                }

            }
        });


        // 输出结果查看
        DataStream<TickingSegFactModel> dwdStream = processedStream.getSideOutput(CommonOutputTags.TICKING_SEG_TAG);
        DataStream<TickingTicFactModel> ticFactModelDataStream = processedStream.getSideOutput(CommonOutputTags.TICKING_TIC_TAG);
        DataStream<DepartSegFactModel> departSegFactModelDataStream = processedStream.getSideOutput(CommonOutputTags.DEPART_SEG_TAG);
        // 创建 Doris Sink 并写入
        dwdStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_SEG_FACT"));
        ticFactModelDataStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_TIC_FACT"));
        departSegFactModelDataStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_DEPART_SEG_FACT"));
    }
}
