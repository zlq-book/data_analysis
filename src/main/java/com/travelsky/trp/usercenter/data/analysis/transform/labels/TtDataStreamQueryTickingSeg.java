package com.travelsky.trp.usercenter.data.analysis.transform.labels;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimSummaryModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingTicFactModel;
import com.travelsky.trp.usercenter.data.analysis.transform.hsd.CommonOutputTags;
import org.apache.commons.lang3.StringUtils;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;


public class TtDataStreamQueryTickingSeg {
    static final Logger logger = LoggerFactory.getLogger(TtDataStreamQueryTickingSeg.class);

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
                TickingTicFactModel model = new TickingTicFactModel();

                if (null != row.getField("TICKET_NUMBER") && !"".equals(row.getField("TICKET_NUMBER"))) {
                    String firstSql = "SELECT AK_ADVBOOK_DAY ,FK_SEG_DATE ,FK_SEG_TIME\n" +
                            "FROM "
                            + Constants.DWD_DB
                            + ".T_DWD_TICKING_SEG_FACT WHERE AK_TICKET_NUMBER='" + row.getField("TICKET_NUMBER").toString() + "'";
                    ResultSet firstSet = DorisUtils.getDorisResult(stmtDim, firstSql);
                    Integer advDay = 0;
                    LocalDateTime localDateTime = null;
                    try {
                        while(null != firstSet && firstSet.next()) {
                            String date = firstSet.getString("FK_SEG_DATE");
                            String time = firstSet.getString("FK_SEG_TIME");
                            if (null == date || null == time) {
                                continue;
                            }
                            if (null == localDateTime) {
                                // 查询成功：补全维度信息
                                advDay = firstSet.getInt("AK_ADVBOOK_DAY");
                                localDateTime = LocalDateTime.parse(date + "T" + time);
                            } else {
                                LocalDateTime tempTime = LocalDateTime.parse(date + "T" + time);
                                if (tempTime.isBefore(localDateTime)) {
                                    // 新的时间更早 更换提前出票天数
                                    advDay = firstSet.getInt("AK_ADVBOOK_DAY");
                                    localDateTime = tempTime;
                                }
                            }

                        }
                    } catch (Exception e) {
                        firstSet.close();
                        logger.error("异步执行异常", e);
                    }
                    model.setAkAdvbookDay(advDay);
                    model.setPkId(row.getField("PK_ID").toString());
                    model.setSystemLastUpdatetime(LocalDateTime.now().toString());
                    firstSet.close();
                    ctx.output(CommonOutputTags.TICKING_TIC_TAG, model);
                }

            }
        });


        // 输出结果查看
        DataStream<TickingTicFactModel> dwdStream = processedStream.getSideOutput(CommonOutputTags.TICKING_TIC_TAG);
        // 创建 Doris Sink 并写入
        dwdStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_TIC_FACT"));

    }
}
