package com.travelsky.dataplatform.main.dwd;

import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.GetTableSql;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.CheckinSegFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kuangaihua
 * @date 2025/8/4 15:10
 * 描述：2025/8/12日 删除该表写入值机航段事实表 的逻辑，该任务下线。
 */
public class TOdsWxapCheckinItineraryToDwd {
    static final Logger logger = LoggerFactory.getLogger(TOdsWxapCheckinItineraryToDwd.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            logger.error("etl_date参数为空");
            System.exit(0);
        }
        String etlDate = args[0];
        logger.info("etl_date:" + etlDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointUtils.setCheckpoint(env, "TOdsWxapCheckinItineraryToDwd");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //注册数据源表
        tEnv.executeSql(GetTableSql.getQueryTOdsWxapCheckinItinerary());
        tEnv.executeSql(GetTableSql.getQueryTOdsWxapCheckinSypr());
        tEnv.executeSql(GetTableSql.getQueryTOdsWxapCheckinChannelUser());
        //查询sql，CHECKIN_ITINERARY (值机行程表)，CHECKIN_SYPR (值机旅客信息)，CHECKIN_CHANNEL_USER (渠道用户值机信息)  三表联查
        String query = "SELECT T1.FLIGHT_DATE,T2.TKT_NUMBER,T1.DEP,T1.ARR,CAST(T3.CHECKIN_CHANNEL AS STRING) CHECKIN_CHANNEL FROM T_ODS_WXAP_CHECKIN_ITINERARY T1 \n" +
                " JOIN T_ODS_WXAP_CHECKIN_SYPR T2 ON T2.CI_ID=T1.ID\n" +
                " LEFT JOIN T_ODS_WXAP_CHECKIN_CHANNEL_USER T3 ON T2.ID=T3.SYPR_ID\n" +
                " WHERE T1.ETL_DATE='" + etlDate + "'";
        logger.info("查询sql：" + query);
        Table table = tEnv.sqlQuery(query);
        // 转换为 DataStream<Row>
        DataStream<CheckinSegFactModel> source = tEnv.toChangelogStream(table).map(
                row -> {
                    //航班日期
                    String flightDate = row.getFieldAs("FLIGHT_DATE");
                    //票号
                    String tktNumber = row.getFieldAs("TKT_NUMBER");
                    //起飞机场
                    String dep = row.getFieldAs("DEP");
                    //到达机场
                    String arr = row.getFieldAs("ARR");
                    //值机渠道
                    String checkinChannel =  row.getFieldAs("CHECKIN_CHANNEL");
                    String currentDateTime = DateTimeUtils.getCurrentDateTime();
                    CheckinSegFactModel checkinSegFactModel = new CheckinSegFactModel();
                    checkinSegFactModel.setPkId(flightDate.replace("-", "") + tktNumber + dep + arr);
                    checkinSegFactModel.setCheckinChannel(checkinChannel);
                    checkinSegFactModel.setSystemCreatetime(currentDateTime);
                    checkinSegFactModel.setSystemLastUpdatetime(currentDateTime);
                    return checkinSegFactModel;
                }
        );
        //  创建 Doris Sink 并写入
        DorisSink<CheckinSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_CHECKIN_SEG_FACT");
        //数据写入doris
        source.sinkTo(dorisSink);
        env.execute("TOdsWxapCheckinItineraryToDwd");
    }
}
