package com.travelsky.dataplatform.main.dwd;

import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.SeatRefundFactModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.SeatTikFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

/**
 * @author kuangaihua
 * @date 2025/8/4 15:10
 */
public class TOdsWxapCheckinTrpOrderPassengerToDwd {
    static final Logger logger = LoggerFactory.getLogger(TOdsWxapCheckinTrpOrderPassengerToDwd.class);

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            logger.error("etl_date参数为空");
            System.exit(0);
        }
        String etlDate = args[0];
        logger.info("etl_date:" + etlDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointUtils.setCheckpoint(env, "TOdsWxapCheckinTrpOrderPassengerToDwd");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //注册数据源表
        tEnv.executeSql(GetTableSql.getQueryTOdsWxapCheckinTrpOrder());
        tEnv.executeSql(GetTableSql.getQueryTOdsWxapCheckinTrpOrderPassenger());
        //查询sql
        String query = "SELECT T1.EMD_NO,T1.DEP,T1.ARR,T1.ORDER_NO,T2.CUSTOMER_ID CUSTOMER_ID,T1.UPDATE_TIME,CAST(T1.SEAT_STATUS AS STRING) SEAT_STATUS FROM T_ODS_WXAP_CHECKIN_TRP_ORDER_PASSENGER T1\n" +
                "LEFT JOIN T_ODS_WXAP_CHECKIN_TRP_ORDER T2 ON T1.ORDER_NO=T2.ORDER_NO\n" +
                "WHERE T1.SEAT_TYPE=1 \n" +
                "AND T1.SEAT_STATUS IN(1,3) \n" +
                "AND T1.ETL_DATE='" + etlDate + "' \n" +
                "AND T1.EMD_NO IS NOT NULL";
        logger.info("查询sql：" + query);
        Table table = tEnv.sqlQuery(query);
        // 转换为 DataStream<Row>
        DataStream<Tuple2<SeatTikFactModel, SeatRefundFactModel>> source = tEnv.toChangelogStream(table).map(
                new MapFunction<Row, Tuple2<SeatTikFactModel, SeatRefundFactModel>>() {
                    @Override
                    public Tuple2<SeatTikFactModel, SeatRefundFactModel> map(Row row) throws Exception {
                        //EMD票号
                        String emdNo = row.getFieldAs("EMD_NO");
                        //出发机场
                        String dep = row.getFieldAs("DEP");
                        //到达机场
                        String arr = row.getFieldAs("ARR");
                        //关联渠道订单号
                        String orderNo = row.getFieldAs("ORDER_NO");
                        //预订渠道
                        String akChannel = "SCXCX";
                        //退票渠道
                        String akRefundChannel = "SCXCX";
                        //CUSTOMER_ID
                        String customerId = row.getFieldAs("CUSTOMER_ID");
                        //源系统更新时间
                        LocalDateTime updateTime = row.getFieldAs("UPDATE_TIME");
                        String seatStatus = row.getFieldAs("SEAT_STATUS");
                        String currentDateTime = DateTimeUtils.getCurrentDateTime();
                        String pkId = emdNo + dep + arr;
                        String updateTimeStr = DateTimeUtils.localDateTimeToString(updateTime);
                        Tid tid = new Tid();
                        tid.setTid(customerId);
                        tid.setCrmCustomerId(customerId);
                        String tidStr = IdMapping.idMappingFunction(tid, "WXAP");
                        if (seatStatus == null) {
                            return null;
                        }
                        SeatTikFactModel seatTikFactModel = null;
                        SeatRefundFactModel seatRefundFactModel = null;
                        if ("1".equals(seatStatus)) {
                            seatTikFactModel = new SeatTikFactModel();
                            seatTikFactModel.setPkId(pkId);
                            seatTikFactModel.setAkEmdnum(emdNo);
                            seatTikFactModel.setAkOrdernum(orderNo);
                            seatTikFactModel.setSourceLastUpdatetime(updateTimeStr);
                            seatTikFactModel.setSystemLastUpdatetime(currentDateTime);
                            seatTikFactModel.setSystemCreatetime(currentDateTime);
                            seatTikFactModel.setFkBookingUserTid(tidStr);
                            seatTikFactModel.setFkBookingUserOriginId(customerId);
                            seatTikFactModel.setAkChannel(akChannel);

                        } else if ("3".equals(seatStatus)) {
                            seatRefundFactModel = new SeatRefundFactModel();
                            seatRefundFactModel.setPkId(pkId);
                            seatRefundFactModel.setAkEmdnum(emdNo);
                            seatRefundFactModel.setAkOrdernum(orderNo);
                            seatRefundFactModel.setSourceLastUpdatetime(updateTimeStr);
                            seatRefundFactModel.setSystemLastUpdatetime(currentDateTime);
                            seatRefundFactModel.setSystemCreatetime(currentDateTime);
                            seatRefundFactModel.setFkRefundUserTid(tidStr);
                            seatRefundFactModel.setFkRefundUserOriginId(customerId);
                            seatRefundFactModel.setAkRefundChannel(akRefundChannel);
                        }
                        Tuple2<SeatTikFactModel, SeatRefundFactModel> userTuple =
                                Tuple2.of(seatTikFactModel, seatRefundFactModel);
                        return userTuple;
                    }
                }
        ).filter(row -> row != null);
//        source.print();
        // 创建 Doris Sink
        DorisSink<SeatTikFactModel> seatTikFactModelDorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_SEAT_TIK_FACT");
        DorisSink<SeatRefundFactModel> seatRefundFactModelDorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_SEAT_REFUND_FACT");
        source.map(tuple -> tuple.f0).filter(row -> row != null).sinkTo(seatTikFactModelDorisSink);
        source.map(tuple -> tuple.f1).filter(row -> row != null).sinkTo(seatRefundFactModelDorisSink);
        env.execute("TOdsWxapCheckinTrpOrderPassengerToDwd");
    }
}
