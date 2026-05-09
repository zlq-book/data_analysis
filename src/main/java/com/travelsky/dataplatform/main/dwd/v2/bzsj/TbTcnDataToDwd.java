//package com.travelsky.dataplatform.main.dwd.v2.bzsj;
//
//import com.alibaba.fastjson.JSONObject;
//import com.travelsky.dataplatform.constans.BzsjCreateToSql;
//import com.travelsky.dataplatform.utils.CheckpointUtils;
//import com.travelsky.dataplatform.utils.FlinkDorisUtils;
//import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.DatechangeSegFactModel;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.doris.flink.sink.DorisSink;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//
//import java.math.BigDecimal;
//import java.time.LocalDateTime;
//import java.util.Objects;
//
///**
// * 标准数据表TB_TCN_DATA
// * 海豚时 T-2
// * * *
// */
//public class TbTcnDataToDwd {
//    public static void main(String[] args) throws Exception {
//        if (args.length < 1) {
//            System.out.println("etl_date参数为空");
//            System.exit(0);
//        }
//        String etlDate = args[0];
//        String startDate = etlDate;
//        String endDate = etlDate;
//        if (args.length > 2) {
//            startDate = args[1];
//            endDate = args[2];
//        }
//
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        CheckpointUtils.setCheckpoint(env, "TbTcnDataToDwd");
//        env.setParallelism(1);
//        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//
//        // 1. 创建Doris目标表
//        tEnv.executeSql(BzsjCreateToSql.TB_TCN_DATA);
//
//        // 2. 从Doris表中读取数据
//        String query = "SELECT \n" +
//                " TK_AIR,\n" +
//                " TK_NUM,\n" +
//                " UP_LOCATION,\n" +
//                " DEP_DATE,\n" +
//                " FARE_TYPE,\n" +
//                " SMFARE,\n" +
//                " OBFARE\n" +
//                " FROM T_ODS_BZSJ_TB_TCN_DATA\n" +
//                " WHERE CHTKNUM IS NOT NULL " +
//                " AND ETL_DATE >= '" + startDate + "'" +
//                " AND ETL_DATE <= '" + endDate + "'";
//
//        System.out.println("日期为：" + etlDate + "的TCN数据开始查询...");
//        Table table = tEnv.sqlQuery(query);
//
//        // 直接转换为目标模型，避免中间JSON转换
//        DataStream<DatechangeSegFactModel> dateChangeSegFactStream = tEnv.toChangelogStream(table)
//                .map(new MapFunction<Row, DatechangeSegFactModel>() {
//                    @Override
//                    public DatechangeSegFactModel map(Row row) throws Exception {
//                        try {
//                            DatechangeSegFactModel model = new DatechangeSegFactModel();
//
//                            // 主键ID：TK_AIR + TK_NUM + UP_LOCATION + DEP_DATE
//                            String tkAir = row.getFieldAs("TK_AIR");
//                            String tkNum = row.getFieldAs("TK_NUM");
//                            String upLocation = row.getFieldAs("UP_LOCATION");
//                            String depDate = row.getFieldAs("DEP_DATE");
//                            if (depDate != null) {
//                                depDate = depDate.replace("-", "");
//                            }
//                            String pkId = tkAir + tkNum + upLocation + depDate;
//                            model.setPkId(pkId);
//
//                            // 销售币种
//                            model.setAkCurrency(row.getFieldAs("FARE_TYPE"));
//
//                            // 销售币种改升支付金额
//                            BigDecimal smFare = row.getFieldAs("SMFARE");
//                            model.setDcCurpayamount(smFare);
//
//                            // 销售币种变更手续费
//                            BigDecimal obFare = row.getFieldAs("OBFARE");
//                            model.setDcCurfee(obFare);
//
//                            // 销售币种舱位差价 = SMFARE - OBFARE
//                            BigDecimal cabinBalanceCur = BigDecimal.ZERO;
//                            if (smFare != null && obFare != null) {
//                                cabinBalanceCur = smFare.subtract(obFare);
//                            } else if (smFare != null) {
//                                cabinBalanceCur = smFare;
//                            }
//                            model.setCabinbalanceCur(cabinBalanceCur);
//
//                            // 判断销售币种是否为CNY
//                            String fareType = row.getFieldAs("FARE_TYPE");
//                            boolean isCNY = "CNY".equalsIgnoreCase(fareType);
//
//                            // 改升支付金额CNY
//                            if (isCNY && smFare != null) {
//                                model.setDcPayamount(smFare);
//                            }
//
//                            // 变更手续费CNY
//                            if (isCNY && obFare != null) {
//                                model.setDatechangeFee(obFare);
//                            }
//
//                            // 舱位差价CNY
//                            if (isCNY) {
//                                model.setCabinbalance(cabinBalanceCur);
//                            }
//
//                            // 改升航段计数
//                            model.setDatechangeCount(1);
//
//                            LocalDateTime now = LocalDateTime.now();
//                            model.setSystemCreatetime(now.toString());
//                            model.setSystemLastUpdatetime(now.toString());
//
//                            return model;
//                        } catch (Exception e) {
//                            System.err.println("数据转换异常: " + e.getMessage());
//                            return null;
//                        }
//                    }
//                })
//                .filter(Objects::nonNull)
//                .returns(DatechangeSegFactModel.class);  // 明确指定返回类型
//
//        // 写入Doris
//        DorisSink<DatechangeSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_DATECHANGE_SEG_FACT");
//        dateChangeSegFactStream.sinkTo(dorisSink);
//
//        //启动任务
//        env.execute("TbTcnDataToDwd");
//    }
//}
