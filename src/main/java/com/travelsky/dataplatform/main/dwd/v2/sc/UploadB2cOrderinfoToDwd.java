package com.travelsky.dataplatform.main.dwd.v2.sc;


import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.main.dwd.TOdsHytdSysRegisterToDwd;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;

/**
 * *TRP数仓 T_ODS_SC_UPLOAD_B2C_ORDERINFO 写入事实表
 * TRP的现金和券支付在源数据种是一条记录，需要拆分成2条记录。
 */
public class UploadB2cOrderinfoToDwd {
    static final Logger logger = LoggerFactory.getLogger(UploadB2cOrderinfoToDwd.class);

    public static void main(String[] args) throws Exception {
        //IdMapping使用批量插入，开启定时器
        Timer timer = IdMapping.autoCommit();
        if (args.length < 1) {
            logger.error("etl_date参数为空");
            System.exit(0);
        }
        String etlDate = args[0];
        logger.info("etl_date:" + etlDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointUtils.setCheckpoint(env, "UploadB2cOrderinfoToDwd");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //注册数据源表
        tEnv.executeSql(GetTableSql.getQueryTOdsUploadB2cOrderinfo());
        //查询sql
        String query = "SELECT \n" +
                "ORDER_NO\n" +
                ",BANK_ORDER_NO\n" +
                ",ORDER_USER\n" +
                ",ORDER_DATE\n" +
                ",PAY_TIME\n" +
                ",ORDER_CHANNEL\n" +
                ",BANK_NAME\n" +
                ",CURRENCY_TYPE\n" +
                ",FARE_TOTAL\n" +
                ",COUPON_FARE\n" +
                ",COUPON_NO\n" +
                ",COUPON_NUM\n" +
                ",COUPON_NAME\n" +
                " FROM T_ODS_SC_UPLOAD_B2C_ORDERINFO \n" +
                "WHERE ETL_DATE='" + etlDate + "' \n" +
                "AND ORDER_STATUS='已出票' ";
        logger.info("查询sql：" + query);
        Table table = tEnv.sqlQuery(query);
        // 转换为 DataStream<Map<String, String>>，生成tid并做标准化
        DataStream<JSONObject> source = tEnv.toChangelogStream(table).map(new MapFunction<Row, JSONObject>() {
            @Override
            public JSONObject map(Row row) throws Exception {
                //常客注册时间
                String createDate = DateTimeUtils.localDateTimeToString(row.getFieldAs("CREATE_DATE"));
                //业务大订单编号
                String orderNo = row.getFieldAs("ORDER_NO");
                //支付流水号
                String bankOrderNo = row.getFieldAs("BANK_ORDER_NO");
                //直销会员CUSTOMER_ID
                String orderUser = row.getFieldAs("ORDER_USER");
                //预定日期
                String orderDate = DateTimeUtils.localDateTimeToDateString(row.getFieldAs("ORDER_DATE"));
                //预定时间
                String orderTime = DateTimeUtils.localDateTimeToTimeString(row.getFieldAs("ORDER_DATE"));
                //支付日期
                String payDate = DateTimeUtils.localDateTimeToDateString(row.getFieldAs("PAY_TIME"));
                //支付时间
                String payTime = DateTimeUtils.localDateTimeToTimeString(row.getFieldAs("PAY_TIME"));
                //支付渠道
                String orderChannel = row.getFieldAs("ORDER_CHANNEL");
                //支付渠道标准化
                orderChannel=NormalizationUtils.standardize(FieldType.PAYMENT_CHANNEL, orderChannel,DataSource.TRP_SC);

                //现金支付平台
                String bankName = row.getFieldAs("BANK_NAME");
                //现金支付平台
                bankName=NormalizationUtils.standardize(FieldType.CASH_PAYMENT_PLATFORM, bankName,DataSource.TRP_SC);

                JSONObject map = new JSONObject();

                Tid tid = new Tid();

                String tidStr = IdMapping.idMappingFunction(tid, "SC", false, true, true);
                map.put("TID", tidStr);
                return map;
            }
        });

        //TODO:支付流水号表

        //IdMapping使用批量插入，flink结束后停止计时器、提交事务和关闭连接
        timer.cancel();
        IdMapping.close();
    }
}
