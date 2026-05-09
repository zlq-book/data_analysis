package com.travelsky.dataplatform.main.dwd.v2.trp;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.TrpCreateToSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.CouponSaleFactModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * TRP-休息室券销售-业务事实表 数据转储
 * 数据流向：T_ODS_TRP_COUPON_SALE_DETAIL -> T_DWD_TRP_COUPON_SALE_FACT
 * 
 * 业务规则：
 * 1. 仅取订单状态='出券成功'且券码非空的数据
 * 2. 主键：订单号+券码
 * 3. 购买渠道需要标准化处理
 * 4. 订单日期拆分为购买日期(YYYY-MM-DD)和购买时间(HH:mm:ss)
 * 5. 用户名(customer ID)转换为TID
 */
public class ScCouponSaleDetailToDwd {

    private static final Logger logger = LoggerFactory.getLogger(ScCouponSaleDetailToDwd.class);

    public static void main(String[] args) throws Exception {
        // 参数校验
        if (args.length < 1) {
            logger.error("缺少ETL日期参数");
            System.exit(0);
        }
        
        String etlDate = args[0];
        String startDate = etlDate;
        String endDate = etlDate;
        
        // 支持批量处理：传入起止日期
        if (args.length > 2) {
            startDate = args[1];
            endDate = args[2];
        }

        logger.info("开始处理TRP休息室券销售数据转储到DWD层，ETL日期: {} - {}", startDate, endDate);

        // 1. 初始化Flink环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointUtils.setCheckpoint(env, "ScCouponSaleDetailToDwd");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 2. 创建ODS源表
        tEnv.executeSql(TrpCreateToSql.COUPON_SALE_DETAIL);

        // 3. 从ODS表中读取数据
        String query = buildQuerySql(startDate, endDate);
        logger.info("执行SQL查询: {}", query);
        
        Table table = tEnv.sqlQuery(query);

        // 4. 数据转换
        DataStream<CouponSaleFactModel> couponSaleFactStream = tEnv.toChangelogStream(table)
                .map(new MapFunction<Row, CouponSaleFactModel>() {
                    @Override
                    public CouponSaleFactModel map(Row row) throws Exception {
                        try {
                            CouponSaleFactModel model = new CouponSaleFactModel();

                            // 主键：订单号 + 券码
                            String orderNumber = row.getFieldAs("ORDER_NUMBER");
                            String voucherCode = row.getFieldAs("VOUCHER_CODE");
                            model.setPkId(orderNumber + voucherCode);

                            // 订单号
                            model.setOrderNumber(orderNumber);

                            // 购买渠道 - 需要标准化
                            String channel = row.getFieldAs("CHANNEL");
                            if (StringUtils.isNotBlank(channel)) {
                                channel = NormalizationUtils.standardize(FieldType.ORDER_CHANNEL, channel, DataSource.TRP_SC);
                            }
                            model.setPurchaseChannel(channel);

                            // 订单日期拆分为购买日期和购买时间
                            String orderDate = row.getFieldAs("ORDER_DATE");
                            if (StringUtils.isNotBlank(orderDate)) {
                                try {
                                    // 假设订单日期格式为 "YYYY-MM-DD HH:mm:ss"
                                    LocalDateTime dateTime = LocalDateTime.parse(orderDate, 
                                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                                    model.setPurchaseDate(dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                                    model.setPurchaseTime(dateTime.format(DateTimeFormatter.ofPattern("HH:mm:ss")));
                                } catch (Exception e) {
                                    logger.warn("订单日期格式解析失败: {}", orderDate, e);
                                    // 尝试其他格式
                                    if (orderDate.contains(" ")) {
                                        String[] parts = orderDate.split(" ");
                                        model.setPurchaseDate(parts[0]);
                                        if (parts.length > 1) {
                                            model.setPurchaseTime(parts[1]);
                                        }
                                    } else {
                                        model.setPurchaseDate(orderDate);
                                    }
                                }
                            }

                            // 券名称
                            model.setVoucherName(row.getFieldAs("VOUCHER_NAME"));

                            // 券面额
                            BigDecimal voucherAmount = row.getFieldAs("VOUCHER_AMOUNT");
                            model.setVoucherAmount(voucherAmount);

                            // 券支付金额
                            BigDecimal paymentAmount = row.getFieldAs("PAYMENT_AMOUNT");
                            model.setVoucherPaymentAmount(paymentAmount);

                            // 券码
                            model.setVoucherCode(voucherCode);

                            // 购买人源ID（用户名即customer ID）
                            String username = row.getFieldAs("USERNAME");
                            model.setFkBuyerOriginId(username);

                            // 购买人TID（用户名转TID）
                            if (StringUtils.isNotBlank(username)) {
                                Tid tid = new Tid();
                                tid.setTid(username);
                                String tidStr = IdMapping.idMappingFunction(tid, "TRP-FTP");
                                model.setFkBuyerTid(tidStr);
                            }

                            // 源系统最后更新时间（支付日期）
                            String paymentDate = row.getFieldAs("PAYMENT_DATE");
                            model.setSourceLastUpdatetime(paymentDate);

                            // 系统时间
                            LocalDateTime now = LocalDateTime.now();
                            model.setSystemCreatetime(now.toString());
                            model.setSystemLastUpdatetime(now.toString());

                            return model;
                        } catch (Exception e) {
                            logger.error("数据转换失败: {}", row, e);
                            return null;
                        }
                    }
                })
                .filter(Objects::nonNull);

        // 5. 写入DWD层Doris
        DorisSink<CouponSaleFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_TRP_COUPON_SALE_FACT");
        couponSaleFactStream.sinkTo(dorisSink);

        // 6. 启动任务
        logger.info("开始执行Flink任务...");
        env.execute("ScCouponSaleDetailToDwd");
        logger.info("任务执行完成");
    }

    /**
     * 构建查询SQL
     * 过滤条件：
     * 1. 订单状态='出券成功'
     * 2. 券码非空
     * 3. ETL_DATE在指定日期范围内
     */
    private static String buildQuerySql(String startDate, String endDate) {
        return "SELECT \n" +
                "    ORDER_NUMBER,\n" +
                "    CHANNEL,\n" +
                "    ORDER_DATE,\n" +
                "    VOUCHER_NAME,\n" +
                "    VOUCHER_AMOUNT,\n" +
                "    PAYMENT_AMOUNT,\n" +
                "    VOUCHER_CODE,\n" +
                "    USERNAME,\n" +
                "    PAYMENT_DATE\n" +
                " FROM T_ODS_TRP_COUPON_SALE_DETAIL\n" +
                " WHERE ORDER_STATUS = '出券成功'\n" +
                "   AND VOUCHER_CODE IS NOT NULL\n" +
                "   AND VOUCHER_CODE <> ''\n" +
                "   AND ETL_DATE >= '" + startDate + "'\n" +
                "   AND ETL_DATE <= '" + endDate + "'";
    }
}
