package com.travelsky.dataplatform.main.dwd.v2.trp;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.TrpCreateToSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.AuisSegFactModel;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * SC 保险销售报表明细表-业务事实表 数据转储
 * 数据流向：T_ODS_TRP_INSURANCE_SALE_DETAIL -> T_DWD_AUIS_SEG_FACT
 * 
 * 业务规则：
 * 1. 每天取前一天的 sc_b2c_report_insurance_sell 报表数据
 * 2. 主键：票号（票号中的横杠去除）+保险单号
 * 3. 关联票号 乘机人姓名 乘机人证件类型 乘机人证件号 保险类型 保险预订渠道 保险状态 需要标准化处理
 * 4. 订票时间 拆分为 保险预订日期(YYYY-MM-DD)和 保险预订时间(HH:mm:ss)
 * 5. 起飞日期 拆分为 航班起飞日期 (YYYY-MM-DD)和 航班起飞时间(HH:mm:ss)
 * 6. 证件号转换为TID
 */
public class ScInsuranceSellToAuisSegDwd {

    private static final Logger logger = LoggerFactory.getLogger(ScInsuranceSellToAuisSegDwd.class);

    public static void main(String[] args) throws Exception {
        // 参数校验
        if (args.length < 1) {
            logger.error("缺少ETL日期参数");
            System.exit(0);
        }

        String etlDate = args[0];
//        String etlDate = "2025-12-04";
        String startDate = etlDate;
        String endDate = etlDate;
        
        // 支持批量处理：传入起止日期
        if (args.length > 2) {
            startDate = args[1];
            endDate = args[2];
        }

        logger.info("开始处理 TRP保险销售报表明细表 数据转储到DWD层，ETL日期: {} - {}", startDate, endDate);

        // 1. 初始化Flink环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointUtils.setCheckpoint(env, "ScInsuranceSellToAuisSegDwd");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 2. 创建ODS源表
        tEnv.executeSql(TrpCreateToSql.INSURANCE_SALE_DETAIL);

        // 3. 从ODS表中读取数据
        String query = buildQuerySql(startDate, endDate);
        logger.info("执行SQL查询: {}", query);
        
        Table table = tEnv.sqlQuery(query);

        // 4. 数据转换
        DataStream<AuisSegFactModel> auisSegFactStream = tEnv.toDataStream(table)
                .map(new MapFunction<Row, AuisSegFactModel>() {
                    @Override
                    public AuisSegFactModel map(Row row) throws Exception {
                        try {
                            AuisSegFactModel model = new AuisSegFactModel();

                            // 主键：
                            String ticketNumber = row.getFieldAs("TICKET_NUMBER");
                            if (StringUtils.isNotBlank(ticketNumber)) {
                                ticketNumber = NormalizationUtils.standardize(FieldType.TICKET_NO, ticketNumber, DataSource.TRP_FTP);
                            }
                            String insNumber = row.getFieldAs("INSURANCE_POLICY_NUMBER");
                            model.setPkId(ticketNumber + insNumber);

                            // 订单号
                            model.setAkOrdernum(row.getFieldAs("ORDER_NUMBER"));

                            // 日期拆分为日期和时间
                            String bookingTime = row.getFieldAs("BOOKING_TIME");
                            if (StringUtils.isNotBlank(bookingTime)) {
                                try {
                                    // 假设订单日期格式为 "YYYY-MM-DD HH:mm:ss"
                                    LocalDateTime dateTime = LocalDateTime.parse(bookingTime,
                                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                                    model.setFkBkauisDate(dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                                    model.setFkBkauisTime(dateTime.format(DateTimeFormatter.ofPattern("HH:mm:ss")));
                                } catch (Exception e) {
                                    logger.warn("订单日期格式解析失败: {}", bookingTime, e);
                                }
                            }

                            // 姓名 证件
                            String psgName = row.getFieldAs("PASSENGER_NAME");
                            if (StringUtils.isNotBlank(psgName)) {
                                psgName = NormalizationUtils.standardize(FieldType.CN_NAME, psgName, DataSource.TRP_FTP);
                            }
                            model.setCnName(psgName);
                            String idType = row.getFieldAs("ID_TYPE");
                            if (StringUtils.isNotBlank(idType)) {
                                idType = NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, idType, DataSource.TRP_FTP);
                            }
                            model.setAkCertType(idType);
                            String idNumber = row.getFieldAs("ID_NUMBER");
                            if (StringUtils.isNotBlank(idNumber)) {
                                idNumber = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, idNumber, DataSource.TRP_FTP);
                            }
                            model.setCertNumber(idNumber);

                            // TID
                            if (StringUtils.isNotBlank(idType) && StringUtils.isNotBlank(idNumber)) {
                                Tid tid = new Tid();
                                tid.setTid(idNumber);
                                Map<String, String> certification = new HashMap<>();
                                certification.put(idType, idNumber);
                                tid.setCertification(certification);
                                String tidStr = IdMapping.idMappingFunction(tid, "TRP");
                                model.setFkPassengerUserTid(tidStr);
                            }
                            // 源id
                            String accountUsername = row.getFieldAs("ACCOUNT_USERNAME");
                            model.setFkBookingUserOriginId(accountUsername);

                            // 预定人TID
                            if (StringUtils.isNotBlank(accountUsername)) {
                                Tid tid = new Tid();
                                tid.setTid(accountUsername);
                                tid.setCrmCustomerId(accountUsername);
                                String tidStr = IdMapping.idMappingFunction(tid, "TRP");
                                model.setFkBookingUserTid(tidStr);
                            }

                            // 起飞日期拆分为日期和时间
                            String departureDate = row.getFieldAs("DEPARTURE_DATE");
                            if (StringUtils.isNotBlank(departureDate)) {
                                try {
                                    if (departureDate.contains(".")) {
                                        departureDate = departureDate.substring(0, departureDate.indexOf('.'));
                                    }
                                    // 假设日期格式为 "YYYY-MM-DD HH:mm:ss"
                                    LocalDateTime dateTime = LocalDateTime.parse(departureDate,
                                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                                    model.setFkSegDate(dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                                    model.setFkSegTime(dateTime.format(DateTimeFormatter.ofPattern("HH:mm:ss")));
                                } catch (Exception e) {
                                    logger.warn("订单日期格式解析失败: {}", departureDate, e);
                                }
                            }

                            // 机场
                            model.setFkDepairport(row.getFieldAs("DEPARTURE_CITY"));
                            model.setFkArriairport(row.getFieldAs("ARRIVAL_CITY"));

                            // 保险号
                            model.setInsurancePolicyNo(insNumber);
                            // 类型
                            String insuranceType = row.getFieldAs("INSURANCE_TYPE");
                            if (StringUtils.isNotBlank(insuranceType)) {
                                insuranceType = NormalizationUtils.standardize(FieldType.INSURANCE_TYPE, insuranceType, DataSource.TRP_FTP);
                            }
                            model.setAkInsuranceType(insuranceType);

                            // 渠道 - 需要标准化
                            String channel = row.getFieldAs("CHANNEL_SOURCE");
                            if (StringUtils.isNotBlank(channel)) {
                                channel = NormalizationUtils.standardize(FieldType.INSURANCE_ORDER_CHANNEL, channel, DataSource.TRP_FTP);
                            }
                            model.setAkChannel(channel);

                            // 状态 - 需要标准化
                            String insuranceStatus = row.getFieldAs("INSURANCE_STATUS");
                            if (StringUtils.isNotBlank(insuranceStatus)) {
                                insuranceStatus = NormalizationUtils.standardize(FieldType.INSURANCE_STATUS, insuranceStatus, DataSource.TRP_FTP);
                            }
                            model.setAkInsurstatus(insuranceStatus);

                            // 费用
                            Object insuranceFee = row.getField("INSURANCE_FEE");
                            model.setInsuranceAmt(null == insuranceFee ?  null : Double.parseDouble(insuranceFee.toString()));
                            model.setInsuranceCount(1l);


                            // 源系统最后更新时间（购保成功时间）
                            model.setUpdateTime(row.getFieldAs("INSURANCE_PURCHASE_SUCCESS_TIME"));

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
        DorisSink<AuisSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_AUIS_SEG_FACT");
        auisSegFactStream.sinkTo(dorisSink);
        // 6. 启动任务
        logger.info("开始执行Flink任务...");
        env.execute("ScCouponRefundDetailToDwd");
        logger.info("任务执行完成");
    }

    /**
     * 构建查询SQL
     * 过滤条件：
     * 1. ‘保险单号‘非空的
     * 2. ETL_DATE在指定日期范围内
     */
    private static String buildQuerySql(String startDate, String endDate) {
        return "SELECT a.TICKET_NUMBER ,\n" +
                "a.INSURANCE_POLICY_NUMBER ,\n" +
                "a.ORDER_NUMBER ,\n" +
                "a.BOOKING_TIME ,\n" +
                "a.PASSENGER_NAME ,\n" +
                "a.ID_TYPE ,\n" +
                "a.ID_NUMBER ,\n" +
                "a.ACCOUNT_USERNAME ,\n" +
                "a.DEPARTURE_DATE ,\n" +
                "a.DEPARTURE_CITY ,\n" +
                "a.ARRIVAL_CITY ,\n" +
                "a.INSURANCE_TYPE ,\n" +
                "a.CHANNEL_SOURCE ,\n" +
                "a.INSURANCE_STATUS ,\n" +
                "a.INSURANCE_FEE ,\n" +
                "a.INSURANCE_PURCHASE_SUCCESS_TIME \n" +
                "FROM T_ODS_TRP_INSURANCE_SALE_DETAIL a " +
                " WHERE a.INSURANCE_POLICY_NUMBER is not null " +
                " AND ETL_DATE >= '" + startDate + "'\n" +
                "   AND ETL_DATE <= '" + endDate + "'";
    }
}
