package com.travelsky.trp.usercenter.data.analysis.transform.zsf;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.AuisSegFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;


/**
 * 掌尚飞写到保险-航段级事实表
 */
// 可以跑
public class OdrOrderDetailInsuranceFactTrans {

    static final Logger logger = LoggerFactory.getLogger(OdrOrderDetailInsuranceFactTrans.class);

    // 将 Object 转为 String，支持 null
    private static String toStringSafe(Object obj) {
        return obj != null ? obj.toString() : null;
    }

    // 将 dateTime 转为 LocalDateTime 字符串
    private static String formatDateTimeSafe(Object ts) {
        if (ts != null) {
            LocalDateTime s = (LocalDateTime) ts;
            return s.toString().replace("T", " ");
        }
        return null;
    }

    public static LocalDate parseFlexibleDate(String s) {
        if (s == null || s.isEmpty()) return null;

        // 尝试 yyyyMMdd
        if (s.length() == 8 && s.chars().allMatch(Character::isDigit)) {
            try {
                return LocalDate.parse(s, DateTimeFormatter.ofPattern("yyyyMMdd"));
            } catch (DateTimeParseException ignored) {}
        }

        // 尝试 yyyy-MM-dd
        try {
            return LocalDate.parse(s, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        } catch (DateTimeParseException ignored) {}

        // 都不行就报错
        throw new IllegalArgumentException("Unrecognized date format: " + s);
    }

    public static void result(StreamTableEnvironment tEnv, String etlDate) throws Exception {

        // 注册表
        tEnv.executeSql(CreateTableSql.ZSF_ODR_ORDER_DETAIL_INSURANCE);
        tEnv.executeSql(CreateTableSql.ZSF_ODR_ORDER_CUSTOMER_RELATION);

        String query = "SELECT \n" +
                "    a.ID AS ID,\n" +
                "    a.ORDER_DETAIL_ID AS ORDER_DETAIL_ID,\n" +
                "    a.ORDER_NO AS ORDER_NO,\n" +
                "    a.MAIN_ORDER_NO AS MAIN_ORDER_NO,\n" +
                "    a.MAIN_ORDER_ID AS MAIN_ORDER_ID,\n" +
                "    a.SERIAL_NUMBER AS SERIAL_NUMBER,\n" +
                "    a.OPERATE_SERIAL_NUMBER AS OPERATE_SERIAL_NUMBER,\n" +
                "    a.INSTANCE_ID AS INSTANCE_ID,\n" +
                "    a.INSTANCE_NO AS INSTANCE_NO,\n" +
                "    a.INS_TRADE_NO AS INS_TRADE_NO,\n" +
                "    a.INS_PRICE AS INS_PRICE,\n" +
                "    a.INS_AMOUNT AS INS_AMOUNT,\n" +
                "    a.INS_TYPE AS INS_TYPE,\n" +
                "    a.INS_STATUS AS INS_STATUS,\n" +
                "    a.INS_COMPANY_CODE AS INS_COMPANY_CODE,\n" +
                "    a.INS_COMPANY AS INS_COMPANY,\n" +
                "    a.INS_CREATE_TIME AS INS_CREATE_TIME,\n" +
                "    a.INS_COMPLETE_TIME AS INS_COMPLETE_TIME,\n" +
                "    a.INS_RETURN_TIME AS INS_RETURN_TIME,\n" +
                "    a.HOLDER_NAME AS HOLDER_NAME,\n" +
                "    a.HOLDER_TYPE AS HOLDER_TYPE,\n" +
                "    a.HOLDER_ID AS HOLDER_ID,\n" +
                "    a.HOLDER_PHONE AS HOLDER_PHONE,\n" +
                "    a.HOLDER_EMAIL AS HOLDER_EMAIL,\n" +
                "    a.HOLDER_ADDRESS AS HOLDER_ADDRESS,\n" +
                "    a.BENEFICIARY_TYPE AS BENEFICIARY_TYPE,\n" +
                "    a.BENEFICIARY_NAME AS BENEFICIARY_NAME,\n" +
                "    a.BENEFICIARY_ID AS BENEFICIARY_ID,\n" +
                "    a.BENEFICIARY_ID_TYPE AS BENEFICIARY_ID_TYPE,\n" +
                "    a.PSGR_NAME AS PSGR_NAME,\n" +
                "    a.CERT_TYPE AS CERT_TYPE,\n" +
                "    a.CERT_NO AS CERT_NO,\n" +
                "    a.PSGR_PHONE AS PSGR_PHONE,\n" +
                "    a.PSGR_MAIL AS PSGR_MAIL,\n" +
                "    a.FLIGHT_DATE AS FLIGHT_DATE,\n" +
                "    a.FLIGHT_TIME AS FLIGHT_TIME,\n" +
                "    a.ORG_CITY AS ORG_CITY,\n" +
                "    a.DST_CITY AS DST_CITY,\n" +
                "    a.FLIGHT_NO AS FLIGHT_NO,\n" +
                "    a.CARRIER AS CARRIER,\n" +
                "    a.TICKET_NO AS TICKET_NO,\n" +
                "    a.TICKET_PRICE AS TICKET_PRICE,\n" +
                "    a.INTERNATIONAL_FLAG AS INTERNATIONAL_FLAG,\n" +
                "    a.ENABLE AS ENABLE,\n" +
                "    a.CREATE_TIME AS CREATE_TIME,\n" +
                "    a.CREATE_USER AS CREATE_USER,\n" +
                "    a.UPDATE_TIME AS UPDATE_TIME,\n" +
                "    a.UPDATE_USER AS UPDATE_USER,\n" +
                "    a.RELATION_INSURANT AS RELATION_INSURANT,\n" +
                "    a.ASSIGN_BENEFICIARY AS ASSIGN_BENEFICIARY,\n" +
                "    a.INS_ORDER_NO AS INS_ORDER_NO,\n" +
                "    a.BIRTH_DATE AS BIRTH_DATE,\n" +
                "    a.CUSTOMER_BIRTHDAY AS CUSTOMER_BIRTHDAY,\n" +
                "    b.CUSTOMER_ID AS CUSTOMER_ID\n" +
                "FROM T_ODS_ZSF_ODR_ORDER_DETAIL_INSURANCE a\n" +
                "LEFT JOIN T_ODS_ZSF_ODR_ORDER_CUSTOMER_RELATION b\n" +
                "ON a.MAIN_ORDER_ID = b.MAIN_ORDER_ID\n" +
                "WHERE a.INS_STATUS ='3' OR  a.INS_STATUS ='6' OR a.INS_STATUS ='7' " +
//                " AND a.ETL_DATE = '" + etlDate + "'";
                " AND  a.CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                + " OR  a.UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' ";
        // v2 新增查询a.INS_STATUS = 3，6，7
        Table dorisTable = tEnv.sqlQuery(query);

        DataStream<Row> rowDataStream = tEnv.toChangelogStream(dorisTable);

        // 后续可以进行 map、filter、sink 操作
        SingleOutputStreamOperator<AuisSegFactModel> mappedStream = rowDataStream.map(row -> {

            AuisSegFactModel model = new AuisSegFactModel();

            // 提取字段值（使用字段名获取）
            String flightDate = toStringSafe(row.getField("FLIGHT_DATE")); // 起飞日期
            String ticketNo = NormalizationUtils.standardize(FieldType.TICKET_NO,toStringSafe(row.getField("TICKET_NO")),DataSource.ZHANGSHANGFEI); // 票号
            String depAirport = toStringSafe(row.getField("ORG_CITY")); // 起飞机场代码
            String arrAirport = toStringSafe(row.getField("DST_CITY")); // 到达机场代码
            Object insTradeNo = row.getField("INS_TRADE_NO");
            // 去除日期中的 "-"
            String cleanFlightDate = flightDate != null ? flightDate.replace("-", "") : "";

            // V2 主键用：ticket_no+id。说明：因为这个保险表的保单号，从24年才开始记录，为了兼容之前数据，用表主键来顶替保单号做主键。
            ticketNo = ticketNo != null ? ticketNo : "";
            insTradeNo = insTradeNo == null ? toStringSafe(row.getField("ID")) : insTradeNo.toString();
            String pkId = ticketNo +insTradeNo ;

            model.setPkId(pkId); // 设置主键

            model.setAkOrdernum(toStringSafe(row.getField("ORDER_NO")));
            model.setAkTiknum(ticketNo);

            // CREATE_TIME 处理（Timestamp -> LocalDateTime）
            Object createTime = row.getField("CREATE_TIME");
            if (createTime != null) {
                LocalDateTime localDateTime = (LocalDateTime) createTime;
                model.setFkBkauisDate(localDateTime.toLocalDate().toString());
                model.setFkBkauisTime(localDateTime.toLocalTime().toString());
            } else {
                model.setFkBkauisDate(null);
                model.setFkBkauisTime(null);
            }

            model.setCnName(NormalizationUtils.standardize(FieldType.CN_NAME,toStringSafe(row.getField("PSGR_NAME")),DataSource.ZHANGSHANGFEI));
            model.setAkCertType(toStringSafe(row.getField("CERT_TYPE")));
            model.setCertNumber(NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,toStringSafe(row.getField("CERT_NO")),DataSource.ZHANGSHANGFEI));

            // T乘机人TID
            String certNo = toStringSafe(row.getField("CERT_NO"));
            String certType = toStringSafe(row.getField("CERT_TYPE"));
            certType = NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, certType,DataSource.ZHANGSHANGFEI);
            certNo = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, certNo,DataSource.ZHANGSHANGFEI);

            Tid tid1 = new Tid();
            tid1.setTid(certNo);
            HashMap<String, String> map = new HashMap<>();
            map.put(certType, certNo);
            tid1.setCertification(map);
            String passengerUserTid = IdMapping.idMappingFunction(tid1, "ZSF");
            model.setFkPassengerUserTid(passengerUserTid);


            // 预订人员TID
            String customerId = toStringSafe(row.getField("CUSTOMER_ID"));
            Tid tid2 = new Tid();
            tid2.setTid(customerId);
            tid2.setCrmCustomerId(customerId);
            String bookingUserTid = IdMapping.idMappingFunction(tid2, "ZSF");
            model.setFkBookingUserTid(bookingUserTid);

            // 预订人员源ID
            model.setFkBookingUserOriginId(customerId);

            // 航班日期
            Object fDate = row.getField("FLIGHT_DATE");
            if (fDate != null) {
                LocalDate flight_data = parseFlexibleDate(fDate.toString());
                model.setFkSegDate(String.valueOf(flight_data));
            } else {
                model.setFkSegDate(null);
            }

            // 航班时间
            model.setFkSegTime(toStringSafe(row.getField("FLIGHT_TIME")));

            // 出发/到达机场
            model.setFkDepairport(depAirport);
            model.setFkArriairport(arrAirport);

            // 保险主键：保险类型 +  保险公司
            String insType = toStringSafe(row.getField("INS_TYPE"));
            String insCompany = toStringSafe(row.getField("INS_COMPANY"));

            model.setAkInsuranceType(NormalizationUtils.standardize(FieldType.INSURANCE_TYPE, insType,DataSource.ZHANGSHANGFEI));
            model.setInsuranceCompany(insCompany);
            model.setInsurancePolicyNo(insTradeNo == null ? null : insTradeNo.toString());


            // 订单渠道
            model.setAkChannel("SCZSF");

            // 保险状态
            model.setAkInsurstatus(NormalizationUtils.standardize(FieldType.INSURANCE_STATUS,toStringSafe(row.getField("INS_STATUS")),DataSource.ZHANGSHANGFEI));

            // 保险金额
            Object insPriceObj = row.getField("INS_PRICE");
            if (insPriceObj instanceof Number) {
                model.setInsuranceAmt(((Number) insPriceObj).doubleValue());
            } else {
                model.setInsuranceAmt(0.0);
            }

            // 保险数量
//            model.setInsuranceCount(1L);

            // 更新时间
            Object updateTimeObj = row.getField("UPDATE_TIME");
            if (updateTimeObj != null) {
                LocalDateTime updateTime = (LocalDateTime) updateTimeObj;
                model.setUpdateTime(updateTime.toString().
                        replace(" ", "T").replace("-", "").replace(":", ""));
            } else {
                model.setUpdateTime(null);
            }

            // 系统时间
            LocalDateTime now = LocalDateTime.now();
            model.setSystemCreatetime(now.toString());
            model.setSystemLastUpdatetime(now.toString());

            return model;
        });

        // 2. 输出到Doris，调试时先打印
        mappedStream.print("PrdProductCouponDouyinFactTrans");
        logger.info("连接 Doris 参数: DB={}, Table={}, FE={}, BE={}, User={}",
                Constants.DWD_DB, "T_DWD_AUIS_SEG_FACT", Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT, Constants.DWD_USER);
        DorisSink<AuisSegFactModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_AUIS_SEG_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        // 数据写入 Doris
        mappedStream.sinkTo(dorisSink);

    }

}
