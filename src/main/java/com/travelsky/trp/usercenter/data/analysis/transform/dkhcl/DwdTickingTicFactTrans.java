package com.travelsky.trp.usercenter.data.analysis.transform.dkhcl;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingTicFactModel;
import com.travelsky.trp.usercenter.data.analysis.transform.zsf.OdrOrderDetailSeatFactTrans;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

// 机票出票客票级事实表
public class DwdTickingTicFactTrans {

    private static final Logger logger = LoggerFactory.getLogger(OdrOrderDetailSeatFactTrans.class);

    /**
     * 空值安全转换为字符串
     */
    private static String toStringSafe(Object obj) {
        return obj != null ? obj.toString() : null;
    }

    /**
     * 时间格式化处理
     */
    private static String formatDateTimeSafe(Object ts) {
        if (ts != null) {
            LocalDateTime s = (LocalDateTime) ts;
            return s.toString().replace("T", " ");
        }
        return null;
    }

    public static void result(StreamTableEnvironment tEnv, String etlDate) throws Exception {
        // 注册表
        tEnv.executeSql(CreateTableSql.T_ODS_DKHCL_AIR_ORDER_CHILD_ORDER);
        tEnv.executeSql(CreateTableSql.T_ODS_DKHCL_AIR_ORDER_PASSENGER_SEGMENT);
        tEnv.executeSql(CreateTableSql.T_ODS_DKHCL_CRM_EMPLOYEE_CERT);

//        大客户差旅系统的数据T+1入库后，AIR_ORDER_CHILD_ORDER 机票订单主表的CREATE_TIME是T时间的，
//        同时CUS_BIG_CODE=99998181的，PNR非空的。连表：AIR_ORDER_CHILD_ORDER的ID=AIR_ORDER_PASSENGER_SEGMENT的AIR_ORDER，
//        同时TICKET_NUM非空的，两张表连表后，取：AIR_ORDER_CHILD_ORDER 机票订单主表的TICKET_OUT_DATE（截取日期）
//        +AIR_ORDER_PASSENGER_SEGMENT的TICKET_NUM（删除横杠），查找到这一条记录，更新下面几个渠道字段。
//        因CREATE_TIME与实际出票日期可能有偏差，为避免跨天问题，需要生成T，T+1，两个主键，都查一下，有相同的就插入那一条数据中。
//        （每天23点这一个小时的订单需要T，T+1，如果找到，就更新那一条，如果找不到，要把两个主键都写进入。PS：后续高频来了，会更新其中一条）
//
//        AIR_ORDER_CHILD_ORDER  AIR_ORDER_PASSENGER_SEGMENT

        String query =
                "SELECT " +
                "  o.ID AS ID, " +
                "  o.PNR AS PNR, " +
                "  o.CREATE_TIME AS CREATE_TIME, " +
                "  o.CREATOR_ID AS CREATOR_ID, " +
                "  o.ORDER_NO AS ORDER_NO, " +
                "  rc.CERT_NO AS CERT_NO, " +
                "  rc.CERT_TYPE AS CERT_TYPE " +
                "FROM T_ODS_DKHCL_AIR_ORDER_CHILD_ORDER o " +
                "INNER JOIN T_ODS_DKHCL_AIR_ORDER_PASSENGER_SEGMENT s " +
                "  ON o.ID = s.AIR_ORDER " +
                "LEFT JOIN ( " +
                "  SELECT " +
                "    EMPLOYEE_ID, " +
                "    CERT_NO, " +
                "    CERT_TYPE, " +
                "    ROW_NUMBER() OVER ( " +
                "      PARTITION BY EMPLOYEE_ID " +
                "      ORDER BY " +
                "        CASE WHEN CERT_TYPE = 'NI' THEN 0 ELSE 1 END, " +
                "        RAND() " +
                "    ) AS rn " +
                "  FROM T_ODS_DKHCL_CRM_EMPLOYEE_CERT " +
                "  WHERE CERT_NO IS NOT NULL AND TRIM(CERT_NO) <> '' " +
                ") rc ON o.CREATOR_ID = rc.EMPLOYEE_ID AND rc.rn = 1 " +
                " WHERE " +
                "  o.CUS_BIG_CODE = '99998181' " +
                "  AND o.PNR IS NOT NULL " +
                "  AND s.TICKET_NUM IS NOT NULL " +
                "  AND TRIM(s.TICKET_NUM) <> ''" +
                "  AND o.ETL_DATE = '"+etlDate+"'";

        Table dorisTable = tEnv.sqlQuery(query);

        DataStream<Row> rowDataStream = tEnv.toChangelogStream(dorisTable);

        SingleOutputStreamOperator<TickingTicFactModel> mappedStream1 = rowDataStream
                .flatMap(new FlatMapFunction<Row, TickingTicFactModel>() {
                    @Override
                    public void flatMap(Row row, Collector<TickingTicFactModel> out) throws Exception {

                        LocalDateTime createTime = (LocalDateTime) row.getField("CREATE_TIME");
                        String pnr = toStringSafe(row.getField("PNR"));
                        if (pnr == null || pnr.trim().isEmpty()) {
                            return;
                        }

                        LocalTime timePart = createTime.toLocalTime();
                        LocalDate datePart = createTime.toLocalDate();

                        List<LocalDate> candidateDates = new ArrayList<>();

                        if (timePart.compareTo(LocalTime.of(23, 0, 0)) >= 0 && timePart.compareTo(LocalTime.of(23, 59, 59)) <= 0) {
                            candidateDates.add(datePart);
                            candidateDates.add(datePart.plusDays(1));
                        } else if (timePart.compareTo(LocalTime.of(0, 0, 0)) >= 0 && timePart.compareTo(LocalTime.of(0, 1, 0)) <= 0) {
                            candidateDates.add(datePart.minusDays(1));
                            candidateDates.add(datePart);
                        } else {
                            candidateDates.add(datePart);
                        }

                        // 公共字段提取
                        String orderNo = toStringSafe(row.getField("ORDER_NO"));
                        String orderSource = "SCSME";
                        String standardizedChannel = NormalizationUtils.standardize(
                                FieldType.ORDER_CHANNEL_AIRTICKET,
                                orderSource,
                                DataSource.KEY_ACCOUNT_TRAVEL);

                        String certType = toStringSafe(row.getField("CERT_TYPE"));
                        certType = NormalizationUtils.standardize(
                                FieldType.DOCUMENT_TYPE,
                                certType,
                                DataSource.KEY_ACCOUNT_TRAVEL);
                        String certNo = toStringSafe(row.getField("CERT_NO"));
                        certNo = NormalizationUtils.standardize(
                                FieldType.DOCUMENT_NUM,
                                certNo,
                                DataSource.KEY_ACCOUNT_TRAVEL);
                        String creatorId = toStringSafe(row.getField("CREATOR_ID"));

                        Tid tid1 = new Tid();
                        tid1.setTid(certNo);
                        HashMap<String, String> map = new HashMap<>();
                        map.put(certType, certNo);
                        tid1.setCertification(map);
                        String bookingUserTid = IdMapping.idMappingFunction(tid1, "DKHCL");

                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
                        for (LocalDate candDate : candidateDates) {
                            TickingTicFactModel model = new TickingTicFactModel();
                            String dateStr = candDate.format(formatter);
                            model.setPkId(pnr + dateStr);
                            // 渠道订单号
                            model.setAkOrdernum(orderNo);
                            // 预订渠道
                            model.setAkChannel(standardizedChannel);
                            // 预定人Id
                            model.setFkBookingUserTid(bookingUserTid);
                            // 预定人源Id
                            model.setFkBookingUserOriginId(creatorId);
                            // 源系统最后更新时间
                            model.setSourceLastUpdatetime(row.getFieldAs("CREATE_TIME").toString());
                            out.collect(model);
                        }
                    }
                });

        // 6. 输出到 Doris
        // 写到  机票预订PNR级事实表  T_DWD_BOOKING_PNR_FACT
        logger.info("写入 Doris 表: DB={}, Table={}", Constants.DWD_DB, "T_DWD_TICKING_TIC_FACT");

        DorisSink<TickingTicFactModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_TICKING_TIC_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        mappedStream1.sinkTo(dorisSink);

        // 打印到控制台（本地测试用）
//        mappedStream1.print("BOOKING_PNR_MODEL");

    }


}
