package com.travelsky.trp.usercenter.data.analysis.transform.dkhcl;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.BookingPnrFactModel;
import com.travelsky.trp.usercenter.data.analysis.transform.zsf.OdrOrderDetailSeatFactTrans;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
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

// 机票预订PNR级事实表
public class DwdBookingPnrFactTrans {

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
        tEnv.executeSql(CreateTableSql.T_ODS_DKHCL_CRM_EMPLOYEE_CERT);

        //    在AIR_ORDER_CHILD_ORDER 机票订单主表里面取CREATOR_ID，关联查询CRM_EMPLOYEE_CERT员工证件信息的EMPLOYEE_ID，
        //    取CRM_EMPLOYEE_CERT表里面CERT_NO，可能有多个CERT_NO，优先取身份证类型的，如果没有非身份证类型的，随机取一个，
        //    作为进行TID查询转化后写入此字段。
        // 查询T_ODS_DKHCL_AIR_ORDER_CHILD_ORDER表数据
        String query = "SELECT " +
                "  o.ID AS ID, " +
                "  o.PNR AS PNR, " +
                "  o.CREATE_TIME AS CREATE_TIME, " +
                "  o.CREATOR_ID AS CREATOR_ID, " +
                "  o.ORDER_NO AS ORDER_NO, " +
                "  o.ORDER_SOURCE AS ORDER_SOURCE, " +
                "  o.CREATE_TIME AS CREATE_TIME, " +
                "  rc.CERT_NO AS CERT_NO, " +
                "  rc.CERT_TYPE AS CERT_TYPE " +
                "FROM T_ODS_DKHCL_AIR_ORDER_CHILD_ORDER o " +
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
                ") rc ON o.CREATOR_ID = rc.EMPLOYEE_ID AND rc.rn = 1" +
                "WHERE o.CUS_BIG_CODE = '99998181' AND o.PNR IS NOT NULL "+
                "AND o.ETL_DATE = '"+etlDate+"'";

        Table dorisTable = tEnv.sqlQuery(query);

        //TableResult result = tEnv.executeSql(query);
        // 打印每一行
        // 获取结果的行迭代器
        //try (CloseableIterator<Row> iterator = result.collect()) {
        //    while (iterator.hasNext()) {
        //        Row row = iterator.next();
        //        System.out.println("---"+row);
        //    }
        //}
        DataStream<Row> rowDataStream = tEnv.toChangelogStream(dorisTable)
                .filter(row -> row.getKind().equals(RowKind.INSERT)||row.getKind().equals(RowKind.UPDATE_AFTER));

//        大客户差旅系统的数据T+1入库后，AIR_ORDER_CHILD_ORDER 机票订单主表CREATE_TIME是T时间的，
//        同时CUS_BIG_CODE=99998181的，PNR非空的。取PNR+CREATE_TIME（截取年月日），
//        查找到这一条记录，更新下面几个渠道字段。因CREATE_TIME与实际PNR生成日期可能有偏差，
//        为避免跨天问题，每天23：00：00-00：00：00的订单需要T，T+1（订单先生成，PNR后生成的情况），
//        00：00：00-00：01：00的订单需要T-1，T（PNR生成，订单后生成的情况），需要先用2条查询，如果找到，
//        就更新那一条（另一条丢弃），如果找不到，要把两个主键都写进入。如果不在这个时间范围的数据，
//        直接按照T生成主键并写入，无需查询。PS：如果渠道数据先写了2条，后续高频来了，会更新其中一条。

        // 机票出票-航段级
        SingleOutputStreamOperator<BookingPnrFactModel> mappedStream1 = rowDataStream
                .filter(row -> row.getField("CERT_NO")!=null)
                .flatMap(new FlatMapFunction<Row, BookingPnrFactModel>() {
                    @Override
                    public void flatMap(Row row, Collector<BookingPnrFactModel> out) throws Exception {
                        System.out.println("row: " + row);
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

                        String bookingUserTid = "";
                        if(certNo!=null){
                            Tid tid1 = new Tid();
                            tid1.setTid(certNo);
                            HashMap<String, String> map = new HashMap<>();
                            map.put(certType, certNo);
                            tid1.setCertification(map);
                            bookingUserTid = IdMapping.idMappingFunction(tid1, "DKHCL");
                        }

                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
                        for (LocalDate candDate : candidateDates) {
                            BookingPnrFactModel model = new BookingPnrFactModel();
                            String dateStr = candDate.format(formatter);
                            model.setPkId(pnr + dateStr);
                            model.setChannelOrderId(orderNo);
                            model.setAkChannel(standardizedChannel);
                            model.setFkBookingUserTid(bookingUserTid);
                            model.setFkBookingUserOriginId(creatorId);
                            model.setSourceLastUpdatetime(row.getFieldAs("CREATE_TIME").toString());
                            out.collect(model);
                        }
                    }
                });

        // 6. 输出到 Doris
        // 写到  机票预订PNR级事实表  T_DWD_BOOKING_PNR_FACT
        logger.info("写入 Doris 表: DB={}, Table={}", Constants.DWD_DB, "T_DWD_BOOKING_PNR_FACT");

        DorisSink<BookingPnrFactModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_BOOKING_PNR_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        mappedStream1.sinkTo(dorisSink);

        // 打印到控制台（本地测试用）
        mappedStream1.print("BOOKING_PNR_MODEL");

    }


}
