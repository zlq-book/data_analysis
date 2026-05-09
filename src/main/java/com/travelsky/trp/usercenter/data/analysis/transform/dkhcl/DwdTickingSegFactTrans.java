package com.travelsky.trp.usercenter.data.analysis.transform.dkhcl;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.TickingSegFactModel;
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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

// 机票出票航段级事实表
public class DwdTickingSegFactTrans {

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
        tEnv.executeSql(CreateTableSql.T_ODS_DKHCL_AIR_ORDER_PASSENGER_SEGMENT);

        //    在AIR_ORDER_CHILD_ORDER 机票订单主表里面取CREATOR_ID，关联查询CRM_EMPLOYEE_CERT员工证件信息的EMPLOYEE_ID，
        //    取CRM_EMPLOYEE_CERT表里面CERT_NO，可能有多个CERT_NO，优先取身份证类型的，如果没有非身份证类型的，随机取一个，
        //    作为进行TID查询转化后写入此字段。
        String query = "SELECT " +
                "  o.ID AS ID, " +
                "  o.PNR AS PNR, " +
                "  o.CREATE_TIME AS CREATE_TIME, " +
                "  o.CREATOR_ID AS CREATOR_ID, " +
                "  o.ORDER_NO AS ORDER_NO, " +
                "  o.ORDER_SOURCE AS ORDER_SOURCE, " +
                "  o.CUS_BIG_CODE AS CUS_BIG_CODE, " +
                "  o.TICKET_OUT_DATE AS TICKET_OUT_DATE, " +
                "  rc.CERT_NO AS CERT_NO, " +
                "  rc.CERT_TYPE AS CERT_TYPE, " +
                // 乘客航段相关字段
                "  ps.TICKET_NUM AS TICKET_NUM " +
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
                ") rc ON o.CREATOR_ID = rc.EMPLOYEE_ID AND rc.rn = 1 " +
                // 关联乘客航段表
                "LEFT JOIN T_ODS_DKHCL_AIR_ORDER_PASSENGER_SEGMENT ps ON o.ID = ps.AIR_ORDER " +
                // 过滤条件
                "WHERE o.CUS_BIG_CODE = '99998181' " +
                "  AND o.PNR IS NOT NULL " +
                "  AND ps.TICKET_NUM IS NOT NULL" +
                "  AND o.ETL_DATE = '"+etlDate+"'";

        Table dorisTable = tEnv.sqlQuery(query);

        DataStream<Row> rowDataStream = tEnv.toChangelogStream(dorisTable);

//        大客户差旅系统的数据T+1入库后，AIR_ORDER_CHILD_ORDER 机票订单主表CREATE_TIME是T时间的，
//        同时CUS_BIG_CODE=99998181的，PNR非空的。取PNR+CREATE_TIME（截取年月日），
//        查找到这一条记录，更新下面几个渠道字段。因CREATE_TIME与实际PNR生成日期可能有偏差，
//        为避免跨天问题，每天23：00：00-00：00：00的订单需要T，T+1（订单先生成，PNR后生成的情况），
//        00：00：00-00：01：00的订单需要T-1，T（PNR生成，订单后生成的情况），需要先用2条查询，如果找到，
//        就更新那一条（另一条丢弃），如果找不到，要把两个主键都写进入。如果不在这个时间范围的数据，
//        直接按照T生成主键并写入，无需查询。PS：如果渠道数据先写了2条，后续高频来了，会更新其中一条。

// flatMap 处理：一条数据可能产生1条或2条记录（跨天情况）
        SingleOutputStreamOperator<TickingSegFactModel> mappedStream1 = rowDataStream
                .flatMap(new FlatMapFunction<Row, TickingSegFactModel>() {
                    @Override
                    public void flatMap(Row row, Collector<TickingSegFactModel> out) throws Exception {
                        // 获取出票日期
                        LocalDateTime ticketOutDate = null;
                        if (row.getField("TICKET_OUT_DATE") != null) {
                            ticketOutDate = (LocalDateTime) row.getField("TICKET_OUT_DATE");
                        }
                        // 获取票号并去掉横杠
                        String ticketNum = toStringSafe(row.getField("TICKET_NUM"));
                        ticketNum = NormalizationUtils.standardize(FieldType.TICKET_NO, ticketNum,  DataSource.KEY_ACCOUNT_TRAVEL);


                        // 判断是否需要生成两个主键（23点的数据）
                        boolean needTwoKeys = false;
                        if (ticketOutDate != null) {
                            int hour = ticketOutDate.getHour();
                            if (hour == 23) {
                                needTwoKeys = true;
                            }
                        }

                        // 生成主键列表
                        List<String> pkIds = new ArrayList<>();
                        if (ticketOutDate != null) {
                            // T 日期
                            String dateT = ticketOutDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                            pkIds.add(dateT + ticketNum);
                            // 如果是23点，添加 T+1 日期
                            if (needTwoKeys) {
                                String dateTPlus1 = ticketOutDate.plusDays(1)
                                        .format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                                pkIds.add(dateTPlus1 + ticketNum);
                            }
                        } else {
                            // 如果出票日期为空，使用票号作为主键
                            pkIds.add(ticketNum);
                        }
                        // 为每个主键生成一条记录
                        for (String pkId : pkIds) {
                            TickingSegFactModel model = new TickingSegFactModel();
                            // 设置主键
                            model.setPkId(pkId);
                            // 渠道订单号
                            String orderNo = toStringSafe(row.getField("ORDER_NO"));
                            model.setChannelOrderno(orderNo);

                            // 预订渠道
                            String orderSource = "SCSME";
                            String standardizedChannel = NormalizationUtils.standardize(
                                    FieldType.ORDER_CHANNEL_AIRTICKET,
                                    orderSource,
                                    DataSource.KEY_ACCOUNT_TRAVEL);
                            model.setAkChannel(standardizedChannel);

                            // TID 字段：由证件号转换而来
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

                            // 预定人ID
                            Tid tid1 = new Tid();
                            tid1.setTid(certNo);
                            HashMap<String, String> map = new HashMap<>();
                            map.put(certType, certNo);
                            tid1.setCertification(map);
                            String bookingUserTid = IdMapping.idMappingFunction(tid1, "DKHCL");
                            model.setFkBookingUserTid(bookingUserTid);

                            // 预定人源ID
                            String creatorId = toStringSafe(row.getField("CREATOR_ID"));
                            model.setFkBookingUserOriginId(creatorId);
                            // 源系统最后更新时间
                            model.setSourceLastUpdatetime(row.getFieldAs("CREATE_TIME").toString());
                            // 输出记录
                            out.collect(model);
                        }
                    }
                });
        //DorisSink<TickingSegFactModel> dorisSink = FlinkDorisUtils.creatDorisSink(
        //        Constants.DWD_DB,
        //        "T_DWD_TICKING_SEG_FACT",
        //        Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
        //        Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
        //        Constants.DWD_USER,
        //        Constants.DWD_PWD);
        //mappedStream1.sinkTo(dorisSink);
        // 写入Doris
        DorisSink<TickingSegFactModel> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_TICKING_SEG_FACT");
        mappedStream1.sinkTo(dorisSink);

    }


}
