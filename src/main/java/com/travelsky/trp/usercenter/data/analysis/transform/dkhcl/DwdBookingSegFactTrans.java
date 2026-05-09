package com.travelsky.trp.usercenter.data.analysis.transform.dkhcl;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.BookingPnrFactModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.BookingSegFactModel;
import com.travelsky.trp.usercenter.data.analysis.transform.zsf.OdrOrderDetailSeatFactTrans;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

// 机票预订航段级事实表
public class DwdBookingSegFactTrans {

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
        tEnv.executeSql(CreateTableSql.T_ODS_DKHCL_AIR_ORDER_PASSENGER);
        tEnv.executeSql(CreateTableSql.T_ODS_DKHCL_AIR_ORDER_PASSENGER_SEGMENT);
        tEnv.executeSql(CreateTableSql.T_ODS_DKHCL_AIR_ORDER_SEGMENT);

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
                "  rc.CERT_NO AS CERT_NO, " +
                "  rc.CERT_TYPE AS CERT_TYPE, " +
                // 航段相关字段
                "  seg.BOARD_POINT AS BOARD_POINT, " +
                "  seg.DEPARTURE_DATE AS DEPARTURE_DATE, " +
                // 乘客相关字段
                "  p.CERT_NO AS PASSENGER_CERT_NO, " +
                "  p.NAME AS PASSENGER_NAME, " +
                "  p.CERT_TYPE AS PASSENGER_CERT_TYPE " +
                "FROM T_ODS_DKHCL_AIR_ORDER_CHILD_ORDER o " +
                // 关联创建人证件信息
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
                // 关联乘客信息
                "LEFT JOIN T_ODS_DKHCL_AIR_ORDER_PASSENGER p ON o.ID = p.AIR_ORDER " +
                // 关联乘客航段信息
                "LEFT JOIN T_ODS_DKHCL_AIR_ORDER_PASSENGER_SEGMENT ps ON p.AIR_ORDER = ps.AIR_ORDER " +
                // 关联航段信息
                "LEFT JOIN T_ODS_DKHCL_AIR_ORDER_SEGMENT seg ON ps.SEGMENT = seg.ID " +
                "WHERE o.CUS_BIG_CODE = '99998181' AND o.PNR IS NOT NULL "+
                "AND o.ETL_DATE = '"+etlDate+"'";

        Table dorisTable = tEnv.sqlQuery(query);

        DataStream<Row> rowDataStream = tEnv.toChangelogStream(dorisTable)
                .filter(row -> row.getKind().equals(RowKind.INSERT)|| row.getKind().equals(RowKind.UPDATE_AFTER));;
//        大客户差旅系统的数据T+1入库后，AIR_ORDER_CHILD_ORDER 机票订单主表CREATE_TIME是T时间的，
//        同时CUS_BIG_CODE=99998181的，PNR非空的。取PNR+CREATE_TIME（截取年月日），
//        查找到这一条记录，更新下面几个渠道字段。因CREATE_TIME与实际PNR生成日期可能有偏差，
//        为避免跨天问题，每天23：00：00-00：00：00的订单需要T，T+1（订单先生成，PNR后生成的情况），
//        00：00：00-00：01：00的订单需要T-1，T（PNR生成，订单后生成的情况），需要先用2条查询，如果找到，
//        就更新那一条（另一条丢弃），如果找不到，要把两个主键都写进入。如果不在这个时间范围的数据，
//        直接按照T生成主键并写入，无需查询。PS：如果渠道数据先写了2条，后续高频来了，会更新其中一条。

        // 后续可以进行 map、filter、sink 操作
        // 机票出票-航段级
        SingleOutputStreamOperator<BookingSegFactModel> mappedStream1 = rowDataStream.map(row -> {

            //System.out.println("row=="+row);

            BookingSegFactModel model = new BookingSegFactModel();

            // 获取各个字段值
            String pnr = toStringSafe(row.getField("PNR"));
            String boardPoint = toStringSafe(row.getField("BOARD_POINT"));

            // 处理出发日期和时间
            String departureDate = "";
            String departureTime = "";
            if (row.getField("DEPARTURE_DATE") != null) {
                LocalDateTime depDateTime = (LocalDateTime) row.getField("DEPARTURE_DATE");
                departureDate = depDateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                departureTime = depDateTime.format(DateTimeFormatter.ofPattern("HHmmss"));
            }

            // 乘客证件号（需要加密）
            String passengerCertNo = toStringSafe(row.getField("PASSENGER_CERT_NO"));
            passengerCertNo = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, passengerCertNo,  DataSource.KEY_ACCOUNT_TRAVEL);
            //String encryptedCertNo = "";
            //if (passengerCertNo != null && !passengerCertNo.isEmpty()) {
            //    encryptedCertNo = SM4Utils.encrypt(passengerCertNo,Constants.SM4_KEY);
            //}

            // 乘客姓名处理（注意英文姓名的顺序和斜杠问题）
            String passengerName = toStringSafe(row.getField("PASSENGER_NAME"));
            passengerName = NormalizationUtils.standardize(FieldType.CN_NAME, passengerName,  DataSource.KEY_ACCOUNT_TRAVEL);

            // 生成主键：PNR + 登机点 + 出发日期 + 出发时间 + 证件号(加密) + 姓名
            StringBuilder pkId = new StringBuilder();
            if (pnr != null) pkId.append(pnr);
            if (boardPoint != null) pkId.append(boardPoint);
            if (StringUtils.isNotBlank(departureDate)) pkId.append(departureDate);
            if (StringUtils.isNotBlank(departureTime)) pkId.append(departureTime);
            if (StringUtils.isNotBlank(passengerCertNo)) pkId.append(passengerCertNo);
            if (StringUtils.isNotBlank(passengerName)) pkId.append(passengerName);

            model.setPkId(pkId.toString());

            // 渠道订单号 - 对应源系统的 ORDER_NO
            String orderNo = toStringSafe(row.getField("ORDER_NO"));
            model.setChannelOrderId(orderNo);

            // 预订渠道 - 对应源系统的 ORDER_SOURCE，需要标准化
            String orderSource = "SCSME";
            String standardizedChannel = NormalizationUtils.standardize(
                    FieldType.ORDER_CHANNEL_AIRTICKET,
                    orderSource,
                    DataSource.KEY_ACCOUNT_TRAVEL);
            model.setAkChannel(standardizedChannel);

            // TID 字段：由证件号转换而来
            String certType = toStringSafe(row.getField("CERT_TYPE"));
            certType = NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, certType,  DataSource.KEY_ACCOUNT_TRAVEL);
            String certNo = toStringSafe(row.getField("CERT_NO"));
            certNo = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, certNo,  DataSource.KEY_ACCOUNT_TRAVEL);

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

            model.setSourceLastUpdatetime(row.getFieldAs("CREATE_TIME").toString());

            return model;
        });

        // 6. 输出到 Doris
        // 写到  机票预订PNR级事实表  T_DWD_BOOKING_PNR_FACT
        logger.info("写入 Doris 表: DB={}, Table={}", Constants.DWD_DB, "T_DWD_BOOKING_SEG_FACT");

        DorisSink<BookingSegFactModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_BOOKING_SEG_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        mappedStream1.sinkTo(dorisSink);

        // 打印到控制台
        mappedStream1.print("BOOKING_Seg_Fact_MODEL");

    }


}
