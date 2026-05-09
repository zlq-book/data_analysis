package com.travelsky.trp.usercenter.data.analysis.transform.zsf;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.ZsfTiktokVchrPurFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

// 掌尚飞-抖音券码购买
// 可以跑，但是有一个字段长度不够
public class PrdProductCouponDouyinFactTrans {

    static final Logger logger = LoggerFactory.getLogger(PrdProductCouponDouyinFactTrans.class);

    // 将 Object 转为 String，支持 null
    private static String toStringSafe(Object obj) {
        return obj != null ? obj.toString() : null;
    }

    // 将 Object 转为 Integer，支持 null
    private static Integer toIntegerSafe(Object obj) {
        return obj instanceof Integer ? (Integer) obj : null;
    }

    // 将 int的时间戳 转为格式化后的 LocalDateTime 字符串
    private static String formatIntTimestampSafe(Object obj, DateTimeFormatter formatter) {
        if (obj instanceof Integer) {
            int timestamp = (Integer) obj;
            try {
                Timestamp ts = new Timestamp(timestamp * 1000L);
                return ts.toLocalDateTime().format(formatter);
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }

    // 将 dateTime 转为 LocalDateTime 字符串
    private static String formatDateTimeSafe(Object ts) {
        if (ts != null) {
            LocalDateTime s = (LocalDateTime) ts;
            return s.toString().replace("T", " ");
        }
        return null;
    }

    public static void result(StreamTableEnvironment tEnv, String etlDate) throws Exception {

        tEnv.executeSql(CreateTableSql.ZSF_PRD_PRODUCT_COUPON_DOUYIN);

        // 查询 源表数据
        String query = "SELECT \n" +
                "    ETL_DATE,\n" +
                "    ID,\n" +
                "    ORDER_ID,\n" +
                "    `COUNT`,\n" +
                "    START_TIME,\n" +
                "    EXPIRE_TIME,\n" +
                "    SKU_NAME,\n" +
                "    SKU_ID,\n" +
                "    CARD_CODE,\n" +
                "    COUPON_CODE,\n" +
                "    PHONE,\n" +
                "    STATUS,\n" +
                "    VERIFICATION_TIME,\n" +
                "    REFUND_TYPE,\n" +
                "    AFTER_SALE_ID,\n" +
                "    CERTIFICATE_ID,\n" +
                "    REFUND_APPLY_TIME,\n" +
                "    REFUND_AUDIT_TIME,\n" +
                "    REFUND_AUDIT_RESULT,\n" +
                "    ENABLE,\n" +
                "    CREATE_TIME,\n" +
                "    CREATE_USER,\n" +
                "    UPDATE_TIME,\n" +
                "    UPDATE_USER\n" +
                "FROM T_ODS_ZSF_PRD_PRODUCT_COUPON_DOUYIN \n" +
//                "WHERE ETL_DATE = '" + etlDate + "'";
        " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                + " OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' ";

        Table dorisTable = tEnv.sqlQuery(query);

        DataStream<Row> rowDataStream = tEnv.toDataStream(dorisTable);

        // 后续可以进行 map、filter、sink 操作
        SingleOutputStreamOperator<ZsfTiktokVchrPurFactModel> mappedStream = rowDataStream.map(row -> {
            ZsfTiktokVchrPurFactModel model = new ZsfTiktokVchrPurFactModel();

            // 时间格式
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            // 设置字段
            model.setPkId(toStringSafe(row.getField("ID")));
            model.setAkOrderId(toStringSafe(row.getField("ORDER_ID")));
            model.setIssuedCount(toIntegerSafe(row.getField("COUNT")));

            // START_TIME 和 EXPIRE_TIME 是 INT 时间戳，转 LocalDateTime
            model.setStartTime(formatIntTimestampSafe(row.getField("START_TIME"), formatter));
            model.setExpireTime(formatIntTimestampSafe(row.getField("EXPIRE_TIME"), formatter));

            model.setDouyinGoodsName(toStringSafe(row.getField("SKU_NAME")));
            model.setDouyinGoodsId(toStringSafe(row.getField("SKU_ID")));
            model.setCardCode(toStringSafe(row.getField("CARD_CODE")));
            model.setCouponCode(toStringSafe(row.getField("COUPON_CODE")));

            model.setPurchaserMobileNumber(NormalizationUtils.standardize(FieldType.MOBILE_NO, toStringSafe(row.getField("PHONE")), DataSource.ZHANGSHANGFEI));

            model.setCouponStatus(toStringSafe(row.getField("STATUS")));

            // TIMESTAMP(6) --> LocalDateTime
            model.setVerificationTime(formatDateTimeSafe(row.getField("VERIFICATION_TIME")));

            model.setRefundType(toIntegerSafe(row.getField("REFUND_TYPE")));
            model.setRefundId(toStringSafe(row.getField("AFTER_SALE_ID")));
            model.setCouponId(toStringSafe(row.getField("CERTIFICATE_ID")));

            // REFUND_APPLY_TIME 和 REFUND_AUDIT_TIME 是 INT 时间戳
            model.setRefundApplyTime(formatIntTimestampSafe(row.getField("REFUND_APPLY_TIME"), formatter));
            model.setRefundAuditTime(formatIntTimestampSafe(row.getField("REFUND_AUDIT_TIME"), formatter));

            model.setRefundAuditResult(toIntegerSafe(row.getField("REFUND_AUDIT_RESULT")));

            // CREATE_TIME 和 UPDATE_TIME 是 Timestamp 类型
            // 预订时间和预订日期
            Object createTime = row.getField("CREATE_TIME");
            if (createTime != null) {
                LocalDateTime createTime1 = (LocalDateTime) createTime;
                model.setFkPurchaseDate(createTime1.toLocalDate().toString());
                model.setFkPurchaseTime(createTime1.format(formatter));
            } else {
                model.setFkPurchaseDate(null);
                model.setFkPurchaseTime(null);
            }

            model.setUpdateTime(formatDateTimeSafe(row.getField("UPDATE_TIME")));

            String phone = NormalizationUtils.standardize(FieldType.MOBILE_NO,toStringSafe(row.getField("PHONE")),DataSource.ZHANGSHANGFEI);


            Tid tid = new Tid();
            tid.setTid(phone);
            tid.setMobilePhone(phone);
            String bookingUserTid = IdMapping.idMappingFunction(tid,"ZSF");
            model.setFkBookingUserTid(bookingUserTid);
            model.setFkBookingUserOriginId(phone);

//            model.setPurchaseCount(1);

            model.setSourceLastUpdatetime(formatDateTimeSafe(row.getField("UPDATE_TIME")));


            LocalDateTime now = LocalDateTime.now();
            model.setSystemCreatetime(now.toString());
            model.setSystemLastUpdatetime(now.toString());

            model.setDouyinGoodsName(toStringSafe(row.getField("SKU_NAME")));
            return model;
        });


        // 2. 输出到Doris，调试时先打印
        logger.info("连接 Doris 参数: DB={}, Table={}, FE={}, BE={}, User={}",
                Constants.DWD_DB, "T_DWD_TIKTOK_VCHR_PUR_FACT", Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT, Constants.DWD_USER);
        DorisSink<ZsfTiktokVchrPurFactModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_TIKTOK_VCHR_PUR_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        // 数据写入 Doris
        mappedStream.print("----mappedStream");
        mappedStream.sinkTo(dorisSink);



    }

}
