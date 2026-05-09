package com.travelsky.trp.usercenter.data.analysis.transform.zsf;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.ZsfMultiCardBookFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;

public class ZsfToMutiCardBookFactTrans {
    static final Logger logger = LoggerFactory.getLogger(ZsfToMutiCardBookFactTrans.class);

    // 将 Object 转为 String，支持 null.
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

    public static void result(DataStream<Row> rowDataStream, String etlDate) throws Exception {

        //1、预订 与 取消
        SingleOutputStreamOperator<ZsfMultiCardBookFactModel> bookStream = rowDataStream.map(row -> {

            // 映射为 ZsfMultiCardBookFactModel
            ZsfMultiCardBookFactModel model = new ZsfMultiCardBookFactModel();

            // 主键：ORDER_NO + ID
            String orderNo = toStringSafe(row.getField("ORDER_NO"));
            String id = toStringSafe(row.getField("ID"));

            // 构建主键，确保不为空
            String pkId = "";
            if (orderNo != null && !orderNo.trim().isEmpty()) {
                pkId += orderNo;
            }
            if (id != null && !id.trim().isEmpty()) {
                pkId += id;
            }
            // 如果两者都为空，设置为null或默认值
            model.setPkId(pkId.isEmpty() ? null : pkId);

            model.setAkOrdernum(orderNo);

            // 关联主订单号
            model.setAkMainOrdernum(toStringSafe(row.getField("MAIN_ORDER_NO")));

            model.setProdNo(toStringSafe(row.getField("PRODUCT_NO")));

            // 销售价格（BigDecimal）
            Object salePriceObj = row.getField("SALE_PRICE");
            if (salePriceObj instanceof BigDecimal) {
                model.setSalePrice((BigDecimal) salePriceObj);
            } else {
                model.setSalePrice(null);
            }

            model.setProdState(toStringSafe(row.getField("STATE")));

            // 受益人证件信息 ODS层没加密
            String certNo = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, SM4Utils.encrypt(toStringSafe(row.getField("CERT_NO")), Constants.SM4_KEY), DataSource.ZHANGSHANGFEI);
            model.setBenefiCertNo(certNo);
            // 受益人证件类型
            String certType = NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, toStringSafe(row.getField("CERT_TYPE")), DataSource.ZHANGSHANGFEI);
            model.setBenefiCertType(certType);
            // 受益人Tid
            Tid tid1 = new Tid();
            tid1.setTid(certNo);
            HashMap<String, String> map = new HashMap<>();
            map.put(certType, certNo);
            tid1.setCertification(map);
            String Tid = IdMapping.idMappingFunction(tid1, "ZSF");
            model.setBenefiTid(Tid);


            // 中文姓名
            model.setCnLastName(toStringSafe(row.getField("CN_LAST_NAME")));
            model.setCnFirstName(toStringSafe(row.getField("CN_FIRST_NAME")));

            // 英文姓名
            model.setEnFirstName(toStringSafe(row.getField("EN_FIRST_NAME")));
            model.setEnLastName(toStringSafe(row.getField("EN_LAST_NAME")));

            // 过期日期（EXPIRATION_DATE 是 String 类型）
            String expirationDateStr = toStringSafe(row.getField("EXPIRATION_DATE"));
            if (expirationDateStr != null && !expirationDateStr.trim().isEmpty()) {
                model.setBenefiExpirationDate(LocalDate.parse(expirationDateStr));
            } else {
                model.setBenefiExpirationDate(null);
            }

            model.setBenefiSex(toStringSafe(row.getField("SEX")));

            // 生日
            String birthday = toStringSafe(row.getField("BIRTHDAY"));
            if (birthday != null && !birthday.trim().isEmpty()) {
                model.setBenefiBirthday(LocalDate.parse(birthday));
            } else {
                model.setBenefiBirthday(null);
            }

            // 护照信息
            model.setBenefiPassportIssueNation(toStringSafe(row.getField("PASSPORT_ISSUE_NATION")));
            String passportNation = NormalizationUtils.standardize(FieldType.NATIONALITY, toStringSafe(row.getField("PASSPORT_NATION")), DataSource.ZHANGSHANGFEI);
            model.setBenefiPassportNation(passportNation);
            // 手机号
            String phoneNum = NormalizationUtils.standardize(FieldType.MOBILE_NO, toStringSafe(row.getField("PHONE_NUM")), DataSource.ZHANGSHANGFEI);
            model.setBenefiMobileNumber(phoneNum);

            // 激活时间（ACTIVE_TIME）
            Object activeTimeObj = row.getField("ACTIVE_TIME");
            if (activeTimeObj != null) {
                LocalDateTime activeLdt = (LocalDateTime) activeTimeObj;
                model.setActTime(activeLdt.toString().replace("T", " ")); // 避免 T
                model.setExpirDate(activeLdt.toLocalDate()); //
            } else {
                model.setActTime(null);
                model.setExpirDate(null);
            }

            model.setInterFlag(NormalizationUtils.standardize(FieldType.INTERNATIONAL_DOMESTIC_FLAG, toStringSafe(row.getField("INTERNATIONAL_FLAG")), DataSource.ZHANGSHANGFEI));

            String exchangeNumStr = toStringSafe(row.getField("EXCHANGE_NUM"));
            if (exchangeNumStr != null && !exchangeNumStr.isEmpty()) {
                model.setExchCount(Integer.parseInt(exchangeNumStr));
            } else {
                model.setExchCount(null);
            }

            // 出行日期
            Object travelStartObj = row.getField("TRAVEL_START_DATE");
            if (travelStartObj != null) {
                LocalDateTime startLdt = ((LocalDateTime) travelStartObj);
                model.setDepDate(startLdt.toLocalDate());
            } else {
                model.setDepDate(null);
            }

            // 失效时间
            Object expiry_date = row.getField("EXPIRY_DATE");
            if (expiry_date != null) {
                LocalDateTime expiryDate = ((LocalDateTime) expiry_date);
                model.setExpirDate(expiryDate.toLocalDate());
            } else {
                model.setDepDate(null);
            }

            // 结束日期
            Object travelEndObj = row.getField("TRAVEL_END_DATE");
            if (travelEndObj != null) {
                LocalDateTime endLdt = ((LocalDateTime) travelEndObj);
                model.setArrDate(endLdt.toLocalDate());
            } else {
                model.setArrDate(null);
            }

            // 渠道
            model.setBookingChannel("掌尚飞");

            // 预订时间和预订日期
            Object createTime = row.getField("CREATE_TIME");
            if (createTime != null) {
                LocalDateTime createTime1 = (LocalDateTime) createTime;
                model.setFkBookingTime(createTime1.toLocalTime().toString());
                model.setFkBookingDate(createTime1.toLocalDate().toString());
            } else {
                model.setFkBookingTime(null);
                model.setFkBookingDate(null);
            }

            // 客户ID（预订人）
            String customerId = toStringSafe(row.getField("CUSTOMER_ID"));

            Tid tid2 = new Tid();
            tid2.setCrmCustomerId(customerId);
            String FkBookingUserTid = IdMapping.idMappingFunction(tid2, "ZSF");
            model.setFkBookingUserTid(FkBookingUserTid);

            model.setFkBookingUserOriginId(customerId);

//            model.setSubscribeCount(1);

            // 源更新时间
            model.setSourceLastUpdatetime(formatDateTimeSafe(row.getField("UPDATE_TIME")));

            // 系统时间
            LocalDateTime now = LocalDateTime.now();
            model.setSystemCreatetime(now.toString());
//            model.setSystemLastUpdatetime(now.toString());

            return model;
        // 过滤掉 PKID 为空的记录
        }).filter(model -> model.getPkId() != null && !model.getPkId().trim().isEmpty());



        // 1、明细事实表-掌尚飞-次卡预订
        logger.info("连接 Doris 参数: DB={}, Table={}, FE={}, BE={}, User={}",
                Constants.DWD_DB, "T_DWD_MULTI_CARD_BOOK_FACT", Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT, Constants.DWD_USER);
        DorisSink<ZsfMultiCardBookFactModel> dorisSink_book = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_MULTI_CARD_BOOK_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        // 数据写入 Doris
        bookStream.print("bookStream");
        bookStream.sinkTo(dorisSink_book);
    }
}
