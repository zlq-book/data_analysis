package com.travelsky.trp.usercenter.data.analysis.transform.zsf;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CertDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.MobileDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.ZsfMultiCardBookFactModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.ZsfMultiCardCancelFactModel;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
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
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;

// 掌尚飞抖音次卡受益人
// 可以跑，但是有字段解密失败  PHONE_NUM 字段，源数据库已经解密的
public class OdrOrderDetailSecondaryDouyinFactTrans {

    static final Logger logger = LoggerFactory.getLogger(OdrOrderDetailSecondaryDouyinFactTrans.class);

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

    // 兼容解析多种日期格式
    private static LocalDate parseLocalDateSafe(String dateStr) {
        if (dateStr == null || dateStr.trim().isEmpty()) {
            return null;
        }
        dateStr = dateStr.trim();
        try {
            // 先尝试标准 ISO 格式 yyyy-MM-dd
            return LocalDate.parse(dateStr);
        } catch (DateTimeParseException e1) {
            try {
                // 尝试兼容 yyyy-M-d 格式 (单个数字的月份和日期)
                DateTimeFormatter flexibleFormatter = DateTimeFormatter.ofPattern("yyyy-M-d");
                return LocalDate.parse(dateStr, flexibleFormatter);
            } catch (DateTimeParseException e2) {
                // 解析失败，返回 null
                return null;
            }
        }
    }

    public static void result(StreamTableEnvironment tEnv, String etlDate) throws Exception {

        tEnv.executeSql(CreateTableSql.ZSF_ODR_ORDER_DETAIL_SECONDARY_DOUYIN);
        tEnv.executeSql(CreateTableSql.ZSF_ODR_ORDER_CUSTOMER_RELATION_DOUYIN);

        // 查询
        String query = "SELECT \n" +
                "    a.ETL_DATE AS ETL_DATE,\n" +
                "    a.ID AS ID,\n" +
                "    a.ORDER_PACKAGE_ID AS ORDER_PACKAGE_ID,\n" +
                "    a.ORDER_DETAIL_ID AS ORDER_DETAIL_ID,\n" +
                "    a.ORDER_NO AS ORDER_NO,\n" +
                "    a.MAIN_ORDER_NO AS MAIN_ORDER_NO,\n" +
                "    a.MAIN_ORDER_ID AS MAIN_ORDER_ID,\n" +
                "    a.INSTANCE_ID AS INSTANCE_ID,\n" +
                "    a.INSTANCE_NO AS INSTANCE_NO,\n" +
                "    a.PRODUCT_NO AS PRODUCT_NO,\n" +
                "    a.PACKAGE_INDEX AS PACKAGE_INDEX,\n" +
                "    a.SALE_PRICE AS SALE_PRICE,\n" +
                "    a.STATE AS STATE,\n" +
                "    a.CUSTOMER_ID AS CUSTOMER_ID,\n" +
                "    a.MEMBER_ID AS MEMBER_ID,\n" +
                "    a.CUSTOMER_NAME AS CUSTOMER_NAME,\n" +
                "    a.ACTIVE_TIME AS ACTIVE_TIME,\n" +
                "    a.REMARK AS REMARK,\n" +
                "    a.INTERNATIONAL_FLAG AS INTERNATIONAL_FLAG,\n" +
                "    a.CERT_TYPE AS CERT_TYPE,\n" +
                "    a.CERT_NO AS CERT_NO,\n" +
                "    a.CN_FIRST_NAME AS CN_FIRST_NAME,\n" +
                "    a.CN_LAST_NAME AS CN_LAST_NAME,\n" +
                "    a.EN_FIRST_NAME AS EN_FIRST_NAME,\n" +
                "    a.EN_LAST_NAME AS EN_LAST_NAME,\n" +
                "    a.EXPIRATION_DATE AS EXPIRATION_DATE,\n" +
                "    a.SEX AS SEX,\n" +
                "    a.BIRTHDAY AS BIRTHDAY,\n" +
                "    a.PASSPORT_ISSUE_NATION AS PASSPORT_ISSUE_NATION,\n" +
                "    a.PASSPORT_NATION AS PASSPORT_NATION,\n" +
                "    a.PHONE_NUM AS PHONE_NUM,\n" +
                "    a.SEND_MSG_FLAG AS SEND_MSG_FLAG,\n" +
                "    a.EXCHANGE_NUM AS EXCHANGE_NUM,\n" +
                "    a.EXPIRY_DATE AS EXPIRY_DATE,\n" +
                "    a.TRAVEL_START_DATE AS TRAVEL_START_DATE,\n" +
                "    a.TRAVEL_END_DATE AS TRAVEL_END_DATE,\n" +
                "    a.ENABLE AS ENABLE,\n" +
                "    a.CREATE_TIME AS CREATE_TIME,\n" +
                "    a.CREATE_USER AS CREATE_USER,\n" +
                "    a.UPDATE_TIME AS UPDATE_TIME,\n" +
                "    a.UPDATE_USER AS UPDATE_USER,\n" +
                "    b.CUSTOMER_ID AS CUSTOMER_ID\n" +
                "FROM T_ODS_ZSF_ODR_ORDER_DETAIL_SECONDARY_DOUYIN a\n" +
                "LEFT JOIN T_ODS_ZSF_ODR_ORDER_CUSTOMER_RELATION_DOUYIN b\n" +
                "ON a.MAIN_ORDER_NO = b.MAIN_ORDER_NO\n" +
//                "WHERE a.ETL_DATE = '" + etlDate + "'"
                " WHERE  a.CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                + " OR  a.UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' ";


        Table dorisTable = tEnv.sqlQuery(query);

        DataStream<Row> rowDataStream = tEnv.toChangelogStream(dorisTable);

        // 后续可以进行 map、filter、sink 操作
        SingleOutputStreamOperator<ZsfMultiCardBookFactModel> bookStream = rowDataStream.map(row -> {

            ZsfMultiCardBookFactModel model = new ZsfMultiCardBookFactModel();

            String orderNo = toStringSafe(row.getField("ORDER_NO"));
            String id = toStringSafe(row.getField("ID"));
            model.setPkId((orderNo != null ? orderNo : "") + (id != null ? id : ""));

            model.setAkOrdernum(orderNo);

            // 关联主订单号
            model.setAkMainOrdernum(toStringSafe(row.getField("MAIN_ORDER_NO")));

            model.setProdNo(toStringSafe(row.getField("PRODUCT_NO")));

            // SALE_PRICE (BigDecimal)
            Object salePriceObj = row.getField("SALE_PRICE");
            if (salePriceObj instanceof BigDecimal) {
                model.setSalePrice((BigDecimal) salePriceObj);
            } else {
                model.setSalePrice(null);
            }

            model.setProdState(toStringSafe(row.getField("STATE")));

            // 受益人证件信息
            String certNo = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,toStringSafe(row.getField("CERT_NO")), DataSource.ZHANGSHANGFEI);
            model.setBenefiCertNo(certNo);
            // 受益人证件类型
            String certType =toStringSafe(row.getField("CERT_TYPE"));
            model.setBenefiCertType(certType);
            // 受益人Tid
            Tid tid1 = new Tid();
            tid1.setTid(certNo);
            HashMap<String, String> map = new HashMap<>();
            map.put(certType,certNo);
            tid1.setCertification(map);
            String benefiTid = IdMapping.idMappingFunction(tid1, "ZSF");
            model.setBenefiTid(benefiTid);

            model.setCnLastName(toStringSafe(row.getField("CN_LAST_NAME")));
            model.setCnFirstName(toStringSafe(row.getField("CN_FIRST_NAME")));
            model.setEnFirstName(toStringSafe(row.getField("EN_FIRST_NAME")));
            model.setEnLastName(toStringSafe(row.getField("EN_LAST_NAME")));

            // EXPIRATION_DATE
            String expirationDateStr = toStringSafe(row.getField("EXPIRATION_DATE"));
            model.setBenefiExpirationDate(parseLocalDateSafe(expirationDateStr));

//            String sex = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,toStringSafe(row.getField("SEX")), DataSource.ZHANGSHANGFEI);
            model.setBenefiSex(toStringSafe(row.getField("SEX")));

            // BIRTHDAY (String -> LocalDate)
            String birthdayStr = toStringSafe(row.getField("BIRTHDAY"));
            model.setBenefiBirthday(parseLocalDateSafe(birthdayStr));

            // 护照信息
            model.setBenefiPassportIssueNation(toStringSafe(row.getField("PASSPORT_ISSUE_NATION")));
            model.setBenefiPassportNation(NormalizationUtils.standardize(FieldType.NATIONALITY,toStringSafe(row.getField("PASSPORT_NATION")),DataSource.ZHANGSHANGFEI));
//            model.setBenefiMobileNumber(NormalizationUtils.standardize(FieldType.MOBILE_NO,toStringSafe(row.getField("PHONE_NUM")),DataSource.ZHANGSHANGFEI));
            model.setBenefiMobileNumber(toStringSafe(row.getField("PHONE_NUM")));


            // ACTIVE_TIME -> ActTime (String)
            Object activeTimeObj = row.getField("ACTIVE_TIME");
            if (activeTimeObj != null) {
                LocalDateTime activeLdt = (LocalDateTime) activeTimeObj;
                model.setActTime(activeLdt.toString().replace("T", " ")); // 避免 T
                model.setExpirDate(activeLdt.toLocalDate()); //
            } else {
                model.setActTime(null);
                model.setExpirDate(null);
            }

            // 失效时间
            Object expiry_date = row.getField("EXPIRY_DATE");
            if (expiry_date != null) {
                LocalDateTime expiryDate = ((LocalDateTime) expiry_date);
                model.setExpirDate(expiryDate.toLocalDate());
            } else {
                model.setDepDate(null);
            }

            // 出行日期
            Object travelStartObj = row.getField("TRAVEL_START_DATE");
            if (travelStartObj != null) {
                LocalDateTime startLdt = ((LocalDateTime) travelStartObj);
                model.setDepDate(startLdt.toLocalDate());
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

            // INTERNATIONAL_FLAG
            model.setInterFlag(NormalizationUtils.standardize(FieldType.INTERNATIONAL_DOMESTIC_FLAG,toStringSafe(row.getField("INTERNATIONAL_FLAG")),DataSource.ZHANGSHANGFEI));


            // EXCHANGE_NUM -> ExchCount
            String exchangeNumStr = toStringSafe(row.getField("EXCHANGE_NUM"));
            if (exchangeNumStr != null && !exchangeNumStr.isEmpty()) {
                model.setExchCount(Integer.parseInt(exchangeNumStr));
            } else {
                model.setExchCount(null);
            }

            // --- 4. 渠道与用户信息 ---
            model.setBookingChannel("抖音"); // 已修改为抖音

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

            // CUSTOMER_ID
            String customerId = toStringSafe(row.getField("CUSTOMER_ID"));

            Tid tid2 = new Tid();
            tid2.setCrmCustomerId(customerId);
            String FkBookingUserTid = IdMapping.idMappingFunction(tid2, "ZSF");
            model.setFkBookingUserTid(FkBookingUserTid);

            model.setFkBookingUserOriginId(customerId);

//            model.setSubscribeCount(1);

            // UPDATE_TIME
            model.setSourceLastUpdatetime(formatDateTimeSafe(row.getField("UPDATE_TIME")));

            // --- 5. 系统时间 ---
            LocalDateTime now = LocalDateTime.now();
            model.setSystemCreatetime(now.toString());
//            model.setSystemLastUpdatetime(now.toString());

            return model;
        });

        // 映射为 ZsfMultiCardCancelFactModel
        DataStream<ZsfMultiCardCancelFactModel> cancelStream = rowDataStream.map(row -> {
            ZsfMultiCardCancelFactModel model = new ZsfMultiCardCancelFactModel();

            String orderNo = toStringSafe(row.getField("ORDER_NO"));
            String id = toStringSafe(row.getField("ID"));
            model.setPkId((orderNo != null ? orderNo : "") + (id != null ? id : ""));

            model.setAkOrdernum(orderNo);

            // 关联主订单号
            model.setAkMainOrdernum(toStringSafe(row.getField("MAIN_ORDER_NO")));

            model.setProdNo(toStringSafe(row.getField("PRODUCT_NO")));

            // SALE_PRICE (BigDecimal)
            Object salePriceObj = row.getField("SALE_PRICE");
            if (salePriceObj instanceof BigDecimal) {
                model.setSalePrice((BigDecimal) salePriceObj);
            } else {
                model.setSalePrice(null);
            }

            model.setProdState(toStringSafe(row.getField("STATE")));

            String certNo = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,toStringSafe(row.getField("CERT_NO")),DataSource.ZHANGSHANGFEI);
            String certType = toStringSafe(row.getField("CERT_TYPE"));
            model.setBenefiCertNo(certNo);
            model.setBenefiCertType(certType);

            Tid tid1 = new Tid();
            tid1.setTid(certNo);
            HashMap<String, String> map = new HashMap<>();
            map.put(certType,certNo);
            tid1.setCertification(map);
            String benefiTid = IdMapping.idMappingFunction(tid1, "ZSF");
            model.setBenefiTid(benefiTid);

            model.setCnLastName(toStringSafe(row.getField("CN_LAST_NAME")));
            model.setCnFirstName(toStringSafe(row.getField("CN_FIRST_NAME")));
            model.setEnFirstName(toStringSafe(row.getField("EN_FIRST_NAME")));
            model.setEnLastName(toStringSafe(row.getField("EN_LAST_NAME")));

            String expirationDateStr = toStringSafe(row.getField("EXPIRATION_DATE"));
            model.setBenefiExpirationDate(parseLocalDateSafe(expirationDateStr));

            model.setBenefiSex(toStringSafe(row.getField("SEX")));

            String birthdayStr = toStringSafe(row.getField("BIRTHDAY"));
            model.setBenefiBirthday(parseLocalDateSafe(birthdayStr));

            // 护照信息
            model.setBenefiPassportIssueNation(toStringSafe(row.getField("PASSPORT_ISSUE_NATION")));
            model.setBenefiPassportNation(NormalizationUtils.standardize(FieldType.NATIONALITY,toStringSafe(row.getField("PASSPORT_NATION")),DataSource.ZHANGSHANGFEI));
//            model.setBenefiMobileNumber(NormalizationUtils.standardize(FieldType.MOBILE_NO,toStringSafe(row.getField("PHONE_NUM")),DataSource.ZHANGSHANGFEI));
            model.setBenefiMobileNumber(toStringSafe(row.getField("PHONE_NUM")));


            Object activeTimeObj = row.getField("ACTIVE_TIME");
            if (activeTimeObj != null) {
                LocalDateTime activeLdt = (LocalDateTime) activeTimeObj;
                model.setActTime(activeLdt.toString().replace("T", " ")); // 避免 T
                model.setExpirDate(activeLdt.toLocalDate()); //
            } else {
                model.setActTime(null);
                model.setExpirDate(null);
            }

            // 失效时间
            Object expiry_date = row.getField("EXPIRY_DATE");
            if (expiry_date != null) {
                LocalDateTime expiryDate = ((LocalDateTime) expiry_date);
                model.setExpirDate(expiryDate.toLocalDate());
            } else {
                model.setDepDate(null);
            }

            // 出行日期
            Object travelStartObj = row.getField("TRAVEL_START_DATE");
            if (travelStartObj != null) {
                LocalDateTime startLdt = ((LocalDateTime) travelStartObj);
                model.setDepDate(startLdt.toLocalDate());
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

            // INTERNATIONAL_FLAG
            model.setInterFlag(NormalizationUtils.standardize(FieldType.INTERNATIONAL_DOMESTIC_FLAG,toStringSafe(row.getField("INTERNATIONAL_FLAG")),DataSource.ZHANGSHANGFEI));

            // EXCHANGE_NUM -> ExchCount
            String exchangeNumStr = toStringSafe(row.getField("EXCHANGE_NUM"));
            if (exchangeNumStr != null && !exchangeNumStr.isEmpty()) {
                model.setExchCount(Integer.parseInt(exchangeNumStr));
            } else {
                model.setExchCount(null);
            }

            model.setBookingChannel("抖音");

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
            // CUSTOMER_ID -> FkBookingUserTid 和 FkBookingUserOriginId
            String customerId = toStringSafe(row.getField("CUSTOMER_ID"));

            Tid tid2 = new Tid();
            tid2.setCrmCustomerId(customerId);
            String FkBookingUserTid = IdMapping.idMappingFunction(tid2, "ZSF");
            model.setFkBookingUserTid(FkBookingUserTid);

//            model.setUnsubscribeCount(1);

            // UPDATE_TIME -> SourceLastUpdatetime
            model.setSourceLastUpdatetime(formatDateTimeSafe(row.getField("UPDATE_TIME")));

            // --- 5. 系统时间 ---
            LocalDateTime now = LocalDateTime.now();
            model.setSystemCreatetime(now.toString());
//            model.setSystemLastUpdatetime(now.toString());

            return model;
        });

        // 映射为 CertDimModel  证件信息维表
        DataStream<CertDimModel> certDimStream = rowDataStream.map(row -> {
            CertDimModel model = new CertDimModel();

            // 证件类型
            String certType =toStringSafe(row.getField("CERT_TYPE"));
            // 证件号码
            String certNumber = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,toStringSafe(row.getField("CERT_NO")),DataSource.ZHANGSHANGFEI);

            //1、tid
            Tid tid = new Tid();
            tid.setTid(certNumber);
            HashMap<String, String> map = new HashMap<>();
            map.put(certType,certNumber);
            tid.setCertification(map);
            String TidStr = IdMapping.idMappingFunction(tid, "ZSF");
            model.settId(TidStr);

            // 2、主键
            model.setSystemKey((TidStr != null ? TidStr : "") +
                    (certType != null ? certType : "") +
                    (certNumber != null ? certNumber : ""));

            //3、证件类型
            model.setCertType(certType);

            //4、证件号码
            model.setCertNumber(certNumber);

            //5、是否直销实名认证证件
            model.setDirectSalesRealNameVerifiedCard(false);

            //6、是否鲁雁行实名认证证件
            model.setLyRegistCard(false);

            //7、是否常客注册证件
            model.setFrequentFlyerRegistrationCard(false);

            //8、是否次卡受益人证件
            model.setPurchasersBeneficiaryCard(false);

            // 是否抖音次卡受益人证件
            model.setDouyinPurchasersBeneficiaryCard(true);

            //9、是否符合编码规则
            model.setCodeRuleCompliant(TransUtils.isCodeRuleCompliant(certNumber));

            //10、证件签发日期  掌尚飞 没这个字段

            //11、证件过期日期
            String expiryDate = toStringSafe(row.getField("EXPIRATION_DATE"));
            model.setCertExpireDate(parseLocalDateSafe(expiryDate));

            //12、证件签发国
            model.setCertIssuingCountry(toStringSafe(row.getField("PASSPORT_ISSUE_NATION")));

            //13、证件签发机构   掌尚飞 没这个字段

            //14、当前最可信来源
            model.setCurrentCertHighestPriority(1);

            LocalDateTime now = LocalDateTime.now();
            //15、update_time
            model.setUpdateTime(now.toString());

            //16、create_time
            model.setCreateTime(now.toString());

            return model;
        });

        // 映射为 UserDimModel  用户维表
        DataStream<UserDimModel> userDimStream = rowDataStream.map(row -> {
            UserDimModel model = new UserDimModel();

            String certType = toStringSafe(row.getField("CERT_TYPE"));
            String certNo = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,toStringSafe(row.getField("CERT_NO")),DataSource.ZHANGSHANGFEI);

            //1、tid
            Tid tid = new Tid();
            tid.setTid(certNo);
            HashMap<String, String> map = new HashMap<>();
            map.put(certType,certNo);
            tid.setCertification(map);
            String pkid = IdMapping.idMappingFunction(tid, "ZSF");
            model.setPkId(pkid);

            //2、中文名字
            String cnLastName = toStringSafe(row.getField("CN_LAST_NAME"));
            String cnFirstName = toStringSafe(row.getField("CN_FIRST_NAME"));
            model.setCnName(NormalizationUtils.standardize(FieldType.CN_NAME,(cnLastName != null ? cnLastName : "") +
                    (cnFirstName != null ? cnFirstName : ""),DataSource.ZHANGSHANGFEI));

            //3、英文名字
            String enLastName = toStringSafe(row.getField("EN_LAST_NAME"));
            String enFirstName = toStringSafe(row.getField("EN_FIRST_NAME"));
            model.setEnName(NormalizationUtils.standardize(FieldType.EN_NAME,(enLastName != null ? enLastName : "") +
                    (enFirstName != null ? enFirstName : ""),DataSource.ZHANGSHANGFEI));

            //4、性别
            model.setSex(toStringSafe(row.getField("SEX")));

            //5、生日
            String birthdayStr = toStringSafe(row.getField("BIRTHDAY"));
            LocalDate birthday = parseLocalDateSafe(birthdayStr);
            model.setBirthday(birthday != null ? birthday.toString() : null);

            //6、是否抖音次卡购买人
            model.setDouyinCardPurchaser(false);

            return model;
        });

        // 映射为 MobileDimModel  手机号维表
        DataStream<MobileDimModel> MobileDimModel = rowDataStream.map(row -> {
            MobileDimModel model = new MobileDimModel();

            String phoneNum = NormalizationUtils.standardize(FieldType.MOBILE_NO,
                    toStringSafe(row.getField("PHONE_NUM")));
            String certType = toStringSafe(row.getField("CERT_TYPE"));
            String certNo = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,toStringSafe(row.getField("CERT_NO")),DataSource.ZHANGSHANGFEI);

            Tid tid = new Tid();
            tid.setTid(certNo);
            Map<String, String> certMap = new HashMap<>();
            certMap.put(certType.toString(), certNo.toString());
            tid.setCertification(certMap);
            String Tid = IdMapping.idMappingFunction(tid, "ZSF");

            model.setSystemKey((Tid==null?"":Tid) + (phoneNum==null?"":phoneNum));
            model.settId(Tid);
            model.setMobileNumber(toStringSafe(row.getField("PHONE_NUM")));

            model.setCreateTime(LocalDateTime.now().toString());
            model.setUpdateTime(LocalDateTime.now().toString());

            return model;
        });

        // 1、掌尚飞-次卡预订
        bookStream.print("odrOrderDetailSecondaryDouyinFactTrans_bookStream");
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
        bookStream.sinkTo(dorisSink_book);


        // 2、掌尚飞-次卡退订
        cancelStream.print("odrOrderDetailSecondaryDouyinFactTrans_cancelStream");
        logger.info("连接 Doris 参数: DB={}, Table={}, FE={}, BE={}, User={}",
                Constants.DWD_DB, "T_DWD_MULTI_CARD_CANCEL_FACT", Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT, Constants.DWD_USER);
        DorisSink<ZsfMultiCardCancelFactModel> dorisSink_cancrl = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_MULTI_CARD_CANCEL_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        // 数据写入 Doris
        cancelStream.sinkTo(dorisSink_cancrl);

        //3、证件信息维表
        certDimStream.print("certStream");
        logger.info("连接 Doris 参数: DB={}, Table={}, FE={}, BE={}, User={}",
                Constants.DIM_DB, "T_DIM_CERT_DIM", Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT, Constants.DIM_USER);
        DorisSink<CertDimModel> dorisSink_cerDim = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_CERT_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);
        // 数据写入 Doris
        certDimStream.sinkTo(dorisSink_cerDim);

        //4、用户维表
        userDimStream.print("userStream");
        logger.info("连接 Doris 参数: DB={}, Table={}, FE={}, BE={}, User={}",
                Constants.DIM_DB, "T_DIM_USER_DIM", Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT, Constants.DIM_USER);
        DorisSink<UserDimModel> dorisSink_userDim = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_USER_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);
        // 数据写入 Doris
        userDimStream.sinkTo(dorisSink_userDim);

        //5、手机号维表
        MobileDimModel.print("mobileStream");
        logger.info("连接 Doris 参数: DB={}, Table={}, FE={}, BE={}, User={}",
                Constants.DIM_DB, "T_DIM_MOBILE_DIM", Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT, Constants.DIM_USER);
        DorisSink<MobileDimModel> dorisSink_mobileDim = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_MOBILE_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);
        // 数据写入 Doris
        MobileDimModel.sinkTo(dorisSink_mobileDim);

    }
}

