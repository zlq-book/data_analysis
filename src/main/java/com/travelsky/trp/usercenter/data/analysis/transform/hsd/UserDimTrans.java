package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.HsdProcessDataModel;
import com.travelsky.trp.usercenter.data.analysis.utils.CustomJsonSerializer;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDateTime;

/**
 * @author TS.SHA.fuhuazhang
 * @date 2025/8/6  13:31
 */
public class UserDimTrans {

    static final Logger logger = LoggerFactory.getLogger(UserDimTrans.class);

    // 客户基础信息涉及事件
    static final String basic_customer_infos_events = "Book|Issue|CheckIn|NameChange|IDChange";
    // 大客户号涉及事件
    static final String key_account_events = "SSRChange";
    // VVIP标识涉及事件
    static final String vvip_events = "OSIChange|RMKChange";
    // 特殊旅客标识(聋、哑旅客)
    static final String special_mark_events = "CKISSRChange";

    public static void output(DataStream<String> source) {
        SingleOutputStreamOperator<Void> processedStream = source.process(new ProcessFunction<String, Void>() {
            private transient Connection connDim;
            private transient Statement stmtDim;

            @Override
            public void open(Configuration parameters) throws Exception {
                connDim = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DIM_USER, Constants.DIM_PWD);
                stmtDim = DorisUtils.getStatement(connDim);
            }

            @Override
            public void close() throws Exception {
                DorisUtils.close(connDim, stmtDim, null);
            }

            @Override
            public void processElement(String value, Context ctx, Collector<Void> out) throws Exception {
                Document document;
                try {
                    document = DocumentUtils.string2Document(value);
                } catch (Exception e) {
                    logger.warn("convert document is error, the content is {}, the error is {}", value, e.getMessage());
                    return;
                }
                processUserDim(value, ctx, document);
            }
        }).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        DataStream<HsdProcessDataModel> hsdProcessDataStream = processedStream.getSideOutput(CommonOutputTags.HSD_PROCESS_DATA_TAG);
        //hsdProcessDataStream.print("hsdProcessDataStream");
        hsdProcessDataStream.sinkTo(FlinkDorisUtils.creatDorisDWDSink("T_DWD_HSD_PROCESS_DATA")).setParallelism(Constants.HSD_KAFKA_PARALLELISM);

        /*// 输出结果查看
        DataStream<UserDimModel> userDimStream = processedStream.getSideOutput(CommonOutputTags.USER_DIM_TAG);
        DataStream<UserDimSummaryModel> userDimSummaryStream = processedStream.getSideOutput(CommonOutputTags.USER_DIM_SUMMARY_TAG);
//        userDimStream.print("userDimStream");
//        userDimSummaryStream.print("userDimSummaryStream");
        // 创建 Doris Sink 并写入
        userDimStream.sinkTo(FlinkDorisUtils.creatDorisDIMSink("T_DIM_USER_DIM")).setParallelism(Constants.HSD_KAFKA_PARALLELISM);
        userDimSummaryStream.sinkTo(FlinkDorisUtils.creatDorisDIMSink("T_DIM_USER_DIM_SUMMARY")).setParallelism(Constants.HSD_KAFKA_PARALLELISM);*/
    }

   /* public static void processUserDim(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
        logger.info("高频数据整合用户维表");
        // 子事件名称
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
        // 证件类型
        String documentType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Type");
        // 证件号
        String documentNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Number");
        // 乘机人类型
        String travellerType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/@Type");
        // 乘机人英文姓
        String surname = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Surname");
        // 乘机人英文名
        String givenName = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/GivenName");
        String enName = NormalizationUtils.standardize(FieldType.EN_NAME, surname + givenName);
        // 乘机人中文姓名
        String nativeGivenName = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/NativeGivenName");
        nativeGivenName = NormalizationUtils.standardize(FieldType.CN_NAME, nativeGivenName);
        // 性别
        String gender = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Gender");
        // 出生日期
        String dateOfBirth = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@DateOfBirth");
        // 国籍
        String nationalityCountry = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@NationalityCountry");

        // 获取所有 Segment 节点
        NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
        for (int a = 0; a < segmentList.getLength(); a++) {
            Node segment = segmentList.item(a);
            // 盲人旅客标识
            Boolean blindPassenger = XpathUtils.getBoolean(segment, "SpecialServiceRequest/SSRCode='BLND'");
            // 失聪旅客标识
            Boolean deafPassenger = XpathUtils.getBoolean(segment, "SpecialServiceRequest/SSRCode='DEAF'");
            // 大客户号
            String keyAccountNumber = TransUtils.getKeyAccount(segment);
            // vvip
            Boolean vvip = XpathUtils.getBoolean(segment, "OtherServiceInformation[Text[starts-with(., " + "'VVIP')]]/Text");
            // tid
            String tid = TransUtils.getTid(documentNumber, documentType, "HSD");

            // 用户信息维表
            UserDimModel userDimModel = new UserDimModel();
            // 用户信息概要维表(中间表)
            UserDimSummaryModel summaryModel = new UserDimSummaryModel();

            // 查询用户概要表
            String querySql = "SELECT \n"
                    + "CN_NAME_SOURCE\n"
                    + ",EN_NAME_SOURCE\n"
                    + ",SEX_SOURCE\n"
                    + ",USER_TYPE_SOURCE\n"
                    + ",BIRTHDAY_SOURCE\n"
                    + ",NATIONALITY_SOURCE\n"
                    + " FROM "
                    + Constants.DIM_DB
                    + ".T_DIM_USER_DIM_SUMMARY WHERE PK_ID='" + tid + "'";
            logger.info("HSD UserDimTrans querySql:{}", querySql);
            //中文名数据来源
            String cnNameSource = null;
            //英文名数据来源
            String enNameSource = null;
            //性别来源
            String genderSource = null;
            //用户类型来源
            String userTypeSource = null;
            //生日数据来源
            String birthdaySource = null;
            //国籍数据来源
            String nationalitySource = null;
            // 为每个查询创建独立的连接
            try (Connection localConn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DIM_USER,
                    Constants.DIM_PWD);
                 Statement stmtDim = localConn.createStatement();
                 ResultSet resultSet = stmtDim.executeQuery(querySql)) {
                logger.info("HSD UserDimTrans querySql:{}, resultSet:{}", querySql, resultSet);
                try {
                    if (resultSet != null && resultSet.next()) {
                        cnNameSource = resultSet.getString("CN_NAME_SOURCE");
                        enNameSource = resultSet.getString("EN_NAME_SOURCE");
                        genderSource = resultSet.getString("SEX_SOURCE");
                        userTypeSource = resultSet.getString("USER_TYPE_SOURCE");
                        birthdaySource = resultSet.getString("BIRTHDAY_SOURCE");
                        nationalitySource = resultSet.getString("NATIONALITY_SOURCE");
                    }
                } catch (Exception e) {
                    try {
                        resultSet.close();
                    } catch (SQLException ex) {
                        throw new RuntimeException(ex);
                    }
                    logger.error("HSD UserDimTrans error:{}", e.getMessage());
                }
            } catch (SQLException e) {
                logger.error("HSD IDChangeFactTrans bookingSegQuerySql error:{}", e.getMessage());
            }
            userDimModel.setPkId(tid);
            // 方便在json中处理进行IDMapping
            userDimModel.setIdCard(NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,
                    SM4Utils.encrypt(documentNumber, Constants.SM4_KEY)));
            userDimModel.setUserType(NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, documentType,
                    DataSource.HIGH_FREQUENCY_DATA));

            summaryModel.setPkId(tid);
            String currentDateTime = DateTimeUtils.getCurrentDateTime();
            // 客户基础信息涉及事件
            if (basic_customer_infos_events.contains(event)) {
                //处理中文名
                if (StringUtils.isNotBlank(nativeGivenName)) {
                    if (StringUtils.isNotBlank(cnNameSource)) {
                        boolean overWrite = UserDimPriorityLevel.cnName(cnNameSource, "HSD");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            userDimModel.setCnName(nativeGivenName);
                            summaryModel.setCnNameSource("HSD");
                            summaryModel.setCnNameUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原中文姓名为空，直接使用新数据覆盖
                        userDimModel.setCnName(nativeGivenName);
                        summaryModel.setCnNameSource("HSD");
                        summaryModel.setCnNameUpdatetime(currentDateTime);
                    }
                }
                //处理英文名
                if (StringUtils.isNotBlank(enName)) {
                    if (StringUtils.isNotBlank(enNameSource)) {
                        boolean overWrite = UserDimPriorityLevel.enName(enNameSource, "HSD");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            userDimModel.setEnName(enName);
                            summaryModel.setEnNameSource("HSD");
                            summaryModel.setEnNameUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原英文姓名为空，直接使用新数据覆盖
                        userDimModel.setEnName(enName);
                        summaryModel.setEnNameSource("HSD");
                        summaryModel.setEnNameUpdatetime(currentDateTime);
                    }
                }
                // 处理性别
                if (StringUtils.isNotBlank(gender)) {
                    if (StringUtils.isNotBlank(genderSource)) {
                        boolean overWrite = UserDimPriorityLevel.sex(genderSource, "HSD");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            userDimModel.setSex(gender);
                            summaryModel.setSexSource("HSD");
                            summaryModel.setSexUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原性别为空，直接使用新数据覆盖
                        userDimModel.setSex(gender);
                        summaryModel.setSexSource("HSD");
                        summaryModel.setSexUpdatetime(currentDateTime);
                    }
                }
                // 用户类型
                if (StringUtils.isNotBlank(travellerType)) {
                    if (StringUtils.isNotBlank(userTypeSource)) {
                        boolean overWrite = UserDimPriorityLevel.userType(userTypeSource, "HSD");
                        if (overWrite) {
                            userDimModel.setUserType(travellerType);
                            summaryModel.setUserTypeSource("HSD");
                            summaryModel.setUserTypeUpdatetime(currentDateTime);
                        }
                    } else {
                        userDimModel.setUserType(travellerType);
                        summaryModel.setUserTypeSource("HSD");
                        summaryModel.setUserTypeUpdatetime(currentDateTime);
                    }
                }
                //处理生日
                if (StringUtils.isNotBlank(dateOfBirth)) {
                    if (StringUtils.isNotBlank(birthdaySource)) {
                        boolean overWrite = UserDimPriorityLevel.handleBirthday(birthdaySource, "HSD");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            userDimModel.setBirthday(dateOfBirth);
                            summaryModel.setBirthdaySource("HSD");
                            summaryModel.setBirthdayUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原生日为空，直接使用新数据覆盖
                        userDimModel.setBirthday(dateOfBirth);
                        summaryModel.setBirthdaySource("HSD");
                        summaryModel.setBirthdayUpdatetime(currentDateTime);
                    }
                }
                //处理国籍
                if (StringUtils.isNotBlank(nationalityCountry)) {
                    if (StringUtils.isNotBlank(nationalitySource)) {
                        boolean overWrite = UserDimPriorityLevel.nationality(nationalitySource, "HSD");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            userDimModel.setNationality(nationalityCountry);
                            summaryModel.setNationalitySource("HSD");
                            summaryModel.setNationalityUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原国籍为空，直接使用新数据覆盖
                        userDimModel.setNationality(nationalityCountry);
                        summaryModel.setNationalitySource("HSD");
                        summaryModel.setNationalityUpdatetime(currentDateTime);
                    }
                }
            }
            // 大客户号涉及事件
            if (key_account_events.equals(event)) {
                userDimModel.setKeyAccountNumber(keyAccountNumber);
                summaryModel.setKeyAccountNumberSource("HSD");
                summaryModel.setKeyAccountNumberUpdatetime(currentDateTime);
            }
            // VVIP标识涉及事件
            if (vvip_events.equals(event)) {
                userDimModel.setVvipFlag(vvip);
                summaryModel.setVvipFlagSource("HSD");
                summaryModel.setVvipFlagUpdatetime(currentDateTime);
            }
            // 特殊旅客标识(聋、哑旅客)
            if (special_mark_events.equals(event)) {
                // 盲人旅客标识
                userDimModel.setBlindPassenger(blindPassenger);
                summaryModel.setBlindPassengerSource("HSD");
                summaryModel.setBlindPassengerUpdatetime(currentDateTime);
                // 失聪旅客标识
                userDimModel.setDeafPassenger(deafPassenger);
                summaryModel.setDeafPassengerSource("HSD");
                summaryModel.setDeafPassengerUpdatetime(currentDateTime);
            }
            userDimModel.setUpdateTime(currentDateTime);

            HsdProcessDataModel hsdProcessDataModel = new HsdProcessDataModel();
            hsdProcessDataModel.setEvent(event);
            hsdProcessDataModel.setSubEvent(subEvent);
            hsdProcessDataModel.setTableName("T_DIM_USER_DIM");
            hsdProcessDataModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
            hsdProcessDataModel.setStamp(stamp);
            // 转换为JSON字符串
            hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(userDimModel));
            hsdProcessDataModel.setProcessed(false);
            hsdProcessDataModel.setUptateTime(LocalDateTime.now().toString());
            if (StringUtils.isNotEmpty(userDimModel.getPkId()) ) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
            }else{
                logger.info("高频-数据整合用户维表[UserDimModel]数据解析异常{}",value);
            }

            hsdProcessDataModel.setTableName("T_DIM_USER_DIM_SUMMARY");
            // 转换为JSON字符串
            hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(summaryModel));
            hsdProcessDataModel.setUptateTime(LocalDateTime.now().toString());
            if (StringUtils.isNotEmpty(summaryModel.getPkId())) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
            }else{
                logger.info("高频-数据整合用户维表[UserDimSummaryModel]数据解析异常{}",value);
            }
        }
    }*/

    public static void processUserDim(String value, ProcessFunction<String, Void>.Context ctx, Document document) {
        // 事件名称
        String event = XpathUtils.getString(document, "/Msg/Hdr/Event");
        logger.info("高频数据整合用户维表");
        // 子事件名称
        String subEvent = XpathUtils.getString(document, "/Msg/Hdr/Subevent");
        String uptm = XpathUtils.getString(document, "/Msg/Hdr/Uptmms");
        String stamp = XpathUtils.getString(document, "/Msg/Hdr/Stamp");
        // 证件类型
        String documentType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Type");
        // 证件号
        String documentNumber = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Number");
        // 乘机人类型
        String travellerType = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/@Type");
        // 乘机人英文姓
        String surname = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Surname");
        // 乘机人英文名
        String givenName = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/GivenName");
        String enName = NormalizationUtils.standardize(FieldType.EN_NAME, surname + givenName);
        // 乘机人中文姓名
        String nativeGivenName = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/NativeGivenName");
        nativeGivenName = NormalizationUtils.standardize(FieldType.CN_NAME, nativeGivenName);
        // 性别
        String gender = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@Gender");
        // 出生日期
        String dateOfBirth = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@DateOfBirth");
        // 国籍
        String nationalityCountry = XpathUtils.getString(document, "/Msg/Dat/PassengerSegment/Traveller/Document/@NationalityCountry");

        // 获取所有 Segment 节点
        NodeList segmentList = XpathUtils.getNodeList(document, "/Msg/Dat/PassengerSegment/Segment");
        for (int a = 0; a < segmentList.getLength(); a++) {
            Node segment = segmentList.item(a);
            // 盲人旅客标识
            Boolean blindPassenger = XpathUtils.getBoolean(segment, "SpecialServiceRequest/SSRCode='BLND'");
            // 失聪旅客标识
            Boolean deafPassenger = XpathUtils.getBoolean(segment, "SpecialServiceRequest/SSRCode='DEAF'");
            // 大客户号
            String keyAccountNumber = TransUtils.getKeyAccount(segment);
            // vvip
            Boolean vvip = XpathUtils.getBoolean(segment, "OtherServiceInformation[Text[starts-with(., " + "'VVIP')]]/Text");
            // tid
            String tid = TransUtils.getTid(documentNumber, documentType, "HSD");

            // 用户信息维表
            UserDimModel userDimModel = new UserDimModel();

            userDimModel.setPkId(tid);
            // 方便在json中处理进行IDMapping
            userDimModel.setIdCard(NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,
                    SM4Utils.encrypt(documentNumber, Constants.SM4_KEY)));
            // 临时存放证件类型
            userDimModel.setHighTravelerType(NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, documentType,
                    DataSource.HIGH_FREQUENCY_DATA));


            String currentDateTime = DateTimeUtils.getCurrentDateTime();
            // 客户基础信息涉及事件
            if (basic_customer_infos_events.contains(event)) {
                //处理中文姓名
                userDimModel.setCnName(nativeGivenName);
                //处理英文名
                userDimModel.setEnName(enName);
                // 处理性别
                userDimModel.setSex(gender);
                // 用户类型
                userDimModel.setUserType(travellerType);
                //处理生日
                userDimModel.setBirthday(dateOfBirth);
                //处理国籍
                userDimModel.setNationality(nationalityCountry);

            }
            // 大客户号涉及事件
            if (key_account_events.equals(event)) {
                userDimModel.setKeyAccountNumber(keyAccountNumber);
            }
            // VVIP标识涉及事件
            if (vvip_events.equals(event)) {
                userDimModel.setVvipFlag(vvip);
            }
            // 特殊旅客标识(聋、哑旅客)
            if (special_mark_events.equals(event)) {
                // 盲人旅客标识
                userDimModel.setBlindPassenger(blindPassenger);
                // 失聪旅客标识
                userDimModel.setDeafPassenger(deafPassenger);
            }
            userDimModel.setCreateTime(currentDateTime);
            userDimModel.setUpdateTime(currentDateTime);

            HsdProcessDataModel hsdProcessDataModel = new HsdProcessDataModel();
            hsdProcessDataModel.setEvent(event);
            hsdProcessDataModel.setSubEvent(subEvent);
            hsdProcessDataModel.setTableName("T_DIM_USER_DIM");
            hsdProcessDataModel.setUptm(TransUtils.parseUptmToTimestamp(uptm));
            hsdProcessDataModel.setStamp(stamp);
            // 转换为JSON字符串
            hsdProcessDataModel.setContent(CustomJsonSerializer.toJsonWithAnnotationsOnly(userDimModel));
            hsdProcessDataModel.setProcessed(false);
            hsdProcessDataModel.setUpdateTime(LocalDateTime.now().toString());
            if (StringUtils.isNotEmpty(userDimModel.getPkId()) ) {
                ctx.output(CommonOutputTags.HSD_PROCESS_DATA_TAG, hsdProcessDataModel);
            }else{
                logger.info("高频-数据整合用户维表[UserDimModel]数据解析异常{}",value);
            }

        }
    }
}
