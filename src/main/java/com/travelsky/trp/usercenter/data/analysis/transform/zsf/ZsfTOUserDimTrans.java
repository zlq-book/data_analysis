package com.travelsky.trp.usercenter.data.analysis.transform.zsf;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimSummaryModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;

public class ZsfTOUserDimTrans {


    static final Logger logger = LoggerFactory.getLogger(ZsfTOUserDimTrans.class);

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

        DataStream<Tuple2<UserDimModel, UserDimSummaryModel>> resultStream = rowDataStream.map(new RichMapFunction<Row, Tuple2<UserDimModel, UserDimSummaryModel>>() {
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
            public Tuple2<UserDimModel, UserDimSummaryModel> map(Row row) throws Exception {
                UserDimModel model = new UserDimModel();
                UserDimSummaryModel summaryModel = new UserDimSummaryModel();

                String certNo = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, SM4Utils.encrypt(toStringSafe(row.getField("CERT_NO")), Constants.SM4_KEY), DataSource.ZHANGSHANGFEI);
                String certType = NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, toStringSafe(row.getField("CERT_TYPE")), DataSource.ZHANGSHANGFEI);
                //2、中文名字
                String cnLastName = toStringSafe(row.getField("CN_LAST_NAME"));
                String cnFirstName = toStringSafe(row.getField("CN_FIRST_NAME"));
                String cnName = cnLastName + cnFirstName;
                //中文姓名标准化
                cnName = NormalizationUtils.standardize(FieldType.CN_NAME, cnName, DataSource.ZHANGSHANGFEI);

                //2、英文名字
                String enLastName = toStringSafe(row.getField("EN_LAST_NAME"));
                String enFirstName = toStringSafe(row.getField("EN_FIRST_NAME"));
                //英文名
                String enName = enLastName + "/" + enFirstName;
                //英文名标准化
                enName = NormalizationUtils.standardize(FieldType.EN_NAME, enName, DataSource.ZHANGSHANGFEI);
                //5、生日
                String birthdayStr = toStringSafe(row.getField("BIRTHDAY"));
                if (birthdayStr != null && !birthdayStr.trim().isEmpty()) {
                    birthdayStr = LocalDate.parse(birthdayStr).toString();
                }
                //6、性别
                String gender = toStringSafe(row.getField("SEX"));

                Tid tid = new Tid();
                tid.setTid(certNo);
                HashMap<String, String> map = new HashMap<>();
                map.put(certType, certNo);
                tid.setCertification(map);
                String pkid = IdMapping.idMappingFunction(tid, "ZSF");

                String querySql = "SELECT \n" +
                        "CN_NAME_SOURCE\n" +
                        ",EN_NAME_SOURCE\n" +
                        ",PROVINCE_SOURCE\n" +
                        ",SYS_REGISTER_ID_SOURCE\n" +
                        ",CITY_SOURCE\n" +
                        ",NATIONALITY_SOURCE\n" +
                        ",EMPLOYER_SOURCE\n" +
                        ",MOBILE_PHONE_SOURCE\n" +
                        ",EMAIL_SOURCE\n" +
                        ",ID_CARD_SOURCE\n" +
                        ",PASSPORT_SOURCE\n" +
                        ",SEAMAN_ID_SOURCE\n" +
                        ",ALIEN_PERMIT_SOURCE\n" +
                        ",DIPLOMATIC_STAFF_CERTIFICATE_SOURCE\n" +
                        ",PERMANENT_RESIDENT_ID_SOURCE\n" +
                        ",CIVILIAN_STAFF_ID_SOURCE\n" +
                        ",STAFF_ID_SOURCE\n" +
                        ",OFFICER_ID_CARD_SOURCE\n" +
                        ",ARMED_POLICE_OFFICER_SOURCE\n" +
                        ",ARMED_POLICE_SOLDIER_SOURCE\n" +
                        ",CIVILIAN_OFFICIAL_ID_SOURCE\n" +
                        ",CONSCRIPT_SOLDIER_ID_SOURCE\n" +
                        ",NON_COMMISSIONED_OFFICER_ID_SOURCE\n" +
                        ",HK_MACAO_RESIDENT_PERMIT_SOURCE\n" +
                        ",TAIWAN_RESIDENT_TRAVEL_PERMIT_SOURCE\n" +
                        ",FREQUENT_TRAVELER_CARDNO_SOURCE\n" +
                        ",FT_REGISTER_TIME_SOURCE\n" +
                        ",ACCEPT_SMS_MARKETING_SOURCE\n" +
                        ",ACCEPT_EMAIL_MARKETING_SOURCE\n" +
                        ",PARENT_FT_CARDNO_SOURCE\n" +
                        ",SYS_REGISTER_ID\n" +
                        " FROM " + Constants.DIM_DB + ".T_DIM_USER_DIM_SUMMARY T1\n" +
                        " LEFT JOIN " + Constants.DIM_DB + ".T_DIM_USER_DIM T2 ON T1.PK_ID=T2.PK_ID\n" +
                        " WHERE T1.PK_ID='" + pkid + "'";
                ResultSet resultSet = DorisUtils.getDorisResult(stmtDim, querySql);

                model.setPkId(pkid);
                summaryModel.setPkId(pkid);
                String currentDateTime = DateTimeUtils.getCurrentDateTime();

                //中文名数据来源
                String cnNameSource = null;
                //英文名数据来源
                String enNameSource = null;
                // 生日来源
                String birthdaySource = null;
                // 性别来源
                String genderSource = null;

                try {
                    if (resultSet.next()) {
                        cnNameSource = resultSet.getString("CN_NAME_SOURCE");
                        enNameSource = resultSet.getString("EN_NAME_SOURCE");
//                        birthdaySource = resultSet.getString("BIRTHDAY_SOURCE");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                //处理中文名
                if (StringUtils.isNotBlank(cnName)) {
                    if (StringUtils.isNotBlank(cnNameSource)) {
                        boolean overWrite = UserDimPriorityLevel.cnName(cnNameSource, "ZSF");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            model.setCnName(cnName);
                            summaryModel.setCnNameSource("ZSF");
                            summaryModel.setCnNameUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原中文姓名为空，直接使用新数据覆盖
                        model.setCnName(cnName);
                        summaryModel.setCnNameSource("ZSF");
                        summaryModel.setCnNameUpdatetime(currentDateTime);
                    }
                }
                //处理英文名
                if (StringUtils.isNotBlank(enName)) {
                    if (StringUtils.isNotBlank(enNameSource)) {
                        boolean overWrite = UserDimPriorityLevel.enName(enNameSource, "ZSF");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            model.setEnName(enName);
                            summaryModel.setEnNameSource("HYTD");
                            summaryModel.setEnNameUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原英文姓名为空，直接使用新数据覆盖
                        model.setEnName(enName);
                        summaryModel.setEnNameSource("ZSF");
                        summaryModel.setEnNameUpdatetime(currentDateTime);
                    }
                }
                // 处理生日
                if (StringUtils.isNotBlank(birthdayStr)) {
                    if (StringUtils.isNotBlank(birthdaySource)) {
                        boolean overWrite = UserDimPriorityLevel.handleBirthday(birthdaySource, "ZSF");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            model.setBirthday(birthdayStr);
                            summaryModel.setBirthdaySource("ZSF");
                            summaryModel.setBirthdayUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原英文姓名为空，直接使用新数据覆盖
                        model.setBirthday(birthdayStr);
                        summaryModel.setBirthdaySource("ZSF");
                        summaryModel.setBirthdayUpdatetime(currentDateTime);
                    }
                }

                // 处理性别
                if (StringUtils.isNotBlank(gender)) {
                    if (StringUtils.isNotBlank(genderSource)) {
                        boolean overWrite = UserDimPriorityLevel.sex(genderSource, "ZSF");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            model.setBirthday(gender);
                            summaryModel.setSexSource("ZSF");
                            summaryModel.setSexUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原英文姓名为空，直接使用新数据覆盖
                        model.setBirthday(gender);
                        summaryModel.setSexSource("ZSF");
                        summaryModel.setSexUpdatetime(currentDateTime);
                    }
                }

                //6、是否抖音次卡购买人
                model.setDouyinCardPurchaser(false);
                Tuple2<UserDimModel, UserDimSummaryModel> userTuple =
                        Tuple2.of(model, summaryModel);
                return userTuple;
            }
        });
        DorisSink<UserDimModel> userDimModelDorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_USER_DIM");
        DorisSink<UserDimSummaryModel> userDimSummaryModelDorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_USER_DIM_SUMMARY");
        //数据写入doris
        DataStream<UserDimModel> userDimModelDataStream = resultStream.map(tuple -> tuple.f0);
        userDimModelDataStream.sinkTo(userDimModelDorisSink);
        resultStream.map(tuple -> tuple.f1).sinkTo(userDimSummaryModelDorisSink);

    }
}
