package com.travelsky.trp.usercenter.data.analysis.transform.lygj;


import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.DorisUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.UserDimPriorityLevel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimSummaryModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 鲁雁管家写入用户维表
 */
public class LygjDpiToUserDimFrans {

    //private static final Logger logger = LoggerFactory.getLogger(LygjDpiToUserDimFrans.class);

    public static void result(DataStream<JSONObject> sourceStream) {
        DataStream<Tuple2<UserDimModel, UserDimSummaryModel>> resultStream = sourceStream.map(new RichMapFunction<JSONObject, Tuple2<UserDimModel, UserDimSummaryModel>>() {
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
            public Tuple2<UserDimModel, UserDimSummaryModel> map(JSONObject row) throws Exception {
                UserDimModel model = new UserDimModel();
                UserDimSummaryModel summaryModel = new UserDimSummaryModel();

                // 获取主键TID
                String tid = row.getString("TID");
                model.setPkId(tid);
                summaryModel.setPkId(tid);

                // 查询Doris中现有数据
                String querySql = "SELECT \n" +
                        "CN_NAME_SOURCE\n" +
                        ",EN_NAME_SOURCE\n" +
                        ",SEX_SOURCE\n" +
                        ",USER_TYPE_SOURCE\n" +
                        ",BIRTHDAY_SOURCE\n" +
                        ",PROVINCE_SOURCE\n" +
                        ",BLIND_PASSENGER_SOURCE\n" +
                        ",DEAF_PASSENGER_SOURCE\n" +
                        ",KEY_ACCOUNT_NUMBER_SOURCE\n" +
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
                        " FROM " + Constants.DIM_DB + ".T_DIM_USER_DIM_SUMMARY WHERE PK_ID='" + tid + "'";
                //logger.info("querySql:" + querySql);
                ResultSet resultSet = DorisUtils.getDorisResult(stmtDim, querySql);

                // 初始化各字段的数据源变量
                String cnNameSource = null;
                String enNameSource = null;
                String sexSource = null;
                String userTypeSource = null;
                String birthdaySource = null;
                //手机号数据来源
                String mobilePhoneSource = null;
                // 盲人标识数据来源
                String blindSource = null;
                // 失聪旅客标识数据来源
                String deafSource = null;
                // 大客户号数据来源
                String keyAccountNumberSource = null;


                try {
                    if (resultSet.next()) {
                        // 从结果集中获取各字段的数据源
                        cnNameSource = resultSet.getString("CN_NAME_SOURCE");
                        enNameSource = resultSet.getString("EN_NAME_SOURCE");
                        sexSource = resultSet.getString("SEX_SOURCE");
                        userTypeSource = resultSet.getString("USER_TYPE_SOURCE");
                        birthdaySource = resultSet.getString("BIRTHDAY_SOURCE");
                        mobilePhoneSource = resultSet.getString("MOBILE_PHONE_SOURCE");
                        blindSource = resultSet.getString("BLIND_PASSENGER_SOURCE");
                        deafSource = resultSet.getString("DEAF_PASSENGER_SOURCE");
                        keyAccountNumberSource = resultSet.getString("KEY_ACCOUNT_NUMBER_SOURCE");

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                String currentDateTime = DateTimeUtils.getCurrentDateTime();

                // 处理中文姓名
                String cnName = row.getString("PSG_NAME_CN");
                if (StringUtils.isNotBlank(cnName)) {
                    if (StringUtils.isNotBlank(cnNameSource)) {
                        boolean overWrite = UserDimPriorityLevel.cnName(cnNameSource, "LYGJ");
                        if (overWrite) {
                            model.setCnName(cnName);
                            summaryModel.setCnNameSource("LYGJ");
                            summaryModel.setCnNameUpdatetime(currentDateTime);
                        }
                    } else {
                        model.setCnName(cnName);
                        summaryModel.setCnNameSource("LYGJ");
                        summaryModel.setCnNameUpdatetime(currentDateTime);
                    }
                }
                // 处理英文姓名
                String enName = row.getString("PSG_NAME_EN");
                if (StringUtils.isNotBlank(enName)) {
                    if (StringUtils.isNotBlank(sexSource)) {
                        boolean overWrite = UserDimPriorityLevel.enName(enNameSource, "LYGJ");
                        if (overWrite) {
                            model.setEnName(enName);
                            summaryModel.setEnNameSource("LYGJ");
                            summaryModel.setEnNameUpdatetime(currentDateTime);
                        }
                    } else {
                        model.setEnName(enName);
                        summaryModel.setEnNameSource("LYGJ");
                    }

                }

                // 处理性别
                String gender = row.getString("GENDER");
                if (StringUtils.isNotBlank(gender)) {
                    if (StringUtils.isNotBlank(sexSource)) {
                        boolean overWrite = UserDimPriorityLevel.sex(sexSource, "LYGJ");
                        if (overWrite) {
                            model.setSex(gender);
                            summaryModel.setSexSource("LYGJ");
                            summaryModel.setSexUpdatetime(currentDateTime);
                        }
                    } else {
                        model.setSex(gender);
                        summaryModel.setSexSource("LYGJ");
                        summaryModel.setSexUpdatetime(currentDateTime);
                    }
                }

                // 处理用户类型
                // IS_CHILD：儿童标识
                // IS_INFANT：婴儿标识 非儿童和婴儿的不写用户类型字段
                String isChild = row.getString("IS_CHILD");
                String isInfant = row.getString("IS_INFANT");
                String certificateType = "Y".equals(isChild) ? "CHD" :
                        "1".equals(isInfant) ? "INF" : "";
                if (StringUtils.isNotBlank(certificateType)) {
                    if (StringUtils.isNotBlank(userTypeSource)) {
                        boolean overWrite = UserDimPriorityLevel.userType(userTypeSource, "LYGJ");
                        if (overWrite) {
                            model.setUserType(certificateType);
                            summaryModel.setUserTypeSource("LYGJ");
                            summaryModel.setUserTypeUpdatetime(currentDateTime);
                        }
                    } else {
                        model.setUserType(certificateType);
                        summaryModel.setUserTypeSource("LYGJ");
                        summaryModel.setUserTypeUpdatetime(currentDateTime);
                    }
                }

                // 处理生日
                String birthday = row.getString("BIRTHDAY");
                if (StringUtils.isNotBlank(birthday)) {
                    if (StringUtils.isNotBlank(birthdaySource)) {
                        boolean overWrite = UserDimPriorityLevel.handleBirthday(birthdaySource, "LYGJ");
                        if (overWrite) {
                            model.setBirthday(birthday);
                            summaryModel.setBirthdaySource("LYGJ");
                            summaryModel.setBirthdayUpdatetime(currentDateTime);
                        }
                    } else {
                        model.setBirthday(birthday);
                        summaryModel.setBirthdaySource("LYGJ");
                        summaryModel.setBirthdayUpdatetime(currentDateTime);
                    }
                }
                //处理手机号
                String phoneNum = row.getString("MOBILE_PHONE");
                if (StringUtils.isNotBlank(phoneNum)) {
                    if (StringUtils.isNotBlank(mobilePhoneSource)) {
                        boolean overWrite = UserDimPriorityLevel.mobilePhone(mobilePhoneSource, "LYGJ");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            model.setMobilePhone(phoneNum);
                            summaryModel.setMobilePhoneSource("LYGJ");
                            summaryModel.setMobilePhoneUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原手机号为空，直接使用新数据覆盖
                        model.setMobilePhone(phoneNum);
                        summaryModel.setMobilePhoneSource("LYGJ");
                        summaryModel.setMobilePhoneUpdatetime(currentDateTime);
                    }
                }
                // 盲人标识
                String blnd = row.getString("IS_BLND");
                String specialType = row.getString("SPECIAL_TYPE");
                Boolean blindPassenger = "4".equals(specialType) || "1".equals(blnd);
                if (StringUtils.isNotBlank(blindSource)) {
                    boolean overWrite = UserDimPriorityLevel.blindPassenger(blindSource, "LYGJ");
                    if (overWrite) {
                        //新数据优先级较高，使用新数据覆盖
                        model.setBlindPassenger(blindPassenger);
                        summaryModel.setBlindPassengerSource("LYGJ");
                        summaryModel.setBlindPassengerUpdatetime(currentDateTime);
                    }
                } else {
                    //用户维表原手机号为空，直接使用新数据覆盖
                    model.setBlindPassenger(blindPassenger);
                    summaryModel.setBlindPassengerSource("LYGJ");
                    summaryModel.setBlindPassengerUpdatetime(currentDateTime);
                }
                // 失聪旅客标识
                String deaf = row.getString("IS_DEAF");
                Boolean deafPassenger = "5".equals(specialType) || "1".equals(deaf);
                if (StringUtils.isNotBlank(deafSource)) {
                    // 优先级与盲人一致
                    boolean overWrite = UserDimPriorityLevel.blindPassenger(deafSource, "LYGJ");
                    if (overWrite) {
                        //新数据优先级较高，使用新数据覆盖
                        model.setDeafPassenger(deafPassenger);
                        summaryModel.setDeafPassengerSource("LYGJ");
                        summaryModel.setDeafPassengerUpdatetime(currentDateTime);
                    }
                } else {
                    //用户维表原手机号为空，直接使用新数据覆盖
                    model.setDeafPassenger(deafPassenger);
                    summaryModel.setDeafPassengerSource("LYGJ");
                    summaryModel.setDeafPassengerUpdatetime(currentDateTime);
                }
                // 大客户号
                String bigCustomer = row.getString("BIG_CUTNUM");
                if (StringUtils.isNotBlank(bigCustomer)) {
                    if (StringUtils.isNotBlank(keyAccountNumberSource)) {
                        // 优先级与盲人一致
                        boolean overWrite = UserDimPriorityLevel.blindPassenger(keyAccountNumberSource, "LYGJ");
                        if (overWrite) {
                            model.setKeyAccountNumber(bigCustomer);
                            summaryModel.setKeyAccountNumberSource("LYGJ");
                            summaryModel.setKeyAccountNumberUpdatetime(currentDateTime);
                            // 大客户标识
                            model.setKeyAccount(true);
                            summaryModel.setIsKeyAccountSource("LYGJ");
                            summaryModel.setIsKeyAccountUpdatetime(currentDateTime);
                        }
                    } else {
                        model.setKeyAccountNumber(bigCustomer);
                        summaryModel.setKeyAccountNumberSource("LYGJ");
                        summaryModel.setKeyAccountNumberUpdatetime(currentDateTime);
                        // 大客户标识
                        model.setKeyAccount(true);
                        summaryModel.setIsKeyAccountSource("LYGJ");
                        summaryModel.setIsKeyAccountUpdatetime(currentDateTime);
                    }
                }

                // 设置系统信息
                model.setCreateTime(currentDateTime);
                model.setUpdateTime(currentDateTime);

                Tuple2<UserDimModel, UserDimSummaryModel> userTuple =
                        Tuple2.of(model, summaryModel);
                return userTuple;
            }
        }).setParallelism(4);

        // 创建 Doris Sink
        DorisSink<UserDimModel> userDimModelDorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_USER_DIM");
        DorisSink<UserDimSummaryModel> userDimSummaryModelDorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_USER_DIM_SUMMARY");
        //数据写入doris
        DataStream<UserDimModel> userDimModelDataStream = resultStream.map(tuple -> tuple.f0);
        userDimModelDataStream.sinkTo(userDimModelDorisSink);
        resultStream.map(tuple -> tuple.f1).sinkTo(userDimSummaryModelDorisSink);
    }
}
