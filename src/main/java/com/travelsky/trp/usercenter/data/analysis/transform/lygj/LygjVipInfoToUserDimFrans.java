package com.travelsky.trp.usercenter.data.analysis.transform.lygj;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
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

public class LygjVipInfoToUserDimFrans {
    //private static final Logger logger = LoggerFactory.getLogger(LygjVipInfoToUserDimFrans.class);

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
                        ",PROVINCE_SOURCE\n" +
                        ",CITY_SOURCE\n" +
                        ",NATIONALITY_SOURCE\n" +
                        ",EMPLOYER_SOURCE\n" +
                        ",MOBILE_PHONE_SOURCE\n" +
                        ",EMAIL_SOURCE\n" +
                        ",ID_CARD_SOURCE\n" +
                        ",PASSPORT_SOURCE\n" +
                        ",SEAMAN_ID_SOURCE\n" +
                        ",SEX_SOURCE\n" +
                        ",USER_TYPE_SOURCE\n" +
                        ",BIRTHDAY_SOURCE\n" +
                        ",ETHNICITY_SOURCE\n" +
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
                //英文名数据来源
                String enNameSource = null;
                String sexSource = null;
                String userTypeSource = null;
                String birthdaySource = null;
                String ethnicitySource = null;
                String employerSource = null;
                //手机号数据来源
                String mobilePhoneSource = null;

                try {
                    if (resultSet.next()) {
                        // 从结果集中获取各字段的数据源
                        cnNameSource = resultSet.getString("CN_NAME_SOURCE");
                        enNameSource = resultSet.getString("EN_NAME_SOURCE");
                        sexSource = resultSet.getString("SEX_SOURCE");
                        userTypeSource = resultSet.getString("USER_TYPE_SOURCE");
                        ethnicitySource = resultSet.getString("ETHNICITY_SOURCE");
                        employerSource = resultSet.getString("EMPLOYER_SOURCE");
                        mobilePhoneSource = resultSet.getString("MOBILE_PHONE_SOURCE");

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                model.setLyVipId(row.getString("ID"));
                String currentDateTime = DateTimeUtils.getCurrentDateTime();

                // 处理中文姓名
                String cnName = row.getString("NAME");
                if (StringUtils.isNotBlank(cnName) && cnName.matches(".*[\\u4e00-\\u9fa5]+.*")) {
                    cnName = NormalizationUtils.standardize(FieldType.CN_NAME, cnName, DataSource.LUYAN_STEWARD);
                    if (StringUtils.isNotBlank(cnNameSource)) {
                        boolean overWrite = UserDimPriorityLevel.cnName(cnNameSource, "GDLK");
                        if (overWrite) {
                            model.setCnName(cnName);
                            summaryModel.setCnNameSource("GDLK");
                            summaryModel.setCnNameUpdatetime(currentDateTime);
                        }
                    } else {
                        model.setCnName(cnName);
                        summaryModel.setCnNameSource("GDLK");
                        summaryModel.setCnNameUpdatetime(currentDateTime);
                    }
                }
                // 处理英文姓名
                if (StringUtils.isNotBlank(cnName) && !cnName.matches(".*[\\u4e00-\\u9fa5]+.*")) {
                    cnName = NormalizationUtils.standardize(FieldType.EN_NAME, cnName, DataSource.LUYAN_STEWARD);
                    if (StringUtils.isNotBlank(sexSource)) {
                        boolean overWrite = UserDimPriorityLevel.enName(enNameSource, "GDLK");
                        if (overWrite) {
                            model.setEnName(cnName);
                            summaryModel.setEnNameSource("GDLK");
                            summaryModel.setEnNameUpdatetime(currentDateTime);
                        }
                    } else {
                        model.setEnName(cnName);
                        summaryModel.setEnNameSource("GDLK");
                    }

                }
                // 处理性别
                String gender = row.getString("GENDER");
                if (StringUtils.isNotBlank(gender)) {
                    if (StringUtils.isNotBlank(sexSource)) {
                        boolean overWrite = UserDimPriorityLevel.sex(sexSource, "GDLV");
                        if (overWrite) {
                            model.setSex(gender);
                            summaryModel.setSexSource("GDLV");
                            summaryModel.setSexUpdatetime(currentDateTime);
                        }
                    } else {
                        model.setSex(gender);
                        summaryModel.setSexSource("GDLV");
                        summaryModel.setSexUpdatetime(currentDateTime);
                    }
                }
                // 处理用户类型
                //String certificateType = row.getString("CERTIFICATE_TYPE");
                //if (StringUtils.isNotBlank(certificateType)) {
                //    if (StringUtils.isNotBlank(userTypeSource)) {
                //        boolean overWrite = UserDimPriorityLevel.userType(userTypeSource, "GDLV");
                //        if (overWrite) {
                //            model.setUserType(certificateType);
                //            summaryModel.setUserTypeSource("GDLV");
                //            summaryModel.setUserTypeUpdatetime(currentDateTime);
                //        }
                //    } else {
                //        model.setUserType(certificateType);
                //        summaryModel.setUserTypeSource("GDLV");
                //        summaryModel.setUserTypeUpdatetime(currentDateTime);
                //    }
                //}

                // 处理民族
                String nationality = row.getString("NATIONALITY");
                if (StringUtils.isNotBlank(nationality)) {
                    if (StringUtils.isNotBlank(ethnicitySource)) {
                        boolean overWrite = UserDimPriorityLevel.ethnicity(ethnicitySource, "GDLK");
                        if (overWrite) {
                            model.setEthnicity(nationality);
                            summaryModel.setEthnicitySource("GDLK");
                            summaryModel.setEthnicityUpdatetime(currentDateTime);
                        }
                    } else {
                        model.setEthnicity(nationality);
                        summaryModel.setEthnicitySource("GDLK");
                        summaryModel.setEthnicityUpdatetime(currentDateTime);
                    }
                }


                //处理工作单位
                String company = row.getString("COMPANY");
                if (StringUtils.isNotBlank(company)) {
                    if (StringUtils.isNotBlank(employerSource)) {
                        boolean overWrite = UserDimPriorityLevel.employer(employerSource, "GDLK");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            model.setEmployer(company);
                            summaryModel.setEmployerSource("GDLK");
                            summaryModel.setEmployerUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原工作单位为空，直接使用新数据覆盖
                        model.setEmployer(company);
                        summaryModel.setEmployerSource("GDLK");
                        summaryModel.setEmployerUpdatetime(currentDateTime);
                    }
                }
                //处理手机号
                String phoneNum = row.getString("MOBILE");
                if (StringUtils.isNotBlank(phoneNum)) {
                    if (StringUtils.isNotBlank(mobilePhoneSource)) {
                        boolean overWrite = UserDimPriorityLevel.mobilePhone(mobilePhoneSource, "GDLK");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            model.setMobilePhone(phoneNum);
                            summaryModel.setMobilePhoneSource("GDLK");
                            summaryModel.setMobilePhoneUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原手机号为空，直接使用新数据覆盖
                        model.setMobilePhone(phoneNum);
                        summaryModel.setMobilePhoneSource("GDLK");
                        summaryModel.setMobilePhoneUpdatetime(currentDateTime);
                    }
                }


                // 是否高端旅客信息
                model.setHighTraveler(true);
                //String layerTypeId = row.getString("LAYER_TYPE_ID");
                //if (StringUtils.isNotBlank(layerTypeId)) {
                //    model.setHighTravelerTier(layerTypeId);
                //    summaryModel.setHighTravelerTierSource("GDLK");
                //    summaryModel.setHighTravelerTierUpdatetime(currentDateTime);
                //}
                //
                //String layerValidity = row.getString("LAYER_VALIDITY");
                //if (StringUtils.isNotBlank(layerValidity)) {
                //    model.setTierPrestigeExpiredate(layerValidity);
                //    model.setTierHonorExpiredate(layerValidity);
                //    summaryModel.setTierPrestigeExpiredateSource("GDLK");
                //    summaryModel.setTierPrestigeExpiredateUpdatetime(currentDateTime);
                //}
                //
                //String foodLoveIds = row.getString("VIP_FOOD_LOVE_IDS");
                //if (StringUtils.isNotBlank(foodLoveIds)) {
                //    model.setFoodPreference(foodLoveIds);
                //    summaryModel.setFoodPreferenceSource("GDLK");
                //    summaryModel.setFoodPreferenceUpdatetime(currentDateTime);
                //}
                //
                //String seatLove = row.getString("SEAT_LOVE");
                //if (StringUtils.isNotBlank(seatLove)) {
                //    model.setSeatPreference(seatLove);
                //    summaryModel.setSeatPreferenceSource("GDLK");
                //    summaryModel.setSeatPreferenceUpdatetime(currentDateTime);
                //}
                //
                //String drinkLoveIds = row.getString("VIP_DRINK_LOVE_IDS");
                //if (StringUtils.isNotBlank(drinkLoveIds)) {
                //    model.setBeveragePreference(drinkLoveIds);
                //    summaryModel.setBeveragePreferenceSource("GDLK");
                //    summaryModel.setBeveragePreferenceUpdatetime(currentDateTime);
                //}
                //
                //String vipSeatLoveNames = row.getString("VIP_SEAT_LOVE_NAMES");
                //if (StringUtils.isNotBlank(vipSeatLoveNames)) {
                //    model.setLongTermSeatPreference(vipSeatLoveNames);
                //    summaryModel.setLongTermSeatPreferenceSource("GDLK");
                //    summaryModel.setLongTermSeatPreferenceUpdatetime(currentDateTime);
                //}
                //
                //String vipLoungeLoveNames = row.getString("VIP_LOUNGE_LOVE_NAMES");
                //if (StringUtils.isNotBlank(vipLoungeLoveNames)) {
                //    model.setFirstClassLoungePreference(vipLoungeLoveNames);
                //    summaryModel.setFirstClassLoungePreferenceSource("GDLK");
                //    summaryModel.setFirstClassLoungePreferenceUpdatetime(currentDateTime);
                //}

                // 设置系统信息
                model.setCreateTime(currentDateTime);
                model.setUpdateTime(currentDateTime);

                Tuple2<UserDimModel, UserDimSummaryModel> userTuple =
                        Tuple2.of(model, summaryModel);
                return userTuple;
            }
        });

        // 创建 Doris Sink
        DorisSink<UserDimModel> userDimModelDorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_USER_DIM");
        DorisSink<UserDimSummaryModel> userDimSummaryModelDorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_USER_DIM_SUMMARY");
        //数据写入doris
        DataStream<UserDimModel> userDimModelDataStream = resultStream.map(tuple -> tuple.f0);
        userDimModelDataStream.sinkTo(userDimModelDorisSink);
        resultStream.map(tuple -> tuple.f1).

                sinkTo(userDimSummaryModelDorisSink);
    }
}
