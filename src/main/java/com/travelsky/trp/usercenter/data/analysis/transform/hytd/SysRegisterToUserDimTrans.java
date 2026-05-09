package com.travelsky.trp.usercenter.data.analysis.transform.hytd;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimSummaryModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.CheckinSegFactModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

/**
 * @author kuangaihua
 * @date 2025/8/6 9:08
 */
public class SysRegisterToUserDimTrans {
    private static final Logger logger = LoggerFactory.getLogger(SysRegisterToUserDimTrans.class);

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
                //常客注册时间
                String createDate = row.getString("CREATE_DATE");
                // 常客卡号
                String memberNumber = row.getString("MEMBER_NUMBER");
                //性别
                String gender = row.getString("GENDER");
                //证件号
                String credentialNum = row.getString("CREDENTIAL_NUM");
                //证件类型
                String credentialType = row.getString("CREDENTIAL_TYPE");
                //生日
                String birthday = row.getString("BIRTHDAY");
                birthday = DateTimeUtils.dateFormatToDateTime(birthday, "MM/dd/yyyy");
                //国籍
                String nationality = row.getString("NATIONALITY");
                //联系语言
                String language = row.getString("LANGUAGE");
                //父母常客卡号
                String parentMemberNum = row.getString("PARENT_MEMBER_NUM");
                //会员天地用户ID
                String crmMemberId = row.getString("CRM_MEMBER_ID");
                //省份
                String state = row.getString("STATE");
                //城市
                String city = row.getString("CITY");
                //公司
                String company = row.getString("COMPANY");
                //电话号码
                String phoneNum = row.getString("PHONE_NUM");
                //邮箱
                String emailAddr = row.getString("EMAIL_ADDR");
                String cnName = row.getString("CN_NAME");
                String enName = row.getString("EN_NAME");
                String tid = row.getString("TID");
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
                        " WHERE T1.PK_ID='" + tid + "'";
//                logger.info("querySql:" + querySql);
                ResultSet resultSet = DorisUtils.getDorisResult(stmtDim, querySql);
                //中文名数据来源
                String cnNameSource = null;
                //英文名数据来源
                String enNameSource = null;
                //省份数据来源
                String provinceSource = null;
                //城市数据来源
                String citySource = null;
                //国籍数据来源
                String nationalitySource = null;
                // 工作单位数据来源
                String employerSource = null;
                //手机号数据来源
                String mobilePhoneSource = null;
                //邮箱数据来源
                String emailSource = null;
                //身份证号数据来源
                String idCardSource = null;
                //护照数据来源
                String passportSource = null;
                //海员证数据来源
                String seamanIdSource = null;
                //外国人出入境证数据来源
                String alienPermitSource = null;
                //外交部签发的驻华外交人员证数据来源
                String diplomaticStaffCertificateSource = null;
                //外国人永久居留证数据来源
                String permanentResidentIdSource = null;
                //文职人员证
                String civilianStaffIdSource = null;
                //职工证
                String staffIdSource = null;
                //军官证
                String officerIdCardSource = null;
                //武警警官证
                String armedPoliceOfficerSource = null;
                //武警士兵证
                String armedPoliceSoldierSource = null;
                //文职干部证
                String civilianOfficialIdSource = null;
                //义务兵证
                String conscriptSoldierIdSource = null;
                //士官证
                String nonCommissionedOfficerIdSource = null;
                //港澳居民来往内地通行证
                String hkMacaoResidentPermitSource = null;
                //台湾居民来往大陆通行证
                String taiwanResidentTravelPermitSource = null;
                //常旅客卡号
                String frequentTravelerCardnoSource = null;
                //常旅客注册时间
                String ftRegisterTimeSource = null;
                //会员天地ID
                String sysRegisterId = null;
                //会员天地IDSource
                String sysRegisterIdSource = null;
                try {
                    if (resultSet.next()) {
                        cnNameSource = resultSet.getString("CN_NAME_SOURCE");
                        enNameSource = resultSet.getString("EN_NAME_SOURCE");
                        provinceSource = resultSet.getString("PROVINCE_SOURCE");
                        citySource = resultSet.getString("CITY_SOURCE");
                        nationalitySource = resultSet.getString("NATIONALITY_SOURCE");
                        employerSource = resultSet.getString("EMPLOYER_SOURCE");
                        mobilePhoneSource = resultSet.getString("MOBILE_PHONE_SOURCE");
                        emailSource = resultSet.getString("EMAIL_SOURCE");
                        idCardSource = resultSet.getString("ID_CARD_SOURCE");
                        passportSource = resultSet.getString("PASSPORT_SOURCE");
                        seamanIdSource = resultSet.getString("SEAMAN_ID_SOURCE");
                        alienPermitSource = resultSet.getString("ALIEN_PERMIT_SOURCE");
                        diplomaticStaffCertificateSource = resultSet.getString("DIPLOMATIC_STAFF_CERTIFICATE_SOURCE");
                        permanentResidentIdSource = resultSet.getString("PERMANENT_RESIDENT_ID_SOURCE");
                        civilianStaffIdSource = resultSet.getString("CIVILIAN_STAFF_ID_SOURCE");
                        staffIdSource = resultSet.getString("STAFF_ID_SOURCE");
                        officerIdCardSource = resultSet.getString("OFFICER_ID_CARD_SOURCE");
                        armedPoliceOfficerSource = resultSet.getString("ARMED_POLICE_OFFICER_SOURCE");
                        armedPoliceSoldierSource = resultSet.getString("ARMED_POLICE_SOLDIER_SOURCE");
                        civilianOfficialIdSource = resultSet.getString("CIVILIAN_OFFICIAL_ID_SOURCE");
                        conscriptSoldierIdSource = resultSet.getString("CONSCRIPT_SOLDIER_ID_SOURCE");
                        nonCommissionedOfficerIdSource = resultSet.getString("NON_COMMISSIONED_OFFICER_ID_SOURCE");
                        hkMacaoResidentPermitSource = resultSet.getString("HK_MACAO_RESIDENT_PERMIT_SOURCE");
                        taiwanResidentTravelPermitSource = resultSet.getString("TAIWAN_RESIDENT_TRAVEL_PERMIT_SOURCE");
                        frequentTravelerCardnoSource = resultSet.getString("FREQUENT_TRAVELER_CARDNO_SOURCE");
                        ftRegisterTimeSource = resultSet.getString("FT_REGISTER_TIME_SOURCE");
                        sysRegisterId = resultSet.getString("SYS_REGISTER_ID");
                        sysRegisterIdSource = resultSet.getString("SYS_REGISTER_ID_SOURCE");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                model.setPkId(tid);
                summaryModel.setPkId(tid);
                String currentDateTime = DateTimeUtils.getCurrentDateTime();
                //处理中文名
                if (StringUtils.isNotBlank(cnName)) {
                    if (StringUtils.isNotBlank(cnNameSource)) {
                        boolean overWrite = UserDimPriorityLevel.cnName(cnNameSource, "HYTD");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            model.setCnName(cnName);
                            summaryModel.setCnNameSource("HYTD");
                            summaryModel.setCnNameUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原中文姓名为空，直接使用新数据覆盖
                        model.setCnName(cnName);
                        summaryModel.setCnNameSource("HYTD");
                        summaryModel.setCnNameUpdatetime(currentDateTime);
                    }
                }
                //处理英文名
                if (StringUtils.isNotBlank(enName)) {
                    if (StringUtils.isNotBlank(enNameSource)) {
                        boolean overWrite = UserDimPriorityLevel.enName(enNameSource, "HYTD");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            model.setEnName(enName);
                            summaryModel.setEnNameSource("HYTD");
                            summaryModel.setEnNameUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原英文姓名为空，直接使用新数据覆盖
                        model.setEnName(enName);
                        summaryModel.setEnNameSource("HYTD");
                        summaryModel.setEnNameUpdatetime(currentDateTime);
                    }
                }
                //处理省份
                if (StringUtils.isNotBlank(state)) {
                    if (StringUtils.isNotBlank(provinceSource)) {
                        boolean overWrite = UserDimPriorityLevel.province(provinceSource, "HYTD");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            model.setProvince(state);
                            summaryModel.setProvinceSource("HYTD");
                            summaryModel.setProvinceUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原省份为空，直接使用新数据覆盖
                        model.setProvince(state);
                        summaryModel.setProvinceSource("HYTD");
                        summaryModel.setProvinceUpdatetime(currentDateTime);
                    }
                }
                //处理城市
                if (StringUtils.isNotBlank(city)) {
                    if (StringUtils.isNotBlank(citySource)) {
                        boolean overWrite = UserDimPriorityLevel.city(citySource, "HYTD");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            model.setCity(city);
                            summaryModel.setCitySource("HYTD");
                            summaryModel.setCityUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原城市为空，直接使用新数据覆盖
                        model.setCity(city);
                        summaryModel.setCitySource("HYTD");
                        summaryModel.setCityUpdatetime(currentDateTime);
                    }
                }
                //处理国籍
                if (StringUtils.isNotBlank(nationality)) {
                    if (StringUtils.isNotBlank(nationalitySource)) {
                        boolean overWrite = UserDimPriorityLevel.nationality(nationalitySource, "HYTD");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            model.setNationality(nationality);
                            summaryModel.setNationalitySource("HYTD");
                            summaryModel.setNationalityUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原国籍为空，直接使用新数据覆盖
                        model.setNationality(nationality);
                        summaryModel.setNationalitySource("HYTD");
                        summaryModel.setNationalityUpdatetime(currentDateTime);
                    }
                }
                //处理邮箱
                if (StringUtils.isNotBlank(emailAddr)) {
                    if (StringUtils.isNotBlank(emailSource)) {
                        boolean overWrite = UserDimPriorityLevel.email(emailSource, "HYTD");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            model.setEmail(emailAddr);
                            summaryModel.setEmailSource("HYTD");
                            summaryModel.setEmailUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原邮箱为空，直接使用新数据覆盖
                        model.setEmail(emailAddr);
                        summaryModel.setEmailSource("HYTD");
                        summaryModel.setEmailUpdatetime(currentDateTime);
                    }
                }

                //处理工作单位
                if (StringUtils.isNotBlank(company)) {
                    if (StringUtils.isNotBlank(employerSource)) {
                        boolean overWrite = UserDimPriorityLevel.employer(emailSource, "HYTD");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            model.setEmployer(company);
                            summaryModel.setEmployerSource("HYTD");
                            summaryModel.setEmployerUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原工作单位为空，直接使用新数据覆盖
                        model.setEmployer(company);
                        summaryModel.setEmployerSource("HYTD");
                        summaryModel.setEmployerUpdatetime(currentDateTime);
                    }
                }
                //处理手机号
                if (StringUtils.isNotBlank(phoneNum)) {
                    if (StringUtils.isNotBlank(mobilePhoneSource)) {
                        boolean overWrite = UserDimPriorityLevel.mobilePhone(mobilePhoneSource, "HYTD");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            model.setMobilePhone(phoneNum);
                            summaryModel.setMobilePhoneSource("HYTD");
                            summaryModel.setMobilePhoneUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原手机号为空，直接使用新数据覆盖
                        model.setMobilePhone(phoneNum);
                        summaryModel.setMobilePhoneSource("HYTD");
                        summaryModel.setMobilePhoneUpdatetime(currentDateTime);
                    }
                }

                //处理常客卡号
                if (StringUtils.isNotBlank(memberNumber)) {
                    if (StringUtils.isNotBlank(frequentTravelerCardnoSource)) {
                        boolean overWrite = UserDimPriorityLevel.frequentTravelerCardno(frequentTravelerCardnoSource, "HYTD");
                        if (overWrite) {
                            //新数据优先级较高，使用新数据覆盖
                            model.setFrequentTravelerCardno(memberNumber);
                            summaryModel.setFrequentTravelerCardnoSource("HYTD");
                            summaryModel.setFrequentTravelerCardnoUpdatetime(currentDateTime);
                        }
                    } else {
                        //用户维表原常客卡号为空，直接使用新数据覆盖
                        model.setFrequentTravelerCardno(memberNumber);
                        summaryModel.setFrequentTravelerCardnoSource("HYTD");
                        summaryModel.setFrequentTravelerCardnoUpdatetime(currentDateTime);
                    }
                }
                //处理证件号和证件类型
                if (StringUtils.isNotBlank(credentialNum) && StringUtils.isNotBlank(credentialType)) {
                    if ("NI".equals(credentialType)) {
                        //身份证,获取性别和生日
                        String decryptDredentialNum = SM4Utils.decrypt(credentialNum, Constants.SM4_KEY);
                        birthday = IdCardUtils.getBirthday(decryptDredentialNum);
                        gender = IdCardUtils.getGender(decryptDredentialNum);
                        //身份证，比较优先级
                        if (StringUtils.isNotBlank(idCardSource)) {
                            boolean overWrite = UserDimPriorityLevel.credential(idCardSource, "HYTD");
                            if (overWrite) {
                                //新数据优先级较高，使用新数据覆盖
                                model.setIdCard(credentialNum);
                                summaryModel.setIdCardSource("HYTD");
                                summaryModel.setIdCardUpdatetime(currentDateTime);
                            }
                        } else {
                            //用户维表原身份证为空，直接使用新数据覆盖
                            model.setIdCard(credentialNum);
                            summaryModel.setIdCardSource("HYTD");
                            summaryModel.setIdCardUpdatetime(currentDateTime);
                        }
                    } else if ("PP".equals(credentialType)) {
                        //护照，比较优先级
                        if (StringUtils.isNotBlank(passportSource)) {
                            boolean overWrite = UserDimPriorityLevel.credential(passportSource, "HYTD");
                            if (overWrite) {
                                //新数据优先级较高，使用新数据覆盖
                                model.setPassport(credentialNum);
                                summaryModel.setPassportSource("HYTD");
                                summaryModel.setPassportUpdatetime(currentDateTime);
                            }
                        } else {
                            //用户维表原数据为空，直接使用新数据覆盖
                            model.setPassport(credentialNum);
                            summaryModel.setPassportSource("HYTD");
                            summaryModel.setPassportUpdatetime(currentDateTime);
                        }
                    } else if ("SC".equals(credentialType)) {
                        //海员证，比较优先级
                        if (StringUtils.isNotBlank(seamanIdSource)) {
                            boolean overWrite = UserDimPriorityLevel.credential(seamanIdSource, "HYTD");
                            if (overWrite) {
                                //新数据优先级较高，使用新数据覆盖
                                model.setSeamanId(credentialNum);
                                summaryModel.setSeamanIdSource("HYTD");
                                summaryModel.setSeamanIdUpdatetime(currentDateTime);
                            }
                        } else {
                            //用户维表原数据为空，直接使用新数据覆盖
                            model.setSeamanId(credentialNum);
                            summaryModel.setSeamanIdSource("HYTD");
                            summaryModel.setSeamanIdUpdatetime(currentDateTime);
                        }
                    } else if ("FR".equals(credentialType)) {
                        //外国人出入境证，比较优先级
                        if (StringUtils.isNotBlank(alienPermitSource)) {
                            boolean overWrite = UserDimPriorityLevel.credential(alienPermitSource, "HYTD");
                            if (overWrite) {
                                //新数据优先级较高，使用新数据覆盖
                                model.setAlienPermit(credentialNum);
                                summaryModel.setAlienPermitSource("HYTD");
                                summaryModel.setAlienPermitUpdatetime(currentDateTime);
                            }
                        } else {
                            //用户维表原数据为空，直接使用新数据覆盖
                            model.setAlienPermit(credentialNum);
                            summaryModel.setAlienPermitSource("HYTD");
                            summaryModel.setAlienPermitUpdatetime(currentDateTime);
                        }
                    } else if ("CD".equals(credentialType)) {
                        //外交部签发的驻华外交人员证
                        if (StringUtils.isNotBlank(diplomaticStaffCertificateSource)) {
                            boolean overWrite = UserDimPriorityLevel.credential(diplomaticStaffCertificateSource, "HYTD");
                            if (overWrite) {
                                model.setDiplomaticStaffCertificate(credentialNum);
                                summaryModel.setDiplomaticStaffCertificateSource("HYTD");
                                summaryModel.setDiplomaticStaffCertificateUpdatetime(currentDateTime);
                            }
                        } else {
                            model.setDiplomaticStaffCertificate(credentialNum);
                            summaryModel.setDiplomaticStaffCertificateSource("HYTD");
                            summaryModel.setDiplomaticStaffCertificateUpdatetime(currentDateTime);
                        }
                    } else if ("PR".equals(credentialType)) {
                        //外国人永久居留证
                        if (StringUtils.isNotBlank(permanentResidentIdSource)) {
                            boolean overWrite = UserDimPriorityLevel.credential(permanentResidentIdSource, "HYTD");
                            if (overWrite) {
                                model.setPermanentResidentId(credentialNum);
                                summaryModel.setPermanentResidentIdSource("HYTD");
                                summaryModel.setPermanentResidentIdUpdatetime(currentDateTime);
                            }
                        } else {
                            model.setPermanentResidentId(credentialNum);
                            summaryModel.setPermanentResidentIdSource("HYTD");
                            summaryModel.setPermanentResidentIdUpdatetime(currentDateTime);
                        }
                    } else if ("CS".equals(credentialType)) {
                        //文职人员证
                        if (StringUtils.isNotBlank(civilianStaffIdSource)) {
                            boolean overWrite = UserDimPriorityLevel.credential(civilianStaffIdSource, "HYTD");
                            if (overWrite) {
                                model.setCivilianStaffId(credentialNum);
                                summaryModel.setCivilianStaffIdSource("HYTD");
                                summaryModel.setCivilianStaffIdUpdatetime(currentDateTime);
                            }
                        } else {
                            model.setCivilianStaffId(credentialNum);
                            summaryModel.setCivilianStaffIdSource("HYTD");
                            summaryModel.setCivilianStaffIdUpdatetime(currentDateTime);
                        }
                    } else if ("SE".equals(credentialType)) {
                        //职工证
                        if (StringUtils.isNotBlank(staffIdSource)) {
                            boolean overWrite = UserDimPriorityLevel.credential(staffIdSource, "HYTD");
                            if (overWrite) {
                                model.setStaffId(credentialNum);
                                summaryModel.setStaffIdSource("HYTD");
                                summaryModel.setStaffIdUpdatetime(currentDateTime);
                            }
                        } else {
                            model.setStaffId(credentialNum);
                            summaryModel.setStaffIdSource("HYTD");
                            summaryModel.setStaffIdUpdatetime(currentDateTime);
                        }
                    } else if ("OF".equals(credentialType)) {
                        //军官证
                        if (StringUtils.isNotBlank(officerIdCardSource)) {
                            boolean overWrite = UserDimPriorityLevel.credential(officerIdCardSource, "HYTD");
                            if (overWrite) {
                                model.setOfficerIdCard(credentialNum);
                                summaryModel.setOfficerIdCardSource("HYTD");
                                summaryModel.setOfficerIdCardUpdatetime(currentDateTime);
                            }
                        } else {
                            model.setOfficerIdCard(credentialNum);
                            summaryModel.setOfficerIdCardSource("HYTD");
                            summaryModel.setOfficerIdCardUpdatetime(currentDateTime);
                        }
                    } else if ("WP".equals(credentialType)) {
                        //武警警官证
                        if (StringUtils.isNotBlank(armedPoliceOfficerSource)) {
                            boolean overWrite = UserDimPriorityLevel.credential(armedPoliceOfficerSource, "HYTD");
                            if (overWrite) {
                                model.setArmedPoliceOfficer(credentialNum);
                                summaryModel.setArmedPoliceOfficerSource("HYTD");
                                summaryModel.setArmedPoliceOfficerUpdatetime(currentDateTime);
                            }
                        } else {
                            model.setArmedPoliceOfficer(credentialNum);
                            summaryModel.setArmedPoliceOfficerSource("HYTD");
                            summaryModel.setArmedPoliceOfficerUpdatetime(currentDateTime);
                        }
                    } else if ("WS".equals(credentialType)) {
                        //武警士兵证
                        if (StringUtils.isNotBlank(armedPoliceSoldierSource)) {
                            boolean overWrite = UserDimPriorityLevel.credential(armedPoliceSoldierSource, "HYTD");
                            if (overWrite) {
                                model.setArmedPoliceSoldier(credentialNum);
                                summaryModel.setArmedPoliceSoldierSource("HYTD");
                                summaryModel.setArmedPoliceSoldierUpdatetime(currentDateTime);
                            }
                        } else {
                            model.setArmedPoliceSoldier(credentialNum);
                            summaryModel.setArmedPoliceSoldierSource("HYTD");
                            summaryModel.setArmedPoliceSoldierUpdatetime(currentDateTime);
                        }
                    } else if ("CW".equals(credentialType)) {
                        //文职干部证
                        if (StringUtils.isNotBlank(civilianOfficialIdSource)) {
                            boolean overWrite = UserDimPriorityLevel.credential(civilianOfficialIdSource, "HYTD");
                            if (overWrite) {
                                model.setCivilianOfficialId(credentialNum);
                                summaryModel.setCivilianOfficialIdSource("HYTD");
                                summaryModel.setCivilianOfficialIdUpdatetime(currentDateTime);
                            }
                        } else {
                            model.setCivilianOfficialId(credentialNum);
                            summaryModel.setCivilianOfficialIdSource("HYTD");
                            summaryModel.setCivilianOfficialIdUpdatetime(currentDateTime);
                        }
                    } else if ("VC".equals(credentialType)) {
                        //义务兵证
                        if (StringUtils.isNotBlank(conscriptSoldierIdSource)) {
                            boolean overWrite = UserDimPriorityLevel.credential(conscriptSoldierIdSource, "HYTD");
                            if (overWrite) {
                                model.setConscriptSoldierId(credentialNum);
                                summaryModel.setConscriptSoldierIdSource("HYTD");
                                summaryModel.setConscriptSoldierIdUpdatetime(currentDateTime);
                            }
                        } else {
                            model.setConscriptSoldierId(credentialNum);
                            summaryModel.setConscriptSoldierIdSource("HYTD");
                            summaryModel.setConscriptSoldierIdUpdatetime(currentDateTime);
                        }
                    } else if ("NC".equals(credentialType)) {
                        //士官证
                        if (StringUtils.isNotBlank(nonCommissionedOfficerIdSource)) {
                            boolean overWrite = UserDimPriorityLevel.credential(nonCommissionedOfficerIdSource, "HYTD");
                            if (overWrite) {
                                model.setNonCommissionedOfficerId(credentialNum);
                                summaryModel.setNonCommissionedOfficerIdSource("HYTD");
                                summaryModel.setNonCommissionedOfficerIdUpdatetime(currentDateTime);
                            }
                        } else {
                            model.setNonCommissionedOfficerId(credentialNum);
                            summaryModel.setNonCommissionedOfficerIdSource("HYTD");
                            summaryModel.setNonCommissionedOfficerIdUpdatetime(currentDateTime);
                        }
                    } else if ("RP".equals(credentialType)) {
                        //港澳居民来往内地通行证
                        if (StringUtils.isNotBlank(hkMacaoResidentPermitSource)) {
                            boolean overWrite = UserDimPriorityLevel.credential(hkMacaoResidentPermitSource, "HYTD");
                            if (overWrite) {
                                model.setHkMacaoResidentPermit(credentialNum);
                                summaryModel.setHkMacaoResidentPermitSource("HYTD");
                                summaryModel.setHkMacaoResidentPermitUpdatetime(currentDateTime);
                            }
                        } else {
                            model.setHkMacaoResidentPermit(credentialNum);
                            summaryModel.setHkMacaoResidentPermitSource("HYTD");
                            summaryModel.setHkMacaoResidentPermitUpdatetime(currentDateTime);
                        }
                    } else if ("TC".equals(credentialType)) {
                        //台湾居民来往大陆通行证
                        if (StringUtils.isNotBlank(taiwanResidentTravelPermitSource)) {
                            boolean overWrite = UserDimPriorityLevel.credential(taiwanResidentTravelPermitSource, "HYTD");
                            if (overWrite) {
                                model.setTaiwanResidentTravelPermit(credentialNum);
                                summaryModel.setTaiwanResidentTravelPermitSource("HYTD");
                                summaryModel.setTaiwanResidentTravelPermitUpdatetime(currentDateTime);
                            }
                        } else {
                            model.setTaiwanResidentTravelPermit(credentialNum);
                            summaryModel.setTaiwanResidentTravelPermitSource("HYTD");
                            summaryModel.setTaiwanResidentTravelPermitUpdatetime(currentDateTime);
                        }
                    }
                }
                //处理会员天地注册ID
                if (StringUtils.isNotBlank(crmMemberId)) {
                    if (StringUtils.isNotBlank(sysRegisterId)) {
                        if (!sysRegisterId.contains(crmMemberId)) {
                            //原该字段未包含 这个id，则添加在后面，逗号分隔
                            model.setSysRegisterId(sysRegisterId+","+crmMemberId);
                            summaryModel.setSysRegisterIdSource("HYTD");
                            summaryModel.setSysRegisterIdUpdatetime(currentDateTime);
                        }
                    } else {
                        //原来该字段为空
                        model.setSysRegisterId(crmMemberId);
                        summaryModel.setSysRegisterIdSource("HYTD");
                        summaryModel.setSysRegisterIdUpdatetime(currentDateTime);
                    }
                }
                if(resultSet !=null){
                    resultSet.close();
                }
                model.setFtRegisterTime(createDate);
                summaryModel.setFtRegisterTimeSource("HYTD");
                summaryModel.setFtRegisterTimeUpdatetime(currentDateTime);
                model.setSex(gender);
                summaryModel.setSexSource("HYTD");
                summaryModel.setSexUpdatetime(currentDateTime);
                model.setBirthday(birthday);
                summaryModel.setBirthdaySource("HYTD");
                summaryModel.setBirthdayUpdatetime(currentDateTime);
                model.setParentFtCardno(parentMemberNum);
                summaryModel.setParentFtCardnoSource("HYTD");
                summaryModel.setParentFtCardnoUpdatetime(currentDateTime);
                model.setCreateTime(currentDateTime);
                model.setUpdateTime(currentDateTime);
                Tuple2<UserDimModel, UserDimSummaryModel> userTuple =
                        Tuple2.of(model, summaryModel);
                return userTuple;
            }
        });
//        resultStream.print("userDim:");
        // 创建 Doris Sink
        DorisSink<UserDimModel> userDimModelDorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_USER_DIM");
        DorisSink<UserDimSummaryModel> userDimSummaryModelDorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_USER_DIM_SUMMARY");
        //数据写入doris
        DataStream<UserDimModel> userDimModelDataStream = resultStream.map(tuple -> tuple.f0);
        userDimModelDataStream.sinkTo(userDimModelDorisSink);
        resultStream.map(tuple -> tuple.f1).sinkTo(userDimSummaryModelDorisSink);

    }
}
