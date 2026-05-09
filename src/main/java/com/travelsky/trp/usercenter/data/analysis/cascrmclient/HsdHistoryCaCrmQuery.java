package com.travelsky.trp.usercenter.data.analysis.cascrmclient;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.cascrmclient.memberinfoquerynew.Credential;
import com.travelsky.trp.usercenter.data.analysis.cascrmclient.memberinfoquerynew.CredentialList;
import com.travelsky.trp.usercenter.data.analysis.cascrmclient.memberinfoquerynew.Member;
import com.travelsky.trp.usercenter.data.analysis.cascrmclient.querycard.MemberInfo;
import com.travelsky.trp.usercenter.data.analysis.cascrmclient.querymemberext.*;
import com.travelsky.trp.usercenter.data.analysis.cascrmclient.utils.CaCrmQueryUtils;
import com.travelsky.trp.usercenter.data.analysis.cascrmclient.utils.CertificateTypeConverter;
import com.travelsky.trp.usercenter.data.analysis.model.dim.*;
import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections4.map.MultiValueMap;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.checkerframework.checker.units.qual.K;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static com.travelsky.trp.usercenter.data.analysis.cascrmclient.utils.CaCrmQueryUtils.*;

public class HsdHistoryCaCrmQuery {
        static final Logger logger = LoggerFactory.getLogger(HsdHistoryCaCrmQuery.class);
        private static Statement stmtDwd = null;
        private static Connection connDwd = null;
        private static final int BATCH_SIZE = 100000;

        public static void main(String[] args) throws Exception {
            if (args.length < 1) {
                /*加日志：etl_date参数为空*/
                System.exit(0);
            }
            String startNo = args[0];

            //try {
            //    Multimap<String, String> map = ArrayListMultimap.create();
            //    connDwd = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
            //    stmtDwd = DorisUtils.getStatement(connDwd);
            //    String querySql = "SELECT \n"
            //            + "t.CERT_NUMBER,\n"
            //            + "t.CERT_TYPE\n"
            //            + " FROM \n"
            //            + Constants.DWD_DB
            //            + ".T_DWD_DEPART_SEG_FACT t"
            //            + " LEFT JOIN "
            //            + Constants.DWD_DB
            //            + ".T_DIM_FFP_CERT_CHECK_DIM c ON t.CERT_NUMBER = c.CREDENTIALNUM"
            //            + " WHERE c.CREDENTIALNUM is null\n"
            //            +" AND t.CERT_NUMBER IS NOT NULL \n"
            //            +" AND t.CERT_NUMBER != ''\n"
            //            +" AND t.FF_AIRLINE = 'CA'\n"
            //            +" AND t.CERT_TYPE IS NOT NULL\n"
            //            +" AND t.FFRF IS NOT NULL \n"
            //            +" AND t.FFRF != ''\n"
            //            +" AND t.DATA_ACTIVE = 1";
            //    ResultSet rs = DorisUtils.getDorisResult(stmtDwd, querySql);
            //    try {
            //        while (rs != null && rs.next()) {
            //            String certNo = rs.getString("CERT_NUMBER");
            //            String idType = rs.getString("CERT_TYPE");
            //            idType = CertificateTypeConverter.convertCertificateType(certNo, idType);
            //            if (StringUtils.isBlank(certNo) || StringUtils.isBlank(idType)) {
            //                logger.error("提取CaCrmQuery失败，证件号证件类型为空: {}", SM4Utils.encrypt(certNo, Constants.SM4_KEY) +" | " + idType);
            //            } else {
            //                map.put(certNo, idType);
            //            }
            //        }
            //    } catch (SQLException e) {
            //        e.printStackTrace();
            //    } finally {
            //        DorisUtils.close(connDwd, stmtDwd, rs);
            //    }
            //    CaCrmQueryUtils.CrmQueryFun(map, "HSD");
            //} catch (Exception e) {
            //    e.printStackTrace();
            //}
            processDataBySerialNo(startNo);

        }

        public static void processDataBySerialNo(String startNo) throws Exception {

                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                CheckpointUtils.setCheckpoint(env, "HsdHistoryCaCrmQuery" + "HSD");
                env.setParallelism(1);
                final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
                int end = Integer.parseInt(startNo) + BATCH_SIZE;
                //注册数据源表
                tEnv.executeSql(CrmTableSql.getQueryDwdCrm());
                String querySql = "SELECT \n"
                        + "SERIAL_NO,\n"
                        + "CERT_NUMBER,\n"
                        + "CERT_TYPE\n"
                        + " FROM \n"
                        + " T_DWD_DEPART_SEG_FACT_CRM\n"
                        + " WHERE SERIAL_NO BETWEEN " + startNo + " AND " + end;
            Table table = tEnv.sqlQuery(querySql);
            System.out.println("起始序号"+ startNo + "，结束序号" +  end);
            // 转换为 DataStream<Map<String, String>>，生成tid并做标准化
            DataStream<Tuple4<List<FfpDimModel>, List<FfpCertCheckDimModel>, List<CertDimModel>, List<UserDimModel>>> source = tEnv.toChangelogStream(table)
                    .map(new RichMapFunction<Row, Tuple4<List<FfpDimModel>, List<FfpCertCheckDimModel>, List<CertDimModel>, List<UserDimModel>>>() {
                        Timer timer;

                        @Override
                        public void open(Configuration parameters) throws Exception {
                            super.open(parameters);
                            timer = IdMapping.autoCommit();
                        }

                        @Override
                        public void close() throws Exception {
                            super.close();
                            timer.cancel();
                            IdMapping.close();
                        }

                        @Override
                        public Tuple4<List<FfpDimModel>, List<FfpCertCheckDimModel>, List<CertDimModel>, List<UserDimModel>> map(Row row) throws Exception {
                            List<FfpDimModel> ffpDimModels = new ArrayList<>();
                            List<FfpCertCheckDimModel> ffpCertCheckDimModels = new ArrayList<>();
                            List<CertDimModel> certDimModels = new ArrayList<>();
                            List<UserDimModel> userDimModels = new ArrayList<>();
                            String certNo = row.getFieldAs("CERT_NUMBER");
                            String idType = row.getFieldAs("CERT_TYPE");
                            idType = CertificateTypeConverter.convertCertificateType(certNo, idType);
                            // 1、会员卡号查询接口——通过证件号查询用户信息
                            List<MemberInfo> memberInfoList = queryCardFun(certNo, idType);
                            MemberInfo cardMemberInfo = (memberInfoList != null && !memberInfoList.isEmpty()) ? memberInfoList.get(0) : null;
                            if (cardMemberInfo == null) {
                                return null;
                            }
                            String memberNumber = cardMemberInfo.getMemberNumber();
                            Member memberInfo = memberInfoQueryNewFun(memberNumber);
                            AdditionInfo additionInfo = queryMemberExtFun(memberNumber);
                            CredentialList credentialList = memberInfo == null ? null : memberInfo.getCredentialList();
                            List<Credential> credential = credentialList == null ? null :credentialList.getCredential();
                            memberNumber = SM4Utils.encrypt(memberNumber, Constants.SM4_KEY);
                            memberNumber = NormalizationUtils.standardize(FieldType.FREQUENT_FLYER_NUM, memberNumber, DataSource.FREQUENT_FLYER);

                            String certEncrypt = SM4Utils.encrypt(certNo, Constants.SM4_KEY);
                            // idmapping
                            Tid tid = new Tid();
                            tid.setTid(certEncrypt);
                            Map<String, String> certification = new HashMap<>();

                            tid.setFrequentTravelerCardno(memberNumber);
                            String currentDateTime = DateTimeUtils.getCurrentDateTime();
                            // 常客卡级别
                            String primaryTierName = org.apache.commons.lang3.StringUtils.isNotBlank(memberInfo.getLifttimeTierENUDesc()) ? memberInfo.getLifttimeTierENUDesc() : memberInfo.getPrimaryTierName();
                            primaryTierName = CertificateTypeConverter.getCardCode(primaryTierName);
                            FfpDimModel ffpDimModel = new FfpDimModel();
                            ffpDimModel.setSystemKey(memberNumber);
                            ffpDimModel.setFfrf(memberNumber);
                            ffpDimModel.setFfLevel(primaryTierName);
                            ffpDimModel.setParentCardNumber(SM4Utils.encrypt(memberInfo.getParentMemberNum(), Constants.SM4_KEY));
                            ffpDimModel.setCreateTime(currentDateTime);
                            ffpDimModel.setUpdateTime(currentDateTime);
                            if (credential != null && !credential.isEmpty()) {
                                for (Credential credentialInfo : credential) {
                                    FfpCertCheckDimModel ffpCertCheckDimModel = new FfpCertCheckDimModel();
                                    // 证件号
                                    String credentialNum = credentialInfo.getCredentialNum();
                                    credentialNum = SM4Utils.encrypt(credentialNum, Constants.SM4_KEY);
                                    credentialNum = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, credentialNum, DataSource.FREQUENT_FLYER);

                                    // 主键  常客卡号+证件号的加密值
                                    ffpCertCheckDimModel.setSystemKey(memberNumber + credentialNum);

                                    // 常客卡号
                                    ffpCertCheckDimModel.setMemberNumber(memberNumber);

                                    ffpCertCheckDimModel.setMemberNumberType(primaryTierName);
                                    // 中文姓名
                                    String nameCn = memberInfo.getCnLastName() + memberInfo.getCnFirstName();
                                    nameCn = NormalizationUtils.standardize(FieldType.CN_NAME, nameCn, DataSource.FREQUENT_FLYER);
                                    ffpCertCheckDimModel.setNameCn(nameCn);
                                    // 英文姓名
                                    String nameEn = memberInfo.getLastName() + memberInfo.getFirstName();
                                    nameEn = NormalizationUtils.standardize(FieldType.EN_NAME, nameEn, DataSource.FREQUENT_FLYER);
                                    ffpCertCheckDimModel.setNameEn(nameEn);
                                    // 主要手机号
                                    String primaryPhoneId = additionInfo == null ? "" :  additionInfo.getPrimaryPhoneId();
                                    String phoneNum = Optional.ofNullable(additionInfo)
                                            .map(AdditionInfo::getPhoneList)
                                            .map(PhoneList::getPhone)
                                            .filter(list -> !list.isEmpty())
                                            .flatMap(list -> list.stream()
                                                    .filter(phone -> primaryPhoneId.equals(phone.getPhoneId()))
                                                    .findFirst())
                                            .map(Phone::getPhoneNum)
                                            .orElse("");
                                    phoneNum = SM4Utils.encrypt(phoneNum, Constants.SM4_KEY);
                                    phoneNum = NormalizationUtils.standardize(FieldType.MOBILE_NO, phoneNum, DataSource.FREQUENT_FLYER);
                                    ffpCertCheckDimModel.setMobile(phoneNum);
                                    // 主地址根据PrimaryAddressId找到AddressList中的State+City+Street
                                    String primaryAddressId = additionInfo == null ? "" :additionInfo.getPrimaryAddressId();
                                    String addressStr = Optional.ofNullable(additionInfo)
                                            .map(AdditionInfo::getAddressList)
                                            .map(AddressList::getAddress)
                                            .filter(list -> !list.isEmpty())
                                            .flatMap(list -> list.stream()
                                                    .filter(address -> address.getAddressId().equals(primaryAddressId))
                                                    .findFirst())
                                            .map(address -> address.getState() + address.getCity() + address.getStreet())
                                            .orElse("");

                                    ffpCertCheckDimModel.setAddress(SM4Utils.encrypt(addressStr, Constants.SM4_KEY));
                                    // 主邮箱
                                    String primaryEmailId = additionInfo == null ? "" : additionInfo.getPrimaryEmailId();
                                    String email = Optional.ofNullable(additionInfo)
                                            .map(AdditionInfo::getEmailList)
                                            .map(EmailList::getEmail)
                                            .flatMap(list -> list.stream()
                                                    .filter(address -> address.getEmailId().equals(primaryEmailId))
                                                    .findFirst()) // 找到第一个匹配的地址
                                            .map(Email::getEmailAddr)
                                            .orElse(null);
                                    ffpCertCheckDimModel.setEmail(SM4Utils.encrypt(email, Constants.SM4_KEY));
                                    // 证件类型
                                    String credentialType = credentialInfo.getCredentialType();
                                    credentialType = CertificateTypeConverter.convertCertificateType2(credentialType);
                                    ffpCertCheckDimModel.setCredentialType(credentialType);
                                    certification.put(credentialType, credentialNum);
                                    // 证件号
                                    ffpCertCheckDimModel.setCredentialNum(credentialNum);
                                    // 父母常客卡号
                                    ffpCertCheckDimModel.setParentMemberNum(SM4Utils.encrypt(memberInfo.getParentMemberNum(), Constants.SM4_KEY));

                                    ffpCertCheckDimModel.setCreateTime(currentDateTime);
                                    ffpCertCheckDimModel.setUpdateTime(currentDateTime);
                                    ffpCertCheckDimModels.add(ffpCertCheckDimModel);
                                }
                            }
                            tid.setCertification(certification);
                            String tidStr = IdMapping.idMappingFunction(tid, "HSD", false,true,true);
                            ffpDimModel.settId(tidStr);
                            ffpDimModels.add(ffpDimModel);
                            certification.forEach((certType, certNum) -> {
                                CertDimModel certDimModel = new CertDimModel();
                                certDimModel.setCertType(certType);
                                certDimModel.setCertNumber(certNum);
                                certDimModel.settId(tidStr);
                                certDimModel.setCurrentCertHighestPriority(1);
                                certDimModel.setSystemKey(tidStr + certType + certNum);
                                certDimModel.setUpdateTime(currentDateTime);
                                certDimModel.setCreateTime(currentDateTime);
                                certDimModels.add(certDimModel);
                            });
                            UserDimModel userDimModel = new UserDimModel();
                            userDimModel.setPkId(tidStr);
                            userDimModel.setFrequentFlyerNumberVerified(true);
                            userDimModel.setUpdateTime(currentDateTime);
                            userDimModel.setCreateTime(currentDateTime);
                            userDimModels.add(userDimModel);

                            return new Tuple4<>(ffpDimModels, ffpCertCheckDimModels, certDimModels, userDimModels);
                        }
                    });
            // 获取类型信息，解决类型擦除问题
            TypeInformation<List<FfpDimModel>> ffpDimTypeInfo = TypeExtractor.getForObject(new ArrayList<FfpDimModel>());
            TypeInformation<List<FfpCertCheckDimModel>> ffpCertCheckDimTypeInfo = TypeExtractor.getForObject(new ArrayList<FfpCertCheckDimModel>());
            TypeInformation<List<CertDimModel>> certDimTypeInfo = TypeExtractor.getForObject(new ArrayList<CertDimModel>());
            TypeInformation<List<UserDimModel>> UserDimTypeInfo = TypeExtractor.getForObject(new ArrayList<UserDimModel>());

            // 使用 returns() 方法显式指定类型信息，并先过滤掉空元组避免空指针
            DataStream<Tuple4<List<FfpDimModel>, List<FfpCertCheckDimModel>, List<CertDimModel>, List<UserDimModel>>> nonNullSource = source.filter(Objects::nonNull); // 过滤空结果避免后续处理出现空指针
            SinkToFfpDim(nonNullSource.map(tuple -> tuple.f0).returns(ffpDimTypeInfo)); // 将常旅客维度数据入库
            SinkToFfpCertCheckDim(nonNullSource.map(tuple -> tuple.f1).returns(ffpCertCheckDimTypeInfo)); // 将常旅客证件校验维度数据入库
            SinkToCertDim(nonNullSource.map(tuple -> tuple.f2).returns(certDimTypeInfo)); // 将证件维度数据入库
            SinkToUserDim(nonNullSource.map(tuple -> tuple.f3).returns(UserDimTypeInfo)); // 将用户维度数据入库
            env.execute("HsdHistoryCaCrmQuery");
            IdMapping.close();

        }

        public static void connCheck() throws SQLException {
            if (null == connDwd || connDwd.isClosed()) {
                System.out.println("create database connection:" + Thread.currentThread().getId() + "," + Thread.currentThread().getName());
                connDwd = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
            }
            if (null == stmtDwd || stmtDwd.isClosed()) {
                System.out.println("create database statement:" + Thread.currentThread().getId() + "," + Thread.currentThread().getName());
                stmtDwd = DorisUtils.getStatement(connDwd);
            }

        }
    }

