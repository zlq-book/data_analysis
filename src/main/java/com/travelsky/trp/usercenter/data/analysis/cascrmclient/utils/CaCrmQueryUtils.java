package com.travelsky.trp.usercenter.data.analysis.cascrmclient.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Multimap;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.cascrmclient.SecurityHandler;
import com.travelsky.trp.usercenter.data.analysis.cascrmclient.memberinfoquerynew.*;
import com.travelsky.trp.usercenter.data.analysis.cascrmclient.querycard.MemberInfo;
import com.travelsky.trp.usercenter.data.analysis.cascrmclient.querycard.*;
import com.travelsky.trp.usercenter.data.analysis.cascrmclient.querymemberext.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.ws.BindingProvider;
import javax.xml.ws.handler.Handler;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import static com.travelsky.dataplatform.constans.Constants.*;

public class CaCrmQueryUtils {
    static final Logger logger = LoggerFactory.getLogger(CaCrmQueryUtils.class);
    public static void CrmQueryFun(Multimap<String, String> queryMap, String idMappingRegion) {
        if (queryMap.isEmpty()) {
            return;
        }
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointUtils.setCheckpoint(env, "CaCrmQueryUtils" + idMappingRegion);
        env.setParallelism(1);
        try {
            List<FfpDimModel> ffpDimModels = new ArrayList<>();
            List<FfpCertCheckDimModel> ffpCertCheckDimModels = new ArrayList<>();
            List<CertDimModel> certDimModels = new ArrayList<>();

            for (Map.Entry<String, String> entry : queryMap.entries()) {
                String certNo = entry.getKey();
                String idType = entry.getValue();
                setModels(ffpDimModels, ffpCertCheckDimModels, certDimModels, certNo, idType, idMappingRegion);
            }
            // 写入常客信息维表
            sinkToFfpDim(ffpDimModels,  env);
            // 写入用户维表-常客卡号证件信息校验表
            sinkToFfpCertCheckDim(ffpCertCheckDimModels, env);
            // 写入证件维表
            sinkToCertDim(certDimModels, env);
            if (!ffpDimModels.isEmpty() || !certDimModels.isEmpty() || !ffpCertCheckDimModels.isEmpty()) {
                env.execute("CaCrmQuery");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void sinkToCertDim(List<CertDimModel> certDimModels, StreamExecutionEnvironment env) {
        if (certDimModels.isEmpty()) {
            return;

        }
        // 直接将 List 转换为 DataStream 并写入 Doris
        DataStreamSource<CertDimModel> certDimStream = env.fromCollection(certDimModels);
        // 写入Doris
        DorisSink<CertDimModel> dorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_CERT_DIM");
        certDimStream.sinkTo(dorisSink);
    }

    private static void setModels(List<FfpDimModel> ffpDimModels, List<FfpCertCheckDimModel> ffpCertCheckDimModels,  List<CertDimModel> certDimModels, String certNo, String idType, String idMappingRegion) throws MalformedURLException {
        // 1、会员卡号查询接口——通过证件号查询用户信息
        List<MemberInfo> memberInfoList = queryCardFun(certNo, idType);
        MemberInfo cardMemberInfo = (memberInfoList != null && !memberInfoList.isEmpty()) ? memberInfoList.get(0) : null;
        if (cardMemberInfo == null) {
            return;
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
        String primaryTierName = memberInfo.getLifttimeTierENUDesc();
        primaryTierName = CertificateTypeConverter.getCardCode(primaryTierName);
        FfpDimModel ffpDimModel = new FfpDimModel();
        ffpDimModel.setSystemKey(memberNumber);
        ffpDimModel.setFfrf(memberNumber);
        ffpDimModel.setFfLevel(primaryTierName);
        ffpDimModel.setParentCardNumber(SM4Utils.encrypt(memberInfo.getParentMemberNum(), Constants.SM4_KEY));
        ffpDimModel.setCreateTime(currentDateTime);
        ffpDimModel.setUpdateTime(currentDateTime);
        ffpDimModels.add(ffpDimModel);
        if (credential != null && !credential.isEmpty()) {
            for (Credential credentialInfo : credential) {
                FfpCertCheckDimModel ffpCertCheckDimModel = new FfpCertCheckDimModel();
                CertDimModel certDimModel = new CertDimModel();
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
        String tidStr = IdMapping.idMappingFunction(tid, idMappingRegion);
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

    }

    private static void sinkToFfpDim(List<FfpDimModel> ffpDimModels, StreamExecutionEnvironment env) {
        if (ffpDimModels.isEmpty()) {
            return;
        }
        // 直接将 List 转换为 DataStream 并写入 Doris
        DataStreamSource<FfpDimModel> ffpDimStream = env.fromCollection(ffpDimModels);
        // 写入Doris
        DorisSink<FfpDimModel> dorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_FFP_DIM");
        ffpDimStream.sinkTo(dorisSink);
    }

    private static void sinkToFfpCertCheckDim(List<FfpCertCheckDimModel> ffpCertCheckDimModels, StreamExecutionEnvironment env) {
        if (ffpCertCheckDimModels.isEmpty()) {
            return;
        }
        // 直接将 List 转换为 DataStream 并写入 Doris
        DataStreamSource<FfpCertCheckDimModel> ffpCertCheckDimStream = env.fromCollection(ffpCertCheckDimModels);
        // 写入Doris
        DorisSink<FfpCertCheckDimModel> dorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_FFP_CERT_CHECK_DIM");
        ffpCertCheckDimStream.sinkTo(dorisSink);
    }


    public static AdditionInfo queryMemberExtFun(String memberNumber) throws MalformedURLException {
        // 检查URL是否为空或null，避免MalformedURLException
        //if (StringUtils.isBlank(CRM_QUERY_MEMBER_EXT_URL)) {
        //    logger.error("CRM_QUERY_MEMBER_EXT_URL配置为空，无法创建URL");
        //    throw new MalformedURLException("CRM_QUERY_MEMBER_EXT_URL配置为空");
        //}
        QueryMemberExtServiceImplService memberExtServiceImplService = new QueryMemberExtServiceImplService();
        QueryMemberExtService port = memberExtServiceImplService.getQueryMemberExtServicePort();
        BindingProvider bindingProvider = (BindingProvider) port;
        getBp(bindingProvider);
        QueryMemberExtInputEntity input = new QueryMemberExtInputEntity();
        input.setChannel(CA_CRM_CHANNEL);
        input.setSubChannel(CA_CRM_SUBCHANNEL);
        input.setSCChannel(CA_CRM_SC_CHANNEL);
        input.setSCSubChannel(CA_CRM_SC_SUB_CHANNEL);
        input.setTradeId(String.valueOf(UUID.randomUUID()));
        input.setMemberNumber(memberNumber);
        QueryMemberExtOutputEntity result = port.getQueryMemberExtInfo(input);
        if (result.getErrorCode().equals("0") && result.getListOfCrmLoyMemberMoreInfo() != null && result.getListOfCrmLoyMemberMoreInfo().getAdditionInfo() != null && result.getListOfCrmLoyMemberMoreInfo().getAdditionInfo() != null) {
            return result.getListOfCrmLoyMemberMoreInfo().getAdditionInfo();
        } else if ("-2".equals(result.getErrorCode())) {
            logger.error("queryMemberExtFun查询不存在");
            return null;
        } else {
            logger.error("queryMemberExtFun查询失败，错误码：{}，错误描述：{}", result.getErrorCode(), result.getErrorDesc());
            return  null;
        }
    }


    public static Member memberInfoQueryNewFun(String memberNumber) throws MalformedURLException {
        // 检查URL是否为空或null，避免MalformedURLException
        //if (StringUtils.isBlank(CRM_MEMBER_INFO_QUERY_NEW_URL)) {
        //    logger.error("CRM_MEMBER_INFO_QUERY_NEW_URL配置为空，无法创建URL");
        //    throw new MalformedURLException("CRM_MEMBER_INFO_QUERY_NEW_URL配置为空");
        //}
        MemberInfoQueryNewServiceImplService memberInfoQueryNewService = new MemberInfoQueryNewServiceImplService();
        MemberInfoQueryNewService port = memberInfoQueryNewService.getMemberInfoQueryNewServicePort();
        BindingProvider bindingProvider = (BindingProvider) port;
        getBp(bindingProvider);
        MemberInfoQueryNewInputEntity input = new MemberInfoQueryNewInputEntity();
        input.setChannel(CA_CRM_CHANNEL);
        input.setSubChannel(CA_CRM_SUBCHANNEL);
        input.setSCChannel(CA_CRM_SC_CHANNEL);
        input.setSCSubChannel(CA_CRM_SC_SUB_CHANNEL);
        input.setTradeId(String.valueOf(UUID.randomUUID()));
        input.setMemberNumber(memberNumber);
        MemberInfoQueryNewOutputEntity result = port.getMemberInfoNew(input);
        if (result.getErrorCode().equals("0") && result.getListOfCrmLoyMemberNew() != null && result.getListOfCrmLoyMemberNew().getMemberInfo() != null) {
           return result.getListOfCrmLoyMemberNew().getMemberInfo().getMember();
        } else if ("-2".equals(result.getErrorCode())) {
            logger.error("memberInfoQueryNewFun查询用户信息不存在");
            return null;
        } else {
            logger.error("memberInfoQueryNewFun查询用户信息失败，错误码：{}，错误描述：{}", result.getErrorCode(), result.getErrorDesc());
            return null;
        }
    }

    public static List<MemberInfo> queryCardFun(String cardNum, String idType) throws MalformedURLException {
        // 检查URL是否为空或null，避免MalformedURLException
        //if (StringUtils.isBlank(CRM_QUERY_CARD_URL)) {
        //    logger.error("CRM_QUERY_CARD_URL配置为空，无法创建URL");
        //    throw new MalformedURLException("CRM_QUERY_CARD_URL配置为空");
        //}
        QueryCardServiceImplService queryCardService = new QueryCardServiceImplService();
        QueryCardService queryCardServicePort = queryCardService.getQueryCardServicePort();
        BindingProvider bindingProvider = (BindingProvider) queryCardServicePort;
        getBp(bindingProvider);
        QueryCardInputEntity input = new QueryCardInputEntity();
        input.setChannel(CA_CRM_CHANNEL);
        input.setSubChannel(CA_CRM_SUBCHANNEL);
        input.setSCChannel(CA_CRM_SC_CHANNEL);
        input.setSCSubChannel(CA_CRM_SC_SUB_CHANNEL);
        input.setTradeId(String.valueOf(UUID.randomUUID()));
        input.setCredentialType(idType);
        input.setCredentialNum(cardNum);
        input.setCountryCode("");
        input.setMobileNum("");
        input.setEmail("");
        QueryCardOutputEntity result = queryCardServicePort.getQueryCardInfo(input);
        if (result != null && "0".equals(result.getErrorCode()) && result.getMemberInfoList() != null) {
            return result.getMemberInfoList().getMemberInfo();
        } else if ("-2".equals(result.getErrorCode())) {
            logger.error("会员不存在: {}", SM4Utils.encrypt(cardNum, Constants.SM4_KEY) +" | " + idType);
            return null;
        } else {
            logger.error("queryCardFun查询用户信息失败，错误码：{}，错误描述：{}", result.getErrorCode(), result.getErrorDesc());
            return null;
        }
    }

    private static BindingProvider getBp(BindingProvider bindingProvider) {
        // 添加 Handler
        List<Handler> handlerChain = new ArrayList<>();
        handlerChain.add(new SecurityHandler());
        bindingProvider.getBinding().setHandlerChain(handlerChain);
        return bindingProvider;
    }


    public static void SinkToCertDim(DataStream<List<CertDimModel>> sourceStream) {

        SingleOutputStreamOperator<CertDimModel> certDimModelDataStream = sourceStream
                .flatMap(new FlatMapFunction<List<CertDimModel>, CertDimModel>() {
                             @Override
                             public void flatMap(List<CertDimModel> certDimModels, Collector<CertDimModel> collector) throws Exception {
                                 for (CertDimModel certDimModel : certDimModels) {
                                     collector.collect(certDimModel);
                                 }
                             }
                         }
                );
        //  创建 Doris Sink 并写入
        DorisSink<CertDimModel> dorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_CERT_DIM");
        //数据写入doris
        certDimModelDataStream.sinkTo(dorisSink);
    }

    public static void SinkToUserDim(DataStream<List<UserDimModel>> sourceStream) {

        SingleOutputStreamOperator<UserDimModel> userDimModelDataStream = sourceStream
                .flatMap(new FlatMapFunction<List<UserDimModel>, UserDimModel>() {
                             @Override
                             public void flatMap(List<UserDimModel> userDimModels, Collector<UserDimModel> collector) throws Exception {
                                 for (UserDimModel userDimModel : userDimModels) {
                                     collector.collect(userDimModel);
                                 }
                             }
                         }
                );
        //  创建 Doris Sink 并写入
        DorisSink<UserDimModel> dorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_USER_DIM");
        //数据写入doris
        userDimModelDataStream.sinkTo(dorisSink);
    }

    public static void SinkToFfpCertCheckDim(DataStream<List<FfpCertCheckDimModel>> sourceStream) {

        SingleOutputStreamOperator<FfpCertCheckDimModel> ffpCertCheckDimModelDataStream = sourceStream
                .flatMap(new FlatMapFunction<List<FfpCertCheckDimModel>, FfpCertCheckDimModel>() {
                             @Override
                             public void flatMap(List<FfpCertCheckDimModel> ffpCertCheckDimModels, Collector<FfpCertCheckDimModel> collector) throws Exception {
                                 for (FfpCertCheckDimModel ffpCertCheckDimModel : ffpCertCheckDimModels) {
                                     collector.collect(ffpCertCheckDimModel);
                                 }
                             }
                         }
                );
        //  创建 Doris Sink 并写入
        DorisSink<FfpCertCheckDimModel> dorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_FFP_CERT_CHECK_DIM");
        //数据写入doris
        ffpCertCheckDimModelDataStream.sinkTo(dorisSink);
    }

    public static void SinkToFfpDim(DataStream<List<FfpDimModel>> sourceStream) {
        // ffpDimModelList 转 DataStream<FfpDimModel>
        SingleOutputStreamOperator<FfpDimModel> ffpDimModelDataStream = sourceStream
                .flatMap(new FlatMapFunction<List<FfpDimModel>, FfpDimModel>() {
                    @Override
                    public void flatMap(List<FfpDimModel> ffpDimModels, Collector<FfpDimModel> collector) throws Exception {
                        for (FfpDimModel ffpDimModel : ffpDimModels) {
                            collector.collect(ffpDimModel);
                        }
                    }
                }
        );

        //  创建 Doris Sink 并写入
        DorisSink<FfpDimModel> dorisSink = FlinkDorisUtils.creatDorisDIMSink("T_DIM_FFP_DIM");
        //数据写入doris
        ffpDimModelDataStream.sinkTo(dorisSink);
    }
}
