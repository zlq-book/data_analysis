package com.travelsky.trp.usercenter.data.analysis.transform.hytd;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;

import com.travelsky.trp.usercenter.data.analysis.utils.ChannelInfo;
import com.travelsky.trp.usercenter.data.analysis.utils.channel.CompositeChannelProcessor;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.MemberRegisterFactModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kuangaihua
 * @date 2025/8/6 9:08
 */
public class SysRegisterToMemberRegisterFactTrans {
    static final CompositeChannelProcessor channelProcessor = new CompositeChannelProcessor();

    public static void result(DataStream<JSONObject> sourceStream) {
        DataStream<MemberRegisterFactModel> memberRegisterFactModelDataStream = sourceStream.map(row -> {
            //DONE:事实表字段对不上，Model未更新

            MemberRegisterFactModel memberRegisterFactModel = new MemberRegisterFactModel();
            //常客卡号
            String memberNumber = row.getString("MEMBER_NUMBER");
            if (StringUtils.isBlank(memberNumber)){
                //常客卡号为空，数据丢弃
                return null;
            }
            //注册日期，需要从时间里面获取日期
            String createDate = row.getString("CREATE_DATE");
            String registerTime = null;
            if (createDate!=null&&createDate.length()>10){
                registerTime=createDate.substring(11, 19);
                createDate=createDate.substring(0, 10);
            }
            //性别
            String gender = row.getString("GENDER");
            String tid = row.getString("TID");
            //证件号码
            String credentialNum = row.getString("CREDENTIAL_NUM");
            //证件类型
            String credentialType = row.getString("CREDENTIAL_TYPE");
            //英文名
            String firstName=row.getString("FIRST_NAME");
            //英文姓
            String lastName=row.getString("LAST_NAME");
            //中文名
            String cnName = row.getString("CN_NAME");
            //中文姓
            String cnLastName = row.getString("CN_LAST_NAME");
            String birthday = row.getString("BIRTHDAY");
            String nationality = row.getString("NATIONALITY");
            String language = row.getString("LANGUAGE");
            String parentMemberNum = row.getString("PARENT_MEMBER_NUM");
            String submitPerson = row.getString("SUBMIT_PERSON");
            //四级发展渠道ID
            String channelId = row.getString("CHANNEL_ID");
            String fastCreateFlag = row.getString("FAST_CREATE_FLAG");
            String currentDateTime = DateTimeUtils.getCurrentDateTime();
            memberRegisterFactModel.setPkId(memberNumber);
            memberRegisterFactModel.setFkTid(tid);
            memberRegisterFactModel.setFkRegisterTime(registerTime);
            memberRegisterFactModel.setFkRegisterDate(createDate);
            memberRegisterFactModel.setGender(gender);
            memberRegisterFactModel.setCertNumber(credentialNum);
            memberRegisterFactModel.setCertType(credentialType);
            memberRegisterFactModel.setEnFirstName(firstName);
            memberRegisterFactModel.setEnLastName(lastName);
            memberRegisterFactModel.setCnName(cnName);
            memberRegisterFactModel.setChLastName(cnLastName);
            memberRegisterFactModel.setBirthday(birthday);
            memberRegisterFactModel.setNationality(nationality);
            memberRegisterFactModel.setContactLanguage(language);
            memberRegisterFactModel.setParentCardNumber(parentMemberNum);
            memberRegisterFactModel.setDevStaffId(submitPerson);
            memberRegisterFactModel.setQuickRegistFlag(fastCreateFlag);
            memberRegisterFactModel.setSystemCreatetime(currentDateTime);
            memberRegisterFactModel.setSystemLastUpdatetime(currentDateTime);
            memberRegisterFactModel.setFromCrmMember(true);
            memberRegisterFactModel.setCrmMemberChannel(channelId);
            memberRegisterFactModel.setFrequentCard(memberNumber);

            if (channelProcessor.supports(channelId)) {
                ChannelInfo info = channelProcessor.map(channelId);
                if (info != null) {
                    memberRegisterFactModel.setDevChannelOne(info.getLevel1Code());
                    memberRegisterFactModel.setDevChannelTow(info.getLevel2Code());
                    memberRegisterFactModel.setDevChannelThree(info.getLevel3Code());
                    memberRegisterFactModel.setDevChannelFour(info.getLevel4Code());
                }
            }
            return memberRegisterFactModel;
        }).filter(row -> row != null);
//        memberRegisterFactModelDataStream.print();
        // 创建 Doris Sink
        DorisSink<MemberRegisterFactModel> memberRegisterFactModelDorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_MEMBER_REGISTER_FACT");
        memberRegisterFactModelDataStream.sinkTo(memberRegisterFactModelDorisSink);
    }
}
