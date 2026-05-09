package com.travelsky.trp.usercenter.data.analysis.transform.clk;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.SM4Utils;
import com.travelsky.trp.usercenter.data.analysis.utils.ChannelInfo;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.MemberRegisterFactModal;
import com.travelsky.trp.usercenter.data.analysis.utils.CardNumberSegmentMatcher;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class DwDFfpToDwdMileRegisterFactOldTrans {
    static final Logger logger = LoggerFactory.getLogger(DwDFfpToDwdMileRegisterFactOldTrans.class);

    private static CardNumberSegmentMatcher matcher = CardNumberSegmentMatcher.getInstance();
    // 常客注册事实表 24年之前旧数据
    public static void output(DataStream<Row> rowDataStream) {

        // 后续可以进行 map、filter、sink 操作
        SingleOutputStreamOperator<MemberRegisterFactModal> registerUdoFactModelStream = rowDataStream.map(row -> {

            MemberRegisterFactModal model = new MemberRegisterFactModal();
            model.setPkId(row.getField("MEM_NUM").toString());
            //  TID
            model.setFkTid(row.getField("TID").toString());
            // 时间
            model.setFrequentCard(row.getField("MEM_NUM").toString());
            model.setFkRegisterDate(row.getField("FFP_CREATED").toString().substring(0, 10));
            model.setFkRegisterTime(row.getField("FFP_CREATED").toString().substring(11));
            // 性别
            model.setGender(row.getField("GENDER_ID") == null ? null : row.getField("GENDER_ID").toString());
            boolean testFlag = false;
            if (null != row.getField("TEST_MEM_FLAG") &&
                    "Y".equalsIgnoreCase(row.getField("TEST_MEM_FLAG").toString())) {
                // 是否为测试会员 Y -是，其他-否
                testFlag = true;
            }
            model.setTestMember(testFlag);
            // 航空首兑资格
            model.setFirstExchQualifi(row.getField("FLIGHT_FRQ_FLAG") == null ? null :
                    row.getField("FLIGHT_FRQ_FLAG").toString());
            model.setExpireTime(row.getField("PR_EXPIRATION_TS") == null ? null :
                    row.getField("PR_EXPIRATION_TS").toString());
            model.setIssueTime(row.getField("PR_ISSUE_TS") == null ? null :
                    row.getField("PR_ISSUE_TS").toString());
            model.setIssueCountry(row.getField("PR_ISSUING_COUNTRY") == null ? null :
                    row.getField("PR_ISSUING_COUNTRY").toString());

            // 证件号
            model.setCertNumber(row.getField("PR_ID") == null ? null :
                    row.getField("PR_ID").toString());
            model.setCertType(row.getField("PR_CATEGORY_CD") == null ? null :
                    row.getField("PR_CATEGORY_CD").toString());
            // 英文名
            model.setEnFirstName(row.getField("EN_FST_NAME") == null ? null :row.getField("EN_FST_NAME").toString());
            model.setEnLastName(row.getField("EN_LAST_NAME") == null ? null :row.getField("EN_LAST_NAME").toString());
            model.setNationality(row.getFieldAs("NATIONALITY"));
            // 中文名
            model.setCnName(row.getField("CN_FST_NAME") == null ? null : row.getField("CN_FST_NAME").toString());
            model.setChLastName(row.getField("CN_LAST_NAME") == null ? null : row.getField("CN_LAST_NAME").toString());
            // 生日
            model.setBirthday(row.getField(10) == null ? null : row.getField(10).toString());
            // 发展渠道级别 23年之前老数据通过号段来进行匹配
            String orgFCard = SM4Utils.decrypt(row.getField("ORG_F_CARD").toString(), Constants.SM4_KEY);
            ChannelInfo channelInfo = matcher.match(orgFCard);
            if (channelInfo != null) {
                model.setDevChannelOne(channelInfo.getLevel1Code());
                model.setDevChannelTwo(channelInfo.getLevel2Code());
                model.setDevChannelThree(channelInfo.getLevel3Code());
                model.setDevChannelFour(channelInfo.getLevel4Code());
            }

            model.setFromIntegration(true);
            model.setRegistCount(1);
            model.setSystemCreatetime(LocalDateTime.now().toString());
            model.setSystemLastUpdatetime(LocalDateTime.now().toString());
            return model;
        });

        DorisSink<MemberRegisterFactModal> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_MEMBER_REGISTER_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        registerUdoFactModelStream.sinkTo(dorisSink);




    }

}
