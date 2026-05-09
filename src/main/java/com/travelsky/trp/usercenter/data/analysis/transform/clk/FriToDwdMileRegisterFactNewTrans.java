package com.travelsky.trp.usercenter.data.analysis.transform.clk;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.MemberRegisterFactModal;
import com.travelsky.trp.usercenter.data.analysis.utils.ChannelInfo;
import com.travelsky.trp.usercenter.data.analysis.utils.channel.ChannelDictService;
import com.travelsky.trp.usercenter.data.analysis.utils.dwd.DwdMemberRegisterFactService;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Set;

public class FriToDwdMileRegisterFactNewTrans {

    static final Logger logger = LoggerFactory.getLogger(FriToDwdMileRegisterFactNewTrans.class);
    // 常客注册事实表 24年开始新数据
    public static void output(DataStream<Row> rowDataStream, Set<String> scanRegisterInfoIdSet) {

        // 后续可以进行 map、filter、sink 操作
        SingleOutputStreamOperator<MemberRegisterFactModal> registerUdoFactModelStream = rowDataStream.map(row -> {
            MemberRegisterFactModal model = new MemberRegisterFactModal();
            // 主键 常客卡号
            model.setPkId(row.getField("FCARD").toString());
            //  TID
            model.setFkTid(row.getField("TID").toString());
            // 日期 时间
            model.setFkRegisterDate(row.getField("CREATE_DATE").toString().substring(0, 10));
            model.setFkRegisterTime(row.getField("CREATE_DATE").toString().substring(11));
            model.setFrequentCard(row.getField("FCARD").toString());
            // 性别
            model.setGender(row.getField("FSEX")==null?null:row.getField("FSEX").toString());
            // 证件号
            model.setCertNumber(row.getField("FIDCARD").toString());
            model.setCertType(row.getField("FCARDTYPE").toString());
            // 英文名
            model.setEnFirstName(row.getField("F_SECOND_NAME_EN").toString());
            model.setEnLastName(row.getField("F_FIRST_NAME_EN").toString());
            // 中文名
            model.setCnName(row.getField("FNAMECN")==null?null:row.getField("FNAMECN").toString());
            model.setChLastName(row.getField("F_FIRTST_NAME_CN")==null?null:row.getField("F_FIRTST_NAME_CN").toString());
            // 生日
            model.setBirthday(row.getField("FBIRTHDAY")==null?null:row.getField("FBIRTHDAY").toString());
            model.setParentCardNumber(row.getField("PARENT_FFP_NUM")==null?null:row.getField("PARENT_FFP_NUM").toString());
            // 发展人工号 优先取OP_ACCOUNT，OP_ACCOUNT为空，就取FUSER 10月10日
            Object opAccount = row.getField("OP_ACCOUNT");
            if (opAccount != null) {
                model.setDevStaffId(opAccount.toString());
            } else {
                Object fuser = row.getField("FUSER");
                if (fuser != null) {
                    model.setDevStaffId(fuser.toString());
                }
            }
            model.setDevStaffId(row.getField("OP_ACCOUNT")==null?null:row.getField("OP_ACCOUNT").toString());
            model.setDevName(row.getField("OP_NAME")==null?null:row.getField("OP_NAME").toString());
            model.setDevDepartment(row.getField("OPERATE_DEPT")==null?null:row.getField("OPERATE_DEPT").toString());
            // 常旅客系统航班日期
            model.setMemberRegistFltDate(row.getField("FFLTDATE") == null ? null : row.getField("FFLTDATE").toString());

            // 基于OPERATE_DEPT按需补齐三级/四级渠道（仅在为空时补充）
            String operateDept = row.getField("OPERATE_DEPT")==null?null:row.getField("OPERATE_DEPT").toString();
            // 常旅客发展标识的数据
            ChannelInfo dictInfo = ChannelDictService.matchByOperateDept(operateDept,"1");
            if (dictInfo != null) {
                if (StringUtils.isBlank(model.getDevChannelThree())) {
                    model.setDevChannelThree(dictInfo.getLevel3Code());
                }
                if (StringUtils.isBlank(model.getDevChannelFour())) {
                    model.setDevChannelFour(dictInfo.getLevel4Code());
                }
                if (StringUtils.isBlank(model.getDevChannelOne())) {
                    model.setDevChannelOne(dictInfo.getLevel1Code());
                }
                if (StringUtils.isBlank(model.getDevChannelTwo())) {
                    model.setDevChannelTwo(dictInfo.getLevel2Code());
                }
            }

            // 4.1 实时方案：对 2023-12-01 至 2025-09-02（含）的注册，按需强制设置一/二级渠道
            // 规则：若该卡在事实表中不存在，或存在但 FROM_CRM_MEMBER、FROM_INTEGRATION 均为空，则设置：
            //  一级：线下发展（XXFZ），二级：常旅客系统（FFP_CLK）
            String regDate = model.getFkRegisterDate();
            if (DwdMemberRegisterFactService.shouldSetClkChannel(model.getPkId(), regDate)) {
                model.setDevChannelOne("XXFZ");
                model.setDevChannelTwo("FFP_CLK");
            }

            // 实时方案：如果该注册信息ID在当天的 ffp_register_scan 表中，设置渠道信息
            Object idObj = row.getField("ID");
            if (idObj != null && scanRegisterInfoIdSet != null && scanRegisterInfoIdSet.contains(idObj.toString())) {
                model.setDevChannelOne("XXFZ");
                model.setDevChannelTwo("FFP_CLK");
                model.setDevChannelThree("FFP_JSSM");
            }

            model.setFromMileRegister(true);
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
