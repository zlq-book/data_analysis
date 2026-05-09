package com.travelsky.trp.usercenter.data.analysis.transform.clk;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.FfpDimModel;
import com.travelsky.trp.usercenter.data.analysis.utils.ChannelInfo;
import com.travelsky.trp.usercenter.data.analysis.utils.channel.ChannelDictService;
import com.travelsky.trp.usercenter.data.analysis.utils.dwd.DwdMemberRegisterFactService;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;
import java.util.Set;

public class FriToDimFfpDimNewTrans {

    // 常客注册事实表 24年开始新数据
    public static void output(DataStream<Row> rowDataStream, Set<String> scanRegisterInfoIdSet) {

        // 常客信息维表
        SingleOutputStreamOperator<FfpDimModel> ffpDimStream = rowDataStream.map(row -> {
            FfpDimModel model = new FfpDimModel();
            //  V3 主键修改为“常客卡号“的加密值
            model.setSystemKey(row.getField("FCARD").toString());

            //  tid
            model.settId(row.getField("TID").toString());

            // 常旅客信息
            model.setFfrf(row.getField("FCARD").toString());
            if (row.getField("PARENT_FFP_NUM") != null) {
                model.setParentCardNumber((String) row.getField("PARENT_FFP_NUM"));
            }
            model.setFtRegisterTime(row.getField("CREATE_DATE").toString());
//            model.setFfCertType(row.getField("FCARDTYPE").toString());
//            model.setFfCertNumber(row.getField("FIDCARD").toString());

            // 渠道逻辑（与 FriToDwdMileRegisterFactNewTrans 保持一致）
            String fcard = row.getField("FCARD").toString();
            String createDate = row.getField("CREATE_DATE").toString();
            String regDate = createDate.length() >= 10 ? createDate.substring(0, 10) : null;

            // 基于OPERATE_DEPT按需补齐三级/四级渠道（仅在为空时补充）
            String operateDept = row.getField("OPERATE_DEPT") == null ? null : row.getField("OPERATE_DEPT").toString();
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
            if (regDate != null && DwdMemberRegisterFactService.shouldSetClkChannel(fcard, regDate)) {
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

            // 当前最可信来源
            model.setMostTrustedSource("1");

            model.setCreateTime(LocalDateTime.now().toString());
            model.setUpdateTime(LocalDateTime.now().toString());
            return model;
        });

        DorisSink<FfpDimModel> ffpDimSink = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_FFP_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);
        ffpDimStream.sinkTo(ffpDimSink);


    }
}
