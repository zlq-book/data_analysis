package com.travelsky.trp.usercenter.data.analysis.transform.lyx;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.UserDimPriorityLevel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.MobileDimModel;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class LmMlsLluToDimMobileDimTrans {

    static final Logger logger = LoggerFactory.getLogger(LmMlsLluToDimMobileDimTrans.class);
    // 常客注册事实表 24年开始新数据
    public static void output(DataStream<Row> rowDataStream) {

        // 手机号维表
        SingleOutputStreamOperator<MobileDimModel> mobileDimStream = rowDataStream.filter(row -> {
            boolean flag = row.getField("MTM_MOBILE") == null;
            if (flag) {
                logger.info("入库手机号维表异常，oriTable：鲁雁行联表查询，" + row.toString());
            }
            return !flag;
        }).map(row -> {
            MobileDimModel model = new MobileDimModel();
            //  tid
            model.setSystemKey(row.getField("TID").toString() + row.getField("MTM_MOBILE").toString());

            model.setMobileNumber(row.getField("MTM_MOBILE").toString());

            if (null != row.getField("REALNAME_ATTESTATION") && null != row.getField("ATTESTATION_MODE")) {
                Integer newSource = 7;
                if ("1".equals(row.getField("REALNAME_ATTESTATION").toString())) {
                    newSource = 5;
                }
                boolean firstFlag = "0".equals(row.getField("REALNAME_ATTESTATION").toString())
                        // 雁行实名认证标识是已认证的且认证方式不为LIP认证的手机号
                        && !"5".equals(row.getField("ATTESTATION_MODE").toString());
                boolean secondFlag = false;
                if (null != row.getField("LIP_ATTESTATION")) {
                    // 鲁雁行认证方式为LIP认证同时三要素认证结果是匹配的手机号。
                    secondFlag = "0".equals(row.getField("REALNAME_ATTESTATION").toString())
                            && "5".equals(row.getField("ATTESTATION_MODE").toString())
                            && "0".equals(row.getField("LIP_ATTESTATION").toString());
                }
                if (firstFlag || secondFlag) {
                    newSource = 1;
                    model.setLyxRealnameMobile(true);
                }
                model.setCurrentPhoneHighestPriority(newSource);

            }

            model.setLyxRealnameMobile(true);

            //是否虚拟手机号
            model.setVirtualMobile(TransUtils.isVirtualMobile(row.getField("MTM_MOBILE").toString()));
            model.setUpdateTime(LocalDateTime.now().toString());
            model.setCreateTime(LocalDateTime.now().toString());
            return model;
        });

        DorisSink<MobileDimModel> mobileDimSink = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_MOBILE_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);
        mobileDimStream.sinkTo(mobileDimSink);



    }
}
