package com.travelsky.trp.usercenter.data.analysis.transform.lyx;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CertDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.LyxDimModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class LmMlsLluToDimLyxDimTrans {

    static final Logger logger = LoggerFactory.getLogger(LmMlsLluToDimLyxDimTrans.class);

    public static void output(DataStream<Row> rowDataStream) {

        // 证件信息维表
        SingleOutputStreamOperator<LyxDimModel> certDimStream = rowDataStream.filter(row -> {
            boolean flag = row.getField("TID") == null;
            if (flag) {
                logger.info("入库鲁雁行维表异常，oriTable：鲁雁行联表查询，" + row.toString());
            }
            return !flag;
        }).map(row -> {
            LyxDimModel model = new LyxDimModel();
            model.setSystemKey(row.getField("TID").toString() + row.getField("ID").toString());

            //  tid
            model.settId(row.getField("TID").toString());
            // 用户id
            model.setLyUserId(row.getField("ID").toString());
            // 注册时间
            model.setLyRegisterTime(row.getField("MTM_REGISTER_TIME") == null ? null :
                    row.getField("MTM_REGISTER_TIME").toString());
            // 鲁雁行 卡号
            model.setLyCardNumber(row.getField("MTM_CARD_NUM").toString());
            // 用户等级
            model.setLyUserLevel(row.getField("LEVEL_CODE") == null ? null :
                    row.getField("LEVEL_CODE").toString());
            // 状态
            model.setLyUserStatus(row.getField("MTM_STATUS") == null ? null :
                    row.getField("MTM_STATUS").toString());
            model.setLyRegisterStatus(row.getField("REGISTER_STATUS") == null ? null :
                    row.getField("REGISTER_STATUS").toString());
            // 认证
            model.setLyVerifyStatus(row.getField("REALNAME_ATTESTATION") == null ? null :
                    row.getField("REALNAME_ATTESTATION").toString());
            model.setLyVerifyMethod(row.getField("ATTESTATION_MODE") == null ? null :
                    row.getField("ATTESTATION_MODE").toString());
            model.setLyVerifyTime(row.getField("ATTESTATION_TIME") == null ? null :
                    row.getField("ATTESTATION_TIME").toString());
            if (null != row.getField("TIMING_TASK_STATUS")
                    && "1".equals(row.getField("TIMING_TASK_STATUS").toString())) {
                // 1是需要
                model.setLyNeedLipVerify(true);
            } else  {
                model.setLyNeedLipVerify(false);
            }
            if (null != row.getField("LIP_ATTESTATION")
                    && "0".equals(row.getField("LIP_ATTESTATION").toString())) {
                // 0是匹配
                model.setLyLipVerifyResult(true);
            } else  {
                model.setLyLipVerifyResult(false);
            }

            // 鲁雁行等级变更日期
            model.setLyLevelChangeDate(row.getField("LEVEL_UPDATE_TIME") == null ? null :
                    LocalDateTime.parse(row.getField("LEVEL_UPDATE_TIME").toString()).toLocalDate());
            // 变更前鲁雁行等级
            model.setLyPrevLevel(row.getField("ORI_LEVEL") == null ? null :
                    row.getField("ORI_LEVEL").toString());

            // 生命周期
            model.setLyLifetimePoints((int) Double.parseDouble(row.getField("CYCLE_LYVAL").toString()));
            // 鲁雁值
            model.setLyAvailablePoints((int) Double.parseDouble(row.getField("USABLE_LYVAL").toString()));




            model.setUpdateTime(LocalDateTime.now().toString());
            model.setCreateTime(LocalDateTime.now().toString());
            return model;
        });

        DorisSink<LyxDimModel> certDimSink = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_LYX_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);
        certDimStream.sinkTo(certDimSink);




    }
}
