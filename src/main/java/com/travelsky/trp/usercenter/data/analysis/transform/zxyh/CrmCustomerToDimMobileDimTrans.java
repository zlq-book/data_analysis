package com.travelsky.trp.usercenter.data.analysis.transform.zxyh;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.FieldType;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.NormalizationUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CustomDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.MobileDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.RegisterUdoFactModel;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class CrmCustomerToDimMobileDimTrans {

    static final Logger logger = LoggerFactory.getLogger(CrmCustomerToDimMobileDimTrans.class);

    public static void output(DataStream<Row> rowDataStream) {

        // 后续可以进行 map、filter、sink 操作
        SingleOutputStreamOperator<MobileDimModel> mobileDimModelStream = rowDataStream.filter(row -> {
            boolean flag = row.getField("CONTACT_NUMBER") == null;
            if (flag) {
                logger.info("入手机维表异常，oriTable：ZXYH，" + row.toString());
            }
            return !flag;
        }).map(row -> {
            MobileDimModel model = new MobileDimModel();
            model.setSystemKey(row.getField("TID").toString() + row.getField("CONTACT_NUMBER").toString());
            model.settId(row.getField("TID").toString());
            // 是否直销实名认证手机号
            if (row.getField("CERTIFICATION_TYPE") != null
                    && row.getField("CERTIFICATION_STATUS") != null
                    && ("1".equals(row.getField("CERTIFICATION_TYPE").toString())
                    || "2".equals(row.getField("CERTIFICATION_TYPE").toString()))
                    && "1".equals(row.getField("CERTIFICATION_STATUS").toString())) {
                model.setDirectSaleMobile(true);
            }
            // 新增手机号
            model.setMobileNumber(row.getField("CONTACT_NUMBER").toString());

            if (row.getField("REAL_NAME_FLAG") != null
                    && "1".equals(row.getField("REAL_NAME_FLAG").toString())) {
                // 当前手机号最高优先级
                if (null != row.getField("REAL_NAME_TYPE") &&
                        ("1".equals(row.getField("REAL_NAME_TYPE").toString()) ||
                        "0".equals(row.getField("REAL_NAME_TYPE").toString()))) {
                    model.setCurrentPhoneHighestPriority(1);
                } else {
                model.setCurrentPhoneHighestPriority(5);
                }
            } else {
                model.setCurrentPhoneHighestPriority(7);
            }

            // 是否虚拟手机号
            if (null != row.getField("CONTACT_NUMBER")) {
                model.setVirtualMobile(TransUtils.isVirtualMobile(row.getField("CONTACT_NUMBER").toString()));
            }


            model.setCreateTime(LocalDateTime.now().toString());
            model.setUpdateTime(LocalDateTime.now().toString());
            return model;
        });


        DorisSink<MobileDimModel> userSink = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_MOBILE_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);
        mobileDimModelStream.sinkTo(userSink);


    }
}
