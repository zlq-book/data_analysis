package com.travelsky.trp.usercenter.data.analysis.transform.zxyh;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.SM4Utils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CustomDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.RegisterUdoFactModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class CrmCustomerToDimCustomDimTrans {

    static final Logger logger = LoggerFactory.getLogger(CrmCustomerToDimCustomDimTrans.class);

    public static void output(DataStream<Row> rowDataStream) {

        // 直销用户维表
        SingleOutputStreamOperator<CustomDimModel> customModelStream = rowDataStream.map(row -> {
            CustomDimModel model = new CustomDimModel();
            model.setSystemKey(row.getField("TID").toString() +
                    SM4Utils.encrypt(row.getField("CUSTOMER_ID").toString(), Constants.SM4_KEY));
            // tid
            model.settId(row.getField("TID").toString());
            //  customerID 和 最新登录时间
            model.setCustomerId(row.getField("CUSTOMER_ID").toString());
            model.setLastLoginTime(row.getField("LAST_LOGIN_TIME") == null ? null :
                    LocalDateTime.parse(row.getField("LAST_LOGIN_TIME").toString()).toLocalDate());
            model.setDirectRegisterChannel(row.getField("REGIST_CHANNEL").toString());
            model.setDirectUserStatus(row.getField("CUSTOMER_STATUS").toString());
            model.setDirectRegisterDate(row.getField("REGIST_DATE") == null ? null :
                    row.getField("REGIST_DATE").toString().substring(0, 10));
            if (null != row.getField("REAL_NAME_FLAG") && "1".equals(row.getField("REAL_NAME_FLAG").toString())) {
                model.setDirectVerifiedFlag(true);
            }
            // 创建一个匹配 "yyyy-MM-dd HH:mm:ss" 格式的 formatter
            // 注意：模式字符串中的 ' ' (空格) 明确表示分隔符是空格
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            model.setDirectVerifyDate(row.getField("REAL_NAME_TIME") == null ? null :
                    row.getField("REAL_NAME_TIME").toString().substring(0, 10));
            model.setWebVirtualStatus(row.getField("WEB_IS_LIMITED") == null ? null : row.getField("WEB_IS_LIMITED").toString());
            model.setWebAccountStatus(row.getField("WEB_AUTHORIZE") == null ? null : row.getField("WEB_AUTHORIZE").toString());

            model.setCreateTime(LocalDateTime.now().toString());
            model.setUpdateTime(LocalDateTime.now().toString());
            return model;
        });

        DorisSink<CustomDimModel> customSink = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_CUSTOM_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);
        customModelStream.sinkTo(customSink);
    }
}
