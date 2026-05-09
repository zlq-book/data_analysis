package com.travelsky.trp.usercenter.data.analysis.transform.zxyh;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.IdMapping;
import com.travelsky.dataplatform.utils.SM4Utils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CustomDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
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

public class CrmCustomerToDwdRegisterUdoFactTrans {

    static final Logger logger = LoggerFactory.getLogger(CrmCustomerToDwdRegisterUdoFactTrans.class);

    /**
     * 直销用户实事表
     *
     * @param rowDataStream
     */
    public static void output(DataStream<Row> rowDataStream) {

        // 后续可以进行 map、filter、sink 操作
        SingleOutputStreamOperator<RegisterUdoFactModel> registerUdoFactModelStream = rowDataStream.map(row -> {
            RegisterUdoFactModel model = new RegisterUdoFactModel();
            model.setPkId(SM4Utils.encrypt(row.getField("CUSTOMER_ID").toString(), Constants.SM4_KEY));
            // tid
            model.setFkRegisterUser((row.getField("TID").toString()));

            model.setFkRegisterDate(row.getField("FK_REGISTER_DATE").toString());
            model.setRegisterTime(row.getField("REGISTER_TIME").toString());
            model.setAkRegisterChannel(row.getField("AK_REGISER_CHANNEL").toString());
            model.setRegisterCount(1);
            model.setSourceLastUpdatetime(row.getField("SOURCE_LAST_UPDATETIME").toString());
            model.setSystemCreatetime(LocalDateTime.now().toString());
            model.setSystemLastUpdatetime(LocalDateTime.now().toString());
            return model;
        });

        DorisSink<RegisterUdoFactModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DWD_DB,
                "T_DWD_REGISTER_UDO_FACT",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
        registerUdoFactModelStream.sinkTo(dorisSink);

    }
}
