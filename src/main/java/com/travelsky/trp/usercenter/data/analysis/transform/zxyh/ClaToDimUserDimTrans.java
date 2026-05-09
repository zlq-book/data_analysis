package com.travelsky.trp.usercenter.data.analysis.transform.zxyh;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimModel;
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

public class ClaToDimUserDimTrans {
    static final Logger logger = LoggerFactory.getLogger(ClaToDimUserDimTrans.class);

    public static void result(StreamTableEnvironment tEnv, String etlDate) {
        //创建Doris目标表
        tEnv.executeSql(CreateTableSql.ZXYH_CRM_CUSTOMER_LOGIN_ACCOUNT);

        // 从 Doris 表中读取数据
        Table dorisTable = tEnv.sqlQuery( "select \n" +
                "a.CUSTOMER_ID ,\n" +
                "a.CUSTOMER_ID TID,\n" +
                "a.ACCOUNT_TYPE ,\n" +
                "a.ACCOUNT_NAME \n" +
                "FROM T_ODS_ZXYH_CRM_CUSTOMER_LOGIN_ACCOUNT a "
                + " WHERE  a.ETL_DATE='" + etlDate + "'");

        DataStream<Row> rowStream = tEnv.toDataStream(dorisTable);

        DataStream<Row> rowDataStream = TransUtils.zxyhPrepareHandle(rowStream);

        // 后续可以进行 map、filter、sink 操作
        SingleOutputStreamOperator<UserDimModel> userModelStream = rowDataStream.map(row -> {
            UserDimModel model = new UserDimModel();
            //  tid
            model.setPkId(row.getField("TID").toString());
            if (row.getField("ACCOUNT_TYPE") == null) {
                return model;
            }
            if ("5".equals(row.getField("ACCOUNT_TYPE").toString())) {
                model.setEmail(row.getField("ACCOUNT_NAME").toString());
            }


            model.setCreateTime(LocalDateTime.now().toString());
            model.setUpdateTime(LocalDateTime.now().toString());
            return model;
        });

        DorisSink<UserDimModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_USER_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);
        userModelStream.sinkTo(dorisSink);

    }

}
