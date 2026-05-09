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

public class CoiToDimUserDimTrans {
    static final Logger logger = LoggerFactory.getLogger(CoiToDimUserDimTrans.class);

    public static void result(StreamTableEnvironment tEnv, String etlDate) {
        //创建Doris目标表
        tEnv.executeSql(CreateTableSql.ZXYH_CRM_CUSTOMER_OPEN_INFO);
        tEnv.executeSql(CreateTableSql.ZXYH_CRM_CUSTOMER);

        // 从 Doris 表中读取数据
        Table dorisTable = tEnv.sqlQuery( "SELECT \n" +
                "a.UNION_ID ,\n" +
                "a.CHANNEL_TYPE ,\n" +
                "b.CUSTOMER_ID ,\n" +
                "b.CUSTOMER_ID TID\n" +
                "FROM T_ODS_ZXYH_CRM_CUSTOMER_OPEN_INFO a\n" +
                "INNER JOIN T_ODS_ZXYH_CRM_CUSTOMER b\n" +
                "ON a.SYS_ID = b.ID "
                + " WHERE  a.ETL_DATE='" + etlDate + "'");

        DataStream<Row> rowStream = tEnv.toDataStream(dorisTable);

        DataStream<Row> rowDataStream = TransUtils.zxyhPrepareHandle(rowStream);

        // 后续可以进行 map、filter、sink 操作
        SingleOutputStreamOperator<UserDimModel> userModelStream = rowDataStream.map(row -> {
            UserDimModel model = new UserDimModel();
            //  tid
            model.setPkId(row.getField("TID").toString());
            if (row.getField("CHANNEL_TYPE") == null) {
                return model;
            }
            // 优先级只有 直销
            if ("1".equals(row.getField("CHANNEL_TYPE").toString())) {
                model.setWechatId(row.getField("UNION_ID").toString());
            }
            if ("4".equals(row.getField("CHANNEL_TYPE").toString())) {
                model.setAlipayId(row.getField("UNION_ID").toString());
            }
            if ("10".equals(row.getField("CHANNEL_TYPE").toString())) {
                model.setDouyinId(row.getField("UNION_ID").toString());
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
