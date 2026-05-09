package com.travelsky.trp.usercenter.data.analysis.transform.zxyh;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CertDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

public class WebNoRegisteredToDimUserDimTrans {
    static final Logger logger = LoggerFactory.getLogger(WebNoRegisteredToDimUserDimTrans.class);

    public static void result(StreamTableEnvironment tEnv, String etlDate) {
        //创建Doris目标表
        tEnv.executeSql(CreateTableSql.CRM_WEB_NO_REGISTERED);

        // 从 Doris 表中读取数据
        Table dorisTable = tEnv.sqlQuery( "SELECT MOBILEPHONE " +
                "FROM T_ODS_ZXYH_CRM_WEB_NO_REGISTERED a \n" +
                "WHERE MOBILEPHONE is not null    "
                + " AND  a.ETL_DATE='" + etlDate + "'"
        );

        DataStream<Row> rowStream = tEnv.toDataStream(dorisTable);

        DataStream<Row> rowDataStream = rowStream.map(row -> {
            Row rowModel = Row.copy(row);

            // 获取tid
            Tid tid = new Tid();
            tid.setTid(NormalizationUtils.standardize(FieldType.MOBILE_NO,
                    row.getField("MOBILEPHONE").toString()));

            String tidStr = IdMapping.idMappingFunction(tid, "ZXYH");
            rowModel.setField("MOBILEPHONE", tidStr);
            return rowModel;
        });

//         后续可以进行 map、filter、sink 操作
        SingleOutputStreamOperator<UserDimModel> userModelStream = rowDataStream.map(row -> {
            UserDimModel model = new UserDimModel();
            //  tid
            model.setPkId(row.getField("MOBILEPHONE").toString());
            //  是否官网非注册
            model.setOfficialWebsiteNonRegistered(true);
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
