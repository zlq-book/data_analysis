package com.travelsky.trp.usercenter.data.analysis.transform.zxyh;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.FieldType;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.dataplatform.utils.NormalizationUtils;
import com.travelsky.dataplatform.utils.SM4Utils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CustomDimModel;
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

public class CcviToDimUserDimTrans {
    static final Logger logger = LoggerFactory.getLogger(CcviToDimUserDimTrans.class);

    public static void result(StreamTableEnvironment tEnv, String etlDate) {
        //创建Doris目标表
        tEnv.executeSql(CreateTableSql.ZXYH_CRM_CUSTOMER_VOCATION_INFO);

        // 从 Doris 表中读取数据
        Table dorisTable = tEnv.sqlQuery( "select \n" +
                "a.CUSTOMER_ID ,\n" +
                "a.CUSTOMER_ID TID ,\n" +
                "a.VOCATION_TYPE \n" +
                "FROM T_ODS_ZXYH_CRM_CUSTOMER_VOCATION_INFO a    "
                + " WHERE  a.ETL_DATE='" + etlDate + "'");

        DataStream<Row> rowStream = tEnv.toDataStream(dorisTable);

        DataStream<Row> rowDataStream = TransUtils.zxyhPrepareHandle(rowStream);

//        // 后续可以进行 map、filter、sink 操作
//        SingleOutputStreamOperator<UserDimModel> userModelStream = rowDataStream.map(row -> {
//            UserDimModel model = new UserDimModel();
//            //  tid
//            model.setPkId(row.getField("TID").toString());
//            //  职业类型逻辑 有就填充覆盖 没有不变
//            Object vocationType = row.getField("VOCATION_TYPE");
//            if (null !=  vocationType) {
//                // 标准化
//                String studentFlag = NormalizationUtils.standardize(
//                        FieldType.STUDENT_FLAG, vocationType.toString());
//                String teacherFlag = NormalizationUtils.standardize(
//                        FieldType.TEACHER_FLAG, vocationType.toString());
//                if ("XS".equals(studentFlag)) {
//                    // 学生
//                    model.setStudentFlag(true);
//                }
//                if ("JS".equals(teacherFlag)) {
//                    // 教师
//                    model.setTeacherFlag(true);
//                }
//            }
//
//            model.setUpdateTime(LocalDateTime.now().toString());
//            model.setCreateTime(LocalDateTime.now().toString());
//            return model;
//        });
//
//        DorisSink<UserDimModel> dorisSink = FlinkDorisUtils.creatDorisSink(
//                Constants.DIM_DB,
//                "T_DIM_USER_DIM",
//                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
//                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
//                Constants.DIM_USER,
//                Constants.DIM_PWD);
//        userModelStream.sinkTo(dorisSink);


        // 直销用户维表
        SingleOutputStreamOperator<CustomDimModel> customModelStream = rowDataStream.map(row -> {
            CustomDimModel model = new CustomDimModel();
            // tid
            model.setSystemKey(row.getField("TID").toString() +
                    SM4Utils.encrypt(row.getField("CUSTOMER_ID").toString(), Constants.SM4_KEY));

            //  职业类型逻辑
            Object vocationType = row.getField("VOCATION_TYPE");
            if (null !=  vocationType) {
                // 标准化
                String teacherFlag = NormalizationUtils.standardize(
                        FieldType.TEACHER_FLAG, vocationType.toString());
                String studentFlag = NormalizationUtils.standardize(
                        FieldType.STUDENT_FLAG, vocationType.toString());
                if ("XS".equals(studentFlag)) {
                    // 学生
                    model.setStudentFlag(true);
                }
                if ("JS".equals(teacherFlag)) {
                    // 教师
                    model.setTeacherFlag(true);
                }
            }
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
