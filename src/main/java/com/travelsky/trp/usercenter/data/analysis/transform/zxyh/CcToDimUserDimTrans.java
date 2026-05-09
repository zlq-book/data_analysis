package com.travelsky.trp.usercenter.data.analysis.transform.zxyh;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CertDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
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
import java.util.HashMap;
import java.util.Map;

public class CcToDimUserDimTrans {
    static final Logger logger = LoggerFactory.getLogger(CcToDimUserDimTrans.class);

    public static void result(StreamTableEnvironment tEnv, String etlDate) {
        //创建Doris目标表
        tEnv.executeSql(CreateTableSql.ZXYH_CRM_CUSTOMER_CERT);

        // 从 Doris 表中读取数据
        Table dorisTable = tEnv.sqlQuery( "SELECT \n" +
                "a.CUSTOMER_ID ,\n" +
                "a.CERT_TYPE ,\n" +
                "a.CERT_NUMBER ,\n" +
                "a.REAL_NAME_FLAG ,\n" +
                "a.ISSUE_DATE ,\n" +
                "a.EXPIRATION_DATE ,\n" +
                "a.SIGNING_AUTHORITY ,\n" +
                "a.ISSUE_AGENCY, \n" +
                "a.CUSTOMER_ID TID\n" +
                "FROM T_ODS_ZXYH_CRM_CUSTOMER_CERT a     "
                + " WHERE  a.ETL_DATE='" + etlDate + "'"
        );

        DataStream<Row> rowStream = tEnv.toDataStream(dorisTable);

        DataStream<Row> rowDataStream = rowStream.map(row -> {
            Row rowModel = Row.copy(row);

            // 证件类型
            Object certType = row.getField("CERT_TYPE");
            if (certType != null) {
                rowModel.setField("CERT_TYPE", NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE,
                        certType.toString(), DataSource.DIRECT_SALES_USER));
            }
            // 证件号
            Object certNumber = row.getField("CERT_NUMBER");
            if (certNumber != null) {
                rowModel.setField("CERT_NUMBER", NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,
                        certNumber.toString()));
            }

            // 获取tid
            Tid tid = new Tid();
            tid.setTid(SM4Utils.encrypt(row.getField("CUSTOMER_ID").toString(), Constants.SM4_KEY));
            // 强id customerId
            tid.setCrmCustomerId(SM4Utils.encrypt(row.getField("CUSTOMER_ID").toString(), Constants.SM4_KEY));
            // 强id 证件信息
            if (null != row.getField("REAL_NAME_FLAG") && null != row.getField("CERT_TYPE") &&
                    null != row.getField("CERT_NUMBER")
                    && "1".equals(row.getField("REAL_NAME_FLAG").toString())) {
                Map<String ,String> certMap = new HashMap<>();
                certMap.put(row.getField("CERT_TYPE").toString(), row.getField("CERT_NUMBER").toString());
                tid.setCertification(certMap);

            }
            String tidStr = IdMapping.idMappingFunction(tid, "ZXYH");
            rowModel.setField("TID", tidStr);
            return rowModel;
        });

        // 后续可以进行 map、filter、sink 操作
//        SingleOutputStreamOperator<UserDimModel> userModelStream = rowDataStream.map(row -> {
//            UserDimModel model = new UserDimModel();
//            //  tid
//            model.setPkId(row.getField("TID").toString());
//            //  证件类型 证件号 优先级
//            if (null != row.getField("CERT_TYPE") && null != row.getField("CERT_NUMBER")) {
//                TransUtils.userDimCertHandle(model, row.getField("CERT_TYPE").toString(),
//                        row.getField("CERT_NUMBER").toString());
//            }
//
//            model.setCreateTime(LocalDateTime.now().toString());
//            model.setUpdateTime(LocalDateTime.now().toString());
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


        // 证件维表
        SingleOutputStreamOperator<CertDimModel> certModelStream = rowDataStream.map(row -> {
            CertDimModel model = new CertDimModel();
            //  tid
            model.setSystemKey(row.getField("TID").toString() + row.getField("CERT_TYPE").toString()
                    + row.getField("CERT_NUMBER").toString());
            model.settId(row.getField("TID").toString());
            // 证件
            model.setCertType(row.getField("CERT_TYPE").toString());
            model.setCertNumber(row.getField("CERT_NUMBER").toString());

            if (null != row.getField("REAL_NAME_FLAG")
                    && "1".equals(row.getField("REAL_NAME_FLAG").toString())) {
                model.setDirectSalesRealNameVerifiedCard(true);
            }
            model.setCertIssueDate(row.getField("ISSUE_DATE") == null ? null :
                    LocalDateTime.parse(row.getField("ISSUE_DATE").toString()).toLocalDate());
            model.setCertExpireDate(row.getField("EXPIRATION_DATE") == null ? null :
                    LocalDateTime.parse(row.getField("EXPIRATION_DATE").toString()).toLocalDate());
            model.setCertIssuingCountry(row.getField("SIGNING_AUTHORITY") == null ? null :
                    row.getField("SIGNING_AUTHORITY").toString());
            model.setCertIssuingAuthority(row.getField("ISSUE_AGENCY") == null ? null :
                    row.getField("ISSUE_AGENCY").toString());

            model.setCreateTime(LocalDateTime.now().toString());
            model.setUpdateTime(LocalDateTime.now().toString());
            return model;
        });

        DorisSink<CertDimModel> certSink = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_CERT_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);
        certModelStream.sinkTo(certSink);

    }
}
