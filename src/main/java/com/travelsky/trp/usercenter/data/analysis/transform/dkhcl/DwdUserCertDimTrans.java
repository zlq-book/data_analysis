package com.travelsky.trp.usercenter.data.analysis.transform.dkhcl;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CertDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.HashMap;

public class DwdUserCertDimTrans {


    private static final Logger logger = LoggerFactory.getLogger(DwdUserCertDimTrans.class);

    /**
     * 空值安全转换为字符串
     */
    private static String toStringSafe(Object obj) {
        return obj != null ? obj.toString() : null;
    }

    public static void result(StreamTableEnvironment tEnv, String etlDate) throws Exception {

        // 注册表
        tEnv.executeSql(CreateTableSql.T_ODS_DKHCL_CRM_COMPANY);
        tEnv.executeSql(CreateTableSql.T_ODS_DKHCL_CRM_EMPLOYEE);
        tEnv.executeSql(CreateTableSql.T_ODS_DKHCL_CRM_EMPLOYEE_CERT);

        // 查询逻辑：
        // 1. 从 CRM_COMPANY 找到 CUS_BIG_CODE=9998181 的公司ID
        // 2. 通过 COMPANY_ID 关联 CRM_EMPLOYEE
        // 3. 通过 EMPLOYEE_ID 关联 CRM_EMPLOYEE_CERT 获取军人证件号
        String query = "SELECT " +
                "  cert.CERT_NO AS CERT_NO, " +
                "  cert.CERT_TYPE AS CERT_TYPE, " +
                "  emp.ID AS EMPLOYEE_ID, " +
                "  comp.CUS_BIG_CODE AS CUS_BIG_CODE " +
                "FROM T_ODS_DKHCL_CRM_COMPANY comp " +
                "INNER JOIN T_ODS_DKHCL_CRM_EMPLOYEE emp " +
                "  ON comp.ID = emp.COMPANY_ID " +
                "INNER JOIN T_ODS_DKHCL_CRM_EMPLOYEE_CERT cert " +
                "  ON emp.ID = cert.EMPLOYEE_ID " +
                "WHERE comp.CUS_BIG_CODE = '99998181' " +
                "  AND cert.CERT_NO IS NOT NULL " +
                "  AND TRIM(cert.CERT_NO) <> ''";

        Table resultTable = tEnv.sqlQuery(query);

        // 转换为 DataStream
        DataStream<Row> rowDataStream = tEnv.toChangelogStream(resultTable)
                .filter(row -> row.getKind().equals(RowKind.INSERT));

        // 映射为证件维度模型
        SingleOutputStreamOperator<CertDimModel> mappedStream = rowDataStream
                .map(new MapFunction<Row, CertDimModel>() {
                    @Override
                    public CertDimModel map(Row row) throws Exception {
                        CertDimModel model = new CertDimModel();

                        String certNo = toStringSafe(row.getField("CERT_NO"));
                        String certType = toStringSafe(row.getField("CERT_TYPE"));
                        if(!certType.equals("身份证"))
                            model.setCorpTravelMilitaryCard(true);
                        else
                            model.setCorpTravelMilitaryCard(false);

                        String T_id = getTid(certNo, certType, "DKHCL");

                        certType = NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, certType, DataSource.KEY_ACCOUNT_TRAVEL);

                        String systemKey = T_id+ certType + certNo;

                        // 设置主键：证件号
                        model.setSystemKey(systemKey);
                        model.settId(T_id);
                        model.setCertType(certType);
                        model.setCertNumber(certNo);
                        model.setUpdateTime(String.valueOf(LocalDateTime.now()));
                        return model;
                    }
                });

        // 写入 Doris 表 T_DIM_CERT_DIM
        logger.info("写入 Doris 表: DB={}, Table={}", Constants.DIM_DB, "T_DIM_CERT_DIM");

        DorisSink<CertDimModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_CERT_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);

        mappedStream.sinkTo(dorisSink);

        // 打印到控制台（本地测试用）
        mappedStream.print("CERT_DIM_MODEL");

        logger.info("证件维度表 ETL 任务完成，数据源：大客户差旅系统军人证件");
    }


    /**
     * 获取TID
     *
     * @param documentNumber 证件号码
     * @param documentType   证件类型
     * @return TID
     */
    public static String getTid(String documentNumber, String documentType, String dataType) {
        Tid tid = new Tid();
        tid.setTid(SM4Utils.encrypt(documentNumber, Constants.SM4_KEY));
        String id_type = "";
        if ("DKHCL".equals(dataType)) {
            id_type = NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, documentType, DataSource.KEY_ACCOUNT_TRAVEL);
            if(documentType.equals("军官证"))
                id_type = "OF";
        }
        String cert_no = NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, SM4Utils.encrypt(documentNumber, Constants.SM4_KEY));
        HashMap<String, String> map = new HashMap<>();
        map.put(id_type, cert_no);
        tid.setCertification(map);
        return IdMapping.idMappingFunction(tid, dataType, false,false,false);
    }
}
