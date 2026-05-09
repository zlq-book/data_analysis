package com.travelsky.trp.usercenter.data.analysis.transform.dkhcl;


import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CertDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.MobileDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.regex.Pattern;

public class DwdMobileDimTrans {

    private static final Logger logger = LoggerFactory.getLogger(DwdMobileDimTrans.class);

    // 手机号正则表达式：11位数字
    private static final Pattern MOBILE_PATTERN = Pattern.compile("^1[3-9]\\d{9}$");

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
        // 2. 通过 COMPANY_ID 关联 CRM_EMPLOYEE 获取军人手机号
        String query = "SELECT " +
                "  emp.ID AS EMPLOYEE_ID, " +
                "  emp.NAME AS NAME, " +
                "  cert.NATIONALITY AS NATIONALITY, "+
                "  emp.MOBILE AS MOBILE, " +
                "  comp.CUS_BIG_CODE AS CUS_BIG_CODE " +
                "FROM T_ODS_DKHCL_CRM_COMPANY comp " +
                "INNER JOIN T_ODS_DKHCL_CRM_EMPLOYEE emp " +
                "  ON comp.ID = emp.COMPANY_ID " +
                "INNER JOIN T_ODS_DKHCL_CRM_EMPLOYEE_CERT cert " +
                "  ON emp.ID = cert.EMPLOYEE_ID " +
                "WHERE comp.CUS_BIG_CODE = '99998181' " +
                "  AND emp.MOBILE IS NOT NULL " +
                "  AND TRIM(emp.MOBILE) <> ''  " +
                "  AND emp.ETL_DATE = '" + etlDate + "'";

        Table resultTable = tEnv.sqlQuery(query);

        // 转换为 DataStream
        DataStream<Row> rowDataStream = tEnv.toDataStream(resultTable);

        // 映射为手机号维度模型
        SingleOutputStreamOperator<MobileDimModel> mobileStream = rowDataStream
                .map(new MapFunction<Row, MobileDimModel>() {
                    @Override
                    public MobileDimModel map(Row row) throws Exception {

                        String rawMobile = toStringSafe(row.getField("MOBILE"));
                        String employeeId = toStringSafe(row.getField("EMPLOYEE_ID"));

                        // 处理手机号（解密或直接使用）
                        String mobile = NormalizationUtils.standardize(FieldType.MOBILE_NO,rawMobile, DataSource.KEY_ACCOUNT_TRAVEL);

                        Tid tid1 = new Tid();
                        tid1.setTid(employeeId);
                        String T_id = IdMapping.idMappingFunction(tid1, "DKHCL");


                        MobileDimModel model = new MobileDimModel();

                        // 设置主键：手机号
                        model.setSystemKey(T_id+mobile);
                        model.settId(T_id);
                        model.setMobileNumber(mobile);
                        // 当前时间
                        model.setUpdateTime(String.valueOf(LocalDateTime.now()));

                        return model;
                    }
                })
                .filter(model -> model != null);  // 过滤掉无效数据


        // 映射为手机号维度模型
        SingleOutputStreamOperator<UserDimModel> userStream = rowDataStream
                .map(new MapFunction<Row, UserDimModel>() {
                    @Override
                    public UserDimModel map(Row row) throws Exception {

                        String name = NormalizationUtils.standardize(FieldType.CN_NAME,toStringSafe(row.getField("NAME")), DataSource.KEY_ACCOUNT_TRAVEL);
                        String nationality = NormalizationUtils.standardize(FieldType.NATIONALITY, toStringSafe(row.getField("NATIONALITY")), DataSource.KEY_ACCOUNT_TRAVEL);
                        String employeeId = toStringSafe(row.getField("EMPLOYEE_ID"));

                        Tid tid1 = new Tid();
                        tid1.setTid(employeeId);
                        String T_id = IdMapping.idMappingFunction(tid1, "DKHCL");

                        UserDimModel model = new UserDimModel();

                        // 设置主键：手机号
                        model.setPkId(T_id);
                        model.setCnName(name);
                        model.setNationality(nationality);
                        // 当前时间
                        model.setUpdateTime(String.valueOf(LocalDateTime.now()));

                        return model;
                    }
                })
                .filter(model -> model.getPkId() != null);  // 过滤掉无效数据


        // 写入 Doris 表 T_DIM_MOBILE_DIM
        logger.info("写入 Doris 表: DB={}, Table={}", Constants.DIM_DB, "T_DIM_MOBILE_DIM");
        DorisSink<MobileDimModel> mobileSink = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_MOBILE_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);

        mobileStream.sinkTo(mobileSink);
        // 打印到控制台（本地测试用）
        mobileStream.print("MOBILE_DIM_MODEL");


        // 写入 Doris 表 T_DIM_USER_DIM
        logger.info("写入 Doris 表: DB={}, Table={}", Constants.DIM_DB, "T_DIM_USER_DIM");
        DorisSink<UserDimModel> userSink = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_USER_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);
        userStream.sinkTo(userSink);
        userStream.print("USER_DIM_MODEL");
        logger.info("手机号维度表、手机号维表 ETL 任务完成，数据源：大客户差旅系统");
    }

}
