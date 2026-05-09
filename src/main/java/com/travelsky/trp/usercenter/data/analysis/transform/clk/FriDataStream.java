package com.travelsky.trp.usercenter.data.analysis.transform.clk;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.toStringSafe;

public class FriDataStream {

    static final Logger logger = LoggerFactory.getLogger(FriDataStream.class);
    // 常客注册事实表 24年开始新数据
    public static void result(StreamTableEnvironment tEnv, String etlDate) {
        //创建Doris目标表
        tEnv.executeSql(CreateTableSql.CLK_FFP_REGISTER_INFO);
        tEnv.executeSql(CreateTableSql.CLK_T_DW_F_SCFFP_CHANNEL);
        tEnv.executeSql(CreateTableSql.CLK_FFP_REGISTER_SCAN);

        // 加载当天的 ffp_register_scan 数据，缓存 FFP_REGISTER_INFO_ID 集合（去重）
        // 对于T+1这一天，常旅客扫码表入事实表的时候，需要常旅客注册信息表FFP_REGISTER_INFO的T的数据已经全部进入ODS层
        // 所以这里加载的是当天的扫码表数据，用于匹配当天的注册信息表数据
        Set<String> scanRegisterInfoIdSet = loadScanRegisterInfoIds(etlDate);
        logger.info("加载 ffp_register_scan 数据，ETL_DATE={}, 去重后的 FFP_REGISTER_INFO_ID 数量={}", etlDate, scanRegisterInfoIdSet.size());

        // 从 Doris 表中读取数据
        Table dorisTable = tEnv.sqlQuery( "SELECT \n" +
                "a.FCARD ,\n" +
                "a.CREATE_DATE ,\n" +
                "a.UPDATE_DATE ,\n" +
                "a.FFLTDATE\n ,\n" +
                "a.FSEX ,\n" +
                "a.FIDCARD ,\n" +
                "a.FCARDTYPE ,\n" +
                "a.F_SECOND_NAME_EN ,\n" +
                "a.F_FIRST_NAME_EN ,\n" +
                "a.FNAMECN ,\n" +
                "a.F_FIRTST_NAME_CN ,\n" +
                "a.FBIRTHDAY ,\n" +
                "a.PARENT_FFP_NUM ,\n" +
                "a.OP_ACCOUNT ,\n" +
                "a.FUSER ,\n" +
                "a.OP_NAME ,\n" +
                "a.OPERATE_DEPT, \n" +
                "a.ID, \n" +
                "a.PROVINCE, \n" +
                "a.CITY, \n" +
                "a.FPHONE, \n" +
                "a.EMAIL ,\n" +
                "a.FCARD TID\n" +
                "FROM T_ODS_CLK_FFP_REGISTER_INFO a\n" +
                "WHERE a.SEND_FLAG = 'Y'\n" +
                "AND a.SEND_STATUS = '1'\n"
                + " AND  ETL_DATE='" + etlDate + "'"
        );

        DataStream<Row> rowStream = tEnv.toChangelogStream(dorisTable)
                .filter((row) -> row.getKind() == RowKind.INSERT || row.getKind() == RowKind.UPDATE_AFTER);

        // 标准化 IdMapping
        DataStream<Row> rowDataStream = rowStream.filter(row -> {
            boolean flag = row.getField("FCARD") == null;
            // 没有 FCARD 无法获得主键
            if (flag) {
                logger.info("标准化异常，oriTable：T_ODS_CLK_FFP_REGISTER_INFO，" + row.toString());
            }
            return !flag;
        }).map(row -> {
            // 常客卡号 标准化
            String fCard = NormalizationUtils.standardize(FieldType.FREQUENT_FLYER_NUM,
                    row.getField("FCARD").toString());
            // 获取tid
            Tid tid = new Tid();
            tid.setTid(fCard);
            tid.setFrequentTravelerCardno(fCard);
            Row rowModel = Row.copy(row);

            // 常客号
            rowModel.setField("FCARD", fCard);
            // 父母常客号
            Object parentFfpNum = row.getField("PARENT_FFP_NUM");
            if (parentFfpNum != null) {
                rowModel.setField("PARENT_FFP_NUM", NormalizationUtils.standardize(FieldType.FREQUENT_FLYER_NUM,
                        parentFfpNum.toString()));
            }
            // 省份
            Object province = row.getField("PROVINCE");
            if (province != null) {
                rowModel.setField("PROVINCE", NormalizationUtils.standardize(FieldType.PROVINCE,
                        province.toString()));
            }
            // 城市
            Object city = row.getField("CITY");
            if (city != null) {
                rowModel.setField("CITY", NormalizationUtils.standardize(FieldType.CITY,
                        city.toString()));
            }
            // 民族

            // 性别
            Object sex = row.getField("FSEX");
            if (sex != null) {
                rowModel.setField("FSEX", NormalizationUtils.standardize(FieldType.GENDER,
                        sex.toString()));
            }
            // 手机号
            Object contactNumber = row.getField("FPHONE");
            if (contactNumber != null) {
                rowModel.setField("FPHONE", NormalizationUtils.standardize(FieldType.MOBILE_NO,
                        contactNumber.toString()));
            }
            // 英文姓名
            Object secondNameEn = row.getField("F_SECOND_NAME_EN");
            if (secondNameEn != null) {
                rowModel.setField("F_SECOND_NAME_EN", NormalizationUtils.standardize(FieldType.EN_NAME,
                        secondNameEn.toString()));
            }
            Object firstNameEn = row.getField("F_FIRST_NAME_EN");
            if (firstNameEn != null) {
                rowModel.setField("F_FIRST_NAME_EN", NormalizationUtils.standardize(FieldType.EN_NAME,
                        firstNameEn.toString()));
            }
            // 中文姓名
            Object cnName = row.getField("FNAMECN");
            if (cnName != null) {
                rowModel.setField("FNAMECN", NormalizationUtils.standardize(FieldType.CN_NAME,
                        cnName.toString()));
            }
            Object firstCnName = row.getField("F_FIRTST_NAME_CN");
            if (firstCnName != null) {
                rowModel.setField("F_FIRTST_NAME_CN", NormalizationUtils.standardize(FieldType.CN_NAME,
                        firstCnName.toString()));
            }

            // 证件类型
            Object certType = row.getField("FCARDTYPE");
            if (certType != null) {
                rowModel.setField("FCARDTYPE", NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE,
                        certType.toString(), DataSource.DATA_WAREHOUSE));
            }
            // 证件号
            Object certNumber = row.getField("FIDCARD");
            if (certNumber != null) {
                rowModel.setField("FIDCARD", NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,
                        certNumber.toString()));
            }
            if (certType != null && certNumber != null) {
                // tid 关联证件号
                Map<String, String> certMap = new HashMap<>();
                certMap.put(certType.toString(), certNumber.toString());
                tid.setCertification(certMap);

            }
            String tidStr = IdMapping.idMappingFunction(tid, "FFP");
            rowModel.setField("TID", tidStr);
            return rowModel;
        });

        // 常客注册事实表
        FriToDwdMileRegisterFactNewTrans.output(rowDataStream, scanRegisterInfoIdSet);
        // 用户维表
        FriToDimUserDimNewTrans.output(rowDataStream);
        // 手机维表
        FriToDimMobileDimNewTrans.output(rowDataStream);
        // 证件维表
        FriToDimCertDimNewTrans.output(rowDataStream);
        // 常客信息维表
        FriToDimFfpDimNewTrans.output(rowDataStream, scanRegisterInfoIdSet);


    }

    private static Set<String> loadScanRegisterInfoIds(String etlDate) {
        Set<String> idSet = new HashSet<>();
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.ODS_USER, Constants.ODS_PWD);
            stmt = DorisUtils.getStatement(conn);
            String sql = "SELECT DISTINCT FFP_REGISTER_INFO_ID " +
                    "FROM " + Constants.ODS_DB + ".T_ODS_CLK_FFP_REGISTER_SCAN " +
                    "WHERE CREATE_TIME = '" + etlDate + "' " +
                    "AND FFP_REGISTER_INFO_ID IS NOT NULL";
            rs = DorisUtils.getDorisResult(stmt, sql);
            if (rs != null) {
                while (rs.next()) {
                    String id = rs.getString("FFP_REGISTER_INFO_ID");
                    if (id != null && !id.trim().isEmpty()) {
                        idSet.add(id.trim());
                    }
                }
            }
        } catch (Exception e) {
            logger.error("加载 ffp_register_scan 数据失败: {}", e.getMessage(), e);
        } finally {
            DorisUtils.close(conn, stmt, rs);
        }
        return idSet;
    }
}
