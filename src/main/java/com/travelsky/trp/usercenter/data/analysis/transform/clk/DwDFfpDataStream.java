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

import java.util.HashMap;
import java.util.Map;

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.toStringSafe;

public class DwDFfpDataStream {
    static final Logger logger = LoggerFactory.getLogger(DwDFfpDataStream.class);

    // 常客注册事实表 24年之前旧数据
    public static void result(StreamTableEnvironment tEnv, String etlDate) {
        //创建Doris目标表
        tEnv.executeSql(CreateTableSql.CLK_T_DW_D_FFP);
        tEnv.executeSql(CreateTableSql.CLK_FFP_REGISTER_INFO);

        // 从 Doris 表中读取数据
        Table dorisTable = tEnv.sqlQuery("SELECT \n" +
                "a.MEM_NUM ,\n" +
                "a.MEM_NUM ORG_F_CARD,\n" +
                "a.FFP_CREATED ,\n" +
                "a.FFP_LAST_UPD ,\n" +
                "a.GENDER_ID ,\n" +
                "a.PR_ID ,\n" +
                "a.PR_CATEGORY_CD ,\n" +
                "a.EN_FST_NAME ,\n" +
                "a.EN_LAST_NAME ,\n" +
                "a.CN_FST_NAME ,\n" +
                "a.CN_LAST_NAME ,\n" +
                "a.BIRTH_DT ,\n" +
                "a.TEST_MEM_FLAG ,\n" +
                "a.FLIGHT_FRQ_FLAG ,\n" +
                "a.PR_EXPIRATION_TS ,\n" +
                "a.PR_ISSUE_TS ,\n" +
                "a.PR_ISSUING_COUNTRY, \n" +
                "a.FFP_ID,  \n" +
                "a.PR_MOBILE,  \n" +
                "a.NATIONALITY,  \n" +
                "a.MEM_NUM TID \n" +
                "FROM T_ODS_CLK_T_DW_D_FFP a\n"
                + " WHERE  ETL_DATE='" + etlDate + "'"
        );

        DataStream<Row> rowStream = tEnv.toChangelogStream(dorisTable)
                .filter((row) -> row.getKind() == RowKind.INSERT || row.getKind() == RowKind.UPDATE_AFTER);

        // 标准化 IdMapping
        DataStream<Row> rowDataStream = rowStream.filter(row -> {
            boolean flag = row.getField("MEM_NUM") == null;
            // 没有 FCARD 无法获得主键
            if (flag) {
                logger.info("标准化异常，oriTable：T_ODS_CLK_T_DW_D_FFP，" + row.toString());
            }
            return !flag;
        }).map(row -> {
            // 常客卡号 原始数据
            String orgFCard = row.getField("MEM_NUM").toString();
            // 常客卡号 标准化
            String fCard = NormalizationUtils.standardize(FieldType.FREQUENT_FLYER_NUM,
                    orgFCard);
            // 获取tid
            Tid tid = new Tid();
            tid.setTid(fCard);
            tid.setFrequentTravelerCardno(fCard);
            Row rowModel = Row.copy(row);

            // 常客号
            rowModel.setField("MEM_NUM", fCard);
            // 原始常客号
            rowModel.setField("ORG_F_CARD",orgFCard);
            // 性别
            Object sex = row.getField("GENDER_ID");
            if (sex != null) {
                rowModel.setField("GENDER_ID", NormalizationUtils.standardize(FieldType.GENDER,
                        sex.toString()));
            }
            // 手机号
            Object contactNumber = row.getField("PR_MOBILE");
            if (contactNumber != null) {
                rowModel.setField("PR_MOBILE", NormalizationUtils.standardize(FieldType.MOBILE_NO,
                        contactNumber.toString()));
            }
            // 英文姓名
            Object enFstName = row.getField("EN_FST_NAME");
            if (enFstName != null) {
                rowModel.setField("EN_FST_NAME", NormalizationUtils.standardize(FieldType.EN_NAME,
                        enFstName.toString()));
            }
            Object enLastName = row.getField("EN_LAST_NAME");
            if (enLastName != null) {
                rowModel.setField("EN_LAST_NAME", NormalizationUtils.standardize(FieldType.EN_NAME,
                        enLastName.toString()));
            }
            // 中文姓名
            Object cnFstName = row.getField("CN_FST_NAME");
            if (cnFstName != null) {
                rowModel.setField("CN_FST_NAME", NormalizationUtils.standardize(FieldType.CN_NAME,
                        cnFstName.toString()));
            }
            Object cnLastName = row.getField("CN_LAST_NAME");
            if (cnLastName != null) {
                rowModel.setField("CN_LAST_NAME", NormalizationUtils.standardize(FieldType.CN_NAME,
                        cnLastName.toString()));
            }
            // 国籍
            String nationality = NormalizationUtils.standardize(FieldType.NATIONALITY, toStringSafe(row.getField("NATIONALITY")), DataSource.FREQUENT_FLYER);
            rowModel.setField("NATIONALITY", nationality);
            // 证件类型
            Object certType = row.getField("PR_CATEGORY_CD");
            if (certType != null) {
                rowModel.setField("PR_CATEGORY_CD", NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE,
                        certType.toString(), DataSource.FREQUENT_FLYER));
            }
            // 证件号
            Object certNumber = row.getField("PR_ID");
            if (certNumber != null) {
                rowModel.setField("PR_ID", NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,
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
        DwDFfpToDwdMileRegisterFactOldTrans.output(rowDataStream);
        // 用户维表
        DwDFfpToDimUserDimOldTrans.output(rowDataStream);
        // 手机维表
        DwDFfpToDimMobileDimOldTrans.output(rowDataStream);
        // 证件维表
        DwDFfpToDimCertDimOldTrans.output(rowDataStream);
        // 常客信息维表
        DwDFfpToDimFfpDimOldTrans.output(rowDataStream);


    }

}
