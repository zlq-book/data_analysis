package com.travelsky.trp.usercenter.data.analysis.transform.lyx;

import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.DataSource;
import com.travelsky.dataplatform.utils.FieldType;
import com.travelsky.dataplatform.utils.IdMapping;
import com.travelsky.dataplatform.utils.NormalizationUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class LmMlsLluDataStream {
    static final Logger logger = LoggerFactory.getLogger(LmMlsLluDataStream.class);

    public static void result(StreamTableEnvironment tEnv, String etlDate) {
        // 创建Doris目标表
        tEnv.executeSql(CreateTableSql.LYX_LY_MTMEMBER);

        // 创建Doris目标表 联表
        tEnv.executeSql(CreateTableSql.LYX_LYMTM_LEVEL_UNIT);
        tEnv.executeSql(CreateTableSql.LYX_MTM_LYVAL_SUMMARY);
        tEnv.executeSql(CreateTableSql.LYX_LYMTM_CERTIFICATES);
        // 从 Doris 表中读取数据
        Table dorisTable = tEnv.sqlQuery( "SELECT \n" +
                "a.ID ,\n" +
                "a.MTM_SURNAME_CN,\n" +
                "a.MTM_NAME_CN,\n" +
                "a.MTM_SURNAME_EN,\n" +
                "a.MTM_NAME_EN,\n" +
                "a.MTM_BIRTHDAY,\n" +
                "a.MTM_NATIONALITY,\n" +
                "a.MTM_MOBILE,\n" +
                "a.MAIN_ID_TYPE,\n" +
                "a.MAIN_ID_NUM,\n" +
                "a.MTM_REGISTER_TIME,\n" +
                "a.MTM_CARD_NUM,\n" +
                "a.MTM_STATUS,\n" +
                "a.REGISTER_STATUS,\n" +
                "a.REALNAME_ATTESTATION,\n" +
                "a.ATTESTATION_MODE,\n" +
                "a.LIP_ATTESTATION,\n" +
                "b.LEVEL_CODE ,\n" +
                "c.CYCLE_LYVAL,\n" +
                "c.USABLE_LYVAL,\n" +
                "d.EFFECTIVE_TIME ,\n" +
                "d.EXPIRES_TIME ,\n" +
                "a.MTM_CARD_NUM as TID,\n" +
                "a.ATTESTATION_TIME,\n" +
                "a.TIMING_TASK_STATUS,\n" +
                "a.LEVEL_UPDATE_TIME,\n" +
                "a.ORI_LEVEL,\n" +
                "b.LEVEL_CODE as OLD_LEVEL\n" +
                "FROM T_ODS_LYX_LY_MTMEMBER a\n" +
                "INNER JOIN T_ODS_LYX_LYMTM_LEVEL_UNIT b\n" +
                "ON a.MTM_CARD_NUM = b.MTM_CARD_NUM \n" +
                "INNER JOIN T_ODS_LYX_MTM_LYVAL_SUMMARY c\n" +
                " ON a.MTM_CARD_NUM = c.MTM_CARD_NUM   \n" +
                "LEFT JOIN T_ODS_LYX_LYMTM_CERTIFICATES d \n" +
                "ON a.MTM_CARD_NUM = d.MTM_CARD_NUM  "
                + " WHERE  a.ETL_DATE='" + etlDate + "'"
                );

        DataStream<Row> rowStream = tEnv.toChangelogStream(dorisTable)
                .filter((row) -> row.getKind() == RowKind.INSERT || row.getKind() == RowKind.UPDATE_AFTER);

        // 标准化 IdMapping
        DataStream<Row> rowDataStream = rowStream.filter(row -> {
            boolean flag = row.getField("MTM_CARD_NUM") == null;
            if (flag) {
                logger.info("缺少鲁雁行卡号，oriTable：T_ODS_LYX_ORDER_CHANGE_REFUND_INFO，" + row.toString());
            }
            return !flag;
        }).map(row -> {
            // 手机号
            Object mtmMobile = row.getField("MTM_MOBILE");
            if (mtmMobile != null) {
                row.setField("MTM_MOBILE", NormalizationUtils.standardize(FieldType.MOBILE_NO,
                        mtmMobile.toString()));
            }
            // 证件类型
            Object certType = row.getField("MAIN_ID_TYPE");
            if (certType != null) {
                row.setField("MAIN_ID_TYPE", NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE,
                        certType.toString(), DataSource.LUYAN_TRIP));
            }
            // 证件号
            Object certNumber = row.getField("MAIN_ID_NUM");
            if (certNumber != null) {
                row.setField("MAIN_ID_NUM", NormalizationUtils.standardize(FieldType.DOCUMENT_NUM,
                        certNumber.toString()));
            }
            // 获取tid
//            Tid tid = new Tid();
//            tid.setTid( row.getField("MTM_CARD_NUM").toString());
//            tid.setLyCardNumber( row.getField("MTM_CARD_NUM").toString());
            // 强id 证件信息
            if (null != row.getField("MAIN_ID_TYPE") && null != row.getField("MAIN_ID_NUM")) {
                Map<String ,String> certMap = new HashMap<>();
                certMap.put(row.getField("MAIN_ID_TYPE").toString(), row.getField("MAIN_ID_NUM").toString());
//                tid.setCertification(certMap);

            }
            // 强id 手机号 雁行实名认证标识是已认证的且认证方式不为LIP认证的手机号，或鲁雁行认证方式为LIP认证同时三要素认证结果是匹配的手机号。
//            if (null != row.getField("REALNAME_ATTESTATION") && null != row.getField("ATTESTATION_MODE")
//                    && mtmMobile != null) {
//                boolean firstFlag = "0".equals(row.getField("REALNAME_ATTESTATION").toString())
//                        // 雁行实名认证标识是已认证的且认证方式不为LIP认证的手机号
//                        && !"5".equals(row.getField("ATTESTATION_MODE").toString());
//                boolean secondFlag = false;
//                if (null != row.getField("LIP_ATTESTATION")) {
//                    // 鲁雁行认证方式为LIP认证同时三要素认证结果是匹配的手机号。
//                    secondFlag = "0".equals(row.getField("REALNAME_ATTESTATION").toString())
//                            && "5".equals(row.getField("ATTESTATION_MODE").toString())
//                            && "0".equals(row.getField("LIP_ATTESTATION").toString());
//                }
//                if (firstFlag || secondFlag) {
//                    tid.setMobilePhone(row.getField("MTM_MOBILE").toString());
//                }
//            }
//            String tidStr = IdMapping.idMappingFunction(tid, "LYC");
            row.setField("TID", row.getField("MTM_CARD_NUM").toString());

            return row;
        });


        // 用户维表
        LmMlsLluToDimUserDimTrans.output(rowDataStream);
        // 证件信息维表
        LmMlsLluToDimCertDimTrans.output(rowDataStream);
        // 手机号维表
        LmMlsLluToDimMobileDimTrans.output(rowDataStream);
        // 鲁雁行用户维表
        LmMlsLluToDimLyxDimTrans.output(rowDataStream);
    }
}
