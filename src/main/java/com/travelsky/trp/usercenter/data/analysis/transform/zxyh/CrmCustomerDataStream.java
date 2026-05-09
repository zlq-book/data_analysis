package com.travelsky.trp.usercenter.data.analysis.transform.zxyh;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.utils.TransUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrmCustomerDataStream {

    static final Logger logger = LoggerFactory.getLogger(CrmCustomerDataStream.class);

    public static void result(StreamTableEnvironment tEnv, String etlDate) {
        //创建Doris目标表
        tEnv.executeSql(CreateTableSql.ZXYH_CRM_CUSTOMER_CONTACT_CERT);
        // 从 Doris 表中读取数据
        Table dorisTable = tEnv.sqlQuery( "SELECT \n" +
                "a.CUSTOMER_ID ,\n" +
                "CAST(a.REGIST_DATE AS DATE) FK_REGISTER_DATE,\n" +
                "DATE_FORMAT(a.REGIST_DATE, 'HH:mm:ss') REGISTER_TIME,\n" +
                "a.REGIST_CHANNEL AK_REGISER_CHANNEL,\n" +
                "a.REGISTER_IP,\n" +
                "a.CN_NAME,\n" +
                "a.ENG_NAME,\n" +
                "a.LAST_CN_NAME,\n" +
                "a.FIRST_CN_NAME,\n" +
                "a.SEX,\n" +
                "a.PROVINCE,\n" +
                "a.`NATIONAL`,\n" +
                "a.ETHNIC_GROUP,\n" +
                "a.CONTACT_NUMBER ,\n" +
                "a.ENABLE,\n" +
                "a.CUSTOMER_STATUS,\n" +
                "a.REGIST_DATE,\n" +
                "a.REAL_NAME_FLAG,\n" +
                "a.REAL_NAME_TYPE,\n" +
                "a.REAL_NAME_TIME,\n" +
                "DATE_FORMAT(a.UPDATE_TIME, 'yyyyMMddHHmmss') SOURCE_LAST_UPDATETIME,\n" +
                "a.CREATE_TIME ,\n" +
                "a.LAST_ENG_NAME ,\n" +
                "a.FIRST_ENG_NAME ,\n" +
                "a.BIRTHDAY ,\n" +
                "a.LAST_LOGIN_TIME, \n" +
                "a.REGIST_CHANNEL, \n" +
                "a.WEB_IS_LIMITED ,\n" +
                "a.WEB_AUTHORIZE ,\n" +
                "a.CUSTOMER_ID TID,\n" +
                "a.REAL_NAME_FLAG_CONTACT,\n" +
                "a.CERTIFICATION_TYPE  ,\n" +
                "a.CERTIFICATION_STATUS\n" +
                " FROM T_ODS_ZXYH_CRM_CUSTOMER_CONTACT_CERT_VIEW a\n"
                + " WHERE  a.ETL_DATE='" + etlDate + "'"
        );

        DataStream<Row> rowStream = tEnv.toChangelogStream(dorisTable)
                .filter((row) -> row.getKind() == RowKind.INSERT || row.getKind() == RowKind.UPDATE_AFTER);



        // 标准化 IdMapping
        DataStream<Row> rowDataStream = rowStream.map(row -> {
            // 获取tid
            Tid tid = new Tid();
            tid.setTid(SM4Utils.encrypt(row.getField("CUSTOMER_ID").toString(), Constants.SM4_KEY));
            tid.setCrmCustomerId(SM4Utils.encrypt(row.getField("CUSTOMER_ID").toString(), Constants.SM4_KEY));
            Row rowModel = Row.copy(row);

            // 国籍
            Object national = row.getField("NATIONAL");
            if (national != null) {
                rowModel.setField("NATIONAL", NormalizationUtils.standardize(FieldType.NATIONALITY,
                        national.toString()));
            }
            // 省份
            Object province = row.getField("PROVINCE");
            if (province != null) {
                rowModel.setField("PROVINCE", NormalizationUtils.standardize(FieldType.PROVINCE,
                        province.toString()));
            }
            // 城市
            // 民族
            Object ethnicGroup = row.getField("ETHNIC_GROUP");
            if (ethnicGroup != null) {
                rowModel.setField("ETHNIC_GROUP", NormalizationUtils.standardize(FieldType.ETHNIC_GROUP,
                        ethnicGroup.toString()));
            }
            // 性别
            Object sex = row.getField("SEX");
            if (sex != null) {
                rowModel.setField("SEX", NormalizationUtils.standardize(FieldType.GENDER,
                        sex.toString()));
            }
            // 手机号
            Object contactNumber = row.getField("CONTACT_NUMBER");
            if (contactNumber != null) {
                rowModel.setField("CONTACT_NUMBER", NormalizationUtils.standardize(FieldType.MOBILE_NO,
                        contactNumber.toString()));

            }
            // 是否直销实名认证手机号
            if (row.getField("CERTIFICATION_TYPE") != null
                    && row.getField("CERTIFICATION_STATUS") != null
                    && ("1".equals(row.getField("CERTIFICATION_TYPE").toString())
                    || "2".equals(row.getField("CERTIFICATION_TYPE").toString()))
                    && "1".equals(row.getField("CERTIFICATION_STATUS").toString())) {
                if (contactNumber != null) {
                    tid.setMobilePhone(rowModel.getField("CONTACT_NUMBER").toString());
                }

            }
            // 英文姓名
            Object engName = row.getField("ENG_NAME");
            if (engName != null) {
                rowModel.setField("ENG_NAME", NormalizationUtils.standardize(FieldType.EN_NAME,
                        engName.toString()));
            }
            Object lastEngName = row.getField("LAST_ENG_NAME");
            if (lastEngName != null) {
                rowModel.setField("LAST_ENG_NAME", NormalizationUtils.standardize(FieldType.EN_NAME,
                        lastEngName.toString()));
            }
            Object firstEngName = row.getField("FIRST_ENG_NAME");
            if (firstEngName != null) {
                rowModel.setField("FIRST_ENG_NAME", NormalizationUtils.standardize(FieldType.EN_NAME,
                        firstEngName.toString()));
            }
            // 中文姓名
            Object cnName = row.getField("CN_NAME");
            if (cnName != null) {
                rowModel.setField("CN_NAME", NormalizationUtils.standardize(FieldType.CN_NAME,
                        cnName.toString()));
            }
            Object lastCnName = row.getField("LAST_CN_NAME");
            if (lastCnName != null) {
                rowModel.setField("LAST_CN_NAME", NormalizationUtils.standardize(FieldType.CN_NAME,
                        lastCnName.toString()));
            }
            Object firstCnName = row.getField("FIRST_CN_NAME");
            if (firstCnName != null) {
                rowModel.setField("FIRST_CN_NAME", NormalizationUtils.standardize(FieldType.CN_NAME,
                        firstCnName.toString()));
            }
            String tidStr = IdMapping.idMappingFunction(tid, "ZXYH");
            rowModel.setField("TID", tidStr);
            return rowModel;
        });

        // 直销用户事实表
        CrmCustomerToDwdRegisterUdoFactTrans.output(rowDataStream);
        // 直销用户维表
        CrmCustomerToDimCustomDimTrans.output(rowDataStream);
        // 用户维表
        CrmCustomerToDimUserDimTrans.output(rowDataStream);
        // 手机维表
        CrmCustomerToDimMobileDimTrans.output(rowDataStream);
    }
}
