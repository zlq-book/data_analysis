package com.travelsky.trp.usercenter.data.analysis.cascrmclient;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.GetTableSql;

public class CrmTableSql {

    public static String getQueryTOdsDpiTmp() {
        String sql = "CREATE TABLE DPI_TMP (\n" +
                "  `FFP` VARCHAR(200),\n" +
                "  `CERT_NO` VARCHAR(200),\n" +
                "  `PSG_NAME_CN` VARCHAR(200),\n" +
                "  `PSG_NAME_EN` VARCHAR(200),\n" +
                "  `ID_TYPE` VARCHAR(200)\n" +
                ") WITH (\n" +
                GetTableSql.getDorisConnector(Constants.ODS_DB, "DPI_TMP", Constants.ODS_USER, Constants.ODS_PWD)
                + ")";
        return sql;
    }

    public static String getQueryDwdCrm() {
        String sql = "CREATE TABLE T_DWD_DEPART_SEG_FACT_CRM (\n" +
                "  `SERIAL_NO` BIGINT,\n" +
                "  `CERT_NUMBER` VARCHAR(65533),\n" +
                "  `CERT_TYPE` VARCHAR(20)\n" +
                ") WITH (\n" +
                GetTableSql.getDorisConnector(Constants.DWD_DB, "T_DWD_DEPART_SEG_FACT_CRM", Constants.DWD_USER, Constants.DWD_PWD)
                + ")";
        return sql;
    }

    public static String getDepartSegDwdCrm() {
        String sql = "CREATE TABLE T_DWD_DEPART_SEG_FACT (\n" +
                "  `PK_ID` varchar(128),\n" +
                "  `CERT_TYPE` varchar(20) ,\n" +
                "  `CERT_NUMBER` varchar(128) ,\n" +
                "  `FF_AIRLINE` varchar(10) ,\n" +
                "  `FFRF` varchar(128) ,\n" +
                "  `DATA_ACTIVE` boolean ,\n" +
                "  `SYSTEM_CREATETIME` TIMESTAMP(6) ,\n" +
                "  `SYSTEM_LAST_UPDATETIME` TIMESTAMP(6) " +
                ") WITH (\n" +
                GetTableSql.getDorisConnector(Constants.DWD_DB, "T_DWD_DEPART_SEG_FACT", Constants.DWD_USER, Constants.DWD_PWD)
                + ")";
        return sql;
    }

    public static String getFfpCertCheckDimCrm() {
        String sql = "CREATE TABLE T_DIM_FFP_CERT_CHECK_DIM (\n" +
                "  `SYSTEM_KEY` varchar(256) ,\n" +
                "  `CREDENTIALNUM` varchar(500)\n" +
                ") WITH (\n" +
                GetTableSql.getDorisConnector(Constants.DIM_DB, "T_DIM_FFP_CERT_CHECK_DIM", Constants.DIM_USER, Constants.DIM_PWD)
                + ")";
        return sql;
    }
}
