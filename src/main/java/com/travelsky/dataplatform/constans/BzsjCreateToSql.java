package com.travelsky.dataplatform.constans;

import com.travelsky.dataplatform.utils.GetTableSql;

public class BzsjCreateToSql {
    public static final String TB_TCN_DATA = "CREATE TABLE T_ODS_BZSJ_TB_TCN_DATA (\n" +
            "ROW_ID VARCHAR(100) , \n" +
            "EX_DATE VARCHAR(10) , \n" +
            "TKS VARCHAR(6), \n" +
            "TKORD BIGINT, \n" +
            "TK_NUM VARCHAR(36), \n" +
            "ORDTP VARCHAR(18), \n" +
            "TK_DATE VARCHAR(10), \n" +
            "DEP_DATE VARCHAR(10), \n" +
            "AIR_CODE VARCHAR(6), \n" +
            "FLIGHT_NO VARCHAR(15), \n" +
            "UP_LOCATION VARCHAR(9), \n" +
            "DIS_LOCATION VARCHAR(9), \n" +
            "CLASS_TYPE VARCHAR(1), \n" +
            "PS_TYPE VARCHAR(15), \n" +
            "FARE_TYPE VARCHAR(18), \n" +
            "AGENT VARCHAR(24), \n" +
            "FARE_TOL DECIMAL(12,2), \n" +
            "SEG_PRICE DECIMAL(12,2), \n" +
            "DIS_PRICE DECIMAL(8,2), \n" +
            "AGT_RATE DECIMAL(8,2), \n" +
            "ZSAL_RATE DECIMAL(8,2), \n" +
            "SEG_FARE DECIMAL(12,2), \n" +
            "SAL_RATE DECIMAL(8,2), \n" +
            "RATE DECIMAL(8,2), \n" +
            "CNAME VARCHAR(60), \n" +
            "AIR_PNR VARCHAR(18), \n" +
            "AGENT_PNR VARCHAR(18), \n" +
            "PNR VARCHAR(18), \n" +
            "PORT_FARE DECIMAL(8,2), \n" +
            "ZSAL_FARE DECIMAL(8,2), \n" +
            "ADD_FARE DECIMAL(8,2), \n" +
            "ORA VARCHAR(60) , \n" +
            "ID DECIMAL(8,2) , \n" +
            "FT_TK_NUM VARCHAR(36), \n" +
            "CP_STR VARCHAR(45), \n" +
            "TK_AGENT VARCHAR(24), \n" +
            "BBCODE VARCHAR(24), \n" +
            "AGT_FARE DECIMAL(8,2), \n" +
            "CLK_NUM VARCHAR(48), \n" +
            "TK_AIR VARCHAR(9), \n" +
            "FARE_RATE DECIMAL(8,4), \n" +
            "CLIENTCODE VARCHAR(45), \n" +
            "STATID VARCHAR(12), \n" +
            "MCAR VARCHAR(12), \n" +
            "TKTYPE VARCHAR(12), \n" +
            "EQFARE DECIMAL(12,2), \n" +
            "SMFARE DECIMAL(12,2), \n" +
            "OBFARE DECIMAL(12,2), \n" +
            "CHTKNUM VARCHAR(39), \n" +
            "GPSTATE VARCHAR(6), \n" +
            "VALAGT_CODE VARCHAR(24), \n" +
            "BGRP VARCHAR(1), \n" +
            "CHG_AIR VARCHAR(9), \n" +
            "CHG_TKNUM VARCHAR(39), \n" +
            "REF_PNR VARCHAR(18), \n" +
            "FPTPCODE VARCHAR(6), \n" +
            "EQFPN DECIMAL(10,2),\n" +
            "  `ETL_CREATE_TIME` TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
            "  `ETL_UPDATE_TIME` TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
            "  `ETL_DATE` date  NULL COMMENT '数据ETL日期'" +
            ") WITH (\n" +

            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_BZSJ_TB_TCN_DATA", Constants.ODS_USER, Constants.ODS_PWD)

            + ")";
}
