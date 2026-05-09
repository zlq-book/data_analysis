package com.travelsky.dataplatform.constans;

import com.travelsky.dataplatform.utils.GetTableSql;

public class ClkCreateToSql {
    public static final String QUALIFIED_NO_REGISTRATION ="CREATE TABLE T_ODS_CLK_QUALIFIED_NO_REGISTRATION (\n"+
            "ID BIGINT ,\n"+
            "ACTIVITY_CHINESE_NAME VARCHAR(500) ,\n"+
            "ACTIVITY_CODE VARCHAR(500) ,\n"+
            "MEMBER_CARD_NUMBER VARCHAR(500) ,\n"+
            "  `ETL_CREATE_TIME` TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n"+
            "  `ETL_UPDATE_TIME` TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n"+
            "  `ETL_DATE` date  NULL COMMENT '数据ETL日期'"+
            ") WITH (\n" +

            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_CLK_QUALIFIED_NO_REGISTRATION", Constants.ODS_USER, Constants.ODS_PWD)

            + ")";
}
