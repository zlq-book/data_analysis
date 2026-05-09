package com.travelsky.dataplatform.main.dwd;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.trp.usercenter.data.analysis.transform.zsf.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

public class ZsfOdrOrderDetailSecondarycardToDwd {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);

        // 1、创建源表
        tEnv.executeSql(CreateTableSql.ZSF_ODR_ORDER_DETAIL_SECONDARYCARD);
        tEnv.executeSql(CreateTableSql.ZSF_ODR_ORDER_CUSTOMER_RELATION);


        // Join 查询
        String query = "SELECT \n" +
                "    a.ETL_DATE AS ETL_DATE,\n" +
                "    a.ID AS ID,\n" +
                "    a.ORDER_PACKAGE_ID AS ORDER_PACKAGE_ID,\n" +
                "    a.ORDER_DETAIL_ID AS ORDER_DETAIL_ID,\n" +
                "    a.ORDER_NO AS ORDER_NO,\n" +
                "    a.MAIN_ORDER_NO AS MAIN_ORDER_NO,\n" +
                "    a.MAIN_ORDER_ID AS MAIN_ORDER_ID,\n" +
                "    a.INSTANCE_ID AS INSTANCE_ID,\n" +
                "    a.INSTANCE_NO AS INSTANCE_NO,\n" +
                "    a.ACTIVITY_NO AS ACTIVITY_NO,\n" +
                "    a.PRODUCT_NO AS PRODUCT_NO,\n" +
                "    a.PACKAGE_INDEX AS PACKAGE_INDEX,\n" +
                "    a.SALE_PRICE AS SALE_PRICE,\n" +
                "    a.STATE AS STATE,\n" +
                "    a.CUSTOMER_ID AS CUSTOMER_ID,\n" +
                "    a.MEMBER_ID AS MEMBER_ID,\n" +
                "    a.CUSTOMER_NAME AS CUSTOMER_NAME,\n" +
                "    a.ACTIVE_TIME AS ACTIVE_TIME,\n" +
                "    a.REMARK AS REMARK,\n" +
                "    a.INTERNATIONAL_FLAG AS INTERNATIONAL_FLAG,\n" +
                "    a.CERT_TYPE AS CERT_TYPE,\n" +
                "    a.CERT_NO AS CERT_NO,\n" +
                "    a.CN_FIRST_NAME AS CN_FIRST_NAME,\n" +
                "    a.CN_LAST_NAME AS CN_LAST_NAME,\n" +
                "    a.EN_FIRST_NAME AS EN_FIRST_NAME,\n" +
                "    a.EN_LAST_NAME AS EN_LAST_NAME,\n" +
                "    a.EXPIRATION_DATE AS EXPIRATION_DATE,\n" +
                "    a.SEX AS SEX,\n" +
                "    a.BIRTHDAY AS BIRTHDAY,\n" +
                "    a.PASSPORT_ISSUE_NATION AS PASSPORT_ISSUE_NATION,\n" +
                "    a.PASSPORT_NATION AS PASSPORT_NATION,\n" +
                "    a.PHONE_NUM AS PHONE_NUM,\n" +
                "    a.ENABLE AS ENABLE,\n" +
                "    a.CREATE_TIME AS CREATE_TIME,\n" +
                "    a.CREATE_USER AS CREATE_USER,\n" +
                "    a.UPDATE_USER AS UPDATE_USER,\n" +
                "    a.UPDATE_TIME AS UPDATE_TIME,\n" +
                "    a.SEND_MSG_FLAG AS SEND_MSG_FLAG,\n" +
                "    a.EXCHANGE_NUM AS EXCHANGE_NUM,\n" +
                "    a.EXPIRY_DATE AS EXPIRY_DATE,\n" +
                "    a.TRAVEL_START_DATE AS TRAVEL_START_DATE,\n" +
                "    a.TRAVEL_END_DATE AS TRAVEL_END_DATE,\n" +
                "    b.CUSTOMER_ID AS CUSTOMER_ID\n" +
                "FROM T_ODS_ZSF_ODR_ORDER_DETAIL_SECONDARYCARD a\n" +
                "LEFT JOIN T_ODS_ZSF_ODR_ORDER_CUSTOMER_RELATION b\n" +
                "ON a.MAIN_ORDER_NO = b.MAIN_ORDER_NO\n" +
//                "WHERE a.ETL_DATE = '" + etlDate + "'";
                " WHERE  a.CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                + " OR  a.UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' ";

        Table dorisTable1 = tEnv.sqlQuery(query);

        DataStream<Row> rowDataStream = tEnv.toChangelogStream(dorisTable1)
                .filter(row -> row.getKind().equals(RowKind.INSERT));


        // 执行任务
//        OdrOrderDetailSecondarycardFactTrans.result(tEnv, etlDate);

        // 次卡预定
        ZsfToMutiCardBookFactTrans.result(rowDataStream, etlDate);
//
//        // 手机号维表
        ZsfToMobileDimTrans.result(rowDataStream, etlDate);
//
//        // 次卡取消
        ZsfToMutiCardCancelFactTrans.result(rowDataStream, etlDate);
//
//        // 证件维表
        ZsfToCertDimTrans.result(rowDataStream, etlDate);

        // 用户维表、数据源表
        ZsfTOUserDimTrans.result(rowDataStream, etlDate);

        env.execute("Flink Consumer ->ZSF Doris TO DWD");


    }
}
