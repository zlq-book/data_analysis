package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableResult;


import java.io.IOException;
import java.util.UUID;

public class ExtractTOdsZsfOdrOrderDetailSecondarycard {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key= Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZsfOdrOrderDetailSecondarycard");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);

        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE ODR_ORDER_DETAIL_SECONDARYCARD (\n" +
                "    ID VARCHAR(96) NOT NULL,\n" +
                "    ORDER_PACKAGE_ID VARCHAR(96),\n" +
                "    ORDER_DETAIL_ID VARCHAR(96),\n" +
                "    ORDER_NO VARCHAR(96),\n" +
                "    MAIN_ORDER_NO VARCHAR(96),\n" +
                "    MAIN_ORDER_ID VARCHAR(96),\n" +
                "    INSTANCE_ID VARCHAR(96),\n" +
                "    INSTANCE_NO VARCHAR(96),\n" +
                "    ACTIVITY_NO VARCHAR(96),\n" +
                "    PRODUCT_NO VARCHAR(96),\n" +
                "    PACKAGE_INDEX INT,\n" +
                "    SALE_PRICE DECIMAL(10,2),\n" +
                "    STATE VARCHAR(24),\n" +
                "    CUSTOMER_ID VARCHAR(96),\n" +
                "    MEMBER_ID VARCHAR(96),\n" +
                "    CUSTOMER_NAME VARCHAR(48),\n" +
                "    ACTIVE_TIME TIMESTAMP(6),\n" +
                "    REMARK VARCHAR(765),\n" +
                "    INTERNATIONAL_FLAG VARCHAR(60),\n" +
                "    CERT_TYPE VARCHAR(15),\n" +
                "    CERT_NO VARCHAR(96),\n" +
                "    CN_FIRST_NAME VARCHAR(765),\n" +
                "    CN_LAST_NAME VARCHAR(765),\n" +
                "    EN_FIRST_NAME VARCHAR(765),\n" +
                "    EN_LAST_NAME VARCHAR(765),\n" +
                "    EXPIRATION_DATE VARCHAR(96),\n" +
                "    SEX CHAR(1),\n" +
                "    BIRTHDAY VARCHAR(192),\n" +
                "    PASSPORT_ISSUE_NATION VARCHAR(192),\n" +
                "    PASSPORT_NATION VARCHAR(192),\n" +
                "    PHONE_NUM VARCHAR(96),\n" +
                "    ENABLE CHAR(1),\n" +
                "    CREATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    CREATE_USER VARCHAR(384) NOT NULL,\n" +
                "    UPDATE_USER VARCHAR(384) NOT NULL,\n" +
                "    UPDATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    SEND_MSG_FLAG CHAR(1),\n" +
                "    EXCHANGE_NUM VARCHAR(30),\n" +
                "    EXPIRY_DATE TIMESTAMP(6),\n" +
                "    TRAVEL_START_DATE TIMESTAMP(6),\n" +
                "    TRAVEL_END_DATE TIMESTAMP(6)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://"+ Constants.ZSF_IP+":"+Constants.ZSF_PORT+"/"+Constants.ZSF_DB+"',\n" +
                "    'table-name' = 'ODR_ORDER_DETAIL_SECONDARYCARD', \n" +
                "    'username' = '"+Constants.ZSF_USER+"',\n" +
                "    'password' = '"+Constants.ZSF_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
                ")");
        //创建Doris目标表
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        String ZSF_ODR_ORDER_DETAIL_SECONDARYCARD = "CREATE TABLE T_ODS_ZSF_ODR_ORDER_DETAIL_SECONDARYCARD (\n" +
                "    ETL_DATE DATE NOT NULL,\n" +
                "    ID VARCHAR(96) NOT NULL,\n" +
                "    ORDER_PACKAGE_ID VARCHAR(96),\n" +
                "    ORDER_DETAIL_ID VARCHAR(96),\n" +
                "    ORDER_NO VARCHAR(96),\n" +
                "    MAIN_ORDER_NO VARCHAR(96),\n" +
                "    MAIN_ORDER_ID VARCHAR(96),\n" +
                "    INSTANCE_ID VARCHAR(96),\n" +
                "    INSTANCE_NO VARCHAR(96),\n" +
                "    ACTIVITY_NO VARCHAR(96),\n" +
                "    PRODUCT_NO VARCHAR(96),\n" +
                "    PACKAGE_INDEX INT,\n" +
                "    SALE_PRICE DECIMAL(10,2),\n" +
                "    STATE VARCHAR(24),\n" +
                "    CUSTOMER_ID VARCHAR(96),\n" +
                "    MEMBER_ID VARCHAR(96),\n" +
                "    CUSTOMER_NAME VARCHAR(48),\n" +
                "    ACTIVE_TIME TIMESTAMP(6),\n" +
                "    REMARK VARCHAR(765),\n" +
                "    INTERNATIONAL_FLAG VARCHAR(60),\n" +
                "    CERT_TYPE VARCHAR(15),\n" +
                "    CERT_NO VARCHAR(96),\n" +
                "    CN_FIRST_NAME VARCHAR(765),\n" +
                "    CN_LAST_NAME VARCHAR(765),\n" +
                "    EN_FIRST_NAME VARCHAR(765),\n" +
                "    EN_LAST_NAME VARCHAR(765),\n" +
                "    EXPIRATION_DATE VARCHAR(96),\n" +
                "    SEX CHAR(1),\n" +
                "    BIRTHDAY VARCHAR(192),\n" +
                "    PASSPORT_ISSUE_NATION VARCHAR(192),\n" +
                "    PASSPORT_NATION VARCHAR(192),\n" +
                "    PHONE_NUM VARCHAR(96),\n" +
                "    ENABLE CHAR(1) ,\n" +
                "    CREATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    CREATE_USER VARCHAR(384) NOT NULL,\n" +
                "    UPDATE_USER VARCHAR(384) NOT NULL,\n" +
                "    UPDATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    SEND_MSG_FLAG CHAR(1),\n" +
                "    EXCHANGE_NUM VARCHAR(30),\n" +
                "    EXPIRY_DATE TIMESTAMP(6),\n" +
                "    TRAVEL_START_DATE TIMESTAMP(6),\n" +
                "    TRAVEL_END_DATE TIMESTAMP(6)" +
//                ") WITH (\n" +
//                "    'connector' = 'jdbc',\n" +
//                "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
//                "    'table-name' = 'T_ODS_ZSF_ODR_ORDER_DETAIL_SECONDARYCARD', -- 替换为实际的表名\n" +
//                "    'username' = '"+Constants.ODS_USER+"',\n" +
//                "    'password' = '"+Constants.ODS_PWD+"'\n" +
//                ")";
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZSF_ODR_ORDER_DETAIL_SECONDARYCARD',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")";
        tEnv.executeSql(ZSF_ODR_ORDER_DETAIL_SECONDARYCARD);

        String extractSql = "INSERT INTO T_ODS_ZSF_ODR_ORDER_DETAIL_SECONDARYCARD(" +
                "ETL_DATE\n" +
                ",ID\n" +
                ",ORDER_PACKAGE_ID\n" +
                ",ORDER_DETAIL_ID\n" +
                ",ORDER_NO\n" +
                ",MAIN_ORDER_NO\n" +
                ",MAIN_ORDER_ID\n" +
                ",INSTANCE_ID\n" +
                ",INSTANCE_NO\n" +
                ",ACTIVITY_NO\n" +
                ",PRODUCT_NO\n" +
                ",PACKAGE_INDEX\n" +
                ",SALE_PRICE\n" +
                ",STATE\n" +
                ",CUSTOMER_ID\n" +
                ",MEMBER_ID\n" +
                ",CUSTOMER_NAME\n" +
                ",ACTIVE_TIME\n" +
                ",REMARK\n" +
                ",INTERNATIONAL_FLAG\n" +
                ",CERT_TYPE\n" +
                ",CERT_NO\n" +
                ",CN_FIRST_NAME\n" +
                ",CN_LAST_NAME\n" +
                ",EN_FIRST_NAME\n" +
                ",EN_LAST_NAME\n" +
                ",EXPIRATION_DATE\n" +
                ",SEX\n" +
                ",BIRTHDAY\n" +
                ",PASSPORT_ISSUE_NATION\n" +
                ",PASSPORT_NATION\n" +
                ",PHONE_NUM\n" +
                ",ENABLE\n" +
                ",CREATE_TIME\n" +
                ",CREATE_USER\n" +
                ",UPDATE_USER\n" +
                ",UPDATE_TIME\n" +
                ",SEND_MSG_FLAG\n" +
                ",EXCHANGE_NUM\n" +
                ",EXPIRY_DATE\n" +
                ",TRAVEL_START_DATE\n" +
                ",TRAVEL_END_DATE\n" +
                ") " +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE \n" +
                ",ID\n" +
                ",ORDER_PACKAGE_ID\n" +
                ",ORDER_DETAIL_ID\n" +
                ",ORDER_NO\n" +
                ",MAIN_ORDER_NO\n" +
                ",MAIN_ORDER_ID\n" +
                ",INSTANCE_ID\n" +
                ",INSTANCE_NO\n" +
                ",ACTIVITY_NO\n" +
                ",PRODUCT_NO\n" +
                ",PACKAGE_INDEX\n" +
                ",SALE_PRICE\n" +
                ",STATE\n" +
                ",sm4_encrypt (CUSTOMER_ID,'" + sm4key + "') CUSTOMER_ID\n" +
                ",MEMBER_ID\n" +
                ",CUSTOMER_NAME\n" +
                ",ACTIVE_TIME\n" +
                ",REMARK\n" +
                ",INTERNATIONAL_FLAG\n" +
                ",CERT_TYPE\n" +
                ",sm4_encrypt (CERT_NO,'" + sm4key + "') CERT_NO\n" +
                ",CN_FIRST_NAME\n" +
                ",CN_LAST_NAME\n" +
                ",EN_FIRST_NAME\n" +
                ",EN_LAST_NAME\n" +
                ",EXPIRATION_DATE\n" +
                ",SEX\n" +
                ",BIRTHDAY\n" +
                ",PASSPORT_ISSUE_NATION\n" +
                ",PASSPORT_NATION\n" +
                ",sm4_encrypt (PHONE_NUM,'" + sm4key + "') PHONE_NUM\n" +
                ",ENABLE\n" +
                ",CREATE_TIME\n" +
                ",CREATE_USER\n" +
                ",UPDATE_USER\n" +
                ",UPDATE_TIME\n" +
                ",SEND_MSG_FLAG\n" +
                ",EXCHANGE_NUM\n" +
                ",EXPIRY_DATE\n" +
                ",TRAVEL_START_DATE\n" +
                ",TRAVEL_END_DATE" +
                " FROM ODR_ORDER_DETAIL_SECONDARYCARD "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' " +
                " OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
