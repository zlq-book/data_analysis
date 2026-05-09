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

public class ExtractTOdsZsfOdrOrderPaySecondarycard {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key= Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZsfOdrOrderPaySecondarycard");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);

        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE ODR_ORDER_PAY_SECONDARYCARD (\n" +
                "    ID VARCHAR(96) NOT NULL,\n" +
                "    ORDER_PAY_ID VARCHAR(96) NOT NULL,\n" +
                "    RESOURCE_ID VARCHAR(96),\n" +
                "    RESOURCE_NO VARCHAR(96),\n" +
                "    INSTANCE_ID VARCHAR(96),\n" +
                "    INSTANCE_NO VARCHAR(96),\n" +
                "    PRODUCT_ID VARCHAR(96),\n" +
                "    PRODUCT_NO VARCHAR(96),\n" +
                "    SALE_ORDER_NO VARCHAR(96),\n" +
                "    PSGR_NAME VARCHAR(300),\n" +
                "    CERT_NO VARCHAR(1200),\n" +
                "    ORG_CITY VARCHAR(9),\n" +
                "    DST_CITY VARCHAR(9),\n" +
                "    CABIN VARCHAR(6),\n" +
                "    DEP_DATE VARCHAR(30),\n" +
                "    DEP_TIME VARCHAR(30),\n" +
                "    ENABLE CHAR(1),\n" +
                "    CREATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    CREATE_USER VARCHAR(384) NOT NULL,\n" +
                "    UPDATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    UPDATE_USER VARCHAR(384) NOT NULL,\n" +
                "    CARD_NO VARCHAR(96),\n" +
                "    CARD_VALUE DECIMAL(10,2),\n" +
                "    CARD_DESCRIBE VARCHAR(12000),\n" +
                "    CARD_PAY_STATUS VARCHAR(765)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://"+ Constants.ZSF_IP+":"+Constants.ZSF_PORT+"/"+Constants.ZSF_DB+"',\n" +
                "    'table-name' = 'ODR_ORDER_PAY_SECONDARYCARD', \n" +
                "    'username' = '"+Constants.ZSF_USER+"',\n" +
                "    'password' = '"+Constants.ZSF_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
                ")");
        //创建Doris目标表
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        String ZSF_ODR_ORDER_PAY_SECONDARYCARD = "CREATE TABLE T_ODS_ZSF_ODR_ORDER_PAY_SECONDARYCARD (\n" +
                "    ETL_DATE DATE NOT NULL,\n" +
                "    ID VARCHAR(96) NOT NULL,\n" +
                "    ORDER_PAY_ID VARCHAR(96) NOT NULL,\n" +
                "    RESOURCE_ID VARCHAR(96),\n" +
                "    RESOURCE_NO VARCHAR(96),\n" +
                "    INSTANCE_ID VARCHAR(96),\n" +
                "    INSTANCE_NO VARCHAR(96),\n" +
                "    PRODUCT_ID VARCHAR(96),\n" +
                "    PRODUCT_NO VARCHAR(96),\n" +
                "    SALE_ORDER_NO VARCHAR(96),\n" +
                "    PSGR_NAME VARCHAR(300),\n" +
                "    CERT_NO VARCHAR(1200),\n" +
                "    ORG_CITY VARCHAR(9),\n" +
                "    DST_CITY VARCHAR(9),\n" +
                "    CABIN VARCHAR(6),\n" +
                "    DEP_DATE VARCHAR(30),\n" +
                "    DEP_TIME VARCHAR(30),\n" +
                "    ENABLE CHAR(1),\n" +
                "    CREATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    CREATE_USER VARCHAR(384) NOT NULL,\n" +
                "    UPDATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    UPDATE_USER VARCHAR(384) NOT NULL,\n" +
                "    CARD_NO VARCHAR(96),\n" +
                "    CARD_VALUE DECIMAL(10,2),\n" +
                "    CARD_DESCRIBE VARCHAR(12000),\n" +
                "    CARD_PAY_STATUS VARCHAR(765)" +
//                ") WITH (\n" +
//                "    'connector' = 'jdbc',\n" +
//                "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
//                "    'table-name' = 'T_ODS_ZSF_ODR_ORDER_PAY_SECONDARYCARD', -- 替换为实际的表名\n" +
//                "    'username' = '"+Constants.ODS_USER+"',\n" +
//                "    'password' = '"+Constants.ODS_PWD+"'\n" +
//                ")";
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZSF_ODR_ORDER_PAY_SECONDARYCARD',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")";
        tEnv.executeSql(ZSF_ODR_ORDER_PAY_SECONDARYCARD);

        String extractSql = "INSERT INTO T_ODS_ZSF_ODR_ORDER_PAY_SECONDARYCARD(" +
                "ETL_DATE\n" +
                ",ID\n" +
                ",ORDER_PAY_ID\n" +
                ",RESOURCE_ID\n" +
                ",RESOURCE_NO\n" +
                ",INSTANCE_ID\n" +
                ",INSTANCE_NO\n" +
                ",PRODUCT_ID\n" +
                ",PRODUCT_NO\n" +
                ",SALE_ORDER_NO\n" +
                ",PSGR_NAME\n" +
                ",CERT_NO\n" +
                ",ORG_CITY\n" +
                ",DST_CITY\n" +
                ",CABIN\n" +
                ",DEP_DATE\n" +
                ",DEP_TIME\n" +
                ",ENABLE\n" +
                ",CREATE_TIME\n" +
                ",CREATE_USER\n" +
                ",UPDATE_TIME\n" +
                ",UPDATE_USER\n" +
                ",CARD_NO\n" +
                ",CARD_VALUE\n" +
                ",CARD_DESCRIBE\n" +
                ",CARD_PAY_STATUS\n" +
                ") " +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE \n" +
                ",ID\n" +
                ",ORDER_PAY_ID\n" +
                ",RESOURCE_ID\n" +
                ",RESOURCE_NO\n" +
                ",INSTANCE_ID\n" +
                ",INSTANCE_NO\n" +
                ",PRODUCT_ID\n" +
                ",PRODUCT_NO\n" +
                ",SALE_ORDER_NO\n" +
                ",PSGR_NAME\n" +
                ",sm4_encrypt (CERT_NO,'" + sm4key + "') CERT_NO\n" +
                ",ORG_CITY\n" +
                ",DST_CITY\n" +
                ",CABIN\n" +
                ",DEP_DATE\n" +
                ",DEP_TIME\n" +
                ",ENABLE\n" +
                ",CREATE_TIME\n" +
                ",CREATE_USER\n" +
                ",UPDATE_TIME\n" +
                ",UPDATE_USER\n" +
                ",CARD_NO\n" +
                ",CARD_VALUE\n" +
                ",CARD_DESCRIBE\n" +
                ",CARD_PAY_STATUS" +
                " FROM ODR_ORDER_PAY_SECONDARYCARD "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' " +
                " OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
