package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.UUID;

public class ExtractTOdsZsfOdrOrderCustomerRelation {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key= Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZsfOdrOrderCustomerRelation");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);

        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE ODR_ORDER_CUSTOMER_RELATION (\n" +
                "    ID VARCHAR(96) NOT NULL,\n" +
                "    MAIN_ORDER_ID VARCHAR(96),\n" +
                "    MAIN_ORDER_NO VARCHAR(96),\n" +
                "    CUSTOMER_ID VARCHAR(96),\n" +
                "    CUSTOMER_NAME VARCHAR(384),\n" +
                "    CUSTOMER_MOBILE VARCHAR(96),\n" +
                "    IS_REAL_NAME CHAR(1),\n" +
                "    CONTACTS_MOBILE VARCHAR(1536),\n" +
                "    CONTACTS_NAME VARCHAR(384),\n" +
                "    CONTACTS_EMAIL VARCHAR(1536),\n" +
                "    OPEN_ID VARCHAR(192),\n" +
                "    MEMBER_ID VARCHAR(96),\n" +
                "    MEMBER_LEVEL VARCHAR(96),\n" +
                "    MEMBER_TYPE VARCHAR(96),\n" +
                "    LIMIT_TYPE VARCHAR(24),\n" +
                "    LIMIT_ID VARCHAR(48),\n" +
                "    ENABLE CHAR(1) NOT NULL,\n" +
                "    CREATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    CREATE_USER VARCHAR(384) NOT NULL,\n" +
                "    UPDATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    UPDATE_USER VARCHAR(384) NOT NULL" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://"+ Constants.ZSF_IP+":"+Constants.ZSF_PORT+"/"+Constants.ZSF_DB+"',\n" +
                "    'table-name' = 'ODR_ORDER_CUSTOMER_RELATION', \n" +
                "    'username' = '"+Constants.ZSF_USER+"',\n" +
                "    'password' = '"+Constants.ZSF_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
                ")");
        //创建Doris目标表
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        String ZSF_ODR_ORDER_CUSTOMER_RELATION = "CREATE TABLE T_ODS_ZSF_ODR_ORDER_CUSTOMER_RELATION (\n" +
                "    ETL_DATE DATE NOT NULL,\n" +
                "    ID VARCHAR(96) NOT NULL,\n" +
                "    MAIN_ORDER_ID VARCHAR(96),\n" +
                "    MAIN_ORDER_NO VARCHAR(96),\n" +
                "    CUSTOMER_ID VARCHAR(96),\n" +
                "    CUSTOMER_NAME VARCHAR(384),\n" +
                "    CUSTOMER_MOBILE VARCHAR(96),\n" +
                "    IS_REAL_NAME CHAR(1),\n" +
                "    CONTACTS_MOBILE VARCHAR(1536),\n" +
                "    CONTACTS_NAME VARCHAR(384),\n" +
                "    CONTACTS_EMAIL VARCHAR(1536),\n" +
                "    OPEN_ID VARCHAR(192),\n" +
                "    MEMBER_ID VARCHAR(96),\n" +
                "    MEMBER_LEVEL VARCHAR(96),\n" +
                "    MEMBER_TYPE VARCHAR(96),\n" +
                "    LIMIT_TYPE VARCHAR(24),\n" +
                "    LIMIT_ID VARCHAR(48),\n" +
                "    ENABLE CHAR(1) NOT NULL,\n" +
                "    CREATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    CREATE_USER VARCHAR(384) NOT NULL,\n" +
                "    UPDATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    UPDATE_USER VARCHAR(384) NOT NULL" +
//                ") WITH (\n" +
//                "    'connector' = 'jdbc',\n" +
//                "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
//                "    'table-name' = 'T_ODS_ZSF_ODR_ORDER_CUSTOMER_RELATION', -- 替换为实际的表名\n" +
//                "    'username' = '"+Constants.ODS_USER+"',\n" +
//                "    'password' = '"+Constants.ODS_PWD+"'\n" +
//                ")";
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZSF_ODR_ORDER_CUSTOMER_RELATION',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")";
        tEnv.executeSql(ZSF_ODR_ORDER_CUSTOMER_RELATION);

        String extractSql = "INSERT INTO T_ODS_ZSF_ODR_ORDER_CUSTOMER_RELATION(" +
                "ETL_DATE\n" +
                ",ID\n" +
                ",MAIN_ORDER_ID\n" +
                ",MAIN_ORDER_NO\n" +
                ",CUSTOMER_ID\n" +
                ",CUSTOMER_NAME\n" +
                ",CUSTOMER_MOBILE\n" +
                ",IS_REAL_NAME\n" +
                ",CONTACTS_MOBILE\n" +
                ",CONTACTS_NAME\n" +
                ",CONTACTS_EMAIL\n" +
                ",OPEN_ID\n" +
                ",MEMBER_ID\n" +
                ",MEMBER_LEVEL\n" +
                ",MEMBER_TYPE\n" +
                ",LIMIT_TYPE\n" +
                ",LIMIT_ID\n" +
                ",ENABLE\n" +
                ",CREATE_TIME\n" +
                ",CREATE_USER\n" +
                ",UPDATE_TIME\n" +
                ",UPDATE_USER\n" +
                ") " +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE \n" +
                ",ID\n" +
                ",MAIN_ORDER_ID\n" +
                ",MAIN_ORDER_NO\n" +
                ",sm4_encrypt (CUSTOMER_ID,'" + sm4key + "') CUSTOMER_ID\n" +
                ",CUSTOMER_NAME\n" +
                ",sm4_encrypt (CUSTOMER_MOBILE,'" + sm4key + "') CUSTOMER_MOBILE\n" +
                ",IS_REAL_NAME\n" +
                ",sm4_encrypt (CONTACTS_MOBILE,'" + sm4key + "') CONTACTS_MOBILE\n" +
                ",CONTACTS_NAME\n" +
                ",sm4_encrypt (CONTACTS_EMAIL,'" + sm4key + "') CONTACTS_EMAIL\n" +
                ",OPEN_ID\n" +
                ",MEMBER_ID\n" +
                ",MEMBER_LEVEL\n" +
                ",MEMBER_TYPE\n" +
                ",LIMIT_TYPE\n" +
                ",LIMIT_ID\n" +
                ",ENABLE\n" +
                ",CREATE_TIME\n" +
                ",CREATE_USER\n" +
                ",UPDATE_TIME\n" +
                ",UPDATE_USER " +
                " FROM ODR_ORDER_CUSTOMER_RELATION "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' " +
                " OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
