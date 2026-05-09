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

public class ExtractTOdsZsfOdrOrderDetailInsurance {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key= Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZsfOdrOrderDetailInsurance");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);

        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE ODR_ORDER_DETAIL_INSURANCE (\n" +
                "    ID VARCHAR(96) NOT NULL,\n" +
                "    ORDER_DETAIL_ID VARCHAR(96) NOT NULL,\n" +
                "    ORDER_NO VARCHAR(96) NOT NULL,\n" +
                "    MAIN_ORDER_NO VARCHAR(96),\n" +
                "    MAIN_ORDER_ID VARCHAR(96),\n" +
                "    SERIAL_NUMBER VARCHAR(96),\n" +
                "    OPERATE_SERIAL_NUMBER VARCHAR(96),\n" +
                "    INSTANCE_ID VARCHAR(96),\n" +
                "    INSTANCE_NO VARCHAR(96),\n" +
                "    INS_TRADE_NO VARCHAR(96),\n" +
                "    INS_PRICE DECIMAL(10,2),\n" +
                "    INS_AMOUNT DECIMAL(10,2),\n" +
                "    INS_TYPE VARCHAR(60),\n" +
                "    INS_STATUS VARCHAR(6),\n" +
                "    INS_COMPANY_CODE VARCHAR(90),\n" +
                "    INS_COMPANY STRING,\n" +
                "    INS_CREATE_TIME TIMESTAMP(6),\n" +
                "    INS_COMPLETE_TIME TIMESTAMP(6),\n" +
                "    INS_RETURN_TIME TIMESTAMP(6),\n" +
                "    HOLDER_NAME VARCHAR(150),\n" +
                "    HOLDER_TYPE VARCHAR(30),\n" +
                "    HOLDER_ID VARCHAR(1200),\n" +
                "    HOLDER_PHONE VARCHAR(1200),\n" +
                "    HOLDER_EMAIL VARCHAR(384),\n" +
                "    HOLDER_ADDRESS VARCHAR(768),\n" +
                "    BENEFICIARY_TYPE VARCHAR(6),\n" +
                "    BENEFICIARY_NAME VARCHAR(150),\n" +
                "    BENEFICIARY_ID VARCHAR(1200),\n" +
                "    BENEFICIARY_ID_TYPE VARCHAR(30),\n" +
                "    PSGR_NAME VARCHAR(300),\n" +
                "    CERT_TYPE VARCHAR(300),\n" +
                "    CERT_NO VARCHAR(1200),\n" +
                "    PSGR_PHONE VARCHAR(1200),\n" +
                "    PSGR_MAIL VARCHAR(1536),\n" +
                "    FLIGHT_DATE VARCHAR(30),\n" +
                "    FLIGHT_TIME VARCHAR(30),\n" +
                "    ORG_CITY VARCHAR(9),\n" +
                "    DST_CITY VARCHAR(9),\n" +
                "    FLIGHT_NO VARCHAR(30),\n" +
                "    CARRIER VARCHAR(15),\n" +
                "    TICKET_NO VARCHAR(60),\n" +
                "    TICKET_PRICE DECIMAL(10,2),\n" +
                "    INTERNATIONAL_FLAG VARCHAR(60),\n" +
                "    ENABLE CHAR(1) NOT NULL,\n" +
                "    CREATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    CREATE_USER VARCHAR(384) NOT NULL,\n" +
                "    UPDATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    UPDATE_USER VARCHAR(384) NOT NULL,\n" +
                "    RELATION_INSURANT VARCHAR(30),\n" +
                "    ASSIGN_BENEFICIARY CHAR(1),\n" +
                "    INS_ORDER_NO VARCHAR(96),\n" +
                "    BIRTH_DATE VARCHAR(150),\n" +
                "    CUSTOMER_BIRTHDAY VARCHAR(150)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://"+ Constants.ZSF_IP+":"+Constants.ZSF_PORT+"/"+Constants.ZSF_DB+"',\n" +
                "    'table-name' = 'ODR_ORDER_DETAIL_INSURANCE', \n" +
                "    'username' = '"+Constants.ZSF_USER+"',\n" +
                "    'password' = '"+Constants.ZSF_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
                ")");
        //创建Doris目标表
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        String ZSF_ODR_ORDER_DETAIL_INSURANCE = "CREATE TABLE T_ODS_ZSF_ODR_ORDER_DETAIL_INSURANCE (\n" +
                "    ETL_DATE DATE NOT NULL,\n" +
                "    ID VARCHAR(96) NOT NULL,\n" +
                "    ORDER_DETAIL_ID VARCHAR(96) NOT NULL,\n" +
                "    ORDER_NO VARCHAR(96) NOT NULL,\n" +
                "    MAIN_ORDER_NO VARCHAR(96),\n" +
                "    MAIN_ORDER_ID VARCHAR(96),\n" +
                "    SERIAL_NUMBER VARCHAR(96),\n" +
                "    OPERATE_SERIAL_NUMBER VARCHAR(96),\n" +
                "    INSTANCE_ID VARCHAR(96),\n" +
                "    INSTANCE_NO VARCHAR(96),\n" +
                "    INS_TRADE_NO VARCHAR(96),\n" +
                "    INS_PRICE DECIMAL(10,2),\n" +
                "    INS_AMOUNT DECIMAL(10,2),\n" +
                "    INS_TYPE VARCHAR(60),\n" +
                "    INS_STATUS VARCHAR(6),\n" +
                "    INS_COMPANY_CODE VARCHAR(90),\n" +
                "    INS_COMPANY STRING,\n" +
                "    INS_CREATE_TIME TIMESTAMP(6),\n" +
                "    INS_COMPLETE_TIME TIMESTAMP(6),\n" +
                "    INS_RETURN_TIME TIMESTAMP(6),\n" +
                "    HOLDER_NAME VARCHAR(150),\n" +
                "    HOLDER_TYPE VARCHAR(30),\n" +
                "    HOLDER_ID VARCHAR(1200),\n" +
                "    HOLDER_PHONE VARCHAR(1200),\n" +
                "    HOLDER_EMAIL VARCHAR(384),\n" +
                "    HOLDER_ADDRESS VARCHAR(768),\n" +
                "    BENEFICIARY_TYPE VARCHAR(6),\n" +
                "    BENEFICIARY_NAME VARCHAR(150),\n" +
                "    BENEFICIARY_ID VARCHAR(1200),\n" +
                "    BENEFICIARY_ID_TYPE VARCHAR(30),\n" +
                "    PSGR_NAME VARCHAR(300),\n" +
                "    CERT_TYPE VARCHAR(300),\n" +
                "    CERT_NO VARCHAR(1200),\n" +
                "    PSGR_PHONE VARCHAR(1200),\n" +
                "    PSGR_MAIL VARCHAR(1536),\n" +
                "    FLIGHT_DATE VARCHAR(30),\n" +
                "    FLIGHT_TIME VARCHAR(30),\n" +
                "    ORG_CITY VARCHAR(9),\n" +
                "    DST_CITY VARCHAR(9),\n" +
                "    FLIGHT_NO VARCHAR(30),\n" +
                "    CARRIER VARCHAR(15),\n" +
                "    TICKET_NO VARCHAR(60),\n" +
                "    TICKET_PRICE DECIMAL(10,2),\n" +
                "    INTERNATIONAL_FLAG VARCHAR(60),\n" +
                "    ENABLE CHAR(1) NOT NULL,\n" +
                "    CREATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    CREATE_USER VARCHAR(384) NOT NULL,\n" +
                "    UPDATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    UPDATE_USER VARCHAR(384) NOT NULL,\n" +
                "    RELATION_INSURANT VARCHAR(30),\n" +
                "    ASSIGN_BENEFICIARY CHAR(1),\n" +
                "    INS_ORDER_NO VARCHAR(96),\n" +
                "    BIRTH_DATE VARCHAR(150),\n" +
                "    CUSTOMER_BIRTHDAY VARCHAR(150)" +
//                ") WITH (\n" +
//                "    'connector' = 'jdbc',\n" +
//                "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
//                "    'table-name' = 'T_ODS_ZSF_ODR_ORDER_DETAIL_INSURANCE', -- 替换为实际的表名\n" +
//                "    'username' = '"+Constants.ODS_USER+"',\n" +
//                "    'password' = '"+Constants.ODS_PWD+"'\n" +
//                ")";
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZSF_ODR_ORDER_DETAIL_INSURANCE',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")";
        tEnv.executeSql(ZSF_ODR_ORDER_DETAIL_INSURANCE);

        String extractSql = "INSERT INTO T_ODS_ZSF_ODR_ORDER_DETAIL_INSURANCE(" +
                "ETL_DATE\n" +
                ",ID\n" +
                ",ORDER_DETAIL_ID\n" +
                ",ORDER_NO\n" +
                ",MAIN_ORDER_NO\n" +
                ",MAIN_ORDER_ID\n" +
                ",SERIAL_NUMBER\n" +
                ",OPERATE_SERIAL_NUMBER\n" +
                ",INSTANCE_ID\n" +
                ",INSTANCE_NO\n" +
                ",INS_TRADE_NO\n" +
                ",INS_PRICE\n" +
                ",INS_AMOUNT\n" +
                ",INS_TYPE\n" +
                ",INS_STATUS\n" +
                ",INS_COMPANY_CODE\n" +
                ",INS_COMPANY\n" +
                ",INS_CREATE_TIME\n" +
                ",INS_COMPLETE_TIME\n" +
                ",INS_RETURN_TIME\n" +
                ",HOLDER_NAME\n" +
                ",HOLDER_TYPE\n" +
                ",HOLDER_ID\n" +
                ",HOLDER_PHONE\n" +
                ",HOLDER_EMAIL\n" +
                ",HOLDER_ADDRESS\n" +
                ",BENEFICIARY_TYPE\n" +
                ",BENEFICIARY_NAME\n" +
                ",BENEFICIARY_ID\n" +
                ",BENEFICIARY_ID_TYPE\n" +
                ",PSGR_NAME\n" +
                ",CERT_TYPE\n" +
                ",CERT_NO\n" +
                ",PSGR_PHONE\n" +
                ",PSGR_MAIL\n" +
                ",FLIGHT_DATE\n" +
                ",FLIGHT_TIME\n" +
                ",ORG_CITY\n" +
                ",DST_CITY\n" +
                ",FLIGHT_NO\n" +
                ",CARRIER\n" +
                ",TICKET_NO\n" +
                ",TICKET_PRICE\n" +
                ",INTERNATIONAL_FLAG\n" +
                ",ENABLE\n" +
                ",CREATE_TIME\n" +
                ",CREATE_USER\n" +
                ",UPDATE_TIME\n" +
                ",UPDATE_USER\n" +
                ",RELATION_INSURANT\n" +
                ",ASSIGN_BENEFICIARY\n" +
                ",INS_ORDER_NO\n" +
                ",BIRTH_DATE\n" +
                ",CUSTOMER_BIRTHDAY\n" +
                ") " +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE \n" +
                ",ID\n" +
                ",ORDER_DETAIL_ID\n" +
                ",ORDER_NO\n" +
                ",MAIN_ORDER_NO\n" +
                ",MAIN_ORDER_ID\n" +
                ",SERIAL_NUMBER\n" +
                ",OPERATE_SERIAL_NUMBER\n" +
                ",INSTANCE_ID\n" +
                ",INSTANCE_NO\n" +
                ",INS_TRADE_NO\n" +
                ",INS_PRICE\n" +
                ",INS_AMOUNT\n" +
                ",INS_TYPE\n" +
                ",INS_STATUS\n" +
                ",INS_COMPANY_CODE\n" +
                ",INS_COMPANY\n" +
                ",INS_CREATE_TIME\n" +
                ",INS_COMPLETE_TIME\n" +
                ",INS_RETURN_TIME\n" +
                ",HOLDER_NAME\n" +
                ",HOLDER_TYPE\n" +
                ",sm4_encrypt (HOLDER_ID,'" + sm4key + "') HOLDER_ID\n" +
                ",sm4_encrypt (HOLDER_PHONE,'" + sm4key + "') HOLDER_PHONE\n" +
                ",sm4_encrypt (HOLDER_EMAIL,'" + sm4key + "') HOLDER_EMAIL\n" +
                ",HOLDER_ADDRESS\n" +
                ",BENEFICIARY_TYPE\n" +
                ",BENEFICIARY_NAME\n" +
                ",sm4_encrypt (BENEFICIARY_ID,'" + sm4key + "') BENEFICIARY_ID\n" +
                ",BENEFICIARY_ID_TYPE\n" +
                ",PSGR_NAME\n" +
                ",CERT_TYPE\n" +
                ",sm4_encrypt (CERT_NO,'" + sm4key + "') CERT_NO\n" +
                ",sm4_encrypt (PSGR_PHONE,'" + sm4key + "') PSGR_PHONE\n" +
                ",PSGR_MAIL\n" +
                ",FLIGHT_DATE\n" +
                ",FLIGHT_TIME\n" +
                ",ORG_CITY\n" +
                ",DST_CITY\n" +
                ",FLIGHT_NO\n" +
                ",CARRIER\n" +
                ",TICKET_NO\n" +
                ",TICKET_PRICE\n" +
                ",INTERNATIONAL_FLAG\n" +
                ",ENABLE\n" +
                ",CREATE_TIME\n" +
                ",CREATE_USER\n" +
                ",UPDATE_TIME\n" +
                ",UPDATE_USER\n" +
                ",RELATION_INSURANT\n" +
                ",ASSIGN_BENEFICIARY\n" +
                ",INS_ORDER_NO\n" +
                ",BIRTH_DATE\n" +
                ",CUSTOMER_BIRTHDAY" +
                " FROM ODR_ORDER_DETAIL_INSURANCE "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' " +
                " OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
