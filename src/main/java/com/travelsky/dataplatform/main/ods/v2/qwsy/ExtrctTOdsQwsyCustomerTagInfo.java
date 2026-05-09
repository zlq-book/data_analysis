package com.travelsky.dataplatform.main.ods.v2.qwsy;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.UUID;

public class ExtrctTOdsQwsyCustomerTagInfo {

    public static void main(String[] args) throws IOException {
        System.out.println(" ExtrctTOdsDkhclCustomerTagInfo data transfer start");
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String startDate = etlDate;
        String endDate = etlDate;
        if (args.length > 2) {
            startDate = args[1];
            endDate = args[2];
        }
        System.out.println(" ExtrctTOdsDkhclCustomerTagInfo etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, " ExtrctTOdsDkhclCustomerTagInfo");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        System.out.println(" ExtrctTOdsDkhclCustomerTagInfo executeSql create table for source start");
        tEnv.executeSql("CREATE TABLE CUSTOMER_TAG_INFO (\n" +
                " ID BIGINT,\n" +
                " CORP_ID VARCHAR(255),\n" +
                " EXTERNAL_USER_ID VARCHAR(255),\n" +
                " FOLLOW_USER_ID VARCHAR(255),\n" +
                " GROUP_NAME VARCHAR(255),\n" +
                " TAG_ID VARCHAR(255),\n" +
                " CREATE_TIME TIMESTAMP(6),\n" +
                " UPDATE_TIME TIMESTAMP(6),\n" +
                " TAG_NAME VARCHAR(255)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.QWSY_IP + ":" + Constants.QWSY_PORT + "/" + Constants.QWSY_DB + "',\n" +
                "    'table-name' = '" + Constants.QWSY_SCHEMA + ".CUSTOMER_TAG_INFO', \n" +
                "    'username' = '" + Constants.QWSY_USER + "',\n" +
                "    'password' = '" + Constants.QWSY_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");

        System.out.println(" ExtrctTOdsDkhclCustomerTagInfo executeSql create table for source end");
        //创建Doris目标表
        System.out.println(" ExtrctTOdsDkhclCustomerTagInfo executeSql create table for doris start");
        tEnv.executeSql("CREATE TABLE T_ODS_QWSY_CUSTOMER_TAG_INFO (\n" +
                " ID BIGINT,\n" +
                " CORP_ID VARCHAR(765),\n" +
                " EXTERNAL_USER_ID VARCHAR(765),\n" +
                " FOLLOW_USER_ID VARCHAR(765),\n" +
                " GROUP_NAME VARCHAR(765),\n" +
                " TAG_ID VARCHAR(765),\n" +
                " CREATE_TIME TIMESTAMP(3),\n" +
                " UPDATE_TIME TIMESTAMP(3),\n" +
                " TAG_NAME VARCHAR(765),\n" +
                " ETL_DATE DATE" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                " 'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "',\n" +
                " 'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "',\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_QWSY_CUSTOMER_TAG_INFO',\n" +
                " 'sink.label-prefix' = '" + timestamp + uuid + "',\n" +
                " 'sink.properties.read_json_by_line' = 'true',\n" +
                " 'sink.properties.format' = 'json',\n" +
                " 'username' = '" + Constants.ODS_USER + "',\n" +
                " 'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        System.out.println(" ExtrctTOdsDkhclCustomerTagInfo executeSql create table for doris end");
        String extractSql = "INSERT INTO T_ODS_QWSY_CUSTOMER_TAG_INFO(\n" +
                " ID,\n" +
                " CORP_ID,\n" +
                " EXTERNAL_USER_ID,\n" +
                " FOLLOW_USER_ID,\n" +
                " GROUP_NAME,\n" +
                " TAG_ID,\n" +
                " CREATE_TIME,\n" +
                " UPDATE_TIME,\n" +
                " TAG_NAME,\n" +
                " ETL_DATE)" +

                " SELECT\n" +
                " ID,\n" +
                " CORP_ID,\n" +
                " EXTERNAL_USER_ID,\n" +
                " FOLLOW_USER_ID,\n" +
                " GROUP_NAME,\n" +
                " TAG_ID,\n" +
                " CREATE_TIME,\n" +
                " UPDATE_TIME,\n" +
                " TAG_NAME,\n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                " FROM CUSTOMER_TAG_INFO " +
                " WHERE CREATE_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' " +
                " OR UPDATE_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999'";
        System.out.println(" ExtrctTOdsDkhclCustomerTagInfo executeSql extract start");
        TableResult result = tEnv.executeSql(extractSql);
        System.out.println(" ExtrctTOdsDkhclCustomerTagInfo executeSql extract end");

        result.print();

        System.out.println(" ExtrctTOdsDkhclCustomerTagInfo data transfer end");


    }

}
