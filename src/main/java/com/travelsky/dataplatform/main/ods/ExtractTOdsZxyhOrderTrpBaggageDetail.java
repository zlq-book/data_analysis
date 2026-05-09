package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.UUID;

/**
 * @author
 * @date 2025/7/2 9:31
 */
public class ExtractTOdsZxyhOrderTrpBaggageDetail {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZxyhOrderTrpBaggageDetail");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE ORDER_TRP_BAGGAGE_DETAIL (\n" +
                "ID BIGINT ,\n" +
                "ORDER_NUM VARCHAR(150) ,\n" +
                "BANK_ORDER_NUM VARCHAR(150) ,\n" +
                "BANK_NAME VARCHAR(150) ,\n" +
                "PNR VARCHAR(150) ,\n" +
                "PASSENGER_NAME VARCHAR(150) ,\n" +
                "TICKET_NUM VARCHAR(150) ,\n" +
                "FLIGHT_NUM VARCHAR(150) ,\n" +
                "CABIN VARCHAR(30) ,\n" +
                "DEPARTURE VARCHAR(150) ,\n" +
                "DESTINATION VARCHAR(150) ,\n" +
                "DEPARTURE_DATE VARCHAR(150) ,\n" +
                "ARRIVAL_DATE VARCHAR(150) ,\n" +
                "TICKET_ISSUE_INITIATION_TIME VARCHAR(150) ,\n" +
                "TICKET_ISSUE_SUCCESS_TIME VARCHAR(150) ,\n" +
                "EMD_NUM VARCHAR(150) ,\n" +
                "BAGGAGE_AMOUNT DECIMAL(10,2) ,\n" +
                "BAGGAGE_WEIGHT VARCHAR(150) ,\n" +
                "BAGGAGE_ORDER_STATUS VARCHAR(150) ,\n" +
                "TICKET_TYPE VARCHAR(150) ,\n" +
                "CHANNEL VARCHAR(150) ,\n" +
                "SITE VARCHAR(150) ,\n" +
                "CURRENCY VARCHAR(30) ,\n" +
                "USER_NAME VARCHAR(300) ,\n" +
                "ETL_INSERT_TIME TIMESTAMP(6) ,\n" +
                "ETL_UPDATE_TIME TIMESTAMP(6) ,\n" +
                "ENABLE CHAR(1) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(96) ,\n" +
                "UPDATE_USER VARCHAR(96),\n" +
                "ENCRYPT_FLAG VARCHAR(3)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.ZXYH_IP+":"+Constants.ZXYH_PORT+"/"+Constants.ZXYH_DB+"',\n" +
                "    'table-name' = '"+Constants.ZXYH_SCHEMA+".ORDER_TRP_BAGGAGE_DETAIL', \n" +
                "    'username' = '"+Constants.ZXYH_USER+"',\n" +
                "    'password' = '"+Constants.ZXYH_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_ZXYH_ORDER_TRP_BAGGAGE_DETAIL (\n" +
                "    ETL_DATE DATE,\n" +
                "ID BIGINT ,\n" +
                "ORDER_NUM VARCHAR(150) ,\n" +
                "BANK_ORDER_NUM VARCHAR(150) ,\n" +
                "BANK_NAME VARCHAR(150) ,\n" +
                "PNR VARCHAR(150) ,\n" +
                "PASSENGER_NAME VARCHAR(150) ,\n" +
                "TICKET_NUM VARCHAR(150) ,\n" +
                "FLIGHT_NUM VARCHAR(150) ,\n" +
                "CABIN VARCHAR(30) ,\n" +
                "DEPARTURE VARCHAR(150) ,\n" +
                "DESTINATION VARCHAR(150) ,\n" +
                "DEPARTURE_DATE VARCHAR(150) ,\n" +
                "ARRIVAL_DATE VARCHAR(150) ,\n" +
                "TICKET_ISSUE_INITIATION_TIME VARCHAR(150) ,\n" +
                "TICKET_ISSUE_SUCCESS_TIME VARCHAR(150) ,\n" +
                "EMD_NUM VARCHAR(150) ,\n" +
                "BAGGAGE_AMOUNT DECIMAL(10,2) ,\n" +
                "BAGGAGE_WEIGHT VARCHAR(150) ,\n" +
                "BAGGAGE_ORDER_STATUS VARCHAR(150) ,\n" +
                "TICKET_TYPE VARCHAR(150) ,\n" +
                "CHANNEL VARCHAR(150) ,\n" +
                "SITE VARCHAR(150) ,\n" +
                "CURRENCY VARCHAR(30) ,\n" +
                "USER_NAME VARCHAR(300) ,\n" +
                "ETL_INSERT_TIME TIMESTAMP(6) ,\n" +
                "ETL_UPDATE_TIME TIMESTAMP(6) ,\n" +
                "ENABLE CHAR(1) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(96) ,\n" +
                "UPDATE_USER VARCHAR(96),\n" +
                "ENCRYPT_FLAG VARCHAR(3)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZXYH_ORDER_TRP_BAGGAGE_DETAIL',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_ZXYH_ORDER_TRP_BAGGAGE_DETAIL(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "ORDER_NUM,\n" +
                "BANK_ORDER_NUM,\n" +
                "BANK_NAME,\n" +
                "PNR,\n" +
                "PASSENGER_NAME,\n" +
                "TICKET_NUM,\n" +
                "FLIGHT_NUM,\n" +
                "CABIN,\n" +
                "DEPARTURE,\n" +
                "DESTINATION,\n" +
                "DEPARTURE_DATE,\n" +
                "ARRIVAL_DATE,\n" +
                "TICKET_ISSUE_INITIATION_TIME,\n" +
                "TICKET_ISSUE_SUCCESS_TIME,\n" +
                "EMD_NUM,\n" +
                "BAGGAGE_AMOUNT,\n" +
                "BAGGAGE_WEIGHT,\n" +
                "BAGGAGE_ORDER_STATUS,\n" +
                "TICKET_TYPE,\n" +
                "CHANNEL,\n" +
                "SITE,\n" +
                "CURRENCY,\n" +
                "USER_NAME,\n" +
                "ETL_INSERT_TIME,\n" +
                "ETL_UPDATE_TIME,\n" +
                "ENABLE,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_USER,\n" +
                "ENCRYPT_FLAG)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "CAST(ID AS BIGINT) ID,\n" +
                "ORDER_NUM,\n" +
                "BANK_ORDER_NUM,\n" +
                "BANK_NAME,\n" +
                "PNR,\n" +
                "PASSENGER_NAME,\n" +
                "TICKET_NUM,\n" +
                "FLIGHT_NUM,\n" +
                "CABIN,\n" +
                "DEPARTURE,\n" +
                "DESTINATION,\n" +
                "DEPARTURE_DATE,\n" +
                "ARRIVAL_DATE,\n" +
                "TICKET_ISSUE_INITIATION_TIME,\n" +
                "TICKET_ISSUE_SUCCESS_TIME,\n" +
                "EMD_NUM,\n" +
                "BAGGAGE_AMOUNT,\n" +
                "BAGGAGE_WEIGHT,\n" +
                "BAGGAGE_ORDER_STATUS,\n" +
                "TICKET_TYPE,\n" +
                "CHANNEL,\n" +
                "SITE,\n" +
                "CURRENCY,\n" +
                "USER_NAME,\n" +
                "ETL_INSERT_TIME,\n" +
                "ETL_UPDATE_TIME,\n" +
                "ENABLE,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_USER,\n" +
                "ENCRYPT_FLAG\n" +
                " FROM ORDER_TRP_BAGGAGE_DETAIL "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
