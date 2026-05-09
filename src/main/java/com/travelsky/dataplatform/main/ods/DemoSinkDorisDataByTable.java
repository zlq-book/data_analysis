package com.travelsky.dataplatform.main.ods;


import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import scala.Tuple2;

import static org.apache.flink.table.api.Expressions.$;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author kuangaihua
 * @date 2025/6/27 15:13
 */
public class DemoSinkDorisDataByTable {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "DemoSinkDorisDataByTable");
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        UUID uuid = UUID.randomUUID();
        long timestamp = System.currentTimeMillis();
        // register a table in the catalog
        tEnv.executeSql("CREATE TABLE T_ODS_WXAP_CHECKIN_TRP_ORDER (\n" +
                "    ETL_DATE DATE,\n" +
                "    ID BIGINT,\n" +
                "    ORDER_NO STRING,\n" +
                "    ORDER_AMOUNT BIGINT,\n" +
                "    SEAT_TYPE TINYINT,\n" +
                "    ORDER_STATUS TINYINT,\n" +
                "    ORDER_TIME TIMESTAMP,\n" +
                "    CONTACT_PERSON STRING,\n" +
                "    CONTACT_PHONE STRING,\n" +
                "    CREATE_TIME TIMESTAMP,\n" +
                "    UPDATE_TIME TIMESTAMP,\n" +
                "    PAY_TYPE TINYINT,\n" +
                "    CUSTOMER_ID STRING,\n" +
                "    OPEN_ID STRING,\n" +
                "    CHANNEL TINYINT,\n" +
                "    PAY_NO STRING,\n" +
                "    THIRD_PAY_NO STRING,\n" +
                "    TRP_PAY_NO STRING,\n" +
                "    PAY_STATUS TINYINT,\n" +
                "    TRP_PAY_STATUS TINYINT,\n" +
                "    PRIMARY KEY (ID) NOT ENFORCED" +
                ") WITH (\n" +
                "  'connector' = 'doris',\n"
                + "  'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "',\n"
                + "  'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "',\n"
                + "  'table.identifier' = 'ODS_TEST.T_ODS_WXAP_CHECKIN_TRP_ORDER',\n"
                + "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "  'username' = '" + Constants.ODS_USER + "',\n"
                + "  'password' = '" + Constants.ODS_PWD + "'\n"
                + ")");

        String sql = "insert into T_ODS_WXAP_CHECKIN_TRP_ORDER (ETL_DATE,ID)  SELECT CAST('2025-07-11' AS DATE) ETL_DATE,287";
        TableResult tableResult = tEnv.executeSql(sql);

        tableResult.print();


    }
}
