package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.GetTableSql;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author kuangaihua
 * @date 2025/6/27 15:39
 */
public class DemoGetDorisTableByTable {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "DemoGetDorisTableByTable");
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

//        getDataByJdbc(tEnv);
        getDataByFE(tEnv);
//        env.execute();
    }

    public static void getDataByFE(StreamTableEnvironment tEnv) {
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
                GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_WXAP_CHECKIN_TRP_ORDER", Constants.ODS_USER, Constants.ODS_PWD)

                + ")");
        TableResult result = tEnv.executeSql("SELECT * FROM T_ODS_WXAP_CHECKIN_TRP_ORDER");
        result.print();
    }

    public static void getDataByJdbc(StreamTableEnvironment tEnv) {
        tEnv.executeSql("CREATE TABLE T_ODS_WXAP_CHECKIN_TRP_ORDER (\n" +
                "    ETL_DATE DATE,\n" +
                "    ID BIGINT,\n" +
                "    ORDER_NO VARCHAR(32),\n" +
                "    ORDER_AMOUNT BIGINT,\n" +
                "    SEAT_TYPE TINYINT,\n" +
                "    ORDER_STATUS TINYINT,\n" +
                "    ORDER_TIME TIMESTAMP,\n" +
                "    CONTACT_PERSON VARCHAR(32),\n" +
                "    CONTACT_PHONE VARCHAR(64),\n" +
                "    CREATE_TIME TIMESTAMP,\n" +
                "    UPDATE_TIME TIMESTAMP,\n" +
                "    PAY_TYPE TINYINT,\n" +
                "    CUSTOMER_ID VARCHAR(32),\n" +
                "    OPEN_ID VARCHAR(32),\n" +
                "    CHANNEL TINYINT,\n" +
                "    PAY_NO VARCHAR(32),\n" +
                "    THIRD_PAY_NO VARCHAR(32),\n" +
                "    TRP_PAY_NO VARCHAR(32),\n" +
                "    PAY_STATUS TINYINT,\n" +
                "    TRP_PAY_STATUS TINYINT,\n" +
                "    PRIMARY KEY (ID) NOT ENFORCED" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/ODS_TEST',\n" +
                "    'table-name' = 'T_ODS_WXAP_CHECKIN_TRP_ORDER', -- 替换为实际的表名\n" +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        TableResult result = tEnv.executeSql("SELECT * FROM T_ODS_WXAP_CHECKIN_TRP_ORDER");
        result.print();
    }
}
