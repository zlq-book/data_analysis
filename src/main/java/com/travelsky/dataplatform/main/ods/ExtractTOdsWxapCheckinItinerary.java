package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.UUID;

/**
 * @author kuangaihua
 * @date 2025/6/26 16:29
 */
public class ExtractTOdsWxapCheckinItinerary {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsWxapCheckinItinerary");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE CHECKIN_ITINERARY (\n" +
                "ID BIGINT, " +
                "DEP VARCHAR(15), " +
                "ARR VARCHAR(15), " +
                "FLIGHT_NO VARCHAR(48), " +
                "CARR_FLIGHT_NO VARCHAR(48), " +
                "FLIGHT_DATE VARCHAR(30), " +
                "SCH_DEPT_TIME VARCHAR(24), " +
                "SCH_ARR_TIME VARCHAR(24), " +
                "EXP_DEPT_TIME VARCHAR(24), " +
                "BOARDING_TIME VARCHAR(24), " +
                "BOARDING_GATE_NUMBER VARCHAR(96), " +
                "FROM_CITY_STATUS VARCHAR(6), " +
                "TO_CITY_STATUS VARCHAR(6), " +
                "DEPT_TERMINAL_NAME VARCHAR(96), " +
                "ARRIVE_TERMINAL_NAME VARCHAR(96), " +
                "ISSUE_AIRLINE VARCHAR(96), " +
                "EXTRA_GATES VARCHAR(96), " +
                "PLANE_TYPE VARCHAR(96), " +
                "PLANE_CLASS VARCHAR(96), " +
                "STOP_OVER_INFO VARCHAR(12000), " +
                "CREATE_TIME TIMESTAMP(6), " +
                "UPDATE_TIME TIMESTAMP(6), " +
                "CREATE_BY VARCHAR(192), " +
                "UPDATE_BY VARCHAR(192), " +
                "ARR_DATE VARCHAR(30)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.WXAP_IP + ":" + Constants.WXAP_PORT + "/" + Constants.WXAP_DB + "',\n" +
                "    'table-name' = '" + Constants.WXAP_SCHEMA + ".CHECKIN_ITINERARY', \n" +
                "    'username' = '" + Constants.WXAP_USER + "',\n" +
                "    'password' = '" + Constants.WXAP_PWD + "'\n" +
                ",\n" + "  'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_WXAP_CHECKIN_ITINERARY (\n" +
                "ETL_DATE DATE," +
                "ID BIGINT, " +
                "DEP VARCHAR(15), " +
                "ARR VARCHAR(15), " +
                "FLIGHT_NO VARCHAR(48), " +
                "CARR_FLIGHT_NO VARCHAR(48), " +
                "FLIGHT_DATE VARCHAR(30), " +
                "SCH_DEPT_TIME VARCHAR(24), " +
                "SCH_ARR_TIME VARCHAR(24), " +
                "EXP_DEPT_TIME VARCHAR(24), " +
                "BOARDING_TIME VARCHAR(24), " +
                "BOARDING_GATE_NUMBER VARCHAR(96), " +
                "FROM_CITY_STATUS VARCHAR(6), " +
                "TO_CITY_STATUS VARCHAR(6), " +
                "DEPT_TERMINAL_NAME VARCHAR(96), " +
                "ARRIVE_TERMINAL_NAME VARCHAR(96), " +
                "ISSUE_AIRLINE VARCHAR(96), " +
                "EXTRA_GATES VARCHAR(96), " +
                "PLANE_TYPE VARCHAR(96), " +
                "PLANE_CLASS VARCHAR(96), " +
                "STOP_OVER_INFO VARCHAR(12000), " +
                "CREATE_TIME TIMESTAMP(6), " +
                "UPDATE_TIME TIMESTAMP(6), " +
                "CREATE_BY VARCHAR(192), " +
                "UPDATE_BY VARCHAR(192), " +
                "ARR_DATE VARCHAR(30)" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_WXAP_CHECKIN_ITINERARY',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        String extractSql = "INSERT INTO T_ODS_WXAP_CHECKIN_ITINERARY(" +
                "ETL_DATE\n" +
                ",ID\n" +
                ",DEP\n" +
                ",ARR\n" +
                ",FLIGHT_NO\n" +
                ",CARR_FLIGHT_NO\n" +
                ",FLIGHT_DATE\n" +
                ",SCH_DEPT_TIME\n" +
                ",SCH_ARR_TIME\n" +
                ",EXP_DEPT_TIME\n" +
                ",BOARDING_TIME\n" +
                ",BOARDING_GATE_NUMBER\n" +
                ",FROM_CITY_STATUS\n" +
                ",TO_CITY_STATUS\n" +
                ",DEPT_TERMINAL_NAME\n" +
                ",ARRIVE_TERMINAL_NAME\n" +
                ",ISSUE_AIRLINE\n" +
                ",EXTRA_GATES\n" +
                ",PLANE_TYPE\n" +
                ",PLANE_CLASS\n" +
                ",STOP_OVER_INFO\n" +
                ",CREATE_TIME\n" +
                ",UPDATE_TIME\n" +
                ",CREATE_BY\n" +
                ",UPDATE_BY\n" +
                ",ARR_DATE)  " +
                "SELECT CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                ",ID\n" +
                ",DEP\n" +
                ",ARR\n" +
                ",FLIGHT_NO\n" +
                ",CARR_FLIGHT_NO\n" +
                ",FLIGHT_DATE\n" +
                ",SCH_DEPT_TIME\n" +
                ",SCH_ARR_TIME\n" +
                ",EXP_DEPT_TIME\n" +
                ",BOARDING_TIME\n" +
                ",BOARDING_GATE_NUMBER\n" +
                ",FROM_CITY_STATUS\n" +
                ",TO_CITY_STATUS\n" +
                ",DEPT_TERMINAL_NAME\n" +
                ",ARRIVE_TERMINAL_NAME\n" +
                ",ISSUE_AIRLINE\n" +
                ",EXTRA_GATES\n" +
                ",PLANE_TYPE\n" +
                ",PLANE_CLASS\n" +
                ",STOP_OVER_INFO\n" +
                ",CREATE_TIME\n" +
                ",UPDATE_TIME\n" +
                ",CREATE_BY\n" +
                ",UPDATE_BY\n" +
                ",ARR_DATE" +
                " FROM CHECKIN_ITINERARY "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' " +
                " OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        TableResult result = tEnv.executeSql(extractSql);

        result.print();


    }

}
