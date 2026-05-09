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
public class ExtractTOdsLyxOrderFlightInfos {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLyxOrderFlightInfos");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();

//创建上游数据源表
        tEnv.executeSql("CREATE TABLE ORDER_FLIGHT_INFOS (\n" +
                "ID BIGINT ,\n" +
                "OLD_FLT_ID BIGINT ,\n" +
                "ORDER_NO VARCHAR(300) ,\n" +
                "FLIGHT_NUM VARCHAR(30) ,\n" +
                "FLIGHT_DATE TIMESTAMP(6) ,\n" +
                "ORIG VARCHAR(30) ,\n" +
                "DEST VARCHAR(30) ,\n" +
                "DEPATURE_TIME TIMESTAMP(6) ,\n" +
                "ARRIVE_TIME TIMESTAMP(6) ,\n" +
                "IS_GO_BACK VARCHAR(6) ,\n" +
                "CABIN VARCHAR(18) ,\n" +
                "FLT_TYPE VARCHAR(6) ,\n" +
                "STOP_CTIY VARCHAR(18) ,\n" +
                "DEPART_TEMINAL VARCHAR(90) ,\n" +
                "ARRIVAL_TEMINAL VARCHAR(90)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.LYX_IP+":"+Constants.LYX_PORT+"/"+Constants.LYX_DB+"',\n" +
                "    'table-name' = '"+Constants.LYX_SCHEMA+".ORDER_FLIGHT_INFOS', \n" +
                "    'username' = '"+Constants.LYX_USER+"',\n" +
                "    'password' = '"+Constants.LYX_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_LYX_ORDER_FLIGHT_INFOS (\n" +
                "    ETL_DATE DATE,\n" +
                "ID BIGINT ,\n" +
                "OLD_FLT_ID BIGINT ,\n" +
                "ORDER_NO VARCHAR(300) ,\n" +
                "FLIGHT_NUM VARCHAR(30) ,\n" +
                "FLIGHT_DATE TIMESTAMP(6) ,\n" +
                "ORIG VARCHAR(30) ,\n" +
                "DEST VARCHAR(30) ,\n" +
                "DEPATURE_TIME TIMESTAMP(6) ,\n" +
                "ARRIVE_TIME TIMESTAMP(6) ,\n" +
                "IS_GO_BACK VARCHAR(6) ,\n" +
                "CABIN VARCHAR(18) ,\n" +
                "FLT_TYPE VARCHAR(6) ,\n" +
                "STOP_CTIY VARCHAR(18) ,\n" +
                "DEPART_TEMINAL VARCHAR(90) ,\n" +
                "ARRIVAL_TEMINAL VARCHAR(90)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYX_ORDER_FLIGHT_INFOS',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_LYX_ORDER_FLIGHT_INFOS(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "OLD_FLT_ID,\n" +
                "ORDER_NO,\n" +
                "FLIGHT_NUM,\n" +
                "FLIGHT_DATE,\n" +
                "ORIG,\n" +
                "DEST,\n" +
                "DEPATURE_TIME,\n" +
                "ARRIVE_TIME,\n" +
                "IS_GO_BACK,\n" +
                "CABIN,\n" +
                "FLT_TYPE,\n" +
                "STOP_CTIY,\n" +
                "DEPART_TEMINAL,\n" +
                "ARRIVAL_TEMINAL)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "OLD_FLT_ID,\n" +
                "ORDER_NO,\n" +
                "FLIGHT_NUM,\n" +
                "FLIGHT_DATE,\n" +
                "ORIG,\n" +
                "DEST,\n" +
                "DEPATURE_TIME,\n" +
                "ARRIVE_TIME,\n" +
                "IS_GO_BACK,\n" +
                "CABIN,\n" +
                "FLT_TYPE,\n" +
                "STOP_CTIY,\n" +
                "DEPART_TEMINAL,\n" +
                "ARRIVAL_TEMINAL\n" +
                " FROM ORDER_FLIGHT_INFOS ";

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
