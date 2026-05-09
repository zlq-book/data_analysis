package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.UUID;

/**
 * @author
 * @date 2025/7/2 9:31
 */
public class ExtractTOdsZxyhRigMedalUserDeltailRecord {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZxyhRigMedalUserDeltailRecord");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE RIG_MEDAL_USER_DELTAIL_RECORD (\n" +
                "ID VARCHAR(96) ,\n" +
                "MEDAL_USER_RECORD_ID VARCHAR(96) ,\n" +
                "MEDAL_ID VARCHAR(96) ,\n" +
                "REWARD_ID VARCHAR(96) ,\n" +
                "GRANT_STATUS CHAR(10) ,\n" +
                "CUSTOMER_ID VARCHAR(600) ,\n" +
                "EXPIRES_TIME TIMESTAMP(6) ,\n" +
                "ENABLE CHAR(10) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_USER VARCHAR(300) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(300)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.ZXYH_IP+":"+Constants.ZXYH_PORT+"/"+Constants.ZXYH_DB+"',\n" +
                "    'table-name' = '"+Constants.ZXYH_SCHEMA+".RIG_MEDAL_USER_DELTAIL_RECORD', \n" +
                "    'username' = '"+Constants.ZXYH_USER+"',\n" +
                "    'password' = '"+Constants.ZXYH_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_ZXYH_RIG_MEDAL_USER_DELTAIL_RECORD (\n" +
                "    ETL_DATE DATE,\n" +
                "ID VARCHAR(96) ,\n" +
                "MEDAL_USER_RECORD_ID VARCHAR(96) ,\n" +
                "MEDAL_ID VARCHAR(96) ,\n" +
                "REWARD_ID VARCHAR(96) ,\n" +
                "GRANT_STATUS CHAR(10) ,\n" +
                "CUSTOMER_ID VARCHAR(600) ,\n" +
                "EXPIRES_TIME TIMESTAMP(6) ,\n" +
                "ENABLE CHAR(10) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_USER VARCHAR(300) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(300)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZXYH_RIG_MEDAL_USER_DELTAIL_RECORD',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_ZXYH_RIG_MEDAL_USER_DELTAIL_RECORD(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "MEDAL_USER_RECORD_ID,\n" +
                "MEDAL_ID,\n" +
                "REWARD_ID,\n" +
                "GRANT_STATUS,\n" +
                "CUSTOMER_ID,\n" +
                "EXPIRES_TIME,\n" +
                "ENABLE,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "MEDAL_USER_RECORD_ID,\n" +
                "MEDAL_ID,\n" +
                "REWARD_ID,\n" +
                "GRANT_STATUS,\n" +
                "CUSTOMER_ID,\n" +
                "EXPIRES_TIME,\n" +
                "ENABLE,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER\n" +
                " FROM RIG_MEDAL_USER_DELTAIL_RECORD "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
