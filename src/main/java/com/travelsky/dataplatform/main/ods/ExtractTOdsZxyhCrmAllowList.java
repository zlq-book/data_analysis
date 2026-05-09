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
public class ExtractTOdsZxyhCrmAllowList {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }

        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZxyhCrmAllowList");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();

//创建上游数据源表
        tEnv.executeSql("CREATE TABLE CRM_ALLOWLIST (\n" +
                "    ID VARCHAR(96),\n" +
                "    TYPE VARCHAR(6),\n" +
                "    CONTEXT VARCHAR(12000),\n" +
                "    REASON VARCHAR(12000),\n" +
                "    EFFECTIVE_STATUS CHAR(1),\n" +
                "    CREATE_TIME TIMESTAMP(6),\n" +
                "    CREATE_USER VARCHAR(96),\n" +
                "    UPDATE_TIME TIMESTAMP(6),\n" +
                "    UPDATE_USER VARCHAR(96),\n" +
                "    ENABLE CHAR(1),\n" +
                "    BLOCK_RULE_ID VARCHAR(300)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.ZXYH_IP+":"+Constants.ZXYH_PORT+"/"+Constants.ZXYH_DB+"',\n" +
                "    'table-name' = '"+Constants.ZXYH_SCHEMA+".CRM_ALLOWLIST', \n" +
                "    'username' = '"+Constants.ZXYH_USER+"',\n" +
                "    'password' = '"+Constants.ZXYH_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_ZXYH_CRM_ALLOWLIST (\n" +
                "    ETL_DATE DATE,\n" +
                "    ID VARCHAR(96),\n" +
                "    TYPE VARCHAR(6),\n" +
                "    CONTEXT VARCHAR(12000),\n" +
                "    REASON VARCHAR(12000),\n" +
                "    EFFECTIVE_STATUS CHAR(1),\n" +
                "    CREATE_TIME TIMESTAMP(6),\n" +
                "    CREATE_USER VARCHAR(96),\n" +
                "    UPDATE_TIME TIMESTAMP(6),\n" +
                "    UPDATE_USER VARCHAR(96),\n" +
                "    ENABLE CHAR(1),\n" +
                "    BLOCK_RULE_ID VARCHAR(300)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZXYH_CRM_ALLOWLIST',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_ZXYH_CRM_ALLOWLIST(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "TYPE,\n" +
                "CONTEXT,\n" +
                "REASON,\n" +
                "EFFECTIVE_STATUS,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "ENABLE,\n" +
                "BLOCK_RULE_ID)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "TYPE,\n" +
                "CONTEXT,\n" +
                "REASON,\n" +
                "EFFECTIVE_STATUS,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "ENABLE,\n" +
                "BLOCK_RULE_ID\n" +
                " FROM CRM_ALLOWLIST "
                        + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                        +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
