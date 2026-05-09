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
public class ExtractTOdsLyxMtmLyvalSummary {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLyxMtmLyvalSummary");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();

//创建上游数据源表
        tEnv.executeSql("CREATE TABLE MTM_LYVAL_SUMMARY (\n" +
                "ID BIGINT ,\n" +
                "SETTLE_CYCLE VARCHAR(24) ,\n" +
                "CYCLE_LYVAL DECIMAL(12,2) ,\n" +
                "USABLE_LYVAL DECIMAL(12,2) ,\n" +
                "PRE_LOSE_LYVAL DECIMAL(12,2) ,\n" +
                "EXPIRED_LYVAL DECIMAL(12,2) ,\n" +
                "CONSUMED_LYVAL DECIMAL(12,2) ,\n" +
                "ACCOUNT_FLG VARCHAR(6) ,\n" +
                "MPUPI VARCHAR(6) ,\n" +
                "DEL_FLG VARCHAR(6) ,\n" +
                "VERSION BIGINT ,\n" +
                "CREATE_ID VARCHAR(60) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_ID VARCHAR(60) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "MTM_CARD_NUM VARCHAR(60)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.LYX_IP+":"+Constants.LYX_PORT+"/"+Constants.LYX_DB+"',\n" +
                "    'table-name' = '"+Constants.LYX_SCHEMA+".MTM_LYVAL_SUMMARY', \n" +
                "    'username' = '"+Constants.LYX_USER+"',\n" +
                "    'password' = '"+Constants.LYX_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_LYX_MTM_LYVAL_SUMMARY (\n" +
                "    ETL_DATE DATE,\n" +
                "ID BIGINT ,\n" +
                "SETTLE_CYCLE VARCHAR(24) ,\n" +
                "CYCLE_LYVAL DECIMAL(12,2) ,\n" +
                "USABLE_LYVAL DECIMAL(12,2) ,\n" +
                "PRE_LOSE_LYVAL DECIMAL(12,2) ,\n" +
                "EXPIRED_LYVAL DECIMAL(12,2) ,\n" +
                "CONSUMED_LYVAL DECIMAL(12,2) ,\n" +
                "ACCOUNT_FLG VARCHAR(6) ,\n" +
                "MPUPI VARCHAR(6) ,\n" +
                "DEL_FLG VARCHAR(6) ,\n" +
                "VERSION BIGINT ,\n" +
                "CREATE_ID VARCHAR(60) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_ID VARCHAR(60) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "MTM_CARD_NUM VARCHAR(300)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYX_MTM_LYVAL_SUMMARY',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_LYX_MTM_LYVAL_SUMMARY(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "SETTLE_CYCLE,\n" +
                "CYCLE_LYVAL,\n" +
                "USABLE_LYVAL,\n" +
                "PRE_LOSE_LYVAL,\n" +
                "EXPIRED_LYVAL,\n" +
                "CONSUMED_LYVAL,\n" +
                "ACCOUNT_FLG,\n" +
                "MPUPI,\n" +
                "DEL_FLG,\n" +
                "VERSION,\n" +
                "CREATE_ID,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_ID,\n" +
                "UPDATE_TIME,\n" +
                "MTM_CARD_NUM)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "SETTLE_CYCLE,\n" +
                "CYCLE_LYVAL,\n" +
                "USABLE_LYVAL,\n" +
                "PRE_LOSE_LYVAL,\n" +
                "EXPIRED_LYVAL,\n" +
                "CONSUMED_LYVAL,\n" +
                "ACCOUNT_FLG,\n" +
                "MPUPI,\n" +
                "DEL_FLG,\n" +
                "VERSION,\n" +
                "CREATE_ID,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_ID,\n" +
                "UPDATE_TIME,\n" +
                " sm4_encrypt (MTM_CARD_NUM,'" + sm4key + "') MTM_CARD_NUM \n" +
                " FROM MTM_LYVAL_SUMMARY "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
