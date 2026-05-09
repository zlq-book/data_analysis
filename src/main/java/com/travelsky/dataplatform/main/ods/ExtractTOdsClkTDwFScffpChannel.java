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
public class ExtractTOdsClkTDwFScffpChannel {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsClkTDwFScffpChannel");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();

//创建上游数据源表
        StringBuffer sourceTableSql = new StringBuffer();
        sourceTableSql.append("CREATE TABLE T_DW_F_SCFFP_CHANNEL (\n");
        sourceTableSql.append("ROW_ID BIGINT,\n");
        sourceTableSql.append("CHANNEL_CODE VARCHAR(150),\n");
        sourceTableSql.append("CHANNEL_NAME VARCHAR(300),\n");
        sourceTableSql.append("DEP_CODE VARCHAR(300),\n");
        sourceTableSql.append("DEP_NAME VARCHAR(300),\n");
        sourceTableSql.append("UNINT_CODE VARCHAR(300),\n");
        sourceTableSql.append("UNINT_NAME VARCHAR(300),\n");
        sourceTableSql.append("USER_CODE VARCHAR(300),\n");
        sourceTableSql.append("USER_NAME VARCHAR(300),\n");
        sourceTableSql.append("START_DATE TIMESTAMP(6),\n");
        sourceTableSql.append("MEM_NUM VARCHAR(150),\n");
        sourceTableSql.append("DATA_SOURCE VARCHAR(30),\n");
        sourceTableSql.append("ETL_INSERT_DATE TIMESTAMP(6),\n");
        sourceTableSql.append("ETL_UPDATE_DATE TIMESTAMP(6)\n");
        sourceTableSql.append(") WITH (\n");
        sourceTableSql.append("    'connector' = 'jdbc',\n");
        sourceTableSql.append("    'url' = 'jdbc:oracle:thin:@//").append(Constants.CLK_DW_IP).append(":").append(Constants.CLK_DW_PORT).append("/").append(Constants.CLK_DW_DB).append("',\n");
        sourceTableSql.append("    'table-name' = '").append(Constants.CLK_DW_SCHEMA).append(".T_DW_F_SCFFP_CHANNEL', \n");
        sourceTableSql.append("    'username' = '").append(Constants.CLK_DW_USER).append("',\n");
        sourceTableSql.append("    'password' = '").append(Constants.CLK_DW_PWD).append("'\n");
        sourceTableSql.append(",\n");
        sourceTableSql.append("  'driver' = '").append(Constants.ORACLE_DRIVER).append("'");
        sourceTableSql.append(")");
        tEnv.executeSql(sourceTableSql.toString());
        
        //创建Doris目标表
        StringBuffer targetTableSql = new StringBuffer();
        targetTableSql.append("CREATE TABLE T_ODS_CLK_T_DW_F_SCFFP_CHANNEL (\n");
        targetTableSql.append("    ETL_DATE DATE,\n");
        targetTableSql.append("ROW_ID BIGINT,\n");
        targetTableSql.append("CHANNEL_CODE VARCHAR(150),\n");
        targetTableSql.append("CHANNEL_NAME VARCHAR(300),\n");
        targetTableSql.append("DEP_CODE VARCHAR(300),\n");
        targetTableSql.append("DEP_NAME VARCHAR(300),\n");
        targetTableSql.append("UNINT_CODE VARCHAR(300),\n");
        targetTableSql.append("UNINT_NAME VARCHAR(300),\n");
        targetTableSql.append("USER_CODE VARCHAR(300),\n");
        targetTableSql.append("USER_NAME VARCHAR(300),\n");
        targetTableSql.append("START_DATE TIMESTAMP(6),\n");
        targetTableSql.append("MEM_NUM VARCHAR(150),\n");
        targetTableSql.append("DATA_SOURCE VARCHAR(30),\n");
        targetTableSql.append("ETL_INSERT_DATE TIMESTAMP(6),\n");
        targetTableSql.append("ETL_UPDATE_DATE TIMESTAMP(6)\n");
        targetTableSql.append(") WITH (\n");
        targetTableSql.append(" 'connector' = 'doris',\n");
        targetTableSql.append("'fenodes' = '").append(Constants.DORIS_FE_IP).append(":").append(Constants.DORIS_FE_PORT).append("', -- 替换为 Doris FE 的地址和端口\n");
        targetTableSql.append("'benodes' = '").append(Constants.DORIS_BE_IP).append(":").append(Constants.DORIS_BE_PORT).append("', -- 替换为 Doris FE 的地址和端口\n");
        targetTableSql.append(" 'table.identifier' = '").append(Constants.ODS_DB).append(".T_ODS_CLK_T_DW_F_SCFFP_CHANNEL',\n");
        targetTableSql.append("'sink.label-prefix' = '").append(timestamp).append(uuid).append("',");
        targetTableSql.append(" 'sink.properties.read_json_by_line' = 'true',");
        targetTableSql.append(" 'sink.properties.format' = 'json',");
        targetTableSql.append("    'username' = '").append(Constants.ODS_USER).append("',\n");
        targetTableSql.append("    'password' = '").append(Constants.ODS_PWD).append("'\n");
        targetTableSql.append(")");
        tEnv.executeSql(targetTableSql.toString());
        
        //数据抽取sql，配置增量字段、数据加密等
        StringBuffer extractSqlBuffer = new StringBuffer();
        extractSqlBuffer.append("INSERT INTO T_ODS_CLK_T_DW_F_SCFFP_CHANNEL(");
        extractSqlBuffer.append("ETL_DATE,\n");
        extractSqlBuffer.append("ROW_ID,\n");
        extractSqlBuffer.append("CHANNEL_CODE,\n");
        extractSqlBuffer.append("CHANNEL_NAME,\n");
        extractSqlBuffer.append("DEP_CODE,\n");
        extractSqlBuffer.append("DEP_NAME,\n");
        extractSqlBuffer.append("UNINT_CODE,\n");
        extractSqlBuffer.append("UNINT_NAME,\n");
        extractSqlBuffer.append("USER_CODE,\n");
        extractSqlBuffer.append("USER_NAME,\n");
        extractSqlBuffer.append("START_DATE,\n");
        extractSqlBuffer.append("MEM_NUM,\n");
        extractSqlBuffer.append("DATA_SOURCE,\n");
        extractSqlBuffer.append("ETL_INSERT_DATE,\n");
        extractSqlBuffer.append("ETL_UPDATE_DATE)\n");
        extractSqlBuffer.append("SELECT ");
        extractSqlBuffer.append("CAST('").append(etlDate).append("' AS DATE) ETL_DATE,");
        extractSqlBuffer.append("ROW_ID,\n");
        extractSqlBuffer.append("CHANNEL_CODE,\n");
        extractSqlBuffer.append("CHANNEL_NAME,\n");
        extractSqlBuffer.append("DEP_CODE,\n");
        extractSqlBuffer.append("DEP_NAME,\n");
        extractSqlBuffer.append("UNINT_CODE,\n");
        extractSqlBuffer.append("UNINT_NAME,\n");
        extractSqlBuffer.append("USER_CODE,\n");
        extractSqlBuffer.append("USER_NAME,\n");
        extractSqlBuffer.append("START_DATE,\n");
        extractSqlBuffer.append(" sm4_encrypt (MEM_NUM,'").append(sm4key).append("') MEM_NUM, \n");
        extractSqlBuffer.append("DATA_SOURCE,\n");
        extractSqlBuffer.append("ETL_INSERT_DATE,\n");
        extractSqlBuffer.append("ETL_UPDATE_DATE\n");
        extractSqlBuffer.append(" FROM T_DW_F_SCFFP_CHANNEL ");
        extractSqlBuffer.append(" WHERE  ETL_INSERT_DATE BETWEEN TIMESTAMP '").append(etlDate).append(" 00:00:00' AND TIMESTAMP '").append(etlDate).append(" 23:59:59.999' ");
        extractSqlBuffer.append(" OR  ETL_UPDATE_DATE BETWEEN TIMESTAMP '").append(etlDate).append(" 00:00:00' AND TIMESTAMP '").append(etlDate).append(" 23:59:59.999' ");
        String extractSql = extractSqlBuffer.toString();

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
