package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.OracleAESDecryptor;
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
public class ExtractTOdsClkSysDict {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
//        String etlDate = "2015-10-21";
        String sm4key =  Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsClkSysDict");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();

//创建上游数据源表
        StringBuffer sysDictSql = new StringBuffer();
        sysDictSql.append("CREATE TABLE SYS_DICT (\n");
        sysDictSql.append("ID VARCHAR(192) ,\n");
        sysDictSql.append("`VALUE` VARCHAR(300) ,\n");
        sysDictSql.append("LABEL VARCHAR(300) ,\n");
        sysDictSql.append("`TYPE` VARCHAR(300) ,\n");
        sysDictSql.append("DESCRIPTION VARCHAR(900) ,\n");
        sysDictSql.append("SORT DECIMAL(10,0) ,\n");
        sysDictSql.append("PARENT_ID VARCHAR(192) ,\n");
        sysDictSql.append("CREATE_BY VARCHAR(192) ,\n");
        sysDictSql.append("CREATE_DATE TIMESTAMP(6) ,\n");
        sysDictSql.append("UPDATE_BY VARCHAR(192) ,\n");
        sysDictSql.append("UPDATE_DATE TIMESTAMP(6) ,\n");
        sysDictSql.append("REMARKS VARCHAR(765),\n");
        sysDictSql.append("DEL_FLAG VARCHAR(3) \n");
        sysDictSql.append(") WITH (\n");
        sysDictSql.append("    'connector' = 'jdbc',\n");
        sysDictSql.append("    'url' = 'jdbc:oracle:thin:@//").append(Constants.CLK_IP).append(":").append(Constants.CLK_PORT).append("/").append(Constants.CLK_DB).append("',\n");
        sysDictSql.append("    'table-name' = '").append(Constants.CLK_SCHEMA).append(".SYS_DICT', \n");
        sysDictSql.append("    'username' = '").append(Constants.CLK_USER).append("',\n");
        sysDictSql.append("    'password' = '").append(Constants.CLK_PWD).append("'\n");
        sysDictSql.append(",\n").append("  'driver' = '").append(Constants.ORACLE_DRIVER).append("'");
        sysDictSql.append(")");
        tEnv.executeSql(sysDictSql.toString());
        //创建Doris目标表
        StringBuffer dorisTableSql = new StringBuffer();
        dorisTableSql.append("CREATE TABLE T_ODS_CLK_SYS_DICT (\n");
        dorisTableSql.append("    ETL_DATE DATE,\n");
        dorisTableSql.append("ID VARCHAR(192) ,\n");
        dorisTableSql.append("`VALUE` VARCHAR(300) ,\n");
        dorisTableSql.append("LABEL VARCHAR(300) ,\n");
        dorisTableSql.append("`TYPE` VARCHAR(300) ,\n");
        dorisTableSql.append("DESCRIPTION VARCHAR(900) ,\n");
        dorisTableSql.append("SORT DECIMAL(10,0) ,\n");
        dorisTableSql.append("PARENT_ID VARCHAR(192) ,\n");
        dorisTableSql.append("CREATE_BY VARCHAR(192) ,\n");
        dorisTableSql.append("CREATE_DATE TIMESTAMP(6) ,\n");
        dorisTableSql.append("UPDATE_BY VARCHAR(192) ,\n");
        dorisTableSql.append("UPDATE_DATE TIMESTAMP(6) ,\n");
        dorisTableSql.append("REMARKS VARCHAR(765),\n");
        dorisTableSql.append("DEL_FLAG VARCHAR(3) \n");
        dorisTableSql.append(") WITH (\n");
        dorisTableSql.append(" 'connector' = 'doris',\n");
        dorisTableSql.append("'fenodes' = '").append(Constants.DORIS_FE_IP).append(":").append(Constants.DORIS_FE_PORT).append("', -- 替换为 Doris FE 的地址和端口\n");
        dorisTableSql.append("'benodes' = '").append(Constants.DORIS_BE_IP).append(":").append(Constants.DORIS_BE_PORT).append("', -- 替换为 Doris FE 的地址和端口\n");
        dorisTableSql.append(" 'table.identifier' = '").append(Constants.ODS_DB).append(".T_ODS_CLK_SYS_DICT',\n");
        dorisTableSql.append("'sink.label-prefix' = '").append(timestamp).append(uuid).append("',");
        dorisTableSql.append(" 'sink.properties.read_json_by_line' = 'true',");
        dorisTableSql.append(" 'sink.properties.format' = 'json',");
        dorisTableSql.append("    'username' = '").append(Constants.ODS_USER).append("',\n");
        dorisTableSql.append("    'password' = '").append(Constants.ODS_PWD).append("'\n");
        dorisTableSql.append(")");
        tEnv.executeSql(dorisTableSql.toString());
        //数据抽取sql，配置增量字段、数据加密等
        StringBuffer extractSqlBuffer = new StringBuffer();
        extractSqlBuffer.append("INSERT INTO T_ODS_CLK_SYS_DICT(");
        extractSqlBuffer.append("ETL_DATE,\n");
        extractSqlBuffer.append("ID,\n");
        extractSqlBuffer.append("`VALUE`,\n");
        extractSqlBuffer.append("LABEL,\n");
        extractSqlBuffer.append("`TYPE`,\n");
        extractSqlBuffer.append("DESCRIPTION,\n");
        extractSqlBuffer.append("SORT,\n");
        extractSqlBuffer.append("PARENT_ID,\n");
        extractSqlBuffer.append("CREATE_BY,\n");
        extractSqlBuffer.append("CREATE_DATE,\n");
        extractSqlBuffer.append("UPDATE_BY,\n");
        extractSqlBuffer.append("UPDATE_DATE,\n");
        extractSqlBuffer.append("REMARKS,\n");
        extractSqlBuffer.append("DEL_FLAG)\n");
        extractSqlBuffer.append("SELECT ");
        extractSqlBuffer.append("CAST('").append(etlDate).append("' AS DATE) ETL_DATE,");
        extractSqlBuffer.append("ID,\n");
        extractSqlBuffer.append("`VALUE`,\n");
        extractSqlBuffer.append("LABEL,\n");
        extractSqlBuffer.append("`TYPE`,\n");
        extractSqlBuffer.append("DESCRIPTION,\n");
        extractSqlBuffer.append("SORT,\n");
        extractSqlBuffer.append("PARENT_ID,\n");
        extractSqlBuffer.append("CREATE_BY,\n");
        extractSqlBuffer.append("CREATE_DATE,\n");
        extractSqlBuffer.append("UPDATE_BY,\n");
        extractSqlBuffer.append("UPDATE_DATE,\n");
        extractSqlBuffer.append("REMARKS,\n");
        extractSqlBuffer.append("DEL_FLAG ");
        extractSqlBuffer.append(" FROM SYS_DICT ");
        extractSqlBuffer.append(" WHERE  CREATE_DATE BETWEEN TIMESTAMP '").append(etlDate).append(" 00:00:00' AND TIMESTAMP '").append(etlDate).append(" 23:59:59.999' ");
        extractSqlBuffer.append(" OR  UPDATE_DATE BETWEEN TIMESTAMP '").append(etlDate).append(" 00:00:00' AND TIMESTAMP '").append(etlDate).append(" 23:59:59.999' ");
        String extractSql = extractSqlBuffer.toString();

        TableResult result = tEnv.executeSql(extractSql);

        result.print();


    }
}
