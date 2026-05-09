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
import java.sql.Connection;
import java.util.UUID;

/**
 * @author kuangaihua
 * @date 2025/6/26 16:29
 */
public class ExtractTOdsLygjCipVipToLoveType {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLygjCipVipToLoveType");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        StringBuffer sourceTableSql = new StringBuffer();
        sourceTableSql.append("CREATE TABLE CIP_VIP_TO_LOVE_TYPE (\n");
        sourceTableSql.append("\n");
        sourceTableSql.append("    ID BIGINT NOT NULL,\n");
        sourceTableSql.append("    TOLOVE_NAME VARCHAR(3000),\n");
        sourceTableSql.append("    TOLOVE_LEVEL VARCHAR(6),\n");
        sourceTableSql.append("    TOLOVE_PARENT_ID BIGINT,\n");
        sourceTableSql.append("    MEMO VARCHAR(3000),\n");
        sourceTableSql.append("    STATUS VARCHAR(6),\n");
        sourceTableSql.append("    VERSION BIGINT");
        sourceTableSql.append("\n");
        sourceTableSql.append(") WITH (\n");
        sourceTableSql.append("    'connector' = 'jdbc',\n");
        sourceTableSql.append("    'url' = 'jdbc:oracle:thin:@//").append(Constants.LYGJ_IP).append(":").append(Constants.LYGJ_PORT).append("/").append(Constants.LYGJ_DB).append("',\n");
        sourceTableSql.append("    'table-name' = '").append(Constants.LYGJ_SCHEMA).append(".CIP_VIP_TO_LOVE_TYPE', \n");
        sourceTableSql.append("    'username' = '").append(Constants.LYGJ_USER).append("',\n");
        sourceTableSql.append("    'password' = '").append(Constants.LYGJ_PWD).append("'\n");
        sourceTableSql.append(",\n");
        sourceTableSql.append("    'driver' = '").append(Constants.ORACLE_DRIVER).append("'");
        sourceTableSql.append(")");
        tEnv.executeSql(sourceTableSql.toString());
        
        //创建Doris目标表
        StringBuffer targetTableSql = new StringBuffer();
        targetTableSql.append("CREATE TABLE T_ODS_LYGJ_CIP_VIP_TO_LOVE_TYPE (\n");
        targetTableSql.append("    ETL_DATE DATE,\n");
        targetTableSql.append("    ID BIGINT NOT NULL,\n");
        targetTableSql.append("    TOLOVE_NAME VARCHAR(3000),\n");
        targetTableSql.append("    TOLOVE_LEVEL VARCHAR(6),\n");
        targetTableSql.append("    TOLOVE_PARENT_ID BIGINT,\n");
        targetTableSql.append("    MEMO VARCHAR(3000),\n");
        targetTableSql.append("    STATUS VARCHAR(6),\n");
        targetTableSql.append("    VERSION BIGINT");
        targetTableSql.append(") WITH (\n");
        targetTableSql.append(" 'connector' = 'doris',\n");
        targetTableSql.append("'fenodes' = '").append(Constants.DORIS_FE_IP).append(":").append(Constants.DORIS_FE_PORT).append("', -- 替换为 Doris FE 的地址和端口\n");
        targetTableSql.append("'benodes' = '").append(Constants.DORIS_BE_IP).append(":").append(Constants.DORIS_BE_PORT).append("', -- 替换为 Doris FE 的地址和端口\n");
        targetTableSql.append(" 'table.identifier' = '").append(Constants.ODS_DB).append(".T_ODS_LYGJ_CIP_VIP_TO_LOVE_TYPE',\n");
        targetTableSql.append("'sink.label-prefix' = '").append(timestamp).append(uuid).append("',");
        targetTableSql.append(" 'sink.properties.read_json_by_line' = 'true',");
        targetTableSql.append(" 'sink.properties.format' = 'json',");
        targetTableSql.append("    'username' = '").append(Constants.ODS_USER).append("',\n");
        targetTableSql.append("    'password' = '").append(Constants.ODS_PWD).append("'\n");
        targetTableSql.append(")");
        tEnv.executeSql(targetTableSql.toString());

        // 1. 先用 JDBC 原生方式清空目标表
        //try {
        //    // 加载 MySQL 驱动（Doris 兼容 MySQL 协议）
        //    Class.forName("com.mysql.cj.jdbc.Driver");
        //    // 连接到 Doris/MySQL 数据库
        //    Connection conn = java.sql.DriverManager.getConnection(
        //        "jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB,
        //        Constants.ODS_USER,
        //        Constants.ODS_PWD
        //    );
        //    // 创建 SQL 执行对象
        //    java.sql.Statement stmt = conn.createStatement();
        //    // 执行 TRUNCATE 语句清空表（也可以用 DELETE FROM ...）
        //    stmt.execute("TRUNCATE TABLE T_ODS_LYGJ_CIP_VIP_TO_LOVE_TYPE");
        //    // 关闭资源
        //    stmt.close();
        //    conn.close();
        //} catch (Exception e) {
        //    // 打印异常信息并退出程序
        //    e.printStackTrace();
        //    System.exit(1);
        //}
        
        StringBuffer extractSqlBuffer = new StringBuffer();
        extractSqlBuffer.append("INSERT INTO T_ODS_LYGJ_CIP_VIP_TO_LOVE_TYPE(");
        extractSqlBuffer.append("ETL_DATE,\n");
        extractSqlBuffer.append("    ID,\n");
        extractSqlBuffer.append("    TOLOVE_NAME,\n");
        extractSqlBuffer.append("    TOLOVE_LEVEL,\n");
        extractSqlBuffer.append("    TOLOVE_PARENT_ID,\n");
        extractSqlBuffer.append("    MEMO,\n");
        extractSqlBuffer.append("    STATUS,\n");
        extractSqlBuffer.append("    VERSION)  ");
        extractSqlBuffer.append("SELECT \n");
        extractSqlBuffer.append("CAST('").append(etlDate).append("' AS DATE) ETL_DATE,");
        extractSqlBuffer.append("    ID,\n");
        extractSqlBuffer.append("    TOLOVE_NAME,\n");
        extractSqlBuffer.append("    TOLOVE_LEVEL,\n");
        extractSqlBuffer.append("    TOLOVE_PARENT_ID,\n");
        extractSqlBuffer.append("    MEMO,\n");
        extractSqlBuffer.append("    STATUS,\n");
        extractSqlBuffer.append("    VERSION ");
        extractSqlBuffer.append(" FROM CIP_VIP_TO_LOVE_TYPE");
        String extractSql = extractSqlBuffer.toString();
        
        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }

}
