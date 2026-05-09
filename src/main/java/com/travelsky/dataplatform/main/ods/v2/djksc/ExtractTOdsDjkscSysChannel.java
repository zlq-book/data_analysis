package com.travelsky.dataplatform.main.ods.v2.djksc;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.OracleAESDecryptor;
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
public class ExtractTOdsDjkscSysChannel {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
//        String etlDate = "2024-03-03";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsDjkscSysChannel");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE SYS_CHANNEL (\n" +
                "ID VARCHAR(384),\n" +
                "CHANNEL_NAME VARCHAR(768),\n" +
                "CHANNEL_CODE VARCHAR(768),\n" +
                "CHANNEL_KEY VARCHAR(768),\n" +
                "CHANNEL_IP VARCHAR(12000),\n" +
                "REMARK VARCHAR(3000),\n" +
                "INTERFACE_JSON VARCHAR(6000),\n" +
                "HANDLE_START_TIME VARCHAR(54),\n" +
                "HANDLE_END_TIME VARCHAR(54),\n" +
                "RESERVATION_START_TIME VARCHAR(54),\n" +
                "RESERVATION_END_TIME VARCHAR(54),\n" +
                "COUNT_DOWN_TIME VARCHAR(54),\n" +
                "RESTRICT_TIMES VARCHAR(54),\n" +
                "RESERVATION_INCREASE_RATIO VARCHAR(54),\n" +
                "BUSINESS_CLASS_PRICE DECIMAL(24,0),\n" +
                "HIGH_CLASS_PRICE DECIMAL(24,0),\n" +
                "UPGRADE_VOUCHER CHAR(1),\n" +
                "UPGRADE_CARD CHAR(1),\n" +
                "ENABLE CHAR(1),\n" +
                "CREATE_TIME TIMESTAMP(6),\n" +
                "CREATE_USER VARCHAR(384),\n" +
                "UPDATE_TIME TIMESTAMP(6),\n" +
                "UPDATE_USER VARCHAR(384),\n" +
                "STATUS CHAR(1)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.DJKSC_IP+":"+Constants.DJKSC_PORT+"/"+Constants.DJKSC_DB+"',\n" +
                "    'table-name' = '"+Constants.DJKSC_SCHEMA+".SYS_CHANNEL', \n" +
                "    'username' = '"+Constants.DJKSC_USER+"',\n" +
                "    'password' = '"+Constants.DJKSC_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_DJKSC_SYS_CHANNEL (\n" +
                "    ETL_DATE DATE,\n" +
                "ID VARCHAR(384),\n" +
                "CHANNEL_NAME VARCHAR(768),\n" +
                "CHANNEL_CODE VARCHAR(768),\n" +
                "CHANNEL_KEY VARCHAR(768),\n" +
                "CHANNEL_IP VARCHAR(12000),\n" +
                "REMARK VARCHAR(3000),\n" +
                "INTERFACE_JSON VARCHAR(6000),\n" +
                "HANDLE_START_TIME VARCHAR(54),\n" +
                "HANDLE_END_TIME VARCHAR(54),\n" +
                "RESERVATION_START_TIME VARCHAR(54),\n" +
                "RESERVATION_END_TIME VARCHAR(54),\n" +
                "COUNT_DOWN_TIME VARCHAR(54),\n" +
                "RESTRICT_TIMES VARCHAR(54),\n" +
                "RESERVATION_INCREASE_RATIO VARCHAR(54),\n" +
                "BUSINESS_CLASS_PRICE DECIMAL(24,0),\n" +
                "HIGH_CLASS_PRICE DECIMAL(24,0),\n" +
                "UPGRADE_VOUCHER CHAR(1),\n" +
                "UPGRADE_CARD CHAR(1),\n" +
                "ENABLE CHAR(1),\n" +
                "CREATE_TIME TIMESTAMP(6),\n" +
                "CREATE_USER VARCHAR(384),\n" +
                "UPDATE_TIME TIMESTAMP(6),\n" +
                "UPDATE_USER VARCHAR(384),\n" +
                "STATUS CHAR(1)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_DJKSC_SYS_CHANNEL',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_DJKSC_SYS_CHANNEL(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "CHANNEL_NAME,\n" +
                "CHANNEL_CODE,\n" +
                "CHANNEL_KEY,\n" +
                "CHANNEL_IP,\n" +
                "REMARK,\n" +
                "INTERFACE_JSON,\n" +
                "HANDLE_START_TIME,\n" +
                "HANDLE_END_TIME,\n" +
                "RESERVATION_START_TIME,\n" +
                "RESERVATION_END_TIME,\n" +
                "COUNT_DOWN_TIME,\n" +
                "RESTRICT_TIMES,\n" +
                "RESERVATION_INCREASE_RATIO,\n" +
                "BUSINESS_CLASS_PRICE,\n" +
                "HIGH_CLASS_PRICE,\n" +
                "UPGRADE_VOUCHER,\n" +
                "UPGRADE_CARD,\n" +
                "ENABLE,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "STATUS)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "CHANNEL_NAME,\n" +
                "CHANNEL_CODE,\n" +
                "CHANNEL_KEY,\n" +
                "CHANNEL_IP,\n" +
                "REMARK,\n" +
                "INTERFACE_JSON,\n" +
                "HANDLE_START_TIME,\n" +
                "HANDLE_END_TIME,\n" +
                "RESERVATION_START_TIME,\n" +
                "RESERVATION_END_TIME,\n" +
                "COUNT_DOWN_TIME,\n" +
                "RESTRICT_TIMES,\n" +
                "RESERVATION_INCREASE_RATIO,\n" +
                "BUSINESS_CLASS_PRICE,\n" +
                "HIGH_CLASS_PRICE,\n" +
                "UPGRADE_VOUCHER,\n" +
                "UPGRADE_CARD,\n" +
                "ENABLE,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "STATUS\n" +
                " FROM SYS_CHANNEL "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
