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
public class ExtractTOdsLyxLymtmLevelUnit {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key =  Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLyxLymtmLevelUnit");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();

//创建上游数据源表
        tEnv.executeSql("CREATE TABLE LYMTM_LEVEL_UNIT (\n" +
                "ID BIGINT,\n" +
                "MTM_CARD_NUM VARCHAR(30),\n" +
                "LEVEL_CODE VARCHAR(24),\n" +
                "SPE_IDENTIFICATION VARCHAR(240),\n" +
                "GUEST_SCODE VARCHAR(90),\n" +
                "MTM_POST VARCHAR(900),\n" +
                "GUEST_SNAME VARCHAR(600),\n" +
                "GUEST_SADDR VARCHAR(240),\n" +
                "EFFECTIVE_TIME TIMESTAMP(6),\n" +
                "EFFECTIVE_OPERATOR VARCHAR(60),\n" +
                "EXPIRES_TIME TIMESTAMP(6),\n" +
                "EXPIRES_OPERATOR VARCHAR(60),\n" +
                "UPD_REMARK VARCHAR(300),\n" +
                "VERSION BIGINT,\n" +
                "DEL_FLG VARCHAR(6),\n" +
                "ORIG_LEVEL_CODE VARCHAR(12),\n" +
                "CREATE_TIME TIMESTAMP(6),\n" +
                "UPDATE_TIME TIMESTAMP(6) \n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.LYX_IP+":"+Constants.LYX_PORT+"/"+Constants.LYX_DB+"',\n" +
                "    'table-name' = '"+Constants.LYX_SCHEMA+".LYMTM_LEVEL_UNIT', \n" +
                "    'username' = '"+Constants.LYX_USER+"',\n" +
                "    'password' = '"+Constants.LYX_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_LYX_LYMTM_LEVEL_UNIT (\n" +
                "    ETL_DATE DATE,\n" +
                "ID BIGINT,\n" +
                "MTM_CARD_NUM VARCHAR(300),\n" +
                "LEVEL_CODE VARCHAR(24),\n" +
                "SPE_IDENTIFICATION VARCHAR(240),\n" +
                "GUEST_SCODE VARCHAR(90),\n" +
                "MTM_POST VARCHAR(900),\n" +
                "GUEST_SNAME VARCHAR(600),\n" +
                "GUEST_SADDR VARCHAR(240),\n" +
                "EFFECTIVE_TIME TIMESTAMP(6),\n" +
                "EFFECTIVE_OPERATOR VARCHAR(60),\n" +
                "EXPIRES_TIME TIMESTAMP(6),\n" +
                "EXPIRES_OPERATOR VARCHAR(60),\n" +
                "UPD_REMARK VARCHAR(300),\n" +
                "VERSION BIGINT,\n" +
                "DEL_FLG VARCHAR(6),\n" +
                "ORIG_LEVEL_CODE VARCHAR(12),\n" +
                "CREATE_TIME TIMESTAMP(6),\n" +
                "UPDATE_TIME TIMESTAMP(6) \n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYX_LYMTM_LEVEL_UNIT',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_LYX_LYMTM_LEVEL_UNIT(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "MTM_CARD_NUM,\n" +
                "LEVEL_CODE,\n" +
                "SPE_IDENTIFICATION,\n" +
                "GUEST_SCODE,\n" +
                "MTM_POST,\n" +
                "GUEST_SNAME,\n" +
                "GUEST_SADDR,\n" +
                "EFFECTIVE_TIME,\n" +
                "EFFECTIVE_OPERATOR,\n" +
                "EXPIRES_TIME,\n" +
                "EXPIRES_OPERATOR,\n" +
                "UPD_REMARK,\n" +
                "VERSION,\n" +
                "DEL_FLG,\n" +
                "ORIG_LEVEL_CODE,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_TIME)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                " sm4_encrypt (MTM_CARD_NUM,'" + sm4key + "') MTM_CARD_NUM, \n" +
                "LEVEL_CODE,\n" +
                "SPE_IDENTIFICATION,\n" +
                "GUEST_SCODE,\n" +
                "MTM_POST,\n" +
                "GUEST_SNAME,\n" +
                "GUEST_SADDR,\n" +
                "EFFECTIVE_TIME,\n" +
                "EFFECTIVE_OPERATOR,\n" +
                "EXPIRES_TIME,\n" +
                "EXPIRES_OPERATOR,\n" +
                "UPD_REMARK,\n" +
                "VERSION,\n" +
                "DEL_FLG,\n" +
                "ORIG_LEVEL_CODE,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_TIME\n" +
                " FROM LYMTM_LEVEL_UNIT "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
