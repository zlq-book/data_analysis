package com.travelsky.dataplatform.main.ods.v2.Lygj;

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
 * * 第一次全取，后续每天更新导入INPUT_TIME是前一天的，以及UPDATE_TIME是前一天的数据
 */
public class ExtractTOdsLygjServiceType {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String startDate = etlDate;
        String endDate = etlDate;
        if (args.length > 2) {
            startDate = args[1];
            endDate = args[2];
        }
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLygjComplaintInfoActiviti");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE SERVICE_TYPE (\n" +
                "    ID  BIGINT,\n" +
                "    NAME            varchar(300),\n" +
                "    VALID           BIGINT ,\n" +
                "    INPUT_PEOPLE    varchar(90) ,\n" +
                "    INPUT_TIME      TIMESTAMP(6) ,\n" +
                "    UPDATE_PEOPLE   varchar(90) ,\n" +
                "    UPDATE_TIME     TIMESTAMP(6)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.LYGJ_IP + ":" + Constants.LYGJ_PORT + "/" + Constants.LYGJ_DB + "',\n" +
                "    'table-name' = '" + Constants.LYGJ_SCHEMA + ".SERVICE_TYPE', \n" +
                "    'username' = '" + Constants.LYGJ_USER + "',\n" +
                "    'password' = '" + Constants.LYGJ_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_LYGJ_SERVICE_TYPE (\n" +
                "    ID  BIGINT,\n" +
                "    NAME            varchar(300),\n" +
                "    VALID           BIGINT ,\n" +
                "    INPUT_PEOPLE    varchar(90) ,\n" +
                "    INPUT_TIME      TIMESTAMP(6) ,\n" +
                "    UPDATE_PEOPLE   varchar(90) ,\n" +
                "    UPDATE_TIME     TIMESTAMP(6)," +
                "    ETL_DATE DATE" +
                //") WITH (\n" +
                //"    'connector' = 'jdbc',\n" +
                //"    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
                //"    'table-name' = 'T_ODS_LYGJ_SERVICE_TYPE', -- 替换为实际的表名\n" +
                //"    'username' = '" + Constants.ODS_USER + "',\n" +
                //"    'password' = '" + Constants.ODS_PWD + "'\n" +
                //",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
                //")");
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "  'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "',\n" +
                "  'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "',\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYGJ_SERVICE_TYPE',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        String extractSql = "INSERT INTO T_ODS_LYGJ_SERVICE_TYPE (" +
                "    ETL_DATE,\n" +
                "    ID,\n" +
                "    NAME,\n" +
                "    VALID ,\n" +
                "    INPUT_PEOPLE ,\n" +
                "    INPUT_TIME ,\n" +
                "    UPDATE_PEOPLE ,\n" +
                "    UPDATE_TIME)  " +
                "    SELECT \n" +
                "    CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "    ID,\n" +
                "    NAME,\n" +
                "    VALID ,\n" +
                "    INPUT_PEOPLE ,\n" +
                "    INPUT_TIME ,\n" +
                "    UPDATE_PEOPLE ,\n" +
                "    UPDATE_TIME " +
                " FROM SERVICE_TYPE "
                + " WHERE  INPUT_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;
        TableResult result = tEnv.executeSql(extractSql);

        result.print();


    }
}
