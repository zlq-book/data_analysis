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
public class ExtractTOdsZxyhCrmCusPassengerCertInfo {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZxyhCrmCusPassengerCertInfo");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();


//创建上游数据源表
        tEnv.executeSql("CREATE TABLE CRM_CUS_PASSENGER_CERT_INFO (\n" +
                "ID VARCHAR(96) ,\n" +
                "PASSENGER_ID VARCHAR(96) ,\n" +
                "REAL_NAME_FLAG CHAR(1) ,\n" +
                "CERT_TYPE VARCHAR(6) ,\n" +
                "CERT_NUMBER VARCHAR(600) ,\n" +
                "CERT_DESC VARCHAR(1500) ,\n" +
                "ISSUE_DATE TIMESTAMP(6) ,\n" +
                "EXPIRATION_DATE TIMESTAMP(6) ,\n" +
                "SIGNING_AUTHORITY VARCHAR(600) ,\n" +
                "ISSUE_AGENCY VARCHAR(600) ,\n" +
                "COMMENTS VARCHAR(1500) ,\n" +
                "ENABLE CHAR(1) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(96) ,\n" +
                "UPDATE_USER VARCHAR(96)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.ZXYH_IP+":"+Constants.ZXYH_PORT+"/"+Constants.ZXYH_DB+"',\n" +
                "    'table-name' = '"+Constants.ZXYH_SCHEMA+".CRM_CUS_PASSENGER_CERT_INFO', \n" +
                "    'username' = '"+Constants.ZXYH_USER+"',\n" +
                "    'password' = '"+Constants.ZXYH_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_ZXYH_CRM_CUS_PASSENGER_CERT_INFO (\n" +
                "    ETL_DATE DATE,\n" +
                "ID VARCHAR(96) ,\n" +
                "PASSENGER_ID VARCHAR(96) ,\n" +
                "REAL_NAME_FLAG CHAR(1) ,\n" +
                "CERT_TYPE VARCHAR(6) ,\n" +
                "CERT_NUMBER VARCHAR(768) ,\n" +
                "CERT_DESC VARCHAR(1500) ,\n" +
                "ISSUE_DATE TIMESTAMP(6) ,\n" +
                "EXPIRATION_DATE TIMESTAMP(6) ,\n" +
                "SIGNING_AUTHORITY VARCHAR(600) ,\n" +
                "ISSUE_AGENCY VARCHAR(600) ,\n" +
                "COMMENTS VARCHAR(1500) ,\n" +
                "ENABLE CHAR(1) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(96) ,\n" +
                "UPDATE_USER VARCHAR(96)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZXYH_CRM_CUS_PASSENGER_CERT_INFO',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_ZXYH_CRM_CUS_PASSENGER_CERT_INFO(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "PASSENGER_ID,\n" +
                "REAL_NAME_FLAG,\n" +
                "CERT_TYPE,\n" +
                "CERT_NUMBER,\n" +
                "CERT_DESC,\n" +
                "ISSUE_DATE,\n" +
                "EXPIRATION_DATE,\n" +
                "SIGNING_AUTHORITY,\n" +
                "ISSUE_AGENCY,\n" +
                "COMMENTS,\n" +
                "ENABLE,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_USER)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "PASSENGER_ID,\n" +
                "REAL_NAME_FLAG,\n" +
                "CERT_TYPE,\n" +
                "  sm4_encrypt(aes_decrypt(CERT_NUMBER, '" + Constants.ZXYH_AES_KEY + "'), '" + sm4key + "') CERT_NUMBER,\n" +
                "CERT_DESC,\n" +
                "ISSUE_DATE,\n" +
                "EXPIRATION_DATE,\n" +
                "SIGNING_AUTHORITY,\n" +
                "ISSUE_AGENCY,\n" +
                "COMMENTS,\n" +
                "ENABLE,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_USER\n" +
                " FROM CRM_CUS_PASSENGER_CERT_INFO "
                        + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                        +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
