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
public class ExtractTOdsZxyhCrmCustomerFamily {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZxyhCrmCustomerFamily");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE CRM_CUSTOMER_FAMILY (\n" +
                "ID VARCHAR(96) ,\n" +
                "CUSTOMER_ID VARCHAR(96) ,\n" +
                "RELATION VARCHAR(96) ,\n" +
                "MOBILE VARCHAR(96) ,\n" +
                "FIRST_CN_NAME VARCHAR(192) ,\n" +
                "LAST_CN_NAME VARCHAR(192) ,\n" +
                "CERT_TYPE VARCHAR(6) ,\n" +
                "CERT_NUMBER VARCHAR(192) ,\n" +
                "REAL_NAME_FLAG CHAR(1) ,\n" +
                "SEX CHAR(1) ,\n" +
                "PER_TYPE VARCHAR(9) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_USER VARCHAR(384) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(384) ,\n" +
                "FAMILY_CUSTOMER_ID VARCHAR(192) ,\n" +
                "CN_NAME VARCHAR(192) ,\n" +
                "ENG_NAME VARCHAR(192) ,\n" +
                "FIRST_ENG_NAME VARCHAR(192) ,\n" +
                "LAST_ENG_NAME VARCHAR(192) ,\n" +
                "BIRTHDAY VARCHAR(192) ,\n" +
                "`NATIONAL` VARCHAR(300) ,\n" +
                "ENABLE CHAR(1) ,\n" +
                "EMAIL VARCHAR(765) ,\n" +
                "AUDIT_STATUS CHAR(10) ,\n" +
                "AUDIT_REMARK VARCHAR(12000) \n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.ZXYH_IP+":"+Constants.ZXYH_PORT+"/"+Constants.ZXYH_DB+"',\n" +
                "    'table-name' = '"+Constants.ZXYH_SCHEMA+".CRM_CUSTOMER_FAMILY', \n" +
                "    'username' = '"+Constants.ZXYH_USER+"',\n" +
                "    'password' = '"+Constants.ZXYH_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_ZXYH_CRM_CUSTOMER_FAMILY (\n" +
                "    ETL_DATE DATE,\n" +
                "ID VARCHAR(96) ,\n" +
                "CUSTOMER_ID VARCHAR(96) ,\n" +
                "RELATION VARCHAR(96) ,\n" +
                "MOBILE VARCHAR(768) ,\n" +
                "FIRST_CN_NAME VARCHAR(192) ,\n" +
                "LAST_CN_NAME VARCHAR(192) ,\n" +
                "CERT_TYPE VARCHAR(6) ,\n" +
                "CERT_NUMBER VARCHAR(768) ,\n" +
                "REAL_NAME_FLAG CHAR(1) ,\n" +
                "SEX CHAR(1) ,\n" +
                "PER_TYPE VARCHAR(9) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_USER VARCHAR(384) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(384) ,\n" +
                "FAMILY_CUSTOMER_ID VARCHAR(192) ,\n" +
                "CN_NAME VARCHAR(192) ,\n" +
                "ENG_NAME VARCHAR(192) ,\n" +
                "FIRST_ENG_NAME VARCHAR(192) ,\n" +
                "LAST_ENG_NAME VARCHAR(192) ,\n" +
                "BIRTHDAY VARCHAR(192) ,\n" +
                "`NATIONAL` VARCHAR(300) ,\n" +
                "ENABLE CHAR(1) ,\n" +
                "EMAIL VARCHAR(765) ,\n" +
                "AUDIT_STATUS CHAR(10) ,\n" +
                "AUDIT_REMARK VARCHAR(12000) \n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZXYH_CRM_CUSTOMER_FAMILY',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_ZXYH_CRM_CUSTOMER_FAMILY(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "CUSTOMER_ID,\n" +
                "RELATION,\n" +
                "MOBILE,\n" +
                "FIRST_CN_NAME,\n" +
                "LAST_CN_NAME,\n" +
                "CERT_TYPE,\n" +
                "CERT_NUMBER,\n" +
                "REAL_NAME_FLAG,\n" +
                "SEX,\n" +
                "PER_TYPE,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER,\n" +
                "FAMILY_CUSTOMER_ID,\n" +
                "CN_NAME,\n" +
                "ENG_NAME,\n" +
                "FIRST_ENG_NAME,\n" +
                "LAST_ENG_NAME,\n" +
                "BIRTHDAY,\n" +
                "`NATIONAL`,\n" +
                "ENABLE,\n" +
                "EMAIL,\n" +
                "AUDIT_STATUS,\n" +
                "AUDIT_REMARK)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "CUSTOMER_ID,\n" +
                "RELATION,\n" +
                "  sm4_encrypt(aes_decrypt(MOBILE, '" + Constants.ZXYH_AES_KEY + "'), '" + sm4key + "') MOBILE,\n" +
                "FIRST_CN_NAME,\n" +
                "LAST_CN_NAME,\n" +
                "CERT_TYPE,\n" +
                "  sm4_encrypt(aes_decrypt(CERT_NUMBER, '" + Constants.ZXYH_AES_KEY + "'), '" + sm4key + "') CERT_NUMBER,\n" +
                "REAL_NAME_FLAG,\n" +
                "SEX,\n" +
                "PER_TYPE,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER,\n" +
                "FAMILY_CUSTOMER_ID,\n" +
                "CN_NAME,\n" +
                "ENG_NAME,\n" +
                "FIRST_ENG_NAME,\n" +
                "LAST_ENG_NAME,\n" +
                "BIRTHDAY,\n" +
                "`NATIONAL`,\n" +
                "ENABLE,\n" +
                "EMAIL,\n" +
                "AUDIT_STATUS,\n" +
                "AUDIT_REMARK\n" +
                " FROM CRM_CUSTOMER_FAMILY "
                        + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                        +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
