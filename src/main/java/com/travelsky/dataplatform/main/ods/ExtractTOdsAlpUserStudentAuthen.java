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
 * @author kuangaihua
 * @date 2025/6/26 16:29
 */
public class ExtractTOdsAlpUserStudentAuthen {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsAlpUserStudentAuthen");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);

        //创建上游ORACLE数据源表
        StringBuffer userStudentAuthenSql = new StringBuffer();
        userStudentAuthenSql.append("CREATE TABLE USER_STUDENT_AUTHEN (\n");
        userStudentAuthenSql.append("ID INT, ");
        userStudentAuthenSql.append("OPEN_ID STRING, ");
        userStudentAuthenSql.append("UNION_ID STRING, ");
        userStudentAuthenSql.append("CHANNEL_TYPE TINYINT, ");
        userStudentAuthenSql.append("CERT_NAME STRING, ");
        userStudentAuthenSql.append("CERT_NO STRING, ");
        userStudentAuthenSql.append("LINK_PHONE STRING, ");
        userStudentAuthenSql.append("SCHOOL_ENROLL_DATE DATE, ");
        userStudentAuthenSql.append("SCHOOL_EXPIRE_DATE DATE, ");
        userStudentAuthenSql.append("SCHOOL_NAME STRING, ");
        userStudentAuthenSql.append("DEGREE TINYINT, ");
        userStudentAuthenSql.append("IS_EXAMINE TINYINT, ");
        userStudentAuthenSql.append("IS_STUDENT TINYINT, ");
        userStudentAuthenSql.append("DELETED TINYINT, ");
        userStudentAuthenSql.append("CREATE_TIME TIMESTAMP(6), ");
        userStudentAuthenSql.append("UPDATE_TIME TIMESTAMP(6), ");
        userStudentAuthenSql.append("AUTH_TYPE TINYINT, ");
        userStudentAuthenSql.append("AUTHEN_TYPE TINYINT, ");
        userStudentAuthenSql.append("CUSTOMER_ID STRING");
        userStudentAuthenSql.append(") WITH (\n");
        userStudentAuthenSql.append("    'connector' = 'jdbc',\n");
        userStudentAuthenSql.append("    'url' = 'jdbc:oracle:thin:@//").append(Constants.ALP_IP).append(":").append(Constants.ALP_PORT).append("/").append(Constants.ALP_DB).append("',\n");
        userStudentAuthenSql.append("    'table-name' = '").append(Constants.ALP_SCHEMA).append(".USER_STUDENT_AUTHEN', \n");
        userStudentAuthenSql.append("    'username' = '").append(Constants.ALP_USER).append("',\n");
        userStudentAuthenSql.append("    'password' = '").append(Constants.ALP_PWD).append("'\n");
        userStudentAuthenSql.append(",\n").append("  'driver' = '").append(Constants.ORACLE_DRIVER).append("'");
        userStudentAuthenSql.append(")");
        tEnv.executeSql(userStudentAuthenSql.toString());
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建Doris目标表
        StringBuffer dorisTableSql = new StringBuffer();
        dorisTableSql.append("CREATE TABLE T_ODS_ALP_USER_STUDENT_AUTHEN (\n");
        dorisTableSql.append("ETL_DATE DATE, ");
        dorisTableSql.append("ID INT, ");
        dorisTableSql.append("OPEN_ID STRING, ");
        dorisTableSql.append("UNION_ID STRING, ");
        dorisTableSql.append("CHANNEL_TYPE TINYINT, ");
        dorisTableSql.append("CERT_NAME STRING, ");
        dorisTableSql.append("CERT_NO STRING, ");
        dorisTableSql.append("LINK_PHONE STRING, ");
        dorisTableSql.append("SCHOOL_ENROLL_DATE DATE, ");
        dorisTableSql.append("SCHOOL_EXPIRE_DATE DATE, ");
        dorisTableSql.append("SCHOOL_NAME STRING, ");
        dorisTableSql.append("DEGREE TINYINT, ");
        dorisTableSql.append("IS_EXAMINE TINYINT, ");
        dorisTableSql.append("IS_STUDENT TINYINT, ");
        dorisTableSql.append("DELETED TINYINT, ");
        dorisTableSql.append("CREATE_TIME TIMESTAMP(6), ");
        dorisTableSql.append("UPDATE_TIME TIMESTAMP(6), ");
        dorisTableSql.append("AUTH_TYPE TINYINT, ");
        dorisTableSql.append("AUTHEN_TYPE TINYINT, ");
        dorisTableSql.append("CUSTOMER_ID STRING ");
        dorisTableSql.append(") WITH (\n");
        dorisTableSql.append(" 'connector' = 'doris',\n");
        dorisTableSql.append("'fenodes' = '").append(Constants.DORIS_FE_IP).append(":").append(Constants.DORIS_FE_PORT).append("', -- 替换为 Doris FE 的地址和端口\n");
        dorisTableSql.append("'benodes' = '").append(Constants.DORIS_BE_IP).append(":").append(Constants.DORIS_BE_PORT).append("', -- 替换为 Doris FE 的地址和端口\n");
        dorisTableSql.append(" 'table.identifier' = '").append(Constants.ODS_DB).append(".T_ODS_ALP_USER_STUDENT_AUTHEN',\n");
        dorisTableSql.append("'sink.label-prefix' = '").append(timestamp).append(uuid).append("',");
        dorisTableSql.append(" 'sink.properties.read_json_by_line' = 'true',");
        dorisTableSql.append(" 'sink.properties.format' = 'json',");
        dorisTableSql.append("    'username' = '").append(Constants.ODS_USER).append("',\n");
        dorisTableSql.append("    'password' = '").append(Constants.ODS_PWD).append("'\n");
        dorisTableSql.append(")");
        tEnv.executeSql(dorisTableSql.toString());
        StringBuffer extractSqlBuffer = new StringBuffer();
        extractSqlBuffer.append("INSERT INTO T_ODS_ALP_USER_STUDENT_AUTHEN(");
        extractSqlBuffer.append("ETL_DATE  ");
        extractSqlBuffer.append(",ID\n");
        extractSqlBuffer.append(",OPEN_ID\n");
        extractSqlBuffer.append(",UNION_ID\n");
        extractSqlBuffer.append(",CHANNEL_TYPE\n");
        extractSqlBuffer.append(",CERT_NAME\n");
        extractSqlBuffer.append(",CERT_NO\n");
        extractSqlBuffer.append(",LINK_PHONE\n");
        extractSqlBuffer.append(",SCHOOL_ENROLL_DATE\n");
        extractSqlBuffer.append(",SCHOOL_EXPIRE_DATE\n");
        extractSqlBuffer.append(",SCHOOL_NAME\n");
        extractSqlBuffer.append(",DEGREE\n");
        extractSqlBuffer.append(",IS_EXAMINE\n");
        extractSqlBuffer.append(",IS_STUDENT\n");
        extractSqlBuffer.append(",DELETED\n");
        extractSqlBuffer.append(",CREATE_TIME\n");
        extractSqlBuffer.append(",UPDATE_TIME\n");
//                        extractSqlBuffer.append(",ETL_CREATE_TIME\n");
//                        extractSqlBuffer.append(",ETL_UPDATE_TIME\n");
        extractSqlBuffer.append(",AUTH_TYPE\n");
        extractSqlBuffer.append(",AUTHEN_TYPE\n");
        extractSqlBuffer.append(",CUSTOMER_ID         \n)  ");
        extractSqlBuffer.append("SELECT CAST('").append(etlDate).append("' AS DATE) ETL_DATE");
        extractSqlBuffer.append(",ID\n");
        extractSqlBuffer.append(",OPEN_ID\n");
        extractSqlBuffer.append(",UNION_ID\n");
        extractSqlBuffer.append(",CHANNEL_TYPE\n");
        extractSqlBuffer.append(",CERT_NAME\n");
        extractSqlBuffer.append(",sm4_encrypt(aes_decrypt(CERT_NO, '").append(Constants.ALP_AES_KEY).append("'), '").append(Constants.SM4_KEY).append("') CERT_NO\n");
        extractSqlBuffer.append(",sm4_encrypt(aes_decrypt(LINK_PHONE, '").append(Constants.ALP_AES_KEY).append("'), '").append(Constants.SM4_KEY).append("') LINK_PHONE\n");
        extractSqlBuffer.append(",SCHOOL_ENROLL_DATE\n");
        extractSqlBuffer.append(",SCHOOL_EXPIRE_DATE\n");
        extractSqlBuffer.append(",SCHOOL_NAME\n");
        extractSqlBuffer.append(",DEGREE\n");
        extractSqlBuffer.append(",IS_EXAMINE\n");
        extractSqlBuffer.append(",IS_STUDENT\n");
        extractSqlBuffer.append(",DELETED\n");
        extractSqlBuffer.append(",CREATE_TIME\n");
        extractSqlBuffer.append(",UPDATE_TIME\n");
//                        extractSqlBuffer.append(",CURRENT_TIMESTAMP(3)\n");
//                        extractSqlBuffer.append(",CURRENT_TIMESTAMP(3)\n");
        extractSqlBuffer.append(",AUTH_TYPE\n");
        extractSqlBuffer.append(",AUTHEN_TYPE\n");
        extractSqlBuffer.append(",CUSTOMER_ID\n");
        extractSqlBuffer.append(" FROM USER_STUDENT_AUTHEN ");
        extractSqlBuffer.append(" WHERE  CREATE_TIME BETWEEN TIMESTAMP '").append(etlDate).append(" 00:00:00' AND TIMESTAMP '").append(etlDate).append(" 23:59:59.999' ");
        extractSqlBuffer.append(" OR  UPDATE_TIME BETWEEN TIMESTAMP '").append(etlDate).append(" 00:00:00' AND TIMESTAMP '").append(etlDate).append(" 23:59:59.999' ");
        String extractSql = extractSqlBuffer.toString();
        TableResult result = tEnv.executeSql(extractSql);

        result.print();


    }

}
