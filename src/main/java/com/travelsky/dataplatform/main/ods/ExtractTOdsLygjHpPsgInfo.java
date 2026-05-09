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
public class ExtractTOdsLygjHpPsgInfo {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLygjHpPsgInfo");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE HP_PSG_INFO (\n" +

                        "    ID_NUM VARCHAR(12000) COMMENT '身份信息',\n" +
                        "    EX_DATE TIMESTAMP(6) COMMENT '过期日期',\n" +
                        "    FLT_DATE TIMESTAMP(6) COMMENT '航班日期',\n" +
                        "    ORIG VARCHAR(30) COMMENT '始发地'" +

                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.LYGJ_IP + ":" + Constants.LYGJ_PORT + "/" + Constants.LYGJ_DB + "',\n" +
                "    'table-name' = '" + Constants.LYGJ_SCHEMA + ".HP_PSG_INFO', \n" +
                "    'username' = '" + Constants.LYGJ_USER + "',\n" +
                "    'password' = '" + Constants.LYGJ_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_LYGJ_HP_PSG_INFO (\n" +
                "    ETL_DATE DATE NOT NULL  COMMENT '数据ETL日期',\n" +
                "    ID_NUM VARCHAR(12000) COMMENT '身份信息',\n" +
                "    EX_DATE TIMESTAMP(6) COMMENT '过期日期',\n" +
                "    FLT_DATE DATE COMMENT '航班日期',\n" +
                "    ORIG VARCHAR(30) COMMENT '始发地'" +
                //") WITH (\n" +
                //"    'connector' = 'jdbc',\n" +
                //"    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
                //"    'table-name' = 'T_ODS_LYGJ_HP_PSG_INFO', -- 替换为实际的表名\n" +
                //"    'username' = '"+Constants.ODS_USER+"',\n" +
                //"    'password' = '"+Constants.ODS_PWD+"'\n" +
                //",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
                //")");
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYGJ_HP_PSG_INFO',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        String extractSql = "INSERT INTO T_ODS_LYGJ_HP_PSG_INFO (" +
                "ETL_DATE\n" +
                ",ID_NUM\n" +
                ",EX_DATE\n" +
                ",FLT_DATE\n" +
                ",ORIG\n)  " +
                "SELECT \n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE" +
                ",sm4_encrypt(aes_decrypt(ID_NUM, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') ID_NUM \n" +
                ",EX_DATE\n" +
                ",FLT_DATE\n" +
                ",ORIG " +
                " FROM HP_PSG_INFO "
                + " WHERE  EX_DATE BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }

}
