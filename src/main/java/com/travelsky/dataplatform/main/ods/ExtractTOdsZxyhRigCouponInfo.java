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
public class ExtractTOdsZxyhRigCouponInfo {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZxyhRigCouponInfo");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE RIG_COUPON_INFO (\n" +
                "ID VARCHAR(96) ,\n" +
                "COUPON_CONFIG_ID VARCHAR(96) ,\n" +
                "COUPON_STATUS VARCHAR(96) ,\n" +
                "BINDING_CUSTOMER_ID VARCHAR(300) ,\n" +
                "BINDING_DATE TIMESTAMP(6) ,\n" +
                "EXPIRATION_DATE TIMESTAMP(6) ,\n" +
                "CUS_NAME VARCHAR(300) ,\n" +
                "CUS_CARD_NO VARCHAR(300) ,\n" +
                "CUS_PHONE VARCHAR(300) ,\n" +
                "ENABLE CHAR(10) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_USER VARCHAR(384) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(384) ,\n" +
                "GROUP_ID VARCHAR(96) ,\n" +
                "COUPON_NO VARCHAR(96)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.ZXYH_IP+":"+Constants.ZXYH_PORT+"/"+Constants.ZXYH_DB+"',\n" +
                "    'table-name' = '"+Constants.ZXYH_SCHEMA+".RIG_COUPON_INFO', \n" +
                "    'username' = '"+Constants.ZXYH_USER+"',\n" +
                "    'password' = '"+Constants.ZXYH_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_ZXYH_RIG_COUPON_INFO (\n" +
                "    ETL_DATE DATE,\n" +
                "ID VARCHAR(96) ,\n" +
                "COUPON_CONFIG_ID VARCHAR(96) ,\n" +
                "COUPON_STATUS VARCHAR(96) ,\n" +
                "BINDING_CUSTOMER_ID VARCHAR(300) ,\n" +
                "BINDING_DATE TIMESTAMP(6) ,\n" +
                "EXPIRATION_DATE TIMESTAMP(6) ,\n" +
                "CUS_NAME VARCHAR(300) ,\n" +
                "CUS_CARD_NO VARCHAR(768) ,\n" +
                "CUS_PHONE VARCHAR(768) ,\n" +
                "ENABLE CHAR(10) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_USER VARCHAR(384) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(384) ,\n" +
                "GROUP_ID VARCHAR(96) ,\n" +
                "COUPON_NO VARCHAR(96)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZXYH_RIG_COUPON_INFO',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_ZXYH_RIG_COUPON_INFO(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "COUPON_CONFIG_ID,\n" +
                "COUPON_STATUS,\n" +
                "BINDING_CUSTOMER_ID,\n" +
                "BINDING_DATE,\n" +
                "EXPIRATION_DATE,\n" +
                "CUS_NAME,\n" +
                "CUS_CARD_NO,\n" +
                "CUS_PHONE,\n" +
                "ENABLE,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER,\n" +
                "GROUP_ID,\n" +
                "COUPON_NO)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "COUPON_CONFIG_ID,\n" +
                "COUPON_STATUS,\n" +
                "BINDING_CUSTOMER_ID,\n" +
                "BINDING_DATE,\n" +
                "EXPIRATION_DATE,\n" +
                "CUS_NAME,\n" +
                "  sm4_encrypt(aes_decrypt(CUS_CARD_NO, '" + Constants.ZXYH_AES_KEY + "'), '" + sm4key + "') CUS_CARD_NO,\n" +
                "  sm4_encrypt(aes_decrypt(CUS_PHONE, '" + Constants.ZXYH_AES_KEY + "'), '" + sm4key + "') CUS_PHONE,\n" +
                "ENABLE,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER,\n" +
                "GROUP_ID,\n" +
                "COUPON_NO\n" +
                " FROM RIG_COUPON_INFO "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
