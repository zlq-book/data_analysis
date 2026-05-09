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
public class ExtractTOdsDjkscOrderInfo {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
//        String etlDate = "2024-03-03";
        String startDate = etlDate;
        String endDate = etlDate;
        if (args.length > 2) {
            startDate = args[1];
            endDate = args[2];
        }
        System.out.println("ExtractTOdsDjkscOrderInfo etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsDjkscOrderInfo");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE ORDER_INFO (\n" +
                "ID VARCHAR(384),\n" +
                "ORDER_NO VARCHAR(96),\n" +
                "USER_ID VARCHAR(96),\n" +
                "ORDER_STATE VARCHAR(15),\n" +
                "PAY_STATE VARCHAR(15),\n" +
                "SEAT_NO VARCHAR(30),\n" +
                "AMOUNT DECIMAL(24,2),\n" +
                "CLASS_CODE VARCHAR(30),\n" +
                "EMD_NO VARCHAR(90),\n" +
                "CONTRACT_NAME VARCHAR(150),\n" +
                "CONTRACT_PHONE VARCHAR(9000),\n" +
                "CREATE_TIME TIMESTAMP(6),\n" +
                "PSR_NAME VARCHAR(90),\n" +
                "CARD_NO VARCHAR(12000),\n" +
                "ADDRESS VARCHAR(1500),\n" +
                "MODIFY_TIME TIMESTAMP(6),\n" +
                "CHANNEL_CODE VARCHAR(384),\n" +
                "INVOICE_STATUS CHAR(1),\n" +
                "REFERRER VARCHAR(384),\n" +
                "HIGH_CLASS_REWARD DECIMAL(24,0),\n" +
                "BUSINESS_CLASS_REWARD DECIMAL(24,0),\n" +
                "BUSINESS_CLASS_VOUCHER__REWARD DECIMAL(24,0),\n" +
                "BUSINESS_CLASS_CARD_REWARD DECIMAL(24,0),\n" +
                "REFUND_REMARK VARCHAR(12000),\n" +
                "AUDIT_STATUS CHAR(1),\n" +
                "ENABLE CHAR(1),\n" +
                "CREATE_USER VARCHAR(768),\n" +
                "UPDATE_TIME TIMESTAMP(6),\n" +
                "UPDATE_USER VARCHAR(768),\n" +
                "UPGRADE_TYPE VARCHAR(765),\n" +
                "UP_ORDER_NO VARCHAR(384),\n" +
                "REMARK VARCHAR(12000),\n" +
                "`SOURCE` VARCHAR(300),\n" +
                "REFUND_USER VARCHAR(768),\n" +
                "ORDER_ATTACHMENT_URL VARCHAR(6000)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.DJKSC_IP+":"+Constants.DJKSC_PORT+"/"+Constants.DJKSC_DB+"',\n" +
                "    'table-name' = '"+Constants.DJKSC_SCHEMA+".ORDER_INFO', \n" +
                "    'username' = '"+Constants.DJKSC_USER+"',\n" +
                "    'password' = '"+Constants.DJKSC_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_DJKSC_ORDER_INFO (\n" +
                "    ETL_DATE DATE,\n" +
                "ID VARCHAR(384),\n" +
                "ORDER_NO VARCHAR(96),\n" +
                "USER_ID VARCHAR(96),\n" +
                "ORDER_STATE VARCHAR(15),\n" +
                "PAY_STATE VARCHAR(15),\n" +
                "SEAT_NO VARCHAR(30),\n" +
                "AMOUNT DECIMAL(24,2),\n" +
                "CLASS_CODE VARCHAR(30),\n" +
                "EMD_NO VARCHAR(90),\n" +
                "CONTRACT_NAME VARCHAR(150),\n" +
                "CONTRACT_PHONE VARCHAR(9000),\n" +
                "CREATE_TIME TIMESTAMP(6),\n" +
                "PSR_NAME VARCHAR(90),\n" +
                "CARD_NO VARCHAR(12000),\n" +
                "ADDRESS VARCHAR(1500),\n" +
                "MODIFY_TIME TIMESTAMP(6),\n" +
                "CHANNEL_CODE VARCHAR(384),\n" +
                "INVOICE_STATUS CHAR(1),\n" +
                "REFERRER VARCHAR(384),\n" +
                "HIGH_CLASS_REWARD DECIMAL(24,0),\n" +
                "BUSINESS_CLASS_REWARD DECIMAL(24,0),\n" +
                "BUSINESS_CLASS_VOUCHER__REWARD DECIMAL(24,0),\n" +
                "BUSINESS_CLASS_CARD_REWARD DECIMAL(24,0),\n" +
                "REFUND_REMARK VARCHAR(12000),\n" +
                "AUDIT_STATUS CHAR(1),\n" +
                "ENABLE CHAR(1),\n" +
                "CREATE_USER VARCHAR(768),\n" +
                "UPDATE_TIME TIMESTAMP(6),\n" +
                "UPDATE_USER VARCHAR(768),\n" +
                "UPGRADE_TYPE VARCHAR(765),\n" +
                "UP_ORDER_NO VARCHAR(384),\n" +
                "REMARK VARCHAR(12000),\n" +
                "`SOURCE` VARCHAR(300),\n" +
                "REFUND_USER VARCHAR(768),\n" +
                "ORDER_ATTACHMENT_URL VARCHAR(6000)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_DJKSC_ORDER_INFO',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_DJKSC_ORDER_INFO(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "ORDER_NO,\n" +
                "USER_ID,\n" +
                "ORDER_STATE,\n" +
                "PAY_STATE,\n" +
                "SEAT_NO,\n" +
                "AMOUNT,\n" +
                "CLASS_CODE,\n" +
                "EMD_NO,\n" +
                "CONTRACT_NAME,\n" +
                "CONTRACT_PHONE,\n" +
                "CREATE_TIME,\n" +
                "PSR_NAME,\n" +
                "CARD_NO,\n" +
                "ADDRESS,\n" +
                "MODIFY_TIME,\n" +
                "CHANNEL_CODE,\n" +
                "INVOICE_STATUS,\n" +
                "REFERRER,\n" +
                "HIGH_CLASS_REWARD,\n" +
                "BUSINESS_CLASS_REWARD,\n" +
                "BUSINESS_CLASS_VOUCHER__REWARD,\n" +
                "BUSINESS_CLASS_CARD_REWARD,\n" +
                "REFUND_REMARK,\n" +
                "AUDIT_STATUS,\n" +
                "ENABLE,\n" +
                "CREATE_USER,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "UPGRADE_TYPE,\n" +
                "UP_ORDER_NO,\n" +
                "REMARK,\n" +
                "`SOURCE`,\n" +
                "REFUND_USER,\n" +
                "ORDER_ATTACHMENT_URL)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "ORDER_NO,\n" +
                "USER_ID,\n" +
                "ORDER_STATE,\n" +
                "PAY_STATE,\n" +
                "SEAT_NO,\n" +
                "AMOUNT,\n" +
                "CLASS_CODE,\n" +
                "EMD_NO,\n" +
                "CONTRACT_NAME,\n" +
                "  sm4_encrypt(aes_decrypt(CONTRACT_PHONE, '" + Constants.DJKSC_AES_KEY + "'), '" + sm4key + "') CONTRACT_PHONE,\n" +
                "CREATE_TIME,\n" +
                "PSR_NAME,\n" +
                "  sm4_encrypt(aes_decrypt(CARD_NO, '" + Constants.DJKSC_AES_KEY + "'), '" + sm4key + "') CARD_NO,\n" +
                "ADDRESS,\n" +
                "MODIFY_TIME,\n" +
                "CHANNEL_CODE,\n" +
                "INVOICE_STATUS,\n" +
                "REFERRER,\n" +
                "HIGH_CLASS_REWARD,\n" +
                "BUSINESS_CLASS_REWARD,\n" +
                "BUSINESS_CLASS_VOUCHER__REWARD,\n" +
                "BUSINESS_CLASS_CARD_REWARD,\n" +
                "REFUND_REMARK,\n" +
                "AUDIT_STATUS,\n" +
                "ENABLE,\n" +
                "CREATE_USER,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "UPGRADE_TYPE,\n" +
                "UP_ORDER_NO,\n" +
                "REMARK,\n" +
                "`SOURCE`,\n" +
                "REFUND_USER,\n" +
                "ORDER_ATTACHMENT_URL\n" +
                " FROM ORDER_INFO "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
