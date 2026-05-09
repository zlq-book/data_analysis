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
public class ExtractTOdsDjkscPaymentInfo {
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
        System.out.println("ExtractTOdsDjkscPaymentInfo etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsDjkscPaymentInfo");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE PAYMENT_INFO (\n" +
                "PAYMENT_ID VARCHAR(384),\n" +
                "ORDER_NO VARCHAR(384),\n" +
                "BILL_NO VARCHAR(90),\n" +
                "BUS_NO VARCHAR(90),\n" +
                "PAY_STATE VARCHAR(30),\n" +
                "PAY_TIME TIMESTAMP(6),\n" +
                "RETURN_TIME TIMESTAMP(6),\n" +
                "PAY_WAY VARCHAR(120),\n" +
                "PAY_TYPE VARCHAR(30),\n" +
                "BANK_TYPE VARCHAR(1500),\n" +
                "BANK_ID VARCHAR(90),\n" +
                "PAY_MSG VARCHAR(300),\n" +
                "PAY_AMOUNT DECIMAL(24,2),\n" +
                "RF_NO VARCHAR(150),\n" +
                "REFUND_STATE VARCHAR(90),\n" +
                "CHANNEL_CODE VARCHAR(384),\n" +
                "BANK_ORDER_NO VARCHAR(384),\n" +
                "ENABLE CHAR(1),\n" +
                "CREATE_TIME TIMESTAMP(6),\n" +
                "CREATE_USER VARCHAR(384),\n" +
                "UPDATE_TIME TIMESTAMP(6),\n" +
                "UPDATE_USER VARCHAR(384),\n" +
                "REFUND_TIME TIMESTAMP(6),\n" +
                "OUT_RF_NO VARCHAR(384)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.DJKSC_IP+":"+Constants.DJKSC_PORT+"/"+Constants.DJKSC_DB+"',\n" +
                "    'table-name' = '"+Constants.DJKSC_SCHEMA+".PAYMENT_INFO', \n" +
                "    'username' = '"+Constants.DJKSC_USER+"',\n" +
                "    'password' = '"+Constants.DJKSC_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_DJKSC_PAYMENT_INFO (\n" +
                "    ETL_DATE DATE,\n" +
                "PAYMENT_ID VARCHAR(384),\n" +
                "ORDER_NO VARCHAR(384),\n" +
                "BILL_NO VARCHAR(90),\n" +
                "BUS_NO VARCHAR(90),\n" +
                "PAY_STATE VARCHAR(30),\n" +
                "PAY_TIME TIMESTAMP(6),\n" +
                "RETURN_TIME TIMESTAMP(6),\n" +
                "PAY_WAY VARCHAR(120),\n" +
                "PAY_TYPE VARCHAR(30),\n" +
                "BANK_TYPE VARCHAR(1500),\n" +
                "BANK_ID VARCHAR(90),\n" +
                "PAY_MSG VARCHAR(300),\n" +
                "PAY_AMOUNT DECIMAL(24,2),\n" +
                "RF_NO VARCHAR(150),\n" +
                "REFUND_STATE VARCHAR(90),\n" +
                "CHANNEL_CODE VARCHAR(384),\n" +
                "BANK_ORDER_NO VARCHAR(384),\n" +
                "ENABLE CHAR(1),\n" +
                "CREATE_TIME TIMESTAMP(6),\n" +
                "CREATE_USER VARCHAR(384),\n" +
                "UPDATE_TIME TIMESTAMP(6),\n" +
                "UPDATE_USER VARCHAR(384),\n" +
                "REFUND_TIME TIMESTAMP(6),\n" +
                "OUT_RF_NO VARCHAR(384)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_DJKSC_PAYMENT_INFO',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_DJKSC_PAYMENT_INFO(" +
                "ETL_DATE,\n" +
                "PAYMENT_ID,\n" +
                "ORDER_NO,\n" +
                "BILL_NO,\n" +
                "BUS_NO,\n" +
                "PAY_STATE,\n" +
                "PAY_TIME,\n" +
                "RETURN_TIME,\n" +
                "PAY_WAY,\n" +
                "PAY_TYPE,\n" +
                "BANK_TYPE,\n" +
                "BANK_ID,\n" +
                "PAY_MSG,\n" +
                "PAY_AMOUNT,\n" +
                "RF_NO,\n" +
                "REFUND_STATE,\n" +
                "CHANNEL_CODE,\n" +
                "BANK_ORDER_NO,\n" +
                "ENABLE,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "REFUND_TIME,\n" +
                "OUT_RF_NO)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "PAYMENT_ID,\n" +
                "ORDER_NO,\n" +
                "BILL_NO,\n" +
                "BUS_NO,\n" +
                "PAY_STATE,\n" +
                "PAY_TIME,\n" +
                "RETURN_TIME,\n" +
                "PAY_WAY,\n" +
                "PAY_TYPE,\n" +
                "BANK_TYPE,\n" +
                "BANK_ID,\n" +
                "PAY_MSG,\n" +
                "PAY_AMOUNT,\n" +
                "RF_NO,\n" +
                "REFUND_STATE,\n" +
                "CHANNEL_CODE,\n" +
                "BANK_ORDER_NO,\n" +
                "ENABLE,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "REFUND_TIME,\n" +
                "OUT_RF_NO\n" +
                " FROM PAYMENT_INFO "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
