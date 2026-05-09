package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
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
public class ExtractTOdsZxyhRigGoldOrder {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZxyhRigGoldOrder");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE RIG_GOLD_ORDER (\n" +
                "ID VARCHAR(96) ,\n" +
                "ORDER_NO VARCHAR(96) ,\n" +
                "MAIN_ORDER_ID VARCHAR(96) ,\n" +
                "MAIN_ORDER_NO VARCHAR(96) ,\n" +
                "CUSTOMER_ID VARCHAR(600) ,\n" +
                "ORDER_STATUS CHAR(10) ,\n" +
                "ORDER_TYPE CHAR(10) ,\n" +
                "PRODUCT_ID VARCHAR(96) ,\n" +
                "PRODUCT_NAME VARCHAR(96) ,\n" +
                "PRODUCT_PIC_URL VARCHAR(600) ,\n" +
                "PRODUCT_NUM BIGINT ,\n" +
                "BALANCE BIGINT ,\n" +
                "ENABLE CHAR(10) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_USER VARCHAR(384) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(384) ,\n" +
                "PRODUCT_TYPE VARCHAR(600) ,\n" +
                "PHONE VARCHAR(600)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.ZXYH_IP+":"+Constants.ZXYH_PORT+"/"+Constants.ZXYH_DB+"',\n" +
                "    'table-name' = '"+Constants.ZXYH_SCHEMA+".RIG_GOLD_ORDER', \n" +
                "    'username' = '"+Constants.ZXYH_USER+"',\n" +
                "    'password' = '"+Constants.ZXYH_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_ZXYH_RIG_GOLD_ORDER (\n" +
                "    ETL_DATE DATE,\n" +
                "ID VARCHAR(96) ,\n" +
                "ORDER_NO VARCHAR(96) ,\n" +
                "MAIN_ORDER_ID VARCHAR(96) ,\n" +
                "MAIN_ORDER_NO VARCHAR(96) ,\n" +
                "CUSTOMER_ID VARCHAR(600) ,\n" +
                "ORDER_STATUS CHAR(10) ,\n" +
                "ORDER_TYPE CHAR(10) ,\n" +
                "PRODUCT_ID VARCHAR(96) ,\n" +
                "PRODUCT_NAME VARCHAR(96) ,\n" +
                "PRODUCT_PIC_URL VARCHAR(600) ,\n" +
                "PRODUCT_NUM BIGINT ,\n" +
                "BALANCE BIGINT ,\n" +
                "ENABLE CHAR(10) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_USER VARCHAR(384) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(384) ,\n" +
                "PRODUCT_TYPE VARCHAR(600) ,\n" +
                "PHONE VARCHAR(600)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZXYH_RIG_GOLD_ORDER',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_ZXYH_RIG_GOLD_ORDER(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "ORDER_NO,\n" +
                "MAIN_ORDER_ID,\n" +
                "MAIN_ORDER_NO,\n" +
                "CUSTOMER_ID,\n" +
                "ORDER_STATUS,\n" +
                "ORDER_TYPE,\n" +
                "PRODUCT_ID,\n" +
                "PRODUCT_NAME,\n" +
                "PRODUCT_PIC_URL,\n" +
                "PRODUCT_NUM,\n" +
                "BALANCE,\n" +
                "ENABLE,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER,\n" +
                "PRODUCT_TYPE,\n" +
                "PHONE)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "ORDER_NO,\n" +
                "MAIN_ORDER_ID,\n" +
                "MAIN_ORDER_NO,\n" +
                "CUSTOMER_ID,\n" +
                "ORDER_STATUS,\n" +
                "ORDER_TYPE,\n" +
                "PRODUCT_ID,\n" +
                "PRODUCT_NAME,\n" +
                "PRODUCT_PIC_URL,\n" +
                "PRODUCT_NUM,\n" +
                "BALANCE,\n" +
                "ENABLE,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER,\n" +
                "PRODUCT_TYPE,\n" +
                "PHONE\n" +
                " FROM RIG_GOLD_ORDER "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
