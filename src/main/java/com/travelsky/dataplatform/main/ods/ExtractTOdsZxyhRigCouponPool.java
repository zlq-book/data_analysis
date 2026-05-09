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
public class ExtractTOdsZxyhRigCouponPool {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZxyhRigCouponPool");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE RIG_COUPON_POOL (\n" +
                "ID VARCHAR(96) ,\n" +
                "COUPON_BATCH_NO VARCHAR(96) ,\n" +
                "SEND_TIME VARCHAR(96) ,\n" +
                "COUPON_CHANNEL VARCHAR(300) ,\n" +
                "COUPON_DIVISION VARCHAR(300) ,\n" +
                "COUPON_CLASS VARCHAR(300) ,\n" +
                "COUPON_GROUP_CODE VARCHAR(96) ,\n" +
                "COUPON_GROUP_NAME VARCHAR(300) ,\n" +
                "COUPON_CODE VARCHAR(96) ,\n" +
                "COUPON_NAME VARCHAR(300) ,\n" +
                "COUPON_ID VARCHAR(96) ,\n" +
                "COUPON_STATUS VARCHAR(6) ,\n" +
                "COUPON_BALANCE VARCHAR(30) ,\n" +
                "BALANCE_TYPE VARCHAR(30) ,\n" +
                "SELL_BALANCE VARCHAR(30) ,\n" +
                "SELL_BALANCE_TYPE VARCHAR(96) ,\n" +
                "STATISTICS_FLAG VARCHAR(60) ,\n" +
                "PROMOTION_CODE VARCHAR(96) ,\n" +
                "END_TIME VARCHAR(96) ,\n" +
                "CUSTOMER_ID VARCHAR(96) ,\n" +
                "COUPON_GROUP_ID VARCHAR(96) ,\n" +
                "POLICY_ID VARCHAR(96) ,\n" +
                "BINDING_TIME TIMESTAMP(6) ,\n" +
                "USED_TYPE VARCHAR(96) ,\n" +
                "TICKET_TYPE VARCHAR(96) ,\n" +
                "OTHER_PRODUCT_TYPE VARCHAR(300) ,\n" +
                "DISCOUNT VARCHAR(96) ,\n" +
                "LABEL VARCHAR(300) ,\n" +
                "MAKE_USER VARCHAR(300) ,\n" +
                "MAKE_TIME VARCHAR(96) ,\n" +
                "SELL_BEGIN_TIME VARCHAR(96) ,\n" +
                "SELL_END_TIME VARCHAR(96) ,\n" +
                "CANCEL_USER VARCHAR(300) ,\n" +
                "CANCEL_TIME VARCHAR(96) ,\n" +
                "USE_TIME TIMESTAMP(6) ,\n" +
                "PASSENGER_TIME VARCHAR(96) ,\n" +
                "SEND_REASON VARCHAR(300) ,\n" +
                "REAL_COUPON_CODE VARCHAR(96) ,\n" +
                "ORDER_CODE VARCHAR(96) ,\n" +
                "DISCOUNT_OTHER VARCHAR(96) ,\n" +
                "BALANCE_TYPE_OTHER VARCHAR(96) ,\n" +
                "ENABLE CHAR(10) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_USER VARCHAR(384) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(384)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.ZXYH_IP+":"+Constants.ZXYH_PORT+"/"+Constants.ZXYH_DB+"',\n" +
                "    'table-name' = '"+Constants.ZXYH_SCHEMA+".RIG_COUPON_POOL', \n" +
                "    'username' = '"+Constants.ZXYH_USER+"',\n" +
                "    'password' = '"+Constants.ZXYH_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_ZXYH_RIG_COUPON_POOL (\n" +
                "    ETL_DATE DATE,\n" +
                "ID VARCHAR(96) ,\n" +
                "COUPON_BATCH_NO VARCHAR(96) ,\n" +
                "SEND_TIME VARCHAR(96) ,\n" +
                "COUPON_CHANNEL VARCHAR(300) ,\n" +
                "COUPON_DIVISION VARCHAR(300) ,\n" +
                "COUPON_CLASS VARCHAR(300) ,\n" +
                "COUPON_GROUP_CODE VARCHAR(96) ,\n" +
                "COUPON_GROUP_NAME VARCHAR(300) ,\n" +
                "COUPON_CODE VARCHAR(96) ,\n" +
                "COUPON_NAME VARCHAR(300) ,\n" +
                "COUPON_ID VARCHAR(96) ,\n" +
                "COUPON_STATUS VARCHAR(6) ,\n" +
                "COUPON_BALANCE VARCHAR(30) ,\n" +
                "BALANCE_TYPE VARCHAR(30) ,\n" +
                "SELL_BALANCE VARCHAR(30) ,\n" +
                "SELL_BALANCE_TYPE VARCHAR(96) ,\n" +
                "STATISTICS_FLAG VARCHAR(60) ,\n" +
                "PROMOTION_CODE VARCHAR(96) ,\n" +
                "END_TIME VARCHAR(96) ,\n" +
                "CUSTOMER_ID VARCHAR(96) ,\n" +
                "COUPON_GROUP_ID VARCHAR(96) ,\n" +
                "POLICY_ID VARCHAR(96) ,\n" +
                "BINDING_TIME TIMESTAMP(6) ,\n" +
                "USED_TYPE VARCHAR(96) ,\n" +
                "TICKET_TYPE VARCHAR(96) ,\n" +
                "OTHER_PRODUCT_TYPE VARCHAR(300) ,\n" +
                "DISCOUNT VARCHAR(96) ,\n" +
                "LABEL VARCHAR(300) ,\n" +
                "MAKE_USER VARCHAR(300) ,\n" +
                "MAKE_TIME VARCHAR(96) ,\n" +
                "SELL_BEGIN_TIME VARCHAR(96) ,\n" +
                "SELL_END_TIME VARCHAR(96) ,\n" +
                "CANCEL_USER VARCHAR(300) ,\n" +
                "CANCEL_TIME VARCHAR(96) ,\n" +
                "USE_TIME TIMESTAMP(6) ,\n" +
                "PASSENGER_TIME VARCHAR(96) ,\n" +
                "SEND_REASON VARCHAR(300) ,\n" +
                "REAL_COUPON_CODE VARCHAR(96) ,\n" +
                "ORDER_CODE VARCHAR(96) ,\n" +
                "DISCOUNT_OTHER VARCHAR(96) ,\n" +
                "BALANCE_TYPE_OTHER VARCHAR(96) ,\n" +
                "ENABLE CHAR(10) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_USER VARCHAR(384) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(384)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZXYH_RIG_COUPON_POOL',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_ZXYH_RIG_COUPON_POOL(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "COUPON_BATCH_NO,\n" +
                "SEND_TIME,\n" +
                "COUPON_CHANNEL,\n" +
                "COUPON_DIVISION,\n" +
                "COUPON_CLASS,\n" +
                "COUPON_GROUP_CODE,\n" +
                "COUPON_GROUP_NAME,\n" +
                "COUPON_CODE,\n" +
                "COUPON_NAME,\n" +
                "COUPON_ID,\n" +
                "COUPON_STATUS,\n" +
                "COUPON_BALANCE,\n" +
                "BALANCE_TYPE,\n" +
                "SELL_BALANCE,\n" +
                "SELL_BALANCE_TYPE,\n" +
                "STATISTICS_FLAG,\n" +
                "PROMOTION_CODE,\n" +
                "END_TIME,\n" +
                "CUSTOMER_ID,\n" +
                "COUPON_GROUP_ID,\n" +
                "POLICY_ID,\n" +
                "BINDING_TIME,\n" +
                "USED_TYPE,\n" +
                "TICKET_TYPE,\n" +
                "OTHER_PRODUCT_TYPE,\n" +
                "DISCOUNT,\n" +
                "LABEL,\n" +
                "MAKE_USER,\n" +
                "MAKE_TIME,\n" +
                "SELL_BEGIN_TIME,\n" +
                "SELL_END_TIME,\n" +
                "CANCEL_USER,\n" +
                "CANCEL_TIME,\n" +
                "USE_TIME,\n" +
                "PASSENGER_TIME,\n" +
                "SEND_REASON,\n" +
                "REAL_COUPON_CODE,\n" +
                "ORDER_CODE,\n" +
                "DISCOUNT_OTHER,\n" +
                "BALANCE_TYPE_OTHER,\n" +
                "ENABLE,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "COUPON_BATCH_NO,\n" +
                "SEND_TIME,\n" +
                "COUPON_CHANNEL,\n" +
                "COUPON_DIVISION,\n" +
                "COUPON_CLASS,\n" +
                "COUPON_GROUP_CODE,\n" +
                "COUPON_GROUP_NAME,\n" +
                "COUPON_CODE,\n" +
                "COUPON_NAME,\n" +
                "COUPON_ID,\n" +
                "COUPON_STATUS,\n" +
                "COUPON_BALANCE,\n" +
                "BALANCE_TYPE,\n" +
                "SELL_BALANCE,\n" +
                "SELL_BALANCE_TYPE,\n" +
                "STATISTICS_FLAG,\n" +
                "PROMOTION_CODE,\n" +
                "END_TIME,\n" +
                "CUSTOMER_ID,\n" +
                "COUPON_GROUP_ID,\n" +
                "POLICY_ID,\n" +
                "BINDING_TIME,\n" +
                "USED_TYPE,\n" +
                "TICKET_TYPE,\n" +
                "OTHER_PRODUCT_TYPE,\n" +
                "DISCOUNT,\n" +
                "LABEL,\n" +
                "MAKE_USER,\n" +
                "MAKE_TIME,\n" +
                "SELL_BEGIN_TIME,\n" +
                "SELL_END_TIME,\n" +
                "CANCEL_USER,\n" +
                "CANCEL_TIME,\n" +
                "USE_TIME,\n" +
                "PASSENGER_TIME,\n" +
                "SEND_REASON,\n" +
                "REAL_COUPON_CODE,\n" +
                "ORDER_CODE,\n" +
                "DISCOUNT_OTHER,\n" +
                "BALANCE_TYPE_OTHER,\n" +
                "ENABLE,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER\n" +
                " FROM RIG_COUPON_POOL "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
