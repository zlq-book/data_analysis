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
public class ExtractTOdsZxyhMetCoupon {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZxyhMetCoupon");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE MET_COUPON (\n" +
                "ID VARCHAR(96) ,\n" +
                "TEMPLETID VARCHAR(96) ,\n" +
                "OWNERID VARCHAR(96) ,\n" +
                "USERID VARCHAR(96) ,\n" +
                "CODE VARCHAR(96) ,\n" +
                "CREATE_DATE VARCHAR(96) ,\n" +
                "STATUS VARCHAR(90) ,\n" +
                "SOURCEID VARCHAR(96) ,\n" +
                "CITY VARCHAR(90) ,\n" +
                "ALLOT_DATE VARCHAR(96) ,\n" +
                "USE_TIME VARCHAR(96) ,\n" +
                "TICKETID VARCHAR(192)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.ZXYH_IP+":"+Constants.ZXYH_PORT+"/"+Constants.ZXYH_DB+"',\n" +
                "    'table-name' = '"+Constants.ZXYH_SCHEMA+".MET_COUPON', \n" +
                "    'username' = '"+Constants.ZXYH_USER+"',\n" +
                "    'password' = '"+Constants.ZXYH_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_ZXYH_MET_COUPON (\n" +
                "    ETL_DATE DATE,\n" +
                "ID VARCHAR(96) ,\n" +
                "TEMPLETID VARCHAR(96) ,\n" +
                "OWNERID VARCHAR(96) ,\n" +
                "USERID VARCHAR(96) ,\n" +
                "CODE VARCHAR(96) ,\n" +
                "CREATE_DATE VARCHAR(96) ,\n" +
                "STATUS VARCHAR(90) ,\n" +
                "SOURCEID VARCHAR(96) ,\n" +
                "CITY VARCHAR(90) ,\n" +
                "ALLOT_DATE VARCHAR(96) ,\n" +
                "USE_TIME VARCHAR(96) ,\n" +
                "TICKETID VARCHAR(192)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZXYH_MET_COUPON',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_ZXYH_MET_COUPON(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "TEMPLETID,\n" +
                "OWNERID,\n" +
                "USERID,\n" +
                "CODE,\n" +
                "CREATE_DATE,\n" +
                "STATUS,\n" +
                "SOURCEID,\n" +
                "CITY,\n" +
                "ALLOT_DATE,\n" +
                "USE_TIME,\n" +
                "TICKETID)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "TEMPLETID,\n" +
                "OWNERID,\n" +
                "USERID,\n" +
                "CODE,\n" +
                "CREATE_DATE,\n" +
                "STATUS,\n" +
                "SOURCEID,\n" +
                "CITY,\n" +
                "ALLOT_DATE,\n" +
                "USE_TIME,\n" +
                "TICKETID\n" +
                " FROM MET_COUPON "
                + " WHERE  CREATE_DATE = '" + etlDate.replace("-","") + "'";

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
