package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.UUID;

public class ExtractTOdsZsfOdrOrderDouyin {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZsfOdrOrderDouyin");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE ODR_ORDER_DOUYIN (\n" +
                "    ID VARCHAR(96) NOT NULL,\n" +
                "    COUPON_ID VARCHAR(96),\n" +
                "    CARD_CODE VARCHAR(96),\n" +
                "    ORDER_NO VARCHAR(96),\n" +
                "    OLD_ORDER_NO VARCHAR(96),\n" +
                "    ORDER_PRICE DECIMAL(10, 2),\n" +
                "    ORDER_SOURCE VARCHAR(60),\n" +
                "    ORDER_TYPE VARCHAR(96),\n" +
                "    ORDER_STATUS VARCHAR(6),\n" +
                "    PARENT_ID VARCHAR(96),\n" +
                "    MAIN_ORDER_NO VARCHAR(96),\n" +
                "    ORDER_TIME TIMESTAMP(6),\n" +
                "    VERSION INT,\n" +
                "    REMARK VARCHAR(6000),\n" +
                "    ENABLE CHAR(1),\n" +
                "    CREATE_TIME TIMESTAMP(6),\n" +
                "    CREATE_USER VARCHAR(384),\n" +
                "    UPDATE_TIME TIMESTAMP(6),\n" +
                "    UPDATE_USER VARCHAR(384)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://"+ Constants.ZSF_IP+":"+Constants.ZSF_PORT+"/"+Constants.ZSF_DB+"',\n" +
                "    'table-name' = 'ODR_ORDER_DOUYIN', \n" +
                "    'username' = '"+Constants.ZSF_USER+"',\n" +
                "    'password' = '"+Constants.ZSF_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
                ")");


        //创建Doris目标表
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        String ZSF_ODR_ORDER_DOUYIN = "CREATE TABLE T_ODS_ZSF_ODR_ORDER_DOUYIN (\n" +
                "    ETL_DATE DATE NOT NULL,\n" +
                "    ID VARCHAR(96) NOT NULL,\n" +
                "    COUPON_ID VARCHAR(96),\n" +
                "    CARD_CODE VARCHAR(96),\n" +
                "    ORDER_NO VARCHAR(96),\n" +
                "    OLD_ORDER_NO VARCHAR(96),\n" +
                "    ORDER_PRICE DECIMAL(10, 2),\n" +
                "    ORDER_SOURCE VARCHAR(60),\n" +
                "    ORDER_TYPE VARCHAR(96),\n" +
                "    ORDER_STATUS VARCHAR(6),\n" +
                "    PARENT_ID VARCHAR(96),\n" +
                "    MAIN_ORDER_NO VARCHAR(96),\n" +
                "    ORDER_TIME TIMESTAMP(6),\n" +
                "    VERSION INT,\n" +
                "    REMARK VARCHAR(6000),\n" +
                "    ENABLE CHAR(1),\n" +
                "    CREATE_TIME TIMESTAMP(6),\n" +
                "    CREATE_USER VARCHAR(384),\n" +
                "    UPDATE_TIME TIMESTAMP(6),\n" +
                "    UPDATE_USER VARCHAR(384)" +
//                ") WITH (\n" +
//                "    'connector' = 'jdbc',\n" +
//                "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
//                "    'table-name' = 'T_ODS_ZSF_ODR_ORDER_DOUYIN', -- 替换为实际的表名\n" +
//                "    'username' = '"+Constants.ODS_USER+"',\n" +
//                "    'password' = '"+Constants.ODS_PWD+"'\n" +
//                ")";
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZSF_ODR_ORDER_DOUYIN',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")";
        tEnv.executeSql(ZSF_ODR_ORDER_DOUYIN);

        String extractSql = "INSERT INTO T_ODS_ZSF_ODR_ORDER_DOUYIN(" +
                "ETL_DATE\n" +
                ",ID\n" +
                ",COUPON_ID\n" +
                ",CARD_CODE\n" +
                ",ORDER_NO\n" +
                ",OLD_ORDER_NO\n" +
                ",ORDER_PRICE\n" +
                ",ORDER_SOURCE\n" +
                ",ORDER_TYPE\n" +
                ",ORDER_STATUS\n" +
                ",PARENT_ID\n" +
                ",MAIN_ORDER_NO\n" +
                ",ORDER_TIME\n" +
                ",VERSION\n" +
                ",REMARK\n" +
                ",ENABLE\n" +
                ",CREATE_TIME\n" +
                ",CREATE_USER\n" +
                ",UPDATE_TIME\n" +
                ",UPDATE_USER\n" +
                ") " +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE \n" +
                ",ID\n" +
                ",COUPON_ID\n" +
                ",CARD_CODE\n" +
                ",ORDER_NO\n" +
                ",OLD_ORDER_NO\n" +
                ",ORDER_PRICE\n" +
                ",ORDER_SOURCE\n" +
                ",ORDER_TYPE\n" +
                ",ORDER_STATUS\n" +
                ",PARENT_ID\n" +
                ",MAIN_ORDER_NO\n" +
                ",ORDER_TIME\n" +
                ",VERSION\n" +
                ",REMARK\n" +
                ",ENABLE\n" +
                ",CREATE_TIME\n" +
                ",CREATE_USER\n" +
                ",UPDATE_TIME\n" +
                ",UPDATE_USER" +
                " FROM ODR_ORDER_DOUYIN "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' " +
                " OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
