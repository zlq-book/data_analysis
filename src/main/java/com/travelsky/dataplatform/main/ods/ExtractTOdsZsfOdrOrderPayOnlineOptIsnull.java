package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.UUID;

public class ExtractTOdsZsfOdrOrderPayOnlineOptIsnull {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        //由于抽取失败，计划使用支付时间作为数据切条件，条件可以通过脚本传入起止时间参数
        String startDate = args[1];
        String endDate = args[2];
        String sm4key= Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZsfOdrOrderPayOnline");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);

        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE ODR_ORDER_PAY_ONLINE (\n" +
                "    ID VARCHAR(96) NOT NULL,\n" +
                "    ORDER_PAY_ID VARCHAR(96) NOT NULL,\n" +
                "    PAY_NO VARCHAR(96) NOT NULL,\n" +
                "    PAY_AMOUNT DECIMAL(20,2),\n" +
                "    PAY_TIME TIMESTAMP(6),\n" +
                "    PAY_REASON VARCHAR(1500),\n" +
                "    PAY_RETURN_MSG VARCHAR(3000),\n" +
                "    PAY_RETURN_TIME TIMESTAMP(6),\n" +
                "    BANK_ORDER_NO VARCHAR(192),\n" +
                "    BANK_TRADE_NO VARCHAR(192),\n" +
                "    PAY_STATUS VARCHAR(6),\n" +
                "    PAY_SOURCE VARCHAR(60),\n" +
                "    PAY_BANK_CODE VARCHAR(150),\n" +
                "    PAY_BANK_NAME VARCHAR(300),\n" +
                "    BUS_PARTER_NO VARCHAR(96),\n" +
                "    PAY_USER_NAME VARCHAR(192),\n" +
                "    EXTENDS1 VARCHAR(765),\n" +
                "    EXTENDS2 VARCHAR(765),\n" +
                "    EXTENDS3 VARCHAR(765),\n" +
                "    EXTENDS4 VARCHAR(765),\n" +
                "    IS_BINDING CHAR(1),\n" +
                "    CUSTOMER_ID VARCHAR(300),\n" +
                "    ENABLE CHAR(1) NOT NULL,\n" +
                "    CREATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    CREATE_USER VARCHAR(384) NOT NULL,\n" +
                "    UPDATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    UPDATE_USER VARCHAR(384) NOT NULL,\n" +
                "    CARD_NO VARCHAR(384),\n" +
                "    PAY_TYPE VARCHAR(108),\n" +
                "    PAY_ACTUAL_AMOUNT DECIMAL(20,0),\n" +
                "    DEDUCTION_AMOUNT DECIMAL(20,0),\n" +
                "    DEDUCTION_TYPE VARCHAR(6),\n" +
                "    TRP_PAYMENT_ID VARCHAR(96)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://"+ Constants.ZSF_IP+":"+Constants.ZSF_PORT+"/"+Constants.ZSF_DB+"',\n" +
                "    'table-name' = 'ODR_ORDER_PAY_ONLINE', \n" +
                "    'username' = '"+Constants.ZSF_USER+"',\n" +
                "    'password' = '"+Constants.ZSF_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
                ")");
        //创建Doris目标表
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        String ZSF_ODR_ORDER_PAY_ONLINE = "CREATE TABLE T_ODS_ZSF_ODR_ORDER_PAY_ONLINE (\n" +
                "    ETL_DATE DATE NOT NULL,\n" +
                "    ID VARCHAR(96) NOT NULL,\n" +
                "    ORDER_PAY_ID VARCHAR(96) NOT NULL,\n" +
                "    PAY_NO VARCHAR(96) NOT NULL,\n" +
                "    PAY_AMOUNT DECIMAL(20,2),\n" +
                "    PAY_TIME TIMESTAMP(6),\n" +
                "    PAY_REASON VARCHAR(1500),\n" +
                "    PAY_RETURN_MSG VARCHAR(3000),\n" +
                "    PAY_RETURN_TIME TIMESTAMP(6),\n" +
                "    BANK_ORDER_NO VARCHAR(192),\n" +
                "    BANK_TRADE_NO VARCHAR(192),\n" +
                "    PAY_STATUS VARCHAR(6),\n" +
                "    PAY_SOURCE VARCHAR(60),\n" +
                "    PAY_BANK_CODE VARCHAR(150),\n" +
                "    PAY_BANK_NAME VARCHAR(300),\n" +
                "    BUS_PARTER_NO VARCHAR(96),\n" +
                "    PAY_USER_NAME VARCHAR(192),\n" +
                "    EXTENDS1 VARCHAR(765),\n" +
                "    EXTENDS2 VARCHAR(765),\n" +
                "    EXTENDS3 VARCHAR(765),\n" +
                "    EXTENDS4 VARCHAR(765),\n" +
                "    IS_BINDING CHAR(1),\n" +
                "    CUSTOMER_ID VARCHAR(300),\n" +
                "    ENABLE CHAR(1) NOT NULL,\n" +
                "    CREATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    CREATE_USER VARCHAR(384) NOT NULL,\n" +
                "    UPDATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    UPDATE_USER VARCHAR(384) NOT NULL,\n" +
                "    CARD_NO VARCHAR(384),\n" +
                "    PAY_TYPE VARCHAR(108),\n" +
                "    PAY_ACTUAL_AMOUNT DECIMAL(20,0),\n" +
                "    DEDUCTION_AMOUNT DECIMAL(20,0),\n" +
                "    DEDUCTION_TYPE VARCHAR(6),\n" +
                "    TRP_PAYMENT_ID VARCHAR(96)" +
//                ") WITH (\n" +
//                "    'connector' = 'jdbc',\n" +
//                "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
//                "    'table-name' = 'T_ODS_ZSF_ODR_ORDER_PAY_ONLINE', -- 替换为实际的表名\n" +
//                "    'username' = '"+Constants.ODS_USER+"',\n" +
//                "    'password' = '"+Constants.ODS_PWD+"'\n" +
//                ")";
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZSF_ODR_ORDER_PAY_ONLINE',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")";
        tEnv.executeSql(ZSF_ODR_ORDER_PAY_ONLINE);

        String extractSql = "INSERT INTO T_ODS_ZSF_ODR_ORDER_PAY_ONLINE(" +
                "ETL_DATE\n" +
                ",ID\n" +
                ",ORDER_PAY_ID\n" +
                ",PAY_NO\n" +
                ",PAY_AMOUNT\n" +
                ",PAY_TIME\n" +
                ",PAY_REASON\n" +
                ",PAY_RETURN_MSG\n" +
                ",PAY_RETURN_TIME\n" +
                ",BANK_ORDER_NO\n" +
                ",BANK_TRADE_NO\n" +
                ",PAY_STATUS\n" +
                ",PAY_SOURCE\n" +
                ",PAY_BANK_CODE\n" +
                ",PAY_BANK_NAME\n" +
                ",BUS_PARTER_NO\n" +
                ",PAY_USER_NAME\n" +
                ",EXTENDS1\n" +
                ",EXTENDS2\n" +
                ",EXTENDS3\n" +
                ",EXTENDS4\n" +
                ",IS_BINDING\n" +
                ",CUSTOMER_ID\n" +
                ",ENABLE\n" +
                ",CREATE_TIME\n" +
                ",CREATE_USER\n" +
                ",UPDATE_TIME\n" +
                ",UPDATE_USER\n" +
                ",CARD_NO\n" +
                ",PAY_TYPE\n" +
                ",PAY_ACTUAL_AMOUNT\n" +
                ",DEDUCTION_AMOUNT\n" +
                ",DEDUCTION_TYPE\n" +
                ",TRP_PAYMENT_ID\n" +
                ") " +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE \n" +
                ",ID\n" +
                ",ORDER_PAY_ID\n" +
                ",PAY_NO\n" +
                ",PAY_AMOUNT\n" +
                ",PAY_TIME\n" +
                ",PAY_REASON\n" +
                ",PAY_RETURN_MSG\n" +
                ",PAY_RETURN_TIME\n" +
                ",BANK_ORDER_NO\n" +
                ",BANK_TRADE_NO\n" +
                ",PAY_STATUS\n" +
                ",PAY_SOURCE\n" +
                ",PAY_BANK_CODE\n" +
                ",PAY_BANK_NAME\n" +
                ",BUS_PARTER_NO\n" +
                ",PAY_USER_NAME\n" +
                ",EXTENDS1\n" +
                ",EXTENDS2\n" +
                ",EXTENDS3\n" +
                ",EXTENDS4\n" +
                ",IS_BINDING\n" +
                ",sm4_encrypt (CUSTOMER_ID,'" + sm4key + "') CUSTOMER_ID\n" +
                ",ENABLE\n" +
                ",CREATE_TIME\n" +
                ",CREATE_USER\n" +
                ",UPDATE_TIME\n" +
                ",UPDATE_USER\n" +
                ",CARD_NO\n" +
                ",PAY_TYPE\n" +
                ",PAY_ACTUAL_AMOUNT\n" +
                ",DEDUCTION_AMOUNT\n" +
                ",DEDUCTION_TYPE\n" +
                ",TRP_PAYMENT_ID" +
                " FROM ODR_ORDER_PAY_ONLINE "+
                " WHERE PAY_TIME IS NULL"
                ;
        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();

    }
}
