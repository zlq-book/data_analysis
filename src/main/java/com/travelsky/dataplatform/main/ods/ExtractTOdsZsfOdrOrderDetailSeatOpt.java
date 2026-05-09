package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.UUID;

public class ExtractTOdsZsfOdrOrderDetailSeatOpt {
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
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZsfOdrOrderDetailSeat");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);

        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE ODR_ORDER_DETAIL_SEAT (\n" +
                "    ID VARCHAR(96) NOT NULL,\n" +
                "    ORDER_DETAIL_ID VARCHAR(96) NOT NULL,\n" +
                "    ORDER_NO VARCHAR(96) NOT NULL,\n" +
                "    MAIN_ORDER_NO VARCHAR(96),\n" +
                "    MAIN_ORDER_ID VARCHAR(96),\n" +
                "    INSTANCE_ID VARCHAR(96),\n" +
                "    INSTANCE_NO VARCHAR(96),\n" +
                "    MAIN_FLAG VARCHAR(60) NOT NULL,\n" +
                "    TICKET_NO VARCHAR(60) NOT NULL,\n" +
                "    ADT_TICKET_NO VARCHAR(60),\n" +
                "    PNR VARCHAR(24),\n" +
                "    CARRIER VARCHAR(765),\n" +
                "    FLIGHT_NO VARCHAR(96),\n" +
                "    DEP_DATE_TIME TIMESTAMP(6),\n" +
                "    DEP_DATE VARCHAR(30),\n" +
                "    ARR_DATE VARCHAR(30),\n" +
                "    DEP_TIME VARCHAR(30),\n" +
                "    ARR_TIME VARCHAR(30),\n" +
                "    DEP_TERMINAL VARCHAR(30),\n" +
                "    ARR_TERMINAL VARCHAR(30),\n" +
                "    FLIGHT_DURATION VARCHAR(90),\n" +
                "    ORG_CITY VARCHAR(9),\n" +
                "    STOP_CITY VARCHAR(9),\n" +
                "    DST_CITY VARCHAR(9),\n" +
                "    CABIN VARCHAR(6),\n" +
                "    PSGR_NAME VARCHAR(300),\n" +
                "    CERT_TYPE VARCHAR(300),\n" +
                "    CERT_NO VARCHAR(1200),\n" +
                "    OLD_SEAT_NO VARCHAR(300),\n" +
                "    SEAT_NO VARCHAR(300),\n" +
                "    MOBILE VARCHAR(1200),\n" +
                "    SEAT_STATUS VARCHAR(18),\n" +
                "    UPDATE_TIMES INT NOT NULL,\n" +
                "    SEAT_TIME TIMESTAMP(6),\n" +
                "    SEAT_SOURCE VARCHAR(765),\n" +
                "    SEAT_PRICE DECIMAL(10,2) NOT NULL,\n" +
                "    PSGR_TYPE VARCHAR(24),\n" +
                "    PSGR_NAME_EN VARCHAR(192),\n" +
                "    CHECKIN_STATUS VARCHAR(15),\n" +
                "    IS_DIFF VARCHAR(6),\n" +
                "    SEAT_TYPE VARCHAR(60),\n" +
                "    CHECK_IN_TYPE VARCHAR(6),\n" +
                "    APP_CHECK_IN_TYPE VARCHAR(6),\n" +
                "    SEG_INDEX INT,\n" +
                "    ENABLE CHAR(1) NOT NULL,\n" +
                "    CREATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    CREATE_USER VARCHAR(384) NOT NULL,\n" +
                "    UPDATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    UPDATE_USER VARCHAR(384) NOT NULL,\n" +
                "    EMD_TICKET_NO VARCHAR(60),\n" +
                "    IS_PRICE_SEAT VARCHAR(6)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://" + Constants.ZSF_IP + ":" + Constants.ZSF_PORT + "/" + Constants.ZSF_DB + "',\n" +
                "    'table-name' = 'ODR_ORDER_DETAIL_SEAT', \n" +
                "    'username' = '" + Constants.ZSF_USER + "',\n" +
                "    'password' = '" + Constants.ZSF_PWD + "'\n" +
                ",\n" + "  'driver' = '" + Constants.MYSQL_DRIVER + "'" +
                ")");
        //创建Doris目标表
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        String ZSF_ODR_ORDER_DETAIL_SEAT = "CREATE TABLE T_ODS_ZSF_ODR_ORDER_DETAIL_SEAT (\n" +
                "    ETL_DATE DATE NOT NULL,\n" +
                "    ID VARCHAR(96) NOT NULL,\n" +
                "    ORDER_DETAIL_ID VARCHAR(96) NOT NULL,\n" +
                "    ORDER_NO VARCHAR(96) NOT NULL,\n" +
                "    MAIN_ORDER_NO VARCHAR(96),\n" +
                "    MAIN_ORDER_ID VARCHAR(96),\n" +
                "    INSTANCE_ID VARCHAR(96),\n" +
                "    INSTANCE_NO VARCHAR(96),\n" +
                "    MAIN_FLAG VARCHAR(60) NOT NULL,\n" +
                "    TICKET_NO VARCHAR(60) NOT NULL,\n" +
                "    ADT_TICKET_NO VARCHAR(60),\n" +
                "    PNR VARCHAR(24),\n" +
                "    CARRIER VARCHAR(765),\n" +
                "    FLIGHT_NO VARCHAR(96),\n" +
                "    DEP_DATE_TIME TIMESTAMP(6),\n" +
                "    DEP_DATE VARCHAR(30),\n" +
                "    ARR_DATE VARCHAR(30),\n" +
                "    DEP_TIME VARCHAR(30),\n" +
                "    ARR_TIME VARCHAR(30),\n" +
                "    DEP_TERMINAL VARCHAR(30),\n" +
                "    ARR_TERMINAL VARCHAR(30),\n" +
                "    FLIGHT_DURATION VARCHAR(90),\n" +
                "    ORG_CITY VARCHAR(9),\n" +
                "    STOP_CITY VARCHAR(9),\n" +
                "    DST_CITY VARCHAR(9),\n" +
                "    CABIN VARCHAR(6),\n" +
                "    PSGR_NAME VARCHAR(300),\n" +
                "    CERT_TYPE VARCHAR(300),\n" +
                "    CERT_NO VARCHAR(1200),\n" +
                "    OLD_SEAT_NO VARCHAR(300),\n" +
                "    SEAT_NO VARCHAR(300),\n" +
                "    MOBILE VARCHAR(1200),\n" +
                "    SEAT_STATUS VARCHAR(18),\n" +
                "    UPDATE_TIMES INT NOT NULL,\n" +
                "    SEAT_TIME TIMESTAMP(6),\n" +
                "    SEAT_SOURCE VARCHAR(765),\n" +
                "    SEAT_PRICE DECIMAL(10,2) NOT NULL,\n" +
                "    PSGR_TYPE VARCHAR(24),\n" +
                "    PSGR_NAME_EN VARCHAR(192),\n" +
                "    CHECKIN_STATUS VARCHAR(15),\n" +
                "    IS_DIFF VARCHAR(6),\n" +
                "    SEAT_TYPE VARCHAR(60),\n" +
                "    CHECK_IN_TYPE VARCHAR(6),\n" +
                "    APP_CHECK_IN_TYPE VARCHAR(6),\n" +
                "    SEG_INDEX INT,\n" +
                "    ENABLE CHAR(1) NOT NULL,\n" +
                "    CREATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    CREATE_USER VARCHAR(384) NOT NULL,\n" +
                "    UPDATE_TIME TIMESTAMP(6) NOT NULL,\n" +
                "    UPDATE_USER VARCHAR(384) NOT NULL,\n" +
                "    EMD_TICKET_NO VARCHAR(60),\n" +
                "    IS_PRICE_SEAT VARCHAR(6)" +
//                ") WITH (\n" +
//                "    'connector' = 'jdbc',\n" +
//                "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
//                "    'table-name' = 'T_ODS_ZSF_ODR_ORDER_DETAIL_SEAT', -- 替换为实际的表名\n" +
//                "    'username' = '"+Constants.ODS_USER+"',\n" +
//                "    'password' = '"+Constants.ODS_PWD+"'\n" +
//                ")";
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZSF_ODR_ORDER_DETAIL_SEAT',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")";
        tEnv.executeSql(ZSF_ODR_ORDER_DETAIL_SEAT);

        String extractSql = "INSERT INTO T_ODS_ZSF_ODR_ORDER_DETAIL_SEAT(" +
                "ETL_DATE\n" +
                ",ID\n" +
                ",ORDER_DETAIL_ID\n" +
                ",ORDER_NO\n" +
                ",MAIN_ORDER_NO\n" +
                ",MAIN_ORDER_ID\n" +
                ",INSTANCE_ID\n" +
                ",INSTANCE_NO\n" +
                ",MAIN_FLAG\n" +
                ",TICKET_NO\n" +
                ",ADT_TICKET_NO\n" +
                ",PNR\n" +
                ",CARRIER\n" +
                ",FLIGHT_NO\n" +
                ",DEP_DATE_TIME\n" +
                ",DEP_DATE\n" +
                ",ARR_DATE\n" +
                ",DEP_TIME\n" +
                ",ARR_TIME\n" +
                ",DEP_TERMINAL\n" +
                ",ARR_TERMINAL\n" +
                ",FLIGHT_DURATION\n" +
                ",ORG_CITY\n" +
                ",STOP_CITY\n" +
                ",DST_CITY\n" +
                ",CABIN\n" +
                ",PSGR_NAME\n" +
                ",CERT_TYPE\n" +
                ",CERT_NO\n" +
                ",OLD_SEAT_NO\n" +
                ",SEAT_NO\n" +
                ",MOBILE\n" +
                ",SEAT_STATUS\n" +
                ",UPDATE_TIMES\n" +
                ",SEAT_TIME\n" +
                ",SEAT_SOURCE\n" +
                ",SEAT_PRICE\n" +
                ",PSGR_TYPE\n" +
                ",PSGR_NAME_EN\n" +
                ",CHECKIN_STATUS\n" +
                ",IS_DIFF\n" +
                ",SEAT_TYPE\n" +
                ",CHECK_IN_TYPE\n" +
                ",APP_CHECK_IN_TYPE\n" +
                ",SEG_INDEX\n" +
                ",ENABLE\n" +
                ",CREATE_TIME\n" +
                ",CREATE_USER\n" +
                ",UPDATE_TIME\n" +
                ",UPDATE_USER\n" +
                ",EMD_TICKET_NO\n" +
                ",IS_PRICE_SEAT\n" +
                ") " +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE \n" +
                ",ID\n" +
                ",ORDER_DETAIL_ID\n" +
                ",ORDER_NO\n" +
                ",MAIN_ORDER_NO\n" +
                ",MAIN_ORDER_ID\n" +
                ",INSTANCE_ID\n" +
                ",INSTANCE_NO\n" +
                ",MAIN_FLAG\n" +
                ",TICKET_NO\n" +
                ",ADT_TICKET_NO\n" +
                ",PNR\n" +
                ",CARRIER\n" +
                ",FLIGHT_NO\n" +
                ",DEP_DATE_TIME\n" +
                ",DEP_DATE\n" +
                ",ARR_DATE\n" +
                ",DEP_TIME\n" +
                ",ARR_TIME\n" +
                ",DEP_TERMINAL\n" +
                ",ARR_TERMINAL\n" +
                ",FLIGHT_DURATION\n" +
                ",ORG_CITY\n" +
                ",STOP_CITY\n" +
                ",DST_CITY\n" +
                ",CABIN\n" +
                ",PSGR_NAME\n" +
                ",CERT_TYPE\n" +
                ",sm4_encrypt (CERT_NO,'" + sm4key + "') CERT_NO\n" +
                ",OLD_SEAT_NO\n" +
                ",SEAT_NO\n" +
                ",sm4_encrypt (MOBILE,'" + sm4key + "') MOBILE\n" +
                ",SEAT_STATUS\n" +
                ",UPDATE_TIMES\n" +
                ",SEAT_TIME\n" +
                ",SEAT_SOURCE\n" +
                ",SEAT_PRICE\n" +
                ",PSGR_TYPE\n" +
                ",PSGR_NAME_EN\n" +
                ",CHECKIN_STATUS\n" +
                ",IS_DIFF\n" +
                ",SEAT_TYPE\n" +
                ",CHECK_IN_TYPE\n" +
                ",APP_CHECK_IN_TYPE\n" +
                ",SEG_INDEX\n" +
                ",ENABLE\n" +
                ",CREATE_TIME\n" +
                ",CREATE_USER\n" +
                ",UPDATE_TIME\n" +
                ",UPDATE_USER\n" +
                ",EMD_TICKET_NO\n" +
                ",IS_PRICE_SEAT" +
                " FROM ODR_ORDER_DETAIL_SEAT "+
                " WHERE CREATE_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
