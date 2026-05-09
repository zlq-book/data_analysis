package com.travelsky.dataplatform.main.ods.v2.dkhcl;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.DorisUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;

public class ExtrctTOdsDkhclAirOrderPassengerSegment {
    public static Logger log = LoggerFactory.getLogger(ExtrctTOdsDkhclAirOrderPassengerSegment.class);

    public static void main(String[] args) throws Exception {
        log.info(" ExtrctTOdsDkhclAirOrderPassenger data transfer start");

        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String startDate = etlDate;
        String endDate = etlDate;
        if (args.length > 2) {
            startDate = args[1];
            endDate = args[2];
        }
        System.out.println(" ExtrctTOdsDkhclAirOrderPassenger etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);

        // === 查询 Doris 目标表当前最大 ID ===
        long lastMaxId = DorisUtils.queryMaxIdFromDoris("T_ODS_DKHCL_AIR_ORDER_PASSENGER_SEGMENT");
        log.info("Last Max ID in Doris: " + lastMaxId);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, " ExtrctTOdsDkhclAirOrderPassengerSegment");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        log.info(" ExtrctTOdsDkhclAirOrderPassengerSegment executeSql create table for source start");
        tEnv.executeSql("CREATE TABLE AIR_ORDER_PASSENGER_SEGMENT (\n" +
                " ID BIGINT,\n" +
                " CHANGE_PRICE DOUBLE,\n" +
                " CREATE_TIME TIMESTAMP(6),\n" +
                " DEL_STATUS INT,\n" +
                " FCNY DOUBLE,\n" +
                " FUEL_TAX DOUBLE,\n" +
                " INSURANCE DOUBLE,\n" +
                " IS_OPEN INT,\n" +
                " LAPSE DOUBLE,\n" +
                " MARK_CODE VARCHAR(100),\n" +
                " OTHER_TAX DOUBLE,\n" +
                " SALE_PRICE DOUBLE,\n" +
                " SAVE_PRICE DOUBLE,\n" +
                " SEAT_NO VARCHAR(255),\n" +
                " STATUS INT,\n" +
                " TICKET_NUM VARCHAR(255),\n" +
                " TKT_TAX DOUBLE,\n" +
                " UP_CABIN_PRICE DOUBLE,\n" +
                " YUAN_TICKET_NUM VARCHAR(255),\n" +
                " AIR_ORDER BIGINT,\n" +
                " PASSENGER BIGINT,\n" +
                " SEGMENT BIGINT,\n" +
                " IS_SEAT_NO BIGINT,\n" +
                " SEAT_PHONE VARCHAR(100),\n" +
                " SEAT_NO_STATUS VARCHAR(10),\n" +
                " SEQUENCE TINYINT,\n" +
                " SYS_PRICE DOUBLE,\n" +
                " CORRELATION_ORDER_ID VARCHAR(100)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.DKHCL_IP + ":" + Constants.DKHCL_PORT + "/" + Constants.DKHCL_DB + "',\n" +
                "    'table-name' = '" + Constants.DKHCL_SCHEMA + ".AIR_ORDER_PASSENGER_SEGMENT', \n" +
                "    'username' = '" + Constants.DKHCL_USER + "',\n" +
                "    'password' = '" + Constants.DKHCL_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        log.info(" ExtrctTOdsDkhclAirOrderPassengerSegment executeSql create table for source end");
        //创建Doris目标表
        log.info(" ExtrctTOdsDkhclAirOrderPassengerSegment executeSql create table for doris start");
        tEnv.executeSql("CREATE TABLE T_ODS_DKHCL_AIR_ORDER_PASSENGER_SEGMENT (\n" +
                " ID BIGINT,\n" +
                " CHANGE_PRICE DOUBLE,\n" +
                " CREATE_TIME TIMESTAMP(6),\n" +
                " DEL_STATUS INT,\n" +
                " FCNY DOUBLE,\n" +
                " FUEL_TAX DOUBLE,\n" +
                " INSURANCE DOUBLE,\n" +
                " IS_OPEN INT,\n" +
                " LAPSE DOUBLE,\n" +
                " MARK_CODE VARCHAR(100),\n" +
                " OTHER_TAX DOUBLE,\n" +
                " SALE_PRICE DOUBLE,\n" +
                " SAVE_PRICE DOUBLE,\n" +
                " SEAT_NO VARCHAR(255),\n" +
                " STATUS INT,\n" +
                " TICKET_NUM VARCHAR(255),\n" +
                " TKT_TAX DOUBLE,\n" +
                " UP_CABIN_PRICE DOUBLE,\n" +
                " YUAN_TICKET_NUM VARCHAR(255),\n" +
                " AIR_ORDER BIGINT,\n" +
                " PASSENGER BIGINT,\n" +
                " SEGMENT BIGINT,\n" +
                " IS_SEAT_NO BIGINT,\n" +
                " SEAT_PHONE VARCHAR(100),\n" +
                " SEAT_NO_STATUS VARCHAR(10),\n" +
                " SEQUENCE TINYINT,\n" +
                " SYS_PRICE DOUBLE,\n" +
                " CORRELATION_ORDER_ID VARCHAR(100),\n" +
                " ETL_CREATE_TIME TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                " ETL_UPDATE_TIME TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                " ETL_DATE date  NULL COMMENT '数据ETL日期'" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_DKHCL_AIR_ORDER_PASSENGER_SEGMENT',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        log.info(" ExtrctTOdsDkhclAirOrderPassengerSegment executeSql create table for doris end");
        String extractSql = "INSERT INTO T_ODS_DKHCL_AIR_ORDER_PASSENGER_SEGMENT(\n" +
                " ID,\n" +
                " CHANGE_PRICE,\n" +
                " CREATE_TIME,\n" +
                " DEL_STATUS,\n" +
                " FCNY,\n" +
                " FUEL_TAX,\n" +
                " INSURANCE,\n" +
                " IS_OPEN,\n" +
                " LAPSE,\n" +
                " MARK_CODE,\n" +
                " OTHER_TAX,\n" +
                " SALE_PRICE,\n" +
                " SAVE_PRICE,\n" +
                " SEAT_NO,\n" +
                " STATUS,\n" +
                " TICKET_NUM,\n" +
                " TKT_TAX,\n" +
                " UP_CABIN_PRICE,\n" +
                " YUAN_TICKET_NUM,\n" +
                " AIR_ORDER,\n" +
                " PASSENGER,\n" +
                " SEGMENT,\n" +
                " IS_SEAT_NO,\n" +
                " SEAT_PHONE,\n" +
                " SEAT_NO_STATUS,\n" +
                " SEQUENCE,\n" +
                " SYS_PRICE,\n" +
                " CORRELATION_ORDER_ID,\n" +
                " ETL_CREATE_TIME,\n" +
                " ETL_UPDATE_TIME,\n" +
                " ETL_DATE)" +
                " SELECT\n" +
                " ID,\n" +
                " CHANGE_PRICE,\n" +
                " CREATE_TIME,\n" +
                " DEL_STATUS,\n" +
                " FCNY,\n" +
                " FUEL_TAX,\n" +
                " INSURANCE,\n" +
                " IS_OPEN,\n" +
                " LAPSE,\n" +
                " MARK_CODE,\n" +
                " OTHER_TAX,\n" +
                " SALE_PRICE,\n" +
                " SAVE_PRICE,\n" +
                " SEAT_NO,\n" +
                " STATUS,\n" +
                " TICKET_NUM,\n" +
                " TKT_TAX,\n" +
                " UP_CABIN_PRICE,\n" +
                " YUAN_TICKET_NUM,\n" +
                " AIR_ORDER,\n" +
                " PASSENGER,\n" +
                " SEGMENT,\n" +
                " IS_SEAT_NO,\n" +
                " SEAT_PHONE,\n" +
                " SEAT_NO_STATUS,\n" +
                " SEQUENCE,\n" +
                " SYS_PRICE,\n" +
                " CORRELATION_ORDER_ID, \n" +
                " CURRENT_TIMESTAMP AS ETL_CREATE_TIME,\n" +
                " CURRENT_TIMESTAMP AS ETL_UPDATE_TIME,\n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                " FROM AIR_ORDER_PASSENGER_SEGMENT " +
                " WHERE ID > " + lastMaxId;
        log.info(" ExtrctTOdsDkhclAirOrderPassengerSegment executeSql extract start");
        TableResult result = tEnv.executeSql(extractSql);
        log.info(" ExtrctTOdsDkhclAirOrderPassengerSegment executeSql extract end");

        result.print();

        log.info(" ExtrctTOdsDkhclAirOrderPassengerSegment data transfer end");


    }

}
