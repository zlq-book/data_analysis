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

public class ExtrctTOdsDkhclAirOrderSegment {
    public static Logger log = LoggerFactory.getLogger(ExtrctTOdsDkhclAirOrderSegment.class);

    public static void main(String[] args) throws IOException, SQLException {
        log.info("ExtrctTOdsDkhclAirOrderSegment data transfer start");

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
        System.out.println(" ExtrctTOdsDkhclAirOrderSegment etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);


        // === 查询 Doris 目标表当前最大 ID ===
        long lastMaxId = DorisUtils.queryMaxIdFromDoris("T_ODS_DKHCL_AIR_ORDER_SEGMENT");
        log.info("Last Max ID in Doris: " + lastMaxId);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtrctTOdsDkhclAirOrderSegment");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();

        // 创建上游ORACLE数据源表
        log.info("ExtrctTOdsDkhclAirOrderSegment executeSql create table for source start");
        tEnv.executeSql("CREATE TABLE AIR_ORDER_SEGMENT (\n" +
                " ID BIGINT,\n" +
                " AIR_CRAFT VARCHAR(255),\n" +
                " AIRPORT_TAX DOUBLE,\n" +
                " ARRIVAL_DATE TIMESTAMP(6),\n" +
                " ARRIVAL_TIME VARCHAR(50),\n" +
                " ASR VARCHAR(50),\n" +
                " BOARD_AIRPORT_NAME VARCHAR(70),\n" +
                " BOARD_POINT VARCHAR(30),\n" +
                " BOARD_POINTAT VARCHAR(80),\n" +
                " BOARD_POINT_NAME VARCHAR(80),\n" +
                " CARRIER VARCHAR(90),\n" +
                " CARRIER_NAME VARCHAR(90),\n" +
                " DEPARTURE_DATE TIMESTAMP(6),\n" +
                " DEPARTURE_TIME VARCHAR(60),\n" +
                " E_TICKET VARCHAR(100),\n" +
                " ELEMENT_NO INT,\n" +
                " FLIGHTID INT,\n" +
                " FLIGHT_NO VARCHAR(20),\n" +
                " FLIGHT_TYPE VARCHAR(20),\n" +
                " FUEL_SUR_TAX DOUBLE,\n" +
                " MEAL VARCHAR(50),\n" +
                " OFF_AIRPORT_NAME VARCHAR(255),\n" +
                " OFF_POINT VARCHAR(255),\n" +
                " OFF_POINTAT VARCHAR(255),\n" +
                " OFF_POINT_NAME VARCHAR(255),\n" +
                " `SEQUENCE` INT,\n" +
                " SHARE_CARRIER VARCHAR(255),\n" +
                " SHARE_CARRIER_NAME VARCHAR(255),\n" +
                " SHARE_FLIGHT VARCHAR(255),\n" +
                " TPM INT,\n" +
                " VIA_POINT VARCHAR(255),\n" +
                " Y_CLASS_PRICE DOUBLE,\n" +
                " CHANGE_RULE VARCHAR(3800),\n" +
                " CLASS_CODE VARCHAR(255),\n" +
                " DISCOUNT DOUBLE,\n" +
                " EXT_CODE VARCHAR(255),\n" +
                " ORIGINAL_PRICE DOUBLE,\n" +
                " PRICE DOUBLE,\n" +
                " RE_FUND_RULE VARCHAR(3800),\n" +
                " SAVE_PRICE DOUBLE,\n" +
                " SEAT_NUM VARCHAR(255),\n" +
                " TICKET_FBC VARCHAR(255),\n" +
                " `ALLOW` VARCHAR(100),\n" +
                " PAT_PARAMS VARCHAR(20),\n" +
                " PNR VARCHAR(10),\n" +
                " PNRX_XML VARCHAR(3500),\n" +
                " STATUS INT,\n" +
                " ZHONG_ZHUAN TINYINT,\n" +
                " AIR_ORDER BIGINT,\n" +
                " CHILD_ID BIGINT,\n" +
                " PRICE_TYPE VARCHAR(6),\n" +
                " SOURCE_PRICE VARCHAR(10),\n" +
                " PNR_XML VARCHAR(10000),\n" +
                " SELLCODE VARCHAR(50),\n" +
                " FLIGHT_RULE VARCHAR(10000),\n" +
                " FLIGHT_RULE_JSON VARCHAR(10000),\n" +
                " CLASS_NAME VARCHAR(20),\n" +
                " LOWPRICE DOUBLE,\n" +
                " STOP_AIRPORT VARCHAR(100),\n" +
                " BAGGAGE VARCHAR(500)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.DKHCL_IP + ":" + Constants.DKHCL_PORT + "/" + Constants.DKHCL_DB + "',\n" +
                "    'table-name' = '" + Constants.DKHCL_SCHEMA + ".AIR_ORDER_SEGMENT', \n" +
                "    'username' = '" + Constants.DKHCL_USER + "',\n" +
                "    'password' = '" + Constants.DKHCL_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        log.info("ExtrctTOdsDkhclAirOrderSegment executeSql create table for source end");

        // 创建Doris目标表
        log.info("ExtrctTOdsDkhclAirOrderSegment executeSql create table for doris start");
        tEnv.executeSql("CREATE TABLE T_ODS_DKHCL_AIR_ORDER_SEGMENT (\n" +
                " ID BIGINT,\n" +
                " AIR_CRAFT VARCHAR(255),\n" +
                " AIRPORT_TAX DOUBLE,\n" +
                " ARRIVAL_DATE TIMESTAMP(6),\n" +
                " ARRIVAL_TIME VARCHAR(50),\n" +
                " ASR VARCHAR(50),\n" +
                " BOARD_AIRPORT_NAME VARCHAR(70),\n" +
                " BOARD_POINT VARCHAR(30),\n" +
                " BOARD_POINTAT VARCHAR(80),\n" +
                " BOARD_POINT_NAME VARCHAR(80),\n" +
                " CARRIER VARCHAR(90),\n" +
                " CARRIER_NAME VARCHAR(90),\n" +
                " DEPARTURE_DATE TIMESTAMP(6),\n" +
                " DEPARTURE_TIME VARCHAR(60),\n" +
                " E_TICKET VARCHAR(100),\n" +
                " ELEMENT_NO INT,\n" +
                " FLIGHTID INT,\n" +
                " FLIGHT_NO VARCHAR(20),\n" +
                " FLIGHT_TYPE VARCHAR(20),\n" +
                " FUEL_SUR_TAX DOUBLE,\n" +
                " MEAL VARCHAR(50),\n" +
                " OFF_AIRPORT_NAME VARCHAR(255),\n" +
                " OFF_POINT VARCHAR(255),\n" +
                " OFF_POINTAT VARCHAR(255),\n" +
                " OFF_POINT_NAME VARCHAR(255),\n" +
                " `SEQUENCE` INT,\n" +
                " SHARE_CARRIER VARCHAR(255),\n" +
                " SHARE_CARRIER_NAME VARCHAR(255),\n" +
                " SHARE_FLIGHT VARCHAR(255),\n" +
                " TPM INT,\n" +
                " VIA_POINT VARCHAR(255),\n" +
                " Y_CLASS_PRICE DOUBLE,\n" +
                " CHANGE_RULE VARCHAR(3800),\n" +
                " CLASS_CODE VARCHAR(255),\n" +
                " DISCOUNT DOUBLE,\n" +
                " EXT_CODE VARCHAR(255),\n" +
                " ORIGINAL_PRICE DOUBLE,\n" +
                " PRICE DOUBLE,\n" +
                " RE_FUND_RULE VARCHAR(3800),\n" +
                " SAVE_PRICE DOUBLE,\n" +
                " SEAT_NUM VARCHAR(255),\n" +
                " TICKET_FBC VARCHAR(255),\n" +
                " `ALLOW` VARCHAR(100),\n" +
                " PAT_PARAMS VARCHAR(20),\n" +
                " PNR VARCHAR(10),\n" +
                " PNRX_XML VARCHAR(3500),\n" +
                " STATUS INT,\n" +
                " ZHONG_ZHUAN TINYINT,\n" +
                " AIR_ORDER BIGINT,\n" +
                " CHILD_ID BIGINT,\n" +
                " PRICE_TYPE VARCHAR(6),\n" +
                " SOURCE_PRICE VARCHAR(10),\n" +
                " PNR_XML VARCHAR(10000),\n" +
                " SELLCODE VARCHAR(50),\n" +
                " FLIGHT_RULE VARCHAR(10000),\n" +
                " FLIGHT_RULE_JSON VARCHAR(10000),\n" +
                " CLASS_NAME VARCHAR(20),\n" +
                " LOWPRICE DOUBLE,\n" +
                " STOP_AIRPORT VARCHAR(100),\n" +
                " BAGGAGE VARCHAR(500),\n" +
                " ETL_CREATE_TIME TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                " ETL_UPDATE_TIME TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                " ETL_DATE date  NULL COMMENT '数据ETL日期'" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "',\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "',\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_DKHCL_AIR_ORDER_SEGMENT',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        log.info("ExtrctTOdsDkhclAirOrderSegment executeSql create table for doris end");

        // 构建抽取SQL（删除ETL_DATE相关处理，增加ID增量过滤）
        String extractSql = "INSERT INTO T_ODS_DKHCL_AIR_ORDER_SEGMENT(\n" +
                " ID,\n" +
                " AIR_CRAFT,\n" +
                " AIRPORT_TAX,\n" +
                " ARRIVAL_DATE,\n" +
                " ARRIVAL_TIME,\n" +
                " ASR,\n" +
                " BOARD_AIRPORT_NAME,\n" +
                " BOARD_POINT,\n" +
                " BOARD_POINTAT,\n" +
                " BOARD_POINT_NAME,\n" +
                " CARRIER,\n" +
                " CARRIER_NAME,\n" +
                " DEPARTURE_DATE,\n" +
                " DEPARTURE_TIME,\n" +
                " E_TICKET,\n" +
                " ELEMENT_NO,\n" +
                " FLIGHTID,\n" +
                " FLIGHT_NO,\n" +
                " FLIGHT_TYPE,\n" +
                " FUEL_SUR_TAX,\n" +
                " MEAL,\n" +
                " OFF_AIRPORT_NAME,\n" +
                " OFF_POINT,\n" +
                " OFF_POINTAT,\n" +
                " OFF_POINT_NAME,\n" +
                " `SEQUENCE`,\n" +
                " SHARE_CARRIER,\n" +
                " SHARE_CARRIER_NAME,\n" +
                " SHARE_FLIGHT,\n" +
                " TPM,\n" +
                " VIA_POINT,\n" +
                " Y_CLASS_PRICE,\n" +
                " CHANGE_RULE,\n" +
                " CLASS_CODE,\n" +
                " DISCOUNT,\n" +
                " EXT_CODE,\n" +
                " ORIGINAL_PRICE,\n" +
                " PRICE,\n" +
                " RE_FUND_RULE,\n" +
                " SAVE_PRICE,\n" +
                " SEAT_NUM,\n" +
                " TICKET_FBC,\n" +
                " `ALLOW`,\n" +
                " PAT_PARAMS,\n" +
                " PNR,\n" +
                " PNRX_XML,\n" +
                " STATUS,\n" +
                " ZHONG_ZHUAN,\n" +
                " AIR_ORDER,\n" +
                " CHILD_ID,\n" +
                " PRICE_TYPE,\n" +
                " SOURCE_PRICE,\n" +
                " PNR_XML,\n" +
                " SELLCODE,\n" +
                " FLIGHT_RULE,\n" +
                " FLIGHT_RULE_JSON,\n" +
                " CLASS_NAME,\n" +
                " LOWPRICE,\n" +
                " STOP_AIRPORT,\n" +
                " BAGGAGE, \n" +
                " ETL_CREATE_TIME,\n" +
                " ETL_UPDATE_TIME,\n" +
                " ETL_DATE)" +
                " SELECT\n" +
                " ID,\n" +
                " AIR_CRAFT,\n" +
                " AIRPORT_TAX,\n" +
                " ARRIVAL_DATE,\n" +
                " ARRIVAL_TIME,\n" +
                " ASR,\n" +
                " BOARD_AIRPORT_NAME,\n" +
                " BOARD_POINT,\n" +
                " BOARD_POINTAT,\n" +
                " BOARD_POINT_NAME,\n" +
                " CARRIER,\n" +
                " CARRIER_NAME,\n" +
                " DEPARTURE_DATE,\n" +
                " DEPARTURE_TIME,\n" +
                " E_TICKET,\n" +
                " ELEMENT_NO,\n" +
                " FLIGHTID,\n" +
                " FLIGHT_NO,\n" +
                " FLIGHT_TYPE,\n" +
                " FUEL_SUR_TAX,\n" +
                " MEAL,\n" +
                " OFF_AIRPORT_NAME,\n" +
                " OFF_POINT,\n" +
                " OFF_POINTAT,\n" +
                " OFF_POINT_NAME,\n" +
                " `SEQUENCE`,\n" +
                " SHARE_CARRIER,\n" +
                " SHARE_CARRIER_NAME,\n" +
                " SHARE_FLIGHT,\n" +
                " TPM,\n" +
                " VIA_POINT,\n" +
                " Y_CLASS_PRICE,\n" +
                " CHANGE_RULE,\n" +
                " CLASS_CODE,\n" +
                " DISCOUNT,\n" +
                " EXT_CODE,\n" +
                " ORIGINAL_PRICE,\n" +
                " PRICE,\n" +
                " RE_FUND_RULE,\n" +
                " SAVE_PRICE,\n" +
                " SEAT_NUM,\n" +
                " TICKET_FBC,\n" +
                " `ALLOW`,\n" +
                " PAT_PARAMS,\n" +
                " PNR,\n" +
                " PNRX_XML,\n" +
                " STATUS,\n" +
                " ZHONG_ZHUAN,\n" +
                " AIR_ORDER,\n" +
                " CHILD_ID,\n" +
                " PRICE_TYPE,\n" +
                " SOURCE_PRICE,\n" +
                " PNR_XML,\n" +
                " SELLCODE,\n" +
                " FLIGHT_RULE,\n" +
                " FLIGHT_RULE_JSON,\n" +
                " CLASS_NAME,\n" +
                " LOWPRICE,\n" +
                " STOP_AIRPORT,\n" +
                " BAGGAGE, \n" +
                " CURRENT_TIMESTAMP AS ETL_CREATE_TIME,\n" +
                " CURRENT_TIMESTAMP AS ETL_UPDATE_TIME,\n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                " FROM AIR_ORDER_SEGMENT " +
                " WHERE ID > " +  lastMaxId;

        log.info("ExtrctTOdsDkhclAirOrderSegment executeSql extract start");
        TableResult result = tEnv.executeSql(extractSql);
        log.info("ExtrctTOdsDkhclAirOrderSegment executeSql extract end");

        result.print();
        log.info("ExtrctTOdsDkhclAirOrderSegment data transfer end");
    }
}