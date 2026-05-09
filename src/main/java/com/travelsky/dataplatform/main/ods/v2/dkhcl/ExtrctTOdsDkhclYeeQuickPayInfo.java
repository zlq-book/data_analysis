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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

public class ExtrctTOdsDkhclYeeQuickPayInfo {
    private static final Logger log = LoggerFactory.getLogger(ExtrctTOdsDkhclYeeQuickPayInfo.class);

    public static void main(String[] args) throws Exception {
        log.info("ExtrctTOdsDkhclYeeQuickPayInfo data transfer start");

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
        System.out.println(" ExtrctTOdsDkhclYeeQuickPayInfo etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);

        // === 查询 Doris 目标表当前最大 ID ===
        long lastMaxId = DorisUtils.queryMaxIdFromDoris("T_ODS_DKHCL_YEE_QUICK_PAY_INFO");
        log.info("Last Max ID in Doris: " + lastMaxId);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtrctTOdsDkhclYeeQuickPayInfo");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();

        // 创建上游ORACLE数据源表
        log.info("ExtrctTOdsDkhclYeeQuickPayInfo executeSql create table for source start");
        tEnv.executeSql("CREATE TABLE YEE_QUICK_PAY_INFO (\n" +
                " ID BIGINT,\n" +
                " BUSINESNO VARCHAR(30),\n" +
                " BUNISSTYPE VARCHAR(20),\n" +
                " ORDERNO VARCHAR(30),\n" +
                " ORGORDERNO VARCHAR(30),\n" +
                " CURTYPE VARCHAR(10),\n" +
                " BANKCODE VARCHAR(20),\n" +
                " BANKBRAN VARCHAR(20),\n" +
                " TRANTYPE VARCHAR(20),\n" +
                " ORDAMOUNT DOUBLE,\n" +
                " AMOUNT DOUBLE,\n" +
                " TRADEDATE VARCHAR(20),\n" +
                " TRADETIME VARCHAR(20),\n" +
                " TRADATE VARCHAR(30),\n" +
                " PAYPLATFORM VARCHAR(20),\n" +
                " BANKORDERID VARCHAR(50),\n" +
                " PAYSTATUS VARCHAR(20),\n" +
                " REFUNDSTATUS VARCHAR(20),\n" +
                " SERIALNUM VARCHAR(50),\n" +
                " TRADECODE VARCHAR(20),\n" +
                " TRADEMSG VARCHAR(100),\n" +
                " PRODUCTNAME VARCHAR(20),\n" +
                " TRANNUMBER BIGINT,\n" +
                " CID BIGINT,\n" +
                " ACCOUNTID BIGINT,\n" +
                " PLATFORM VARCHAR(20),\n" +
                " SCENARIO VARCHAR(20),\n" +
                " PAYPLATFORM_NAME VARCHAR(100),\n" +
                " BANKCODE_NAME VARCHAR(100)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.DKHCL_IP + ":" + Constants.DKHCL_PORT + "/" + Constants.DKHCL_DB + "',\n" +
                "    'table-name' = '" + Constants.DKHCL_SCHEMA + ".YEE_QUICK_PAY_INFO', \n" +
                "    'username' = '" + Constants.DKHCL_USER + "',\n" +
                "    'password' = '" + Constants.DKHCL_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        log.info(" ExtrctTOdsYeeQuickPayInfo executeSql create table for source end");
        //创建Doris目标表
        log.info(" ExtrctTOdsYeeQuickPayInfo executeSql create table for doris start");
        tEnv.executeSql("CREATE TABLE T_ODS_DKHCL_YEE_QUICK_PAY_INFO (\n" +
                " ID BIGINT,\n" +
                " BUSINESNO VARCHAR(90),\n" +
                " BUNISSTYPE VARCHAR(60),\n" +
                " ORDERNO VARCHAR(90),\n" +
                " ORGORDERNO VARCHAR(90),\n" +
                " CURTYPE VARCHAR(30),\n" +
                " BANKCODE VARCHAR(60),\n" +
                " BANKBRAN VARCHAR(60),\n" +
                " TRANTYPE VARCHAR(60),\n" +
                " ORDAMOUNT DOUBLE,\n" +
                " AMOUNT DOUBLE,\n" +
                " TRADEDATE VARCHAR(60),\n" +
                " TRADETIME VARCHAR(60),\n" +
                " TRADATE VARCHAR(90),\n" +
                " PAYPLATFORM VARCHAR(60),\n" +
                " BANKORDERID VARCHAR(150),\n" +
                " PAYSTATUS VARCHAR(60),\n" +
                " REFUNDSTATUS VARCHAR(60),\n" +
                " SERIALNUM VARCHAR(150),\n" +
                " TRADECODE VARCHAR(60),\n" +
                " TRADEMSG VARCHAR(300),\n" +
                " PRODUCTNAME VARCHAR(60),\n" +
                " TRANNUMBER BIGINT,\n" +
                " CID BIGINT,\n" +
                " ACCOUNTID BIGINT,\n" +
                " PLATFORM VARCHAR(60),\n" +
                " SCENARIO VARCHAR(60),\n" +
                " PAYPLATFORM_NAME VARCHAR(300),\n" +
                " BANKCODE_NAME VARCHAR(300),\n" +
                " ETL_CREATE_TIME TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                " ETL_UPDATE_TIME TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                " ETL_DATE date  NULL COMMENT '数据ETL日期'" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "',\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "',\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_DKHCL_YEE_QUICK_PAY_INFO',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "',\n" +
                " 'sink.properties.read_json_by_line' = 'true',\n" +
                " 'sink.properties.format' = 'json',\n" +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        log.info("ExtrctTOdsDkhclYeeQuickPayInfo executeSql create table for doris end");

        // 构建抽取SQL（删除ETL_DATE，保留日期过滤条件，增加ID增量过滤）
        String extractSql = "INSERT INTO T_ODS_DKHCL_YEE_QUICK_PAY_INFO(\n" +
                " ID,\n" +
                " BUSINESNO,\n" +
                " BUNISSTYPE,\n" +
                " ORDERNO,\n" +
                " ORGORDERNO,\n" +
                " CURTYPE,\n" +
                " BANKCODE,\n" +
                " BANKBRAN,\n" +
                " TRANTYPE,\n" +
                " ORDAMOUNT,\n" +
                " AMOUNT,\n" +
                " TRADEDATE,\n" +
                " TRADETIME,\n" +
                " TRADATE,\n" +
                " PAYPLATFORM,\n" +
                " BANKORDERID,\n" +
                " PAYSTATUS,\n" +
                " REFUNDSTATUS,\n" +
                " SERIALNUM,\n" +
                " TRADECODE,\n" +
                " TRADEMSG,\n" +
                " PRODUCTNAME,\n" +
                " TRANNUMBER,\n" +
                " CID,\n" +
                " ACCOUNTID,\n" +
                " PLATFORM,\n" +
                " SCENARIO,\n" +
                " PAYPLATFORM_NAME,\n" +
                " BANKCODE_NAME, \n" +
                " ETL_CREATE_TIME,\n" +
                " ETL_UPDATE_TIME,\n" +
                " ETL_DATE)" +
                " SELECT\n" +
                " ID,\n" +
                " BUSINESNO,\n" +
                " BUNISSTYPE,\n" +
                " ORDERNO,\n" +
                " ORGORDERNO,\n" +
                " CURTYPE,\n" +
                " BANKCODE,\n" +
                " BANKBRAN,\n" +
                " TRANTYPE,\n" +
                " ORDAMOUNT,\n" +
                " AMOUNT,\n" +
                " TRADEDATE,\n" +
                " TRADETIME,\n" +
                " TRADATE,\n" +
                " PAYPLATFORM,\n" +
                " BANKORDERID,\n" +
                " PAYSTATUS,\n" +
                " REFUNDSTATUS,\n" +
                " SERIALNUM,\n" +
                " TRADECODE,\n" +
                " TRADEMSG,\n" +
                " PRODUCTNAME,\n" +
                " TRANNUMBER,\n" +
                " CID,\n" +
                " ACCOUNTID,\n" +
                " PLATFORM,\n" +
                " SCENARIO,\n" +
                " PAYPLATFORM_NAME,\n" +
                " BANKCODE_NAME, \n" +
                " CURRENT_TIMESTAMP AS ETL_CREATE_TIME,\n" +
                " CURRENT_TIMESTAMP AS ETL_UPDATE_TIME,\n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                " FROM YEE_QUICK_PAY_INFO " +
                " WHERE ID > " + lastMaxId;

        log.info("ExtrctTOdsDkhclYeeQuickPayInfo executeSql extract start");
        TableResult result = tEnv.executeSql(extractSql);
        log.info("ExtrctTOdsDkhclYeeQuickPayInfo executeSql extract end");

        result.print();
        log.info("ExtrctTOdsDkhclYeeQuickPayInfo data transfer end");
    }
}