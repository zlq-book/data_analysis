package com.travelsky.dataplatform.main.ods.v2.Lygj;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * @author
 * @date 2025/11/3 16:29
 */
public class ExtractTOdsLygjTraceEMDInfo {
    public static Logger log = LoggerFactory.getLogger(ExtractTOdsLygjTraceEMDInfo.class);
    public static void main(String[] args) throws IOException {
        log.info("ExtractTOdsLygjTraceEMDInfo data transfer start");
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
        log.info("ExtractTOdsLygjTraceEMDInfo etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLygjTraceEMDInfo");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        log.info("ExtractTOdsLygjTraceEMDInfo executeSql create table for source start");
        tEnv.executeSql("CREATE TABLE TRACE_EMDINFO (\n" +
                "    EMD_IDX BIGINT, -- NUMBER(38,0) -> BIGINT\n" +
                "    PSR_IDX BIGINT, -- NUMBER(38,0) -> BIGINT\n" +
                "    FLT_NUM STRING, -- VARCHAR2(10) -> STRING\n" +
                "    FLT_DATE DATE,   -- DATE -> DATE\n" +
                "    ORIG STRING,   -- VARCHAR2(3) -> STRING\n" +
                "    DEST STRING,   -- VARCHAR2(3) -> STRING\n" +
                "    ET_NUM STRING,   -- VARCHAR2(20) -> STRING\n" +
                "    EMD_COUPONSTATUS STRING,  -- VARCHAR2(2) -> STRING\n" +
                "    EMDTICKETNUMBER STRING,  -- VARCHAR2(20) -> STRING\n" +
                "    EMDISSUEOFFICE STRING,  -- VARCHAR2(10) -> STRING\n" +
                "    EMDIATACODE STRING,  -- VARCHAR2(10) -> STRING\n" +
                "    EMD_TYPE STRING,  -- VARCHAR2(2) -> STRING\n" +
                "    EMDISSUEDATE DATE,  -- DATE -> DATE\n" +
                "    EMDTICKETTYPE STRING,  -- VARCHAR2(2) -> STRING\n" +
                "    PAYMENTTYPE STRING,  -- VARCHAR2(3) -> STRING\n" +
                "    PAYAMOUNT STRING,  -- VARCHAR2(10) -> STRING\n" +
                "    PAYCURRENCYCODE STRING,  -- VARCHAR2(10) -> STRING\n" +
                "    CARDNUMBER STRING,  -- VARCHAR2(20) -> STRING\n" +
                "    CARDCODE STRING,  -- VARCHAR2(3) -> STRING\n" +
                "    REDEMPTIONQUANTITY STRING, -- VARCHAR2(10) -> STRING\n" +
                "    ISSUANCE_CODE STRING,  -- VARCHAR2(2) -> STRING\n" +
                "    ISSUANCE_SUBCODE STRING, -- VARCHAR2(10) -> STRING\n" +
                "    ISSUANCE_DESCRIPTION STRING,  -- VARCHAR2(100) -> STRING\n" +
                "    BASEFARE_PURPOSE STRING,  -- VARCHAR2(10) -> STRING\n" +
                "    BASEFARE_CURRENCYCODE STRING,  -- VARCHAR2(3) -> STRING\n" +
                "    BASEFARE_AMOUNT STRING,  -- VARCHAR2(10) -> STRING\n" +
                "    EQUIVFARE_PURPOSE STRING,  -- VARCHAR2(10) -> STRING\n" +
                "    EQUIVFARE_CURRENCYCODE STRING,  -- VARCHAR2(3) -> STRING\n" +
                "    EQUIVFARE_AMOUNT STRING,  -- VARCHAR2(10) -> STRING\n" +
                "    TOTALFARE STRING,     -- VARCHAR2(1000) -> STRING\n" +
                "    EMD_TAXES STRING,     -- VARCHAR2(1000) -> STRING\n" +
                "    CREATE_TIME TIMESTAMP, -- DATE -> TIMESTAMP，Flink没有直接DATE类型\n" +
                "    UPDATE_TIME TIMESTAMP   -- DATE -> TIMESTAMP, Flink没有直接DATE类型\n" +

                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.LYGJ_IP + ":" + Constants.LYGJ_PORT + "/" + Constants.LYGJ_DB + "',\n" +
                "    'table-name' = '" + Constants.LYGJ_SCHEMA + ".TRACE_EMDINFO', \n" +
                "    'username' = '" + Constants.LYGJ_USER + "',\n" +
                "    'password' = '" + Constants.LYGJ_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        log.info("ExtractTOdsLygjTraceEMDInfo executeSql create table for source end");
        //创建Doris目标表
        log.info("ExtractTOdsLygjTraceEMDInfo executeSql create table for doris start");
        tEnv.executeSql("CREATE TABLE `T_ODS_LYGJ_TRACE_EMDINFO` (\n" +
                "  `EMD_IDX` BIGINT COMMENT '主键',\n" +
                "  `PSR_IDX` BIGINT COMMENT 'DPI序列号',\n" +
                "  `FLT_NUM` STRING COMMENT '航班号',\n" +
                "  `FLT_DATE` DATE COMMENT '航班日期',\n" +
                "  `ORIG` STRING COMMENT '始发地',\n" +
                "  `DEST` STRING COMMENT '目的地',\n" +
                "  `ET_NUM` STRING COMMENT '票号',\n" +
                "  `EMD_COUPONSTATUS` STRING COMMENT 'EMD客票状态',\n" +
                "  `EMDTICKETNUMBER` STRING COMMENT 'EMD票号',\n" +
                "  `EMDISSUEOFFICE` STRING COMMENT 'EMD出票Office',\n" +
                "  `EMDIATACODE` STRING COMMENT 'EMD出票Office对应IataCode',\n" +
                "  `EMD_TYPE` STRING COMMENT '\"EMD类型包括J和Y,其中J代表是EMD-A,Y表示EMD-S\"',\n" +
                "  `EMDISSUEDATE` DATE COMMENT 'EMD出票日期',\n" +
                "  `EMDTICKETTYPE` STRING COMMENT '\"EMD国际国内标识D：国内票I：国际票\"',\n" +
                "  `PAYMENTTYPE` STRING COMMENT '支付类型',\n" +
                "  `PAYAMOUNT` STRING COMMENT '支付金额',\n" +
                "  `PAYCURRENCYCODE` STRING COMMENT '支付货币代码',\n" +
                "  `CARDNUMBER` STRING COMMENT '信用卡号',\n" +
                "  `CARDCODE` STRING COMMENT '卡代码',\n" +
                "  `REDEMPTIONQUANTITY` STRING COMMENT '里程积分',\n" +
                "  `ISSUANCE_CODE` STRING COMMENT '出票原因代码',\n" +
                "  `ISSUANCE_SUBCODE` STRING COMMENT '出票原因子码',\n" +
                "  `ISSUANCE_DESCRIPTION` STRING COMMENT '出票原因子码描述',\n" +
                "  `BASEFARE_PURPOSE` STRING COMMENT '基础运价目的',\n" +
                "  `BASEFARE_CURRENCYCODE` STRING COMMENT '基础运价货币单位',\n" +
                "  `BASEFARE_AMOUNT` STRING COMMENT '基础运价金额',\n" +
                "  `EQUIVFARE_PURPOSE` STRING COMMENT '等值运价目的',\n" +
                "  `EQUIVFARE_CURRENCYCODE` STRING COMMENT '等值运价货币单位',\n" +
                "  `EQUIVFARE_AMOUNT` STRING COMMENT '等值运价金额',\n" +
                "  `TOTALFARE` STRING COMMENT '总运价节点',\n" +
                "  `EMD_TAXES` STRING COMMENT 'EMD票-所有税项',\n" +
                "  `CREATE_TIME` TIMESTAMP(3) COMMENT '创建时间',\n" +
                "  `UPDATE_TIME` TIMESTAMP(3) COMMENT '修改时间',\n" +
                "  `ETL_CREATE_TIME` TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                "  `ETL_UPDATE_TIME` TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                "  `ETL_DATE` date  NULL COMMENT '数据ETL日期'" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYGJ_TRACE_EMDINFO',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //") WITH (\n" +
        //"    'connector' = 'jdbc',\n" +
        //"    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
        //"    'table-name' = 'T_ODS_LYGJ_TRACE_EMDINFO', -- 替换为实际的表名\n" +
        //"    'username' = '"+Constants.ODS_USER+"',\n" +
        //"    'password' = '"+Constants.ODS_PWD+"'\n" +
        //",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
        //")");
        log.info("ExtractTOdsLygjTraceEMDInfo executeSql create table for doris end");
        String extractSql = "INSERT INTO T_ODS_LYGJ_TRACE_EMDINFO\n" +
                "(EMD_IDX,\n" +
                " PSR_IDX,\n" +
                " FLT_NUM,\n" +
                " FLT_DATE,\n" +
                " ORIG,\n" +
                " DEST,\n" +
                " ET_NUM,\n" +
                " EMD_COUPONSTATUS,\n" +
                " EMDTICKETNUMBER,\n" +
                " EMDISSUEOFFICE,\n" +
                " EMDIATACODE,\n" +
                " EMD_TYPE,\n" +
                " EMDISSUEDATE,\n" +
                " EMDTICKETTYPE,\n" +
                " PAYMENTTYPE,\n" +
                " PAYAMOUNT,\n" +
                " PAYCURRENCYCODE,\n" +
                " CARDNUMBER,\n" +
                " CARDCODE,\n" +
                " REDEMPTIONQUANTITY,\n" +
                " ISSUANCE_CODE,\n" +
                " ISSUANCE_SUBCODE,\n" +
                " ISSUANCE_DESCRIPTION,\n" +
                " BASEFARE_PURPOSE,\n" +
                " BASEFARE_CURRENCYCODE,\n" +
                " BASEFARE_AMOUNT,\n" +
                " EQUIVFARE_PURPOSE,\n" +
                " EQUIVFARE_CURRENCYCODE,\n" +
                " EQUIVFARE_AMOUNT,\n" +
                " TOTALFARE,\n" +
                " EMD_TAXES,\n" +
                " CREATE_TIME,\n" +
                " UPDATE_TIME,\n" +
                " ETL_CREATE_TIME,\n" +
                " ETL_UPDATE_TIME,\n" +
                " ETL_DATE)" +

                "SELECT\n" +
                " EMD_IDX,\n" +
                " PSR_IDX,\n" +
                " FLT_NUM,\n" +
                " FLT_DATE,\n" +
                " ORIG,\n" +
                " DEST,\n" +
                " ET_NUM,\n" +
                " EMD_COUPONSTATUS,\n" +
                " EMDTICKETNUMBER,\n" +
                " EMDISSUEOFFICE,\n" +
                " EMDIATACODE,\n" +
                " EMD_TYPE,\n" +
                " EMDISSUEDATE,\n" +
                " EMDTICKETTYPE,\n" +
                " PAYMENTTYPE,\n" +
                " PAYAMOUNT,\n" +
                " PAYCURRENCYCODE,\n" +
                " CARDNUMBER,\n" +
                " CARDCODE,\n" +
                " REDEMPTIONQUANTITY,\n" +
                " ISSUANCE_CODE,\n" +
                " ISSUANCE_SUBCODE,\n" +
                " ISSUANCE_DESCRIPTION,\n" +
                " BASEFARE_PURPOSE,\n" +
                " BASEFARE_CURRENCYCODE,\n" +
                " BASEFARE_AMOUNT,\n" +
                " EQUIVFARE_PURPOSE,\n" +
                " EQUIVFARE_CURRENCYCODE,\n" +
                " EQUIVFARE_AMOUNT,\n" +
                " TOTALFARE,\n" +
                " EMD_TAXES,\n" +
                " CREATE_TIME,\n" +
                " UPDATE_TIME,\n" +
                " CURRENT_TIMESTAMP AS ETL_CREATE_TIME,\n" +
                " CURRENT_TIMESTAMP AS ETL_UPDATE_TIME,\n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                " FROM TRACE_EMDINFO "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;
        log.info("ExtractTOdsLygjTraceEMDInfo executeSql extract start");
        TableResult result = tEnv.executeSql(extractSql);
        log.info("ExtractTOdsLygjTraceEMDInfo executeSql extract end");

        result.print();

        log.info("ExtractTOdsLygjTraceEMDInfo data transfer end");


    }

}
