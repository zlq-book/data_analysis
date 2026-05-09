package com.travelsky.dataplatform.main.ods.v2.bzsj;

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
 * @date 2025/7/2 9:31
 */
public class ExtractTOdsBzsjTbTcnData {
    public static Logger log = LoggerFactory.getLogger(ExtractTOdsBzsjTbTcnData.class);
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
//        String etlDate = "2022-03-27";
        String startDate = etlDate;
        String endDate = etlDate;
        if (args.length > 2) {
            startDate = args[1];
            endDate = args[2];
        }
        log.info("ExtractTOdsBzsjTbTcnData etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsBzsjTbTcnData");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();

//创建上游数据源表
        tEnv.executeSql("CREATE TABLE TB_TCN_DATA (\n" +
                "EX_DATE VARCHAR(10) , \n" +
                "TKS VARCHAR(6), \n" +
                "TKORD BIGINT, \n" +
                "TK_NUM VARCHAR(36), \n" +
                "ORDTP VARCHAR(18), \n" +
                "TK_DATE VARCHAR(10), \n" +
                "DEP_DATE VARCHAR(10), \n" +
                "AIR_CODE VARCHAR(6), \n" +
                "FLIGHT_NO VARCHAR(15), \n" +
                "UP_LOCATION VARCHAR(9), \n" +
                "DIS_LOCATION VARCHAR(9), \n" +
                "CLASS_TYPE VARCHAR(1), \n" +
                "PS_TYPE VARCHAR(15), \n" +
                "FARE_TYPE VARCHAR(18), \n" +
                "AGENT VARCHAR(24), \n" +
                "FARE_TOL DECIMAL(12,2), \n" +
                "SEG_PRICE DECIMAL(12,2), \n" +
                "DIS_PRICE DECIMAL(8,2), \n" +
                "AGT_RATE DECIMAL(8,2), \n" +
                "ZSAL_RATE DECIMAL(8,2), \n" +
                "SEG_FARE DECIMAL(12,2), \n" +
                "SAL_RATE DECIMAL(8,2), \n" +
                "RATE DECIMAL(8,2), \n" +
                "CNAME VARCHAR(60), \n" +
                "AIR_PNR VARCHAR(18), \n" +
                "AGENT_PNR VARCHAR(18), \n" +
                "PNR VARCHAR(18), \n" +
                "PORT_FARE DECIMAL(8,2), \n" +
                "ZSAL_FARE DECIMAL(8,2), \n" +
                "ADD_FARE DECIMAL(8,2), \n" +
                "ORA VARCHAR(60) , \n" +
                "ID DECIMAL(8,2) , \n" +
                "FT_TK_NUM VARCHAR(36), \n" +
                "CP_STR VARCHAR(45), \n" +
                "TK_AGENT VARCHAR(24), \n" +
                "BBCODE VARCHAR(24), \n" +
                "AGT_FARE DECIMAL(8,2), \n" +
                "CLK_NUM VARCHAR(48), \n" +
                "TK_AIR VARCHAR(9), \n" +
                "FARE_RATE DECIMAL(8,4), \n" +
                "CLIENTCODE VARCHAR(45), \n" +
                "STATID VARCHAR(12), \n" +
                "MCAR VARCHAR(12), \n" +
                "TKTYPE VARCHAR(12), \n" +
                "EQFARE DECIMAL(12,2), \n" +
                "SMFARE DECIMAL(12,2), \n" +
                "OBFARE DECIMAL(12,2), \n" +
                "CHTKNUM VARCHAR(39), \n" +
                "GPSTATE VARCHAR(6), \n" +
                "VALAGT_CODE VARCHAR(24), \n" +
                "BGRP VARCHAR(1), \n" +
                "CHG_AIR VARCHAR(9), \n" +
                "CHG_TKNUM VARCHAR(39), \n" +
                "REF_PNR VARCHAR(18), \n" +
                "FPTPCODE VARCHAR(6), \n" +
                "EQFPN DECIMAL(10,2)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.BZSJ_IP+":"+Constants.BZSJ_PORT+"/"+Constants.BZSJ_DB+"',\n" +
                "    'table-name' = '"+Constants.BZSJ_SCHEMA+".TB_TCN_DATA', \n" +
                "    'username' = '"+Constants.BZSJ_USER+"',\n" +
                "    'password' = '"+Constants.BZSJ_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_BZSJ_TB_TCN_DATA (\n" +
                "    ETL_DATE DATE,\n" +
                "ROW_ID VARCHAR(100) , \n" +
                "EX_DATE VARCHAR(10) , \n" +
                "TKS VARCHAR(6), \n" +
                "TKORD BIGINT, \n" +
                "TK_NUM VARCHAR(36), \n" +
                "ORDTP VARCHAR(18), \n" +
                "TK_DATE VARCHAR(10), \n" +
                "DEP_DATE VARCHAR(10), \n" +
                "AIR_CODE VARCHAR(6), \n" +
                "FLIGHT_NO VARCHAR(15), \n" +
                "UP_LOCATION VARCHAR(9), \n" +
                "DIS_LOCATION VARCHAR(9), \n" +
                "CLASS_TYPE VARCHAR(1), \n" +
                "PS_TYPE VARCHAR(15), \n" +
                "FARE_TYPE VARCHAR(18), \n" +
                "AGENT VARCHAR(24), \n" +
                "FARE_TOL DECIMAL(12,2), \n" +
                "SEG_PRICE DECIMAL(12,2), \n" +
                "DIS_PRICE DECIMAL(8,2), \n" +
                "AGT_RATE DECIMAL(8,2), \n" +
                "ZSAL_RATE DECIMAL(8,2), \n" +
                "SEG_FARE DECIMAL(12,2), \n" +
                "SAL_RATE DECIMAL(8,2), \n" +
                "RATE DECIMAL(8,2), \n" +
                "CNAME VARCHAR(60), \n" +
                "AIR_PNR VARCHAR(18), \n" +
                "AGENT_PNR VARCHAR(18), \n" +
                "PNR VARCHAR(18), \n" +
                "PORT_FARE DECIMAL(8,2), \n" +
                "ZSAL_FARE DECIMAL(8,2), \n" +
                "ADD_FARE DECIMAL(8,2), \n" +
                "ORA VARCHAR(60) , \n" +
                "ID DECIMAL(8,2) , \n" +
                "FT_TK_NUM VARCHAR(36), \n" +
                "CP_STR VARCHAR(45), \n" +
                "TK_AGENT VARCHAR(24), \n" +
                "BBCODE VARCHAR(24), \n" +
                "AGT_FARE DECIMAL(8,2), \n" +
                "CLK_NUM VARCHAR(48), \n" +
                "TK_AIR VARCHAR(9), \n" +
                "FARE_RATE DECIMAL(8,4), \n" +
                "CLIENTCODE VARCHAR(45), \n" +
                "STATID VARCHAR(12), \n" +
                "MCAR VARCHAR(12), \n" +
                "TKTYPE VARCHAR(12), \n" +
                "EQFARE DECIMAL(12,2), \n" +
                "SMFARE DECIMAL(12,2), \n" +
                "OBFARE DECIMAL(12,2), \n" +
                "CHTKNUM VARCHAR(39), \n" +
                "GPSTATE VARCHAR(6), \n" +
                "VALAGT_CODE VARCHAR(24), \n" +
                "BGRP VARCHAR(1), \n" +
                "CHG_AIR VARCHAR(9), \n" +
                "CHG_TKNUM VARCHAR(39), \n" +
                "REF_PNR VARCHAR(18), \n" +
                "FPTPCODE VARCHAR(6), \n" +
                "EQFPN DECIMAL(10,2)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_BZSJ_TB_TCN_DATA',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_BZSJ_TB_TCN_DATA(" +
                "ETL_DATE,\n" +
                "ROW_ID,\n" +
                "EX_DATE,\n" +
                "TKS,\n" +
                "TKORD,\n" +
                "TK_NUM,\n" +
                "ORDTP,\n" +
                "TK_DATE,\n" +
                "DEP_DATE,\n" +
                "AIR_CODE,\n" +
                "FLIGHT_NO,\n" +
                "UP_LOCATION,\n" +
                "DIS_LOCATION,\n" +
                "CLASS_TYPE,\n" +
                "PS_TYPE,\n" +
                "FARE_TYPE,\n" +
                "AGENT,\n" +
                "FARE_TOL,\n" +
                "SEG_PRICE,\n" +
                "DIS_PRICE,\n" +
                "AGT_RATE,\n" +
                "ZSAL_RATE,\n" +
                "SEG_FARE,\n" +
                "SAL_RATE,\n" +
                "RATE,\n" +
                "CNAME,\n" +
                "AIR_PNR,\n" +
                "AGENT_PNR,\n" +
                "PNR,\n" +
                "PORT_FARE,\n" +
                "ZSAL_FARE,\n" +
                "ADD_FARE,\n" +
                "ORA,\n" +
                "ID,\n" +
                "FT_TK_NUM,\n" +
                "CP_STR,\n" +
                "TK_AGENT,\n" +
                "BBCODE,\n" +
                "AGT_FARE,\n" +
                "CLK_NUM,\n" +
                "TK_AIR,\n" +
                "FARE_RATE,\n" +
                "CLIENTCODE,\n" +
                "STATID,\n" +
                "MCAR,\n" +
                "TKTYPE,\n" +
                "EQFARE,\n" +
                "SMFARE,\n" +
                "OBFARE,\n" +
                "CHTKNUM,\n" +
                "GPSTATE,\n" +
                "VALAGT_CODE,\n" +
                "BGRP,\n" +
                "CHG_AIR,\n" +
                "CHG_TKNUM,\n" +
                "REF_PNR,\n" +
                "FPTPCODE,\n" +
                "EQFPN)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "EX_DATE||ORA||CAST(ID AS STRING) AS ROW_ID,\n" +
                "EX_DATE,\n" +
                "TKS,\n" +
                "TKORD,\n" +
                "TK_NUM,\n" +
                "ORDTP,\n" +
                "TK_DATE,\n" +
                "DEP_DATE,\n" +
                "AIR_CODE,\n" +
                "FLIGHT_NO,\n" +
                "UP_LOCATION,\n" +
                "DIS_LOCATION,\n" +
                "CLASS_TYPE,\n" +
                "PS_TYPE,\n" +
                "FARE_TYPE,\n" +
                "AGENT,\n" +
                "FARE_TOL,\n" +
                "SEG_PRICE,\n" +
                "DIS_PRICE,\n" +
                "AGT_RATE,\n" +
                "ZSAL_RATE,\n" +
                "SEG_FARE,\n" +
                "SAL_RATE,\n" +
                "RATE,\n" +
                "CNAME,\n" +
                "AIR_PNR,\n" +
                "AGENT_PNR,\n" +
                "PNR,\n" +
                "PORT_FARE,\n" +
                "ZSAL_FARE,\n" +
                "ADD_FARE,\n" +
                "ORA,\n" +
                "ID,\n" +
                "FT_TK_NUM,\n" +
                "CP_STR,\n" +
                "TK_AGENT,\n" +
                "BBCODE,\n" +
                "AGT_FARE,\n" +
                "CLK_NUM,\n" +
                "TK_AIR,\n" +
                "FARE_RATE,\n" +
                "CLIENTCODE,\n" +
                "STATID,\n" +
                "MCAR,\n" +
                "TKTYPE,\n" +
                "EQFARE,\n" +
                "SMFARE,\n" +
                "OBFARE,\n" +
                "CHTKNUM,\n" +
                "GPSTATE,\n" +
                "VALAGT_CODE,\n" +
                "BGRP,\n" +
                "CHG_AIR,\n" +
                "CHG_TKNUM,\n" +
                "REF_PNR,\n" +
                "FPTPCODE,\n" +
                "EQFPN\n" +
                " FROM TB_TCN_DATA "
                + " WHERE  EX_DATE >=  '" + startDate + "' AND EX_DATE <= '" + endDate + "' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
