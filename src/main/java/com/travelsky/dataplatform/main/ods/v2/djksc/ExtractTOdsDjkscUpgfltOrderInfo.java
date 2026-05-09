package com.travelsky.dataplatform.main.ods.v2.djksc;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.OracleAESDecryptor;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.UUID;

/**
 * @author
 * @date 2025/7/2 9:31
 */
public class ExtractTOdsDjkscUpgfltOrderInfo {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
//        String etlDate = "2024-03-03";
        String startDate = etlDate;
        String endDate = etlDate;
        if (args.length > 2) {
            startDate = args[1];
            endDate = args[2];
        }
        System.out.println("ExtractTOdsDjkscUpgfltOrderInfo etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsDjkscUpgfltOrderInfo");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE UPGFLT_ORDER_INFO (\n" +
                "USER_ID VARCHAR(96),\n" +
                "FLIGHT_NO VARCHAR(30),\n" +
                "FLIGHT_DATE TIMESTAMP(6),\n" +
                "FLIGHT_TIME VARCHAR(60),\n" +
                "ORGCITY_NAME VARCHAR(90),\n" +
                "DSTCITY_NAME VARCHAR(90),\n" +
                "UPGRADE_BEGIN_TIME VARCHAR(90),\n" +
                "UPGRADE_END_TIME VARCHAR(90),\n" +
                "PSR_NAME VARCHAR(150),\n" +
                "CARD_NO VARCHAR(6000),\n" +
                "CARD_TYPE VARCHAR(90),\n" +
                "CONTACT_INFO VARCHAR(6000),\n" +
                "PSR_LEVEL VARCHAR(30),\n" +
                "FFP_NO VARCHAR(90),\n" +
                "PNR_NO VARCHAR(30),\n" +
                "TICKET_NO VARCHAR(90),\n" +
                "TOUR_INDEX VARCHAR(60),\n" +
                "CURRENT_CLASS VARCHAR(15),\n" +
                "CURRENT_SEAT VARCHAR(30),\n" +
                "UPCLASS_INFO VARCHAR(90),\n" +
                "ERROR_REASON VARCHAR(6000),\n" +
                "ERROR_CODE VARCHAR(90),\n" +
                "IS_PUSH VARCHAR(15),\n" +
                "CREATE_DATE TIMESTAMP(6),\n" +
                "ORGCITY_CODE VARCHAR(90),\n" +
                "DSTCITY_CODE VARCHAR(90),\n" +
                "ENABLE CHAR(1),\n" +
                "CREATE_TIME TIMESTAMP(6),\n" +
                "CREATE_USER VARCHAR(768),\n" +
                "UPDATE_TIME TIMESTAMP(6),\n" +
                "UPDATE_USER VARCHAR(768),\n" +
                "ORG_AIRPORT_NAME VARCHAR(300),\n" +
                "DST_AIRPORT_NAME VARCHAR(300),\n" +
                "BOARDING_GATE_NUMBER VARCHAR(300),\n" +
                "BORDING_TIME VARCHAR(300)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.DJKSC_IP+":"+Constants.DJKSC_PORT+"/"+Constants.DJKSC_DB+"',\n" +
                "    'table-name' = '"+Constants.DJKSC_SCHEMA+".UPGFLT_ORDER_INFO', \n" +
                "    'username' = '"+Constants.DJKSC_USER+"',\n" +
                "    'password' = '"+Constants.DJKSC_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_DJKSC_UPGFLT_ORDER_INFO (\n" +
                "    ETL_DATE DATE,\n" +
                "USER_ID VARCHAR(96),\n" +
                "FLIGHT_NO VARCHAR(30),\n" +
                "FLIGHT_DATE TIMESTAMP(6),\n" +
                "FLIGHT_TIME VARCHAR(60),\n" +
                "ORGCITY_NAME VARCHAR(90),\n" +
                "DSTCITY_NAME VARCHAR(90),\n" +
                "UPGRADE_BEGIN_TIME VARCHAR(90),\n" +
                "UPGRADE_END_TIME VARCHAR(90),\n" +
                "PSR_NAME VARCHAR(150),\n" +
                "CARD_NO VARCHAR(6000),\n" +
                "CARD_TYPE VARCHAR(90),\n" +
                "CONTACT_INFO VARCHAR(6000),\n" +
                "PSR_LEVEL VARCHAR(30),\n" +
                "FFP_NO VARCHAR(90),\n" +
                "PNR_NO VARCHAR(30),\n" +
                "TICKET_NO VARCHAR(90),\n" +
                "TOUR_INDEX VARCHAR(60),\n" +
                "CURRENT_CLASS VARCHAR(15),\n" +
                "CURRENT_SEAT VARCHAR(30),\n" +
                "UPCLASS_INFO VARCHAR(90),\n" +
                "ERROR_REASON VARCHAR(6000),\n" +
                "ERROR_CODE VARCHAR(90),\n" +
                "IS_PUSH VARCHAR(15),\n" +
                "CREATE_DATE TIMESTAMP(6),\n" +
                "ORGCITY_CODE VARCHAR(90),\n" +
                "DSTCITY_CODE VARCHAR(90),\n" +
                "ENABLE CHAR(1),\n" +
                "CREATE_TIME TIMESTAMP(6),\n" +
                "CREATE_USER VARCHAR(768),\n" +
                "UPDATE_TIME TIMESTAMP(6),\n" +
                "UPDATE_USER VARCHAR(768),\n" +
                "ORG_AIRPORT_NAME VARCHAR(300),\n" +
                "DST_AIRPORT_NAME VARCHAR(300),\n" +
                "BOARDING_GATE_NUMBER VARCHAR(300),\n" +
                "BORDING_TIME VARCHAR(300)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_DJKSC_UPGFLT_ORDER_INFO',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_DJKSC_UPGFLT_ORDER_INFO(" +
                "ETL_DATE,\n" +
                "USER_ID,\n" +
                "FLIGHT_NO,\n" +
                "FLIGHT_DATE,\n" +
                "FLIGHT_TIME,\n" +
                "ORGCITY_NAME,\n" +
                "DSTCITY_NAME,\n" +
                "UPGRADE_BEGIN_TIME,\n" +
                "UPGRADE_END_TIME,\n" +
                "PSR_NAME,\n" +
                "CARD_NO,\n" +
                "CARD_TYPE,\n" +
                "CONTACT_INFO,\n" +
                "PSR_LEVEL,\n" +
                "FFP_NO,\n" +
                "PNR_NO,\n" +
                "TICKET_NO,\n" +
                "TOUR_INDEX,\n" +
                "CURRENT_CLASS,\n" +
                "CURRENT_SEAT,\n" +
                "UPCLASS_INFO,\n" +
                "ERROR_REASON,\n" +
                "ERROR_CODE,\n" +
                "IS_PUSH,\n" +
                "CREATE_DATE,\n" +
                "ORGCITY_CODE,\n" +
                "DSTCITY_CODE,\n" +
                "ENABLE,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "ORG_AIRPORT_NAME,\n" +
                "DST_AIRPORT_NAME,\n" +
                "BOARDING_GATE_NUMBER,\n" +
                "BORDING_TIME)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "USER_ID,\n" +
                "FLIGHT_NO,\n" +
                "FLIGHT_DATE,\n" +
                "FLIGHT_TIME,\n" +
                "ORGCITY_NAME,\n" +
                "DSTCITY_NAME,\n" +
                "UPGRADE_BEGIN_TIME,\n" +
                "UPGRADE_END_TIME,\n" +
                "PSR_NAME,\n" +
                "CARD_NO,\n" +
                "CARD_TYPE,\n" +
                "CONTACT_INFO,\n" +
                "PSR_LEVEL,\n" +
                "FFP_NO,\n" +
                "PNR_NO,\n" +
                "TICKET_NO,\n" +
                "TOUR_INDEX,\n" +
                "CURRENT_CLASS,\n" +
                "CURRENT_SEAT,\n" +
                "UPCLASS_INFO,\n" +
                "ERROR_REASON,\n" +
                "ERROR_CODE,\n" +
                "IS_PUSH,\n" +
                "CREATE_DATE,\n" +
                "ORGCITY_CODE,\n" +
                "DSTCITY_CODE,\n" +
                "ENABLE,\n" +
                "CREATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_TIME,\n" +
                "UPDATE_USER,\n" +
                "ORG_AIRPORT_NAME,\n" +
                "DST_AIRPORT_NAME,\n" +
                "BOARDING_GATE_NUMBER,\n" +
                "BORDING_TIME\n" +
                " FROM UPGFLT_ORDER_INFO "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
