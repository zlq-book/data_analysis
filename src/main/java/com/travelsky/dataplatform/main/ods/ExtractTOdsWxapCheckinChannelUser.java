package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.KerberosAuthUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;
import java.io.IOException;
import java.util.UUID;

/**
 * @author kuangaihua
 * @date 2025/6/26 16:29
 */
public class ExtractTOdsWxapCheckinChannelUser {
    private static final Logger logger = LoggerFactory.getLogger(ExtractTOdsHytdSysRegister.class);

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsWxapCheckinChannelUser");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE CHECKIN_CHANNEL_USER (\n" +
                "    ID BIGINT NOT NULL,\n" +
                "    SYPR_ID BIGINT NOT NULL,\n" +
                "    USER_ID VARCHAR(96),\n" +
                "    OPENID VARCHAR(96),\n" +
                "    UNIONID VARCHAR(96),\n" +
                "    CHECKIN_CHANNEL TINYINT NOT NULL,\n" +
                "    DEL_STATUS TINYINT,\n" +
                "    CREATE_TIME TIMESTAMP(6),\n" +
                "    UPDATE_TIME TIMESTAMP(6),\n" +
                "    CREATE_BY VARCHAR(192),\n" +
                "    UPDATE_BY VARCHAR(192),\n" +
                "    MAIN_OPERATOR TINYINT" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+ Constants.WXAP_IP+":"+Constants.WXAP_PORT+"/"+Constants.WXAP_DB+"',\n" +
                "    'table-name' = '"+Constants.WXAP_SCHEMA+".CHECKIN_CHANNEL_USER', \n" +
                "    'username' = '"+Constants.WXAP_USER+"',\n" +
                "    'password' = '"+Constants.WXAP_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_WXAP_CHECKIN_CHANNEL_USER (\n" +
                "    ETL_DATE DATE,\n" +
                "    ID BIGINT NOT NULL,\n" +
                "    SYPR_ID BIGINT NOT NULL,\n" +
                "    USER_ID VARCHAR(96),\n" +
                "    OPENID VARCHAR(96),\n" +
                "    UNIONID VARCHAR(96),\n" +
                "    CHECKIN_CHANNEL TINYINT NOT NULL,\n" +
                "    DEL_STATUS TINYINT,\n" +
                "    CREATE_TIME TIMESTAMP(6),\n" +
                "    UPDATE_TIME TIMESTAMP(6),\n" +
                "    CREATE_BY VARCHAR(192),\n" +
                "    UPDATE_BY VARCHAR(192),\n" +
                "    MAIN_OPERATOR TINYINT" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_WXAP_CHECKIN_CHANNEL_USER',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        String extractSql = "INSERT INTO T_ODS_WXAP_CHECKIN_CHANNEL_USER(" +
                "ETL_DATE\n" +
                ",ID\n" +
                ",SYPR_ID\n" +
                ",USER_ID\n" +
                ",OPENID\n" +
                ",UNIONID\n" +
                ",CHECKIN_CHANNEL\n" +
                ",DEL_STATUS\n" +
                ",CREATE_TIME\n" +
                ",UPDATE_TIME\n" +
                ",CREATE_BY\n" +
                ",UPDATE_BY\n" +
                ",MAIN_OPERATOR\n)  " +
                "SELECT \n" +
                "DATE '"+etlDate+"' AS ETL_DATE" +
                ",ID\n" +
                ",SYPR_ID\n" +
                ",USER_ID\n" +
                ",OPENID\n" +
                ",UNIONID\n" +
                ",CHECKIN_CHANNEL\n" +
                ",DEL_STATUS\n" +
                ",CREATE_TIME\n" +
                ",UPDATE_TIME\n" +
                ",CREATE_BY\n" +
                ",UPDATE_BY\n" +
                ",MAIN_OPERATOR" +
                " FROM CHECKIN_CHANNEL_USER "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '"+etlDate+" 00:00:00' AND TIMESTAMP '"+etlDate+" 23:59:59.999' " +
                " OR  UPDATE_TIME BETWEEN TIMESTAMP '"+etlDate+" 00:00:00' AND TIMESTAMP '"+etlDate+" 23:59:59.999' "
                ;
        logger.info("extractSql:"+extractSql);
        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }

}
