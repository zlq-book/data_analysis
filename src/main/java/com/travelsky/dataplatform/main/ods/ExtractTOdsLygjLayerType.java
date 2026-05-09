package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.UUID;

/**
 * @author kuangaihua
 * @date 2025/6/26 16:29
 */
public class ExtractTOdsLygjLayerType {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLygjLayerType");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE LAYER_TYPE (\n" +

                "ID BIGINT NOT NULL COMMENT '唯一标识符',\n" +
                        "    TYPE_NAME VARCHAR(192) NOT NULL COMMENT '类型名称',\n" +
                        "    VIP_TYPE_ID BIGINT COMMENT 'VIP类型ID',\n" +
                        "    PRIORITY BIGINT NOT NULL COMMENT '优先级',\n" +
                        "    CREATE_TM TIMESTAMP(6) COMMENT '创建时间',\n" +
                        "    CREATOR VARCHAR(384) COMMENT '创建人',\n" +
                        "    CANCEL_FLAG VARCHAR(6) COMMENT '取消标志',\n" +
                        "    UPDATE_TM TIMESTAMP(6) COMMENT '更新时间',\n" +
                        "    UPDATER VARCHAR(384) COMMENT '更新人',\n" +
                        "    MEMO VARCHAR(3072) COMMENT '备注',\n" +
                        "    PHONE_REMINDER VARCHAR(6) COMMENT '电话提醒标志',\n" +
                        "    CHANGE_SMS_FLAG VARCHAR(6) COMMENT '变更短信通知标志',\n" +
                        "    REAL_NAME_FLAG VARCHAR(6) COMMENT '实名认证标志'" +

                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.LYGJ_IP + ":" + Constants.LYGJ_PORT + "/" + Constants.LYGJ_DB + "',\n" +
                "    'table-name' = '" + Constants.LYGJ_SCHEMA + ".LAYER_TYPE', \n" +
                "    'username' = '" + Constants.LYGJ_USER + "',\n" +
                "    'password' = '" + Constants.LYGJ_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_LYGJ_LAYER_TYPE (\n" +
                "   ETL_DATE DATE NOT NULL  COMMENT '数据ETL日期',\n" +
                        "    ID BIGINT NOT NULL COMMENT '唯一标识符',\n" +
                        "    TYPE_NAME VARCHAR(192) NOT NULL COMMENT '类型名称',\n" +
                        "    VIP_TYPE_ID BIGINT COMMENT 'VIP类型ID',\n" +
                        "    PRIORITY BIGINT NOT NULL COMMENT '优先级',\n" +
                        "    CREATE_TM TIMESTAMP(6) COMMENT '创建时间',\n" +
                        "    CREATOR VARCHAR(384) COMMENT '创建人',\n" +
                        "    CANCEL_FLAG VARCHAR(6) COMMENT '取消标志',\n" +
                        "    UPDATE_TM TIMESTAMP(6) COMMENT '更新时间',\n" +
                        "    UPDATER VARCHAR(384) COMMENT '更新人',\n" +
                        "    MEMO VARCHAR(3072) COMMENT '备注',\n" +
                        "    PHONE_REMINDER VARCHAR(6) COMMENT '电话提醒标志',\n" +
                        "    CHANGE_SMS_FLAG VARCHAR(6) COMMENT '变更短信通知标志',\n" +
                        "    REAL_NAME_FLAG VARCHAR(6) COMMENT '实名认证标志'" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYGJ_LAYER_TYPE',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
                //") WITH (\n" +
                //"    'connector' = 'jdbc',\n" +
                //"    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
                //"    'table-name' = 'T_ODS_LYGJ_LAYER_TYPE', -- 替换为实际的表名\n" +
                //"    'username' = '"+Constants.ODS_USER+"',\n" +
                //"    'password' = '"+Constants.ODS_PWD+"'\n" +
                //",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
                //")");
        String extractSql = "INSERT INTO T_ODS_LYGJ_LAYER_TYPE(" +
                "ETL_DATE,\n" +
                "    ID,\n" +
                "    TYPE_NAME,\n" +
                "    VIP_TYPE_ID,\n" +
                "    PRIORITY,\n" +
                "    CREATE_TM,\n" +
                "    CREATOR,\n" +
                "    CANCEL_FLAG,\n" +
                "    UPDATE_TM,\n" +
                "    UPDATER,\n" +
                "    MEMO,\n" +
                "    PHONE_REMINDER,\n" +
                "    CHANGE_SMS_FLAG,\n" +
                "    REAL_NAME_FLAG\n)  " +
                "SELECT \n" +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE" +
                "    ,ID,\n" +
                "    TYPE_NAME,\n" +
                "    VIP_TYPE_ID,\n" +
                "    PRIORITY,\n" +
                "    CREATE_TM,\n" +
                "    CREATOR,\n" +
                "    CANCEL_FLAG,\n" +
                "    UPDATE_TM,\n" +
                "    UPDATER,\n" +
                "    MEMO,\n" +
                "    PHONE_REMINDER,\n" +
                "    CHANGE_SMS_FLAG,\n" +
                "    REAL_NAME_FLAG" +
                " FROM LAYER_TYPE "
                + " WHERE  CREATE_TM BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TM BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }

}
