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
 * @author
 * 定期全量拉取
 * @date 2025/6/26 16:29
 */
public class ExtractTOdsLygjVipType {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLygjVipType");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE VIP_TYPE (\n" +
                        "    ID BIGINT NOT NULL COMMENT 'VIP类型ID',\n" +
                        "    CODE VARCHAR(48) NOT NULL COMMENT 'VIP类型编码',\n" +
                        "    NAME VARCHAR(192) NOT NULL COMMENT 'VIP类型名称',\n" +
                        "    MEMO VARCHAR(3072) COMMENT '备注',\n" +
                        "    BOOK_TICKET VARCHAR(3) COMMENT '订票标识',\n" +
                        "    PROVIDE_FOOD VARCHAR(3) COMMENT '提供餐饮标识',\n" +
                        "    SENT_CAR VARCHAR(3) COMMENT '派车标识',\n" +
                        "    BOARD_CHECK VARCHAR(3) COMMENT '登机检查标识',\n" +
                        "    RECEIVED_MAN VARCHAR(3) COMMENT '接待人员标识',\n" +
                        "    VISITANT_ROOM VARCHAR(3) COMMENT '贵宾室标识',\n" +
                        "    CABIN_SERVICE VARCHAR(3) COMMENT '客舱服务标识',\n" +
                        "    RECEIVED_CAR VARCHAR(3) COMMENT '接车标识',\n" +
                        "    RECEIVED_OFFICE VARCHAR(3) COMMENT '接待办公室标识',\n" +
                        "    OTHER VARCHAR(3) COMMENT '其他服务标识',\n" +
                        "    BL1 VARCHAR(180) COMMENT '保留字段1',\n" +
                        "    BL2 VARCHAR(180) COMMENT '保留字段2',\n" +
                        "    BL3 VARCHAR(180) COMMENT '保留字段3',\n" +
                        "    BL4 VARCHAR(60) COMMENT '保留字段4',\n" +
                        "    BL5 VARCHAR(60) COMMENT '保留字段5',\n" +
                        "    BL6 VARCHAR(60) COMMENT '保留字段6',\n" +
                        "    BL7 VARCHAR(60) COMMENT '保留字段7',\n" +
                        "    BL8 VARCHAR(60) COMMENT '保留字段8',\n" +
                        "    IS_VIP VARCHAR(60) COMMENT '是否VIP标识',\n" +
                        "    REAL_NAME_FLAG VARCHAR(6) COMMENT '实名认证标志'" +

                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.LYGJ_IP + ":" + Constants.LYGJ_PORT + "/" + Constants.LYGJ_DB + "',\n" +
                "    'table-name' = '" + Constants.LYGJ_SCHEMA + ".VIP_TYPE', \n" +
                "    'username' = '" + Constants.LYGJ_USER + "',\n" +
                "    'password' = '" + Constants.LYGJ_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_LYGJ_VIP_TYPE (\n" +
                "    ETL_DATE DATE NOT NULL  COMMENT '数据ETL日期',\n" +
                "    ID BIGINT NOT NULL COMMENT 'VIP类型ID',\n" +
                "    CODE VARCHAR(48) NOT NULL COMMENT 'VIP类型编码',\n" +
                "    NAME VARCHAR(192) NOT NULL COMMENT 'VIP类型名称',\n" +
                "    MEMO VARCHAR(3072) COMMENT '备注',\n" +
                "    BOOK_TICKET VARCHAR(3) COMMENT '订票标识',\n" +
                "    PROVIDE_FOOD VARCHAR(3) COMMENT '提供餐饮标识',\n" +
                "    SENT_CAR VARCHAR(3) COMMENT '派车标识',\n" +
                "    BOARD_CHECK VARCHAR(3) COMMENT '登机检查标识',\n" +
                "    RECEIVED_MAN VARCHAR(3) COMMENT '接待人员标识',\n" +
                "    VISITANT_ROOM VARCHAR(3) COMMENT '贵宾室标识',\n" +
                "    CABIN_SERVICE VARCHAR(3) COMMENT '客舱服务标识',\n" +
                "    RECEIVED_CAR VARCHAR(3) COMMENT '接车标识',\n" +
                "    RECEIVED_OFFICE VARCHAR(3) COMMENT '接待办公室标识',\n" +
                "    OTHER VARCHAR(3) COMMENT '其他服务标识',\n" +
                "    BL1 VARCHAR(180) COMMENT '保留字段1',\n" +
                "    BL2 VARCHAR(180) COMMENT '保留字段2',\n" +
                "    BL3 VARCHAR(180) COMMENT '保留字段3',\n" +
                "    BL4 VARCHAR(60) COMMENT '保留字段4',\n" +
                "    BL5 VARCHAR(60) COMMENT '保留字段5',\n" +
                "    BL6 VARCHAR(60) COMMENT '保留字段6',\n" +
                "    BL7 VARCHAR(60) COMMENT '保留字段7',\n" +
                "    BL8 VARCHAR(60) COMMENT '保留字段8',\n" +
                "    IS_VIP VARCHAR(60) COMMENT '是否VIP标识',\n" +
                "    REAL_NAME_FLAG VARCHAR(6) COMMENT '实名认证标志'" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYGJ_VIP_TYPE',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
                //") WITH (\n" +
                //"    'connector' = 'jdbc',\n" +
                //"    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
                //"    'table-name' = 'T_ODS_LYGJ_VIP_TYPE', -- 替换为实际的表名\n" +
                //"    'username' = '"+Constants.ODS_USER+"',\n" +
                //"    'password' = '"+Constants.ODS_PWD+"'\n" +
                //",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
                //")");
        String extractSql = "INSERT INTO T_ODS_LYGJ_VIP_TYPE(" +
                "    ETL_DATE,\n" +
                "    ID,\n" +
                "    CODE,\n" +
                "    NAME,\n" +
                "    MEMO,\n" +
                "    BOOK_TICKET,\n" +
                "    PROVIDE_FOOD,\n" +
                "    SENT_CAR,\n" +
                "    BOARD_CHECK,\n" +
                "    RECEIVED_MAN,\n" +
                "    VISITANT_ROOM,\n" +
                "    CABIN_SERVICE,\n" +
                "    RECEIVED_CAR,\n" +
                "    RECEIVED_OFFICE,\n" +
                "    OTHER,\n" +
                "    BL1,\n" +
                "    BL2,\n" +
                "    BL3,\n" +
                "    BL4,\n" +
                "    BL5,\n" +
                "    BL6,\n" +
                "    BL7,\n" +
                "    BL8,\n" +
                "    IS_VIP,\n" +
                "    REAL_NAME_FLAG\n)  " +

                "SELECT \n" +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE" +
                ",ID,\n" +
                "    CODE,\n" +
                "    NAME,\n" +
                "    MEMO,\n" +
                "    BOOK_TICKET,\n" +
                "    PROVIDE_FOOD,\n" +
                "    SENT_CAR,\n" +
                "    BOARD_CHECK,\n" +
                "    RECEIVED_MAN,\n" +
                "    VISITANT_ROOM,\n" +
                "    CABIN_SERVICE,\n" +
                "    RECEIVED_CAR,\n" +
                "    RECEIVED_OFFICE,\n" +
                "    OTHER,\n" +
                "    BL1,\n" +
                "    BL2,\n" +
                "    BL3,\n" +
                "    BL4,\n" +
                "    BL5,\n" +
                "    BL6,\n" +
                "    BL7,\n" +
                "    BL8,\n" +
                "    IS_VIP,\n" +
                "    REAL_NAME_FLAG" +
                " FROM VIP_TYPE ";
        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }

}
