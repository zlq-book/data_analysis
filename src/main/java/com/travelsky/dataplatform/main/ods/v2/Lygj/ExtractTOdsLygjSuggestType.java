package com.travelsky.dataplatform.main.ods.v2.Lygj;

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
 * 提取建议类型数据到ODS层
 * 对应源表：SUGGEST_TYPE
 * 目标表：T_ODS_LYGJ_SUGGEST_TYPE
 * 第一次全取，后续每天更新导入INPUT_TIME是前一天的，以及UPDATE_TIME是前一天的数据
 */
public class ExtractTOdsLygjSuggestType {

    public static void main(String[] args) throws IOException {
        // 检查ETL日期参数是否为空
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

        // 创建Flink流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLygjSuggestType");
        env.setParallelism(1); // 设置并行度为1

        // 创建流表环境
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 生成时间戳和UUID用于标识
        long timestamp = System.currentTimeMillis(); // 获取当前时间戳
        UUID uuid = UUID.randomUUID(); // 生成随机UUID

        // 注册SM4加密UDF函数
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF函数
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);

        // 创建上游ORACLE数据源表 - SUGGEST_TYPE
        tEnv.executeSql("CREATE TABLE SUGGEST_TYPE (\n" +
                "    ID              BIGINT,                    -- 主键ID\n" +
                "    COMPLAINT_NAME  VARCHAR(300),              -- 投诉名称\n" +
                "    `DESCRIBE`        VARCHAR(2000),             -- 描述信息\n" +
                "    VALID           BIGINT,                    -- 有效标识\n" +
                "    INPUT_PEOPLE    VARCHAR(30),               -- 录入人\n" +
                "    INPUT_TIME      TIMESTAMP(6),              -- 录入时间\n" +
                "    UPDATE_PEOPLE   VARCHAR(30),               -- 更新人\n" +
                "    UPDATE_TIME     TIMESTAMP(6)               -- 更新时间\n" +

                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.LYGJ_IP + ":" + Constants.LYGJ_PORT + "/" + Constants.LYGJ_DB + "',\n" +
                "    'table-name' = '" + Constants.LYGJ_SCHEMA + ".SUGGEST_TYPE', \n" +
                "    'username' = '" + Constants.LYGJ_USER + "',\n" +
                "    'password' = '" + Constants.LYGJ_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");

        // 创建Doris目标表 - T_ODS_LYGJ_SUGGEST_TYPE
        tEnv.executeSql("CREATE TABLE T_ODS_LYGJ_SUGGEST_TYPE (\n" +
                "    ID              BIGINT,                    -- 主键ID\n" +
                "    COMPLAINT_NAME  VARCHAR(300),              -- 投诉名称\n" +
                "    `DESCRIBE`        VARCHAR(2000),             -- 描述信息\n" +
                "    VALID           BIGINT,                    -- 有效标识\n" +
                "    INPUT_PEOPLE    VARCHAR(30),               -- 录入人\n" +
                "    INPUT_TIME      TIMESTAMP(6),              -- 录入时间\n" +
                "    UPDATE_PEOPLE   VARCHAR(30),               -- 更新人\n" +
                "    UPDATE_TIME     TIMESTAMP(6),              -- 更新时间\n" +
                "    ETL_DATE        DATE                       -- ETL日期\n" +
                //") WITH (\n" +
                //"    'connector' = 'jdbc',                      -- 使用JDBC连接器\n" +
                //"    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
                //"    'table-name' = 'T_ODS_LYGJ_SUGGEST_TYPE',\n" +
                //"    'username' = '" + Constants.ODS_USER + "',\n" +
                //"    'password' = '" + Constants.ODS_PWD + "'\n" +
                //",\n" +
                //"    'driver' = '" + Constants.MYSQL_DRIVER + "'" +
                //")");
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "  'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "',\n" +
                "  'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "',\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYGJ_SUGGEST_TYPE',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        // 构建数据提取SQL语句
        String extractSql = "INSERT INTO T_ODS_LYGJ_SUGGEST_TYPE (" +
                "    ETL_DATE,           -- ETL日期\n" +
                "    ID,                 -- 主键ID\n" +
                "    COMPLAINT_NAME,     -- 投诉名称\n" +
                "    `DESCRIBE`,           -- 描述信息\n" +
                "    VALID,              -- 有效标识\n" +
                "    INPUT_PEOPLE,       -- 录入人\n" +
                "    INPUT_TIME,         -- 录入时间\n" +
                "    UPDATE_PEOPLE,      -- 更新人\n" +
                "    UPDATE_TIME         -- 更新时间\n" +
                ")  " +
                "SELECT \n" +
                "    CAST('" + etlDate + "' AS DATE) ETL_DATE,    -- 转换ETL日期为DATE类型\n" +
                "    ID,                                         -- 主键ID\n" +
                "    COMPLAINT_NAME,                             -- 投诉名称\n" +
                "    `DESCRIBE`,                                   -- 描述信息\n" +
                "    VALID,                                      -- 有效标识\n" +
                "    INPUT_PEOPLE,                               -- 录入人\n" +
                "    INPUT_TIME,                                 -- 录入时间\n" +
                "    UPDATE_PEOPLE,                              -- 更新人\n" +
                "    UPDATE_TIME                                 -- 更新时间\n" +
                " FROM SUGGEST_TYPE "
                + " WHERE  INPUT_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;

        // 执行数据提取SQL
        TableResult result = tEnv.executeSql(extractSql);

        // 打印执行结果
        result.print();
    }
}
