package com.travelsky.dataplatform.main.ods.v2.Jssc;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.OracleAESDecryptor;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.time.LocalDate;
import java.util.UUID;

/**
 * *按照PUBLISHTIME时间，更新过去一年的更新到ODS，过去一年时间更新DWD？
 */
public class ExtractTOdsJsscTbUpgradeTicket {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        // 获取一年前日期
        // 将字符串转换为 LocalDate
        LocalDate currentDate = LocalDate.parse(etlDate);
        // 计算一年前的日期
        LocalDate oneYearAgo = currentDate.minusYears(1);
        // 格式化为字符串
        String oneYearAgoStr = oneYearAgo.toString();
        String startDate = oneYearAgoStr;
        String endDate = etlDate;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsJsscTbUpgradeTicket");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE TB_UPGRADE_TICKET (\n" +
                "    ID               BIGINT,\n" +
                "    AMOUNT           BIGINT,\n" +
                "    AREATYPE         BIGINT,\n" +
                "    BOOKID           BIGINT,\n" +
                "    COUNTTYPE        BIGINT,\n" +
                "    GIFMESSAGE       VARCHAR(255),\n" +
                "    NUMBERS          VARCHAR(90),\n" +
                "    PASSWORDS        VARCHAR(90),\n" +
                "    PUBLISHTIME      TIMESTAMP(6),\n" +
                "    SELLTIME         TIMESTAMP(6),\n" +
                "    STATUS           BIGINT,\n" +
                "    VALIDDATE        TIMESTAMP(6),\n" +
                "    AREA_ID          BIGINT,\n" +
                "    GROUP_ID         BIGINT,\n" +
                "    ORDER_ID         BIGINT,\n" +
                "    USER_ID          BIGINT,\n" +
                "    USETIME          TIMESTAMP(6),\n" +
                "    LOCKTIME         TIMESTAMP(6),\n" +
                "    LOCK_USER_ID     BIGINT,\n" +
                "    ISAVAILABLESEG   BIGINT,\n" +
                "    PLATFORM         VARCHAR(255),\n" +
                "    PRODUCTNO        VARCHAR(255),\n" +
                "    REMARK           VARCHAR(3000),\n" +
                "    VALIDMONTH       BIGINT,\n" +
                "    USECHANNEL       VARCHAR(255),\n" +
                "    ORGANIZE_ID      BIGINT,\n" +
                "    EMDNO            VARCHAR(90),\n" +
                "    USE_USER_ID      BIGINT,\n" +
                "    DISTANCE         BIGINT,\n" +
                "    PRODTYPE         BIGINT,\n" +
                "    PRODUCTNAME      VARCHAR(255),\n" +
                "    TICKETPRODUCT_ID BIGINT,\n" +
                "    CHECKCODE        VARCHAR(255),\n" +
                "    BIND_USER        BIGINT,\n" +
                "    BIND_USER_ID     BIGINT,\n" +
                "    REMOVEBINDDATE   TIMESTAMP(6),\n" +
                "    FROMCARTORSELL   BIGINT,\n" +
                "    QRCODEPATH       VARCHAR(1000),\n" +
                "    DISTANCETYPE     VARCHAR(255),\n" +
                "    BUSINESSTYPE     VARCHAR(90),\n" +
                "    SYNCHROSTATUS    BIGINT,\n" +
                "    REMARKQ          VARCHAR(1000)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.JSSC_IP + ":" + Constants.JSSC_PORT + "/" + Constants.JSSC_DB + "',\n" +
                "    'table-name' = '" + Constants.JSSC_SCHEMA + ".TB_UPGRADE_TICKET', \n" +
                "    'username' = '" + Constants.JSSC_USER + "',\n" +
                "    'password' = '" + Constants.JSSC_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_JSSC_TB_UPGRADE_TICKET (\n" +
                "    ID               BIGINT,\n" +
                "    AMOUNT           BIGINT,\n" +
                "    AREATYPE         BIGINT,\n" +
                "    BOOKID           BIGINT,\n" +
                "    COUNTTYPE        BIGINT,\n" +
                "    GIFMESSAGE       VARCHAR(255),\n" +
                "    NUMBERS          VARCHAR(90),\n" +
                "    PASSWORDS        VARCHAR(90),\n" +
                "    PUBLISHTIME      TIMESTAMP(6),\n" +
                "    SELLTIME         TIMESTAMP(6),\n" +
                "    STATUS           BIGINT,\n" +
                "    VALIDDATE        TIMESTAMP(6),\n" +
                "    AREA_ID          BIGINT,\n" +
                "    GROUP_ID         BIGINT,\n" +
                "    ORDER_ID         BIGINT,\n" +
                "    USER_ID          BIGINT,\n" +
                "    USETIME          TIMESTAMP(6),\n" +
                "    LOCKTIME         TIMESTAMP(6),\n" +
                "    LOCK_USER_ID     BIGINT,\n" +
                "    ISAVAILABLESEG   BIGINT,\n" +
                "    PLATFORM         VARCHAR(255),\n" +
                "    PRODUCTNO        VARCHAR(255),\n" +
                "    REMARK           VARCHAR(3000),\n" +
                "    VALIDMONTH       BIGINT,\n" +
                "    USECHANNEL       VARCHAR(255),\n" +
                "    ORGANIZE_ID      BIGINT,\n" +
                "    EMDNO            VARCHAR(90),\n" +
                "    USE_USER_ID      BIGINT,\n" +
                "    DISTANCE         BIGINT,\n" +
                "    PRODTYPE         BIGINT,\n" +
                "    PRODUCTNAME      VARCHAR(255),\n" +
                "    TICKETPRODUCT_ID BIGINT,\n" +
                "    CHECKCODE        VARCHAR(255),\n" +
                "    BIND_USER        BIGINT,\n" +
                "    BIND_USER_ID     BIGINT,\n" +
                "    REMOVEBINDDATE   TIMESTAMP(6),\n" +
                "    FROMCARTORSELL   BIGINT,\n" +
                "    QRCODEPATH       VARCHAR(1000),\n" +
                "    DISTANCETYPE     VARCHAR(255),\n" +
                "    BUSINESSTYPE     VARCHAR(90),\n" +
                "    SYNCHROSTATUS    BIGINT,\n" +
                "    REMARKQ          VARCHAR(1000)," +
                "    ETL_DATE DATE" +
                //") WITH (\n" +
                //"    'connector' = 'jdbc',\n" +
                //"    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
                //"    'table-name' = 'T_ODS_JSSC_TB_UPGRADE_TICKET',\n" +
                //"    'username' = '" + Constants.ODS_USER + "',\n" +
                //"    'password' = '" + Constants.ODS_PWD + "'\n" +
                //",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
                //")");
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "  'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "',\n" +
                "  'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "',\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_JSSC_TB_UPGRADE_TICKET',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        String extractSql = "INSERT INTO T_ODS_JSSC_TB_UPGRADE_TICKET (" +
                "    ETL_DATE,\n" +
                "    ID,\n" +
                "    AMOUNT,\n" +
                "    AREATYPE,\n" +
                "    BOOKID,\n" +
                "    COUNTTYPE,\n" +
                "    GIFMESSAGE,\n" +
                "    NUMBERS,\n" +
                "    PASSWORDS,\n" +
                "    PUBLISHTIME,\n" +
                "    SELLTIME,\n" +
                "    STATUS,\n" +
                "    VALIDDATE,\n" +
                "    AREA_ID,\n" +
                "    GROUP_ID,\n" +
                "    ORDER_ID,\n" +
                "    USER_ID,\n" +
                "    USETIME,\n" +
                "    LOCKTIME,\n" +
                "    LOCK_USER_ID,\n" +
                "    ISAVAILABLESEG,\n" +
                "    PLATFORM,\n" +
                "    PRODUCTNO,\n" +
                "    REMARK,\n" +
                "    VALIDMONTH,\n" +
                "    USECHANNEL,\n" +
                "    ORGANIZE_ID,\n" +
                "    EMDNO,\n" +
                "    USE_USER_ID,\n" +
                "    DISTANCE,\n" +
                "    PRODTYPE,\n" +
                "    PRODUCTNAME,\n" +
                "    TICKETPRODUCT_ID,\n" +
                "    CHECKCODE,\n" +
                "    BIND_USER,\n" +
                "    BIND_USER_ID,\n" +
                "    REMOVEBINDDATE,\n" +
                "    FROMCARTORSELL,\n" +
                "    QRCODEPATH,\n" +
                "    DISTANCETYPE,\n" +
                "    BUSINESSTYPE,\n" +
                "    SYNCHROSTATUS,\n" +
                "    REMARKQ)  " +
                "    SELECT \n" +
                "    CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "    ID,\n" +
                "    AMOUNT,\n" +
                "    AREATYPE,\n" +
                "    BOOKID,\n" +
                "    COUNTTYPE,\n" +
                "    GIFMESSAGE,\n" +
                "    NUMBERS,\n" +
                "    PASSWORDS,\n" +
                "    PUBLISHTIME,\n" +
                "    SELLTIME,\n" +
                "    STATUS,\n" +
                "    VALIDDATE,\n" +
                "    AREA_ID,\n" +
                "    GROUP_ID,\n" +
                "    ORDER_ID,\n" +
                "    USER_ID,\n" +
                "    USETIME,\n" +
                "    LOCKTIME,\n" +
                "    LOCK_USER_ID,\n" +
                "    ISAVAILABLESEG,\n" +
                "    PLATFORM,\n" +
                "    PRODUCTNO,\n" +
                "    REMARK,\n" +
                "    VALIDMONTH,\n" +
                "    USECHANNEL,\n" +
                "    ORGANIZE_ID,\n" +
                "    EMDNO,\n" +
                "    USE_USER_ID,\n" +
                "    DISTANCE,\n" +
                "    PRODTYPE,\n" +
                "    PRODUCTNAME,\n" +
                "    TICKETPRODUCT_ID,\n" +
                "    CHECKCODE,\n" +
                "    BIND_USER,\n" +
                "    BIND_USER_ID,\n" +
                "    REMOVEBINDDATE,\n" +
                "    FROMCARTORSELL,\n" +
                "    QRCODEPATH,\n" +
                "    DISTANCETYPE,\n" +
                "    BUSINESSTYPE,\n" +
                "    SYNCHROSTATUS,\n" +
                "    REMARKQ " +
                " FROM TB_UPGRADE_TICKET"
                + " WHERE  PUBLISHTIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' ";
        TableResult result = tEnv.executeSql(extractSql);

        result.print();

    }
}
