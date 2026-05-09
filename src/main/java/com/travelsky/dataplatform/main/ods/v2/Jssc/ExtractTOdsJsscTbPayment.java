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
 * *第一次取PAYDATE是2022年1月1日后的，后续每天更新近4个月的数据
 */
public class ExtractTOdsJsscTbPayment {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        // 获取4个月前日期
        // 将字符串转换为 LocalDate
        LocalDate currentDate = LocalDate.parse(etlDate);
        // 计算4个月前的日期
        LocalDate fourMonthsAgo = currentDate.minusMonths(4);
        // 格式化为字符串
        String fourMonthsAgoStr = fourMonthsAgo .toString();
        String startDate = fourMonthsAgoStr;
        String endDate = etlDate;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsJsscTbPayment");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE TB_PAYMENT (\n" +
                "    ID                    BIGINT,\n" +
                "    AMOUNT                BIGINT,\n" +
                "    BANKNAME              VARCHAR(255),\n" +
                "    CARDNUMBER            VARCHAR(4),\n" +
                "    `DATETIME`              TIMESTAMP(6),\n" +
                "    `EXTEND`                VARCHAR(30),\n" +
                "    PAYDATE               VARCHAR(255),\n" +
                "    PAYSTATUS             BIGINT,\n" +
                "    PAYTIME               VARCHAR(255),\n" +
                "    REQNO                 VARCHAR(30),\n" +
                "    SERIALNO              VARCHAR(50),\n" +
                "    USER_ID               BIGINT,\n" +
                "    ORDER_ID              BIGINT,\n" +
                "    ACCTNAME              VARCHAR(90),\n" +
                "    BANKCODE              VARCHAR(255),\n" +
                "    CVVCODE               VARCHAR(90),\n" +
                "    CARDEXP               VARCHAR(90),\n" +
                "    CERTNO                VARCHAR(255),\n" +
                "    EXTEND7               VARCHAR(90),\n" +
                "    ISMISTAKEREFUND       BIGINT,\n" +
                "    EXTEND6               VARCHAR(255),\n" +
                "    ORGANIZE_ID           BIGINT,\n" +
                "    PAYTYPE               VARCHAR(50),\n" +
                "    SUGGESTION            VARCHAR(255),\n" +
                "    ISONLINEMISTAKEREFUND TINYINT" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.JSSC_IP + ":" + Constants.JSSC_PORT + "/" + Constants.JSSC_DB + "',\n" +
                "    'table-name' = '" + Constants.JSSC_SCHEMA + ".TB_PAYMENT', \n" +
                "    'username' = '" + Constants.JSSC_USER + "',\n" +
                "    'password' = '" + Constants.JSSC_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_JSSC_TB_PAYMENT (\n" +
                "    ID                    BIGINT,\n" +
                "    AMOUNT                BIGINT,\n" +
                "    BANKNAME              VARCHAR(255),\n" +
                "    CARDNUMBER            VARCHAR(4),\n" +
                "    `DATETIME`              TIMESTAMP(6),\n" +
                "    `EXTEND`                VARCHAR(30),\n" +
                "    PAYDATE               VARCHAR(255),\n" +
                "    PAYSTATUS             BIGINT,\n" +
                "    PAYTIME               VARCHAR(255),\n" +
                "    REQNO                 VARCHAR(30),\n" +
                "    SERIALNO              VARCHAR(50),\n" +
                "    USER_ID               BIGINT,\n" +
                "    ORDER_ID              BIGINT,\n" +
                "    ACCTNAME              VARCHAR(90),\n" +
                "    BANKCODE              VARCHAR(255),\n" +
                "    CVVCODE               VARCHAR(90),\n" +
                "    CARDEXP               VARCHAR(90),\n" +
                "    CERTNO                VARCHAR(255),\n" +
                "    EXTEND7               VARCHAR(90),\n" +
                "    ISMISTAKEREFUND       BIGINT,\n" +
                "    EXTEND6               VARCHAR(255),\n" +
                "    ORGANIZE_ID           BIGINT,\n" +
                "    PAYTYPE               VARCHAR(50),\n" +
                "    SUGGESTION            VARCHAR(255),\n" +
                "    ISONLINEMISTAKEREFUND TINYINT," +
                "    ETL_DATE DATE" +
                //") WITH (\n" +
                //"    'connector' = 'jdbc',\n" +
                //"    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
                //"    'table-name' = 'T_ODS_JSSC_TB_PAYMENT',\n" +
                //"    'username' = '" + Constants.ODS_USER + "',\n" +
                //"    'password' = '" + Constants.ODS_PWD + "'\n" +
                //",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
                //")");
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "  'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "',\n" +
                "  'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "',\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_JSSC_TB_PAYMENT',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        String extractSql = "INSERT INTO T_ODS_JSSC_TB_PAYMENT (" +
                "    ETL_DATE,\n" +
                "    ID,\n" +
                "    AMOUNT,\n" +
                "    BANKNAME,\n" +
                "    CARDNUMBER,\n" +
                "    `DATETIME`,\n" +
                "    `EXTEND`,\n" +
                "    PAYDATE,\n" +
                "    PAYSTATUS,\n" +
                "    PAYTIME,\n" +
                "    REQNO,\n" +
                "    SERIALNO,\n" +
                "    USER_ID,\n" +
                "    ORDER_ID,\n" +
                "    ACCTNAME,\n" +
                "    BANKCODE,\n" +
                "    CVVCODE,\n" +
                "    CARDEXP,\n" +
                "    CERTNO,\n" +
                "    EXTEND7,\n" +
                "    ISMISTAKEREFUND,\n" +
                "    EXTEND6,\n" +
                "    ORGANIZE_ID,\n" +
                "    PAYTYPE,\n" +
                "    SUGGESTION,\n" +
                "    ISONLINEMISTAKEREFUND)  " +
                "    SELECT \n" +
                "    CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "    ID,\n" +
                "    AMOUNT,\n" +
                "    BANKNAME,\n" +
                "    CARDNUMBER,\n" +
                "    `DATETIME`,\n" +
                "    `EXTEND`,\n" +
                "    PAYDATE,\n" +
                "    PAYSTATUS,\n" +
                "    PAYTIME,\n" +
                "    REQNO,\n" +
                "    SERIALNO,\n" +
                "    USER_ID,\n" +
                "    ORDER_ID,\n" +
                "    ACCTNAME,\n" +
                "    BANKCODE,\n" +
                "    CVVCODE,\n" +
                "    CARDEXP,\n" +
                "    sm4_encrypt(CERTNO, '" + Constants.SM4_KEY + "') CERTNO, \n" +
                "    EXTEND7,\n" +
                "    ISMISTAKEREFUND,\n" +
                "    EXTEND6,\n" +
                "    ORGANIZE_ID,\n" +
                "    PAYTYPE,\n" +
                "    SUGGESTION,\n" +
                "    ISONLINEMISTAKEREFUND " +
                " FROM TB_PAYMENT"
                + " WHERE  PAYDATE >=" + startDate.replace("-", "")  + " and  PAYDATE <= " + endDate.replace("-", "");
        TableResult result = tEnv.executeSql(extractSql);

        result.print();

    }
}
