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
 * *按照COMPLAINT_DATE导入2022年1月1日至今的数据，
 * * 后续每天更新导入COMPLAINT_DATE是前一天的，以及UPDATE_DATE是前一天的数据
 */
public class ExtractTOdsLygjComplaintInfoActiviti {
    public static void main(String[] args) throws IOException {
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
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLygjComplaintInfoActiviti");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE COMPLAINT_INFO_ACTIVITI (\n" +
                "    ID  BIGINT,\n" +
                "    COMPLAINT_SERIAL_NUM         varchar(300),\n" +
                "    COMPLAINT_TITLE              varchar(1200),\n" +
                "    PAX_NAME                     varchar(300),\n" +
                "    IS_GROUP                     CHAR(2),\n" +
                "    SEX                          VARCHAR(2),\n" +
                "    TK_LUGGAGE                   VARCHAR(100),\n" +
                "    CARD_NUM                     VARCHAR(600),\n" +
                "    CARD_TYPE                    VARCHAR(30),\n" +
                "    PSG_TYPE                     VARCHAR(30),\n" +
                "    COMP_DUTY_FFP                VARCHAR(400),\n" +
                "    CABIN_CLASS                  VARCHAR(30),\n" +
                "    PHONENUM                     VARCHAR(100),\n" +
                "    ADDRESS                      VARCHAR(1200),\n" +
                "    EMAIL                        VARCHAR(200),\n" +
                "    MEMO                         VARCHAR(12000),\n" +
                "    FEED_NAME                    VARCHAR(200),\n" +
                "    FEED_PHONE                   VARCHAR(100),\n" +
                "    FLIGHT_NUM                   VARCHAR(10),\n" +
                "    FLIGHT_DATE                  TIMESTAMP(6),\n" +
                "    DEP_CITY                     VARCHAR(60),\n" +
                "    ARR_CITY                     VARCHAR(60),\n" +
                "    COMPLAINT_WAY                VARCHAR(180),\n" +
                "    COMPLAINT_ORI                VARCHAR(180),\n" +
                "    SUGGEST_TYPE                 BIGINT,\n" +
                "    SERVICE_TYPE                 BIGINT,\n" +
                "    SERVICE_TYPE_DETAIL          BIGINT,\n" +
                "    COMPLAINT_PRIORITY           BIGINT,\n" +
                "    COMPLAINT_CONTENT            VARCHAR(12000),\n" +
                "    COMPLAINT_SUPPLEMENT_CONTENT VARCHAR(12000),\n" +
                "    PSG_REQ                      VARCHAR(12000),\n" +
                "    SERVICES_REMIND              VARCHAR(3000),\n" +
                "    DEAL_DEPT                    VARCHAR(20),\n" +
                "    ASSIT_DEPT                   VARCHAR(1200),\n" +
                "    COMPLAINT_DATE               TIMESTAMP(6),\n" +
                "    COMPLAINT_STATE              BIGINT,\n" +
                "    ACCEPT_PEOPLE                VARCHAR(30),\n" +
                "    ACCEPT_DEPT                  VARCHAR(20),\n" +
                "    RECALL_TIME                  TIMESTAMP(6),\n" +
                "    PROCESS_ID                   VARCHAR(40),\n" +
                "    ATTACH_COMPLAINT             VARCHAR(12000),\n" +
                "    CLOSE_CONTENT                VARCHAR(6000),\n" +
                "    CLOSE_DATE                   TIMESTAMP(6),\n" +
                "    CLOSE_DAYS                   BIGINT,\n" +
                "    CLOSE_OP                     VARCHAR(30),\n" +
                "    VALID                        BIGINT,\n" +
                "    COMPLAINT_RESULT             VARCHAR(12000),\n" +
                "    ARCHIVE_VALID                VARCHAR(4),\n" +
                "    FIRST_DUTY_DEPT              VARCHAR(100),\n" +
                "    FIRST_DUTY_DEPT_SCORE        VARCHAR(100),\n" +
                "    INVOLVED_PERSON_ACCT         VARCHAR(1200),\n" +
                "    INVOLVED_PERSONNEL_NAME      VARCHAR(1200),\n" +
                "    UPDATE_PERSON                VARCHAR(50),\n" +
                "    UPDATE_DATE                  TIMESTAMP(6),\n" +
                "    RESULT_ATTACH                VARCHAR(1200),\n" +
                "    TICKET_CHANNEL               VARCHAR(20),\n" +
                "    CLOSE_ADDITIONAL_CONTENT     VARCHAR(3000),\n" +
                "    CLOSE_ADDITIONAL_DATE        TIMESTAMP(6),\n" +
                "    CLOSE_ADDITIONAL_OPERATOR    VARCHAR(90),\n" +
                "    CLOSE_ADDITIONAL_FILE        VARCHAR(900),\n" +
                "    COMMENT_SUMMARY              VARCHAR(12000),\n" +
                "    IS_CONVERT_MEDIATION         CHAR(1),\n" +
                "    BEFORE_COMPLAINT_ID          BIGINT,\n" +
                "    MEDIATE_FALG                 BIGINT,\n" +
                "    NOT_MEDIATE_REASON           VARCHAR(6000),\n" +
                "    MEDIATION_CLAIM              VARCHAR(6000),\n" +
                "    NEW_COMPLAINT_INFO           VARCHAR(1),\n" +
                "    ATTACH_RECOMMUNICATE         VARCHAR(12000)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.LYGJ_IP + ":" + Constants.LYGJ_PORT + "/" + Constants.LYGJ_DB + "',\n" +
                "    'table-name' = '" + Constants.LYGJ_SCHEMA + ".COMPLAINT_INFO_ACTIVITI', \n" +
                "    'username' = '" + Constants.LYGJ_USER + "',\n" +
                "    'password' = '" + Constants.LYGJ_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_LYGJ_COMPLAINT_INFO_ACTIVITI (\n" +
                "    ID  BIGINT,\n" +
                "    COMPLAINT_SERIAL_NUM         varchar(300),\n" +
                "    COMPLAINT_TITLE              varchar(1200),\n" +
                "    PAX_NAME                     varchar(300),\n" +
                "    IS_GROUP                     CHAR(2),\n" +
                "    SEX                          VARCHAR(2),\n" +
                "    TK_LUGGAGE                   VARCHAR(100),\n" +
                "    CARD_NUM                     VARCHAR(600),\n" +
                "    CARD_TYPE                    VARCHAR(30),\n" +
                "    PSG_TYPE                     VARCHAR(30),\n" +
                "    COMP_DUTY_FFP                VARCHAR(400),\n" +
                "    CABIN_CLASS                  VARCHAR(30),\n" +
                "    PHONENUM                     VARCHAR(100),\n" +
                "    ADDRESS                      VARCHAR(1200),\n" +
                "    EMAIL                        VARCHAR(200),\n" +
                "    MEMO                         VARCHAR(12000),\n" +
                "    FEED_NAME                    VARCHAR(200),\n" +
                "    FEED_PHONE                   VARCHAR(100),\n" +
                "    FLIGHT_NUM                   VARCHAR(10),\n" +
                "    FLIGHT_DATE                  TIMESTAMP(6),\n" +
                "    DEP_CITY                     VARCHAR(60),\n" +
                "    ARR_CITY                     VARCHAR(60),\n" +
                "    COMPLAINT_WAY                VARCHAR(180),\n" +
                "    COMPLAINT_ORI                VARCHAR(180),\n" +
                "    SUGGEST_TYPE                 BIGINT,\n" +
                "    SERVICE_TYPE                 BIGINT,\n" +
                "    SERVICE_TYPE_DETAIL          BIGINT,\n" +
                "    COMPLAINT_PRIORITY           BIGINT,\n" +
                "    COMPLAINT_CONTENT            VARCHAR(12000),\n" +
                "    COMPLAINT_SUPPLEMENT_CONTENT VARCHAR(12000),\n" +
                "    PSG_REQ                      VARCHAR(12000),\n" +
                "    SERVICES_REMIND              VARCHAR(3000),\n" +
                "    DEAL_DEPT                    VARCHAR(20),\n" +
                "    ASSIT_DEPT                   VARCHAR(1200),\n" +
                "    COMPLAINT_DATE               TIMESTAMP(6),\n" +
                "    COMPLAINT_STATE              BIGINT,\n" +
                "    ACCEPT_PEOPLE                VARCHAR(30),\n" +
                "    ACCEPT_DEPT                  VARCHAR(20),\n" +
                "    RECALL_TIME                  TIMESTAMP(6),\n" +
                "    PROCESS_ID                   VARCHAR(40),\n" +
                "    ATTACH_COMPLAINT             VARCHAR(12000),\n" +
                "    CLOSE_CONTENT                VARCHAR(6000),\n" +
                "    CLOSE_DATE                   TIMESTAMP(6),\n" +
                "    CLOSE_DAYS                   BIGINT,\n" +
                "    CLOSE_OP                     VARCHAR(30),\n" +
                "    VALID                        BIGINT,\n" +
                "    COMPLAINT_RESULT             VARCHAR(12000),\n" +
                "    ARCHIVE_VALID                VARCHAR(4),\n" +
                "    FIRST_DUTY_DEPT              VARCHAR(100),\n" +
                "    FIRST_DUTY_DEPT_SCORE        VARCHAR(100),\n" +
                "    INVOLVED_PERSON_ACCT         VARCHAR(1200),\n" +
                "    INVOLVED_PERSONNEL_NAME      VARCHAR(1200),\n" +
                "    UPDATE_PERSON                VARCHAR(50),\n" +
                "    UPDATE_DATE                  TIMESTAMP(6),\n" +
                "    RESULT_ATTACH                VARCHAR(1200),\n" +
                "    TICKET_CHANNEL               VARCHAR(20),\n" +
                "    CLOSE_ADDITIONAL_CONTENT     VARCHAR(3000),\n" +
                "    CLOSE_ADDITIONAL_DATE        TIMESTAMP(6),\n" +
                "    CLOSE_ADDITIONAL_OPERATOR    VARCHAR(90),\n" +
                "    CLOSE_ADDITIONAL_FILE        VARCHAR(900),\n" +
                "    COMMENT_SUMMARY              VARCHAR(12000),\n" +
                "    IS_CONVERT_MEDIATION         CHAR(1),\n" +
                "    BEFORE_COMPLAINT_ID          BIGINT,\n" +
                "    MEDIATE_FALG                 BIGINT,\n" +
                "    NOT_MEDIATE_REASON           VARCHAR(6000),\n" +
                "    MEDIATION_CLAIM              VARCHAR(6000),\n" +
                "    NEW_COMPLAINT_INFO           VARCHAR(1),\n" +
                "    ATTACH_RECOMMUNICATE         VARCHAR(12000)," +
                "    ETL_DATE DATE" +
                //") WITH (\n" +
                //"    'connector' = 'jdbc',\n" +
                //"    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
                //"    'table-name' = 'T_ODS_LYGJ_COMPLAINT_INFO_ACTIVITI', -- 替换为实际的表名\n" +
                //"    'username' = '" + Constants.ODS_USER + "',\n" +
                //"    'password' = '" + Constants.ODS_PWD + "'\n" +
                //",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
                //")");
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "  'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "',\n" +
                "  'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "',\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYGJ_COMPLAINT_INFO_ACTIVITI',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        String extractSql = "INSERT INTO T_ODS_LYGJ_COMPLAINT_INFO_ACTIVITI(" +
                "    ETL_DATE,\n" +
                "    ID,\n" +
                "    COMPLAINT_SERIAL_NUM,\n" +
                "    COMPLAINT_TITLE,\n" +
                "    PAX_NAME,\n" +
                "    IS_GROUP,\n" +
                "    SEX,\n" +
                "    TK_LUGGAGE,\n" +
                "    CARD_NUM,\n" +
                "    CARD_TYPE,\n" +
                "    PSG_TYPE,\n" +
                "    COMP_DUTY_FFP,\n" +
                "    CABIN_CLASS,\n" +
                "    PHONENUM,\n" +
                "    ADDRESS,\n" +
                "    EMAIL,\n" +
                "    MEMO,\n" +
                "    FEED_NAME,\n" +
                "    FEED_PHONE,\n" +
                "    FLIGHT_NUM,\n" +
                "    FLIGHT_DATE,\n" +
                "    DEP_CITY,\n" +
                "    ARR_CITY,\n" +
                "    COMPLAINT_WAY,\n" +
                "    COMPLAINT_ORI,\n" +
                "    SUGGEST_TYPE,\n" +
                "    SERVICE_TYPE,\n" +
                "    SERVICE_TYPE_DETAIL,\n" +
                "    COMPLAINT_PRIORITY,\n" +
                "    COMPLAINT_CONTENT,\n" +
                "    COMPLAINT_SUPPLEMENT_CONTENT,\n" +
                "    PSG_REQ,\n" +
                "    SERVICES_REMIND,\n" +
                "    DEAL_DEPT,\n" +
                "    ASSIT_DEPT,\n" +
                "    COMPLAINT_DATE,\n" +
                "    COMPLAINT_STATE,\n" +
                "    ACCEPT_PEOPLE,\n" +
                "    ACCEPT_DEPT,\n" +
                "    RECALL_TIME,\n" +
                "    PROCESS_ID,\n" +
                "    ATTACH_COMPLAINT,\n" +
                "    CLOSE_CONTENT,\n" +
                "    CLOSE_DATE,\n" +
                "    CLOSE_DAYS,\n" +
                "    CLOSE_OP,\n" +
                "    VALID,\n" +
                "    COMPLAINT_RESULT,\n" +
                "    ARCHIVE_VALID,\n" +
                "    FIRST_DUTY_DEPT,\n" +
                "    FIRST_DUTY_DEPT_SCORE,\n" +
                "    INVOLVED_PERSON_ACCT,\n" +
                "    INVOLVED_PERSONNEL_NAME,\n" +
                "    UPDATE_PERSON,\n" +
                "    UPDATE_DATE,\n" +
                "    RESULT_ATTACH,\n" +
                "    TICKET_CHANNEL,\n" +
                "    CLOSE_ADDITIONAL_CONTENT,\n" +
                "    CLOSE_ADDITIONAL_DATE,\n" +
                "    CLOSE_ADDITIONAL_OPERATOR,\n" +
                "    CLOSE_ADDITIONAL_FILE,\n" +
                "    COMMENT_SUMMARY,\n" +
                "    IS_CONVERT_MEDIATION,\n" +
                "    BEFORE_COMPLAINT_ID,\n" +
                "    MEDIATE_FALG,\n" +
                "    NOT_MEDIATE_REASON,\n" +
                "    MEDIATION_CLAIM,\n" +
                "    NEW_COMPLAINT_INFO,\n" +
                "    ATTACH_RECOMMUNICATE)  " +
                "    SELECT \n" +
                "    CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "    ID,\n" +
                "    COMPLAINT_SERIAL_NUM,\n" +
                "    COMPLAINT_TITLE,\n" +
                "    PAX_NAME,\n" +
                "    IS_GROUP,\n" +
                "    SEX,\n" +
                "    TK_LUGGAGE,\n" +
                "    sm4_encrypt(aes_decrypt(CARD_NUM, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') CARD_NUM, \n" +
                "    CARD_TYPE,\n" +
                "    PSG_TYPE,\n" +
                "    COMP_DUTY_FFP,\n" +
                "    CABIN_CLASS,\n" +
                "    sm4_encrypt(aes_decrypt(PHONENUM, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') PHONENUM, \n" +
                "    ADDRESS,\n" +
                "    sm4_encrypt(EMAIL, '" + Constants.SM4_KEY + "') EMAIL, \n" +
                "    MEMO,\n" +
                "    FEED_NAME,\n" +
                "    sm4_encrypt(aes_decrypt(FEED_PHONE, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') FEED_PHONE, \n" +
                "    FLIGHT_NUM,\n" +
                "    FLIGHT_DATE,\n" +
                "    DEP_CITY,\n" +
                "    ARR_CITY,\n" +
                "    COMPLAINT_WAY,\n" +
                "    COMPLAINT_ORI,\n" +
                "    SUGGEST_TYPE,\n" +
                "    SERVICE_TYPE,\n" +
                "    SERVICE_TYPE_DETAIL,\n" +
                "    COMPLAINT_PRIORITY,\n" +
                "    COMPLAINT_CONTENT,\n" +
                "    COMPLAINT_SUPPLEMENT_CONTENT,\n" +
                "    PSG_REQ,\n" +
                "    SERVICES_REMIND,\n" +
                "    DEAL_DEPT,\n" +
                "    ASSIT_DEPT,\n" +
                "    COMPLAINT_DATE,\n" +
                "    COMPLAINT_STATE,\n" +
                "    ACCEPT_PEOPLE,\n" +
                "    ACCEPT_DEPT,\n" +
                "    RECALL_TIME,\n" +
                "    PROCESS_ID,\n" +
                "    ATTACH_COMPLAINT,\n" +
                "    CLOSE_CONTENT,\n" +
                "    CLOSE_DATE,\n" +
                "    CLOSE_DAYS,\n" +
                "    CLOSE_OP,\n" +
                "    VALID,\n" +
                "    COMPLAINT_RESULT,\n" +
                "    ARCHIVE_VALID,\n" +
                "    FIRST_DUTY_DEPT,\n" +
                "    FIRST_DUTY_DEPT_SCORE,\n" +
                "    INVOLVED_PERSON_ACCT,\n" +
                "    INVOLVED_PERSONNEL_NAME,\n" +
                "    UPDATE_PERSON,\n" +
                "    UPDATE_DATE,\n" +
                "    RESULT_ATTACH,\n" +
                "    TICKET_CHANNEL,\n" +
                "    CLOSE_ADDITIONAL_CONTENT,\n" +
                "    CLOSE_ADDITIONAL_DATE,\n" +
                "    CLOSE_ADDITIONAL_OPERATOR,\n" +
                "    CLOSE_ADDITIONAL_FILE,\n" +
                "    COMMENT_SUMMARY,\n" +
                "    IS_CONVERT_MEDIATION,\n" +
                "    BEFORE_COMPLAINT_ID,\n" +
                "    MEDIATE_FALG,\n" +
                "    NOT_MEDIATE_REASON,\n" +
                "    MEDIATION_CLAIM,\n" +
                "    NEW_COMPLAINT_INFO,\n" +
                "    ATTACH_RECOMMUNICATE" +
                " FROM COMPLAINT_INFO_ACTIVITI "
                + " WHERE  COMPLAINT_DATE BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  UPDATE_DATE BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;
        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }

}
