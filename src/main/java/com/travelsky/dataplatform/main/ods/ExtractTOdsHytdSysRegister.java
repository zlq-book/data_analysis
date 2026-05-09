package com.travelsky.dataplatform.main.ods;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.TOdsHytdSysRegister;
import com.travelsky.dataplatform.source.DMDBSourceFunction;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.KerberosAuthUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author kuangaihua
 * @date 2025/7/4 17:30
 */
public class ExtractTOdsHytdSysRegister {
    private static final Logger logger = LoggerFactory.getLogger(ExtractTOdsHytdSysRegister.class);

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            logger.error("etl_date参数为空");
            System.exit(0);
        }
        String etlDate = args[0];
        logger.info("etl_date:" + etlDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsHytdSysRegister");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        StringBuffer urlBuffer = new StringBuffer();
        urlBuffer.append("jdbc:dm://").append(Constants.HYTD_IP).append(":").append(Constants.HYTD_PORT).append("/").append(Constants.HYTD_DB);
        String url = urlBuffer.toString();
        String user = Constants.HYTD_USER;
        String password = Constants.HYTD_PWD;
        
        StringBuffer queryBuffer = new StringBuffer();
        queryBuffer.append("SELECT ID\n");
        queryBuffer.append(",CRM_MEMBER_ID\n");
        queryBuffer.append(",MEMBER_NUMBER\n");
        queryBuffer.append(",CN_LAST_NAME\n");
        queryBuffer.append(",CN_FIRST_NAME\n");
        queryBuffer.append(",LAST_NAME\n");
        queryBuffer.append(",FIRST_NAME\n");
        queryBuffer.append(",GENDER\n");
        queryBuffer.append(",BIRTHDAY\n");
        queryBuffer.append(",CREDENTIAL_TYPE\n");
        queryBuffer.append(",").append(Constants.HYTD_SCHEMA).append(".AES_DECRYPT(CREDENTIAL_NUM,'").append(Constants.HYTD_AES_KEY).append("') AS CREDENTIAL_NUM\n");
        queryBuffer.append(",NATIONALITY\n");
        queryBuffer.append(",LANGUAGE\n");
        queryBuffer.append(",PIN_QUESTION\n");
        queryBuffer.append(",PIN_ANSWER\n");
        queryBuffer.append(",EMAIL_TYPE\n");
        queryBuffer.append(",EMAIL_ADDR\n");
        queryBuffer.append(",COMPANY\n");
        queryBuffer.append(",DEPARTMENT\n");
        queryBuffer.append(",CARD_STATUS\n");
        queryBuffer.append(",PIN_STATUS\n");
        queryBuffer.append(",ADDRESS_TYPE\n");
        queryBuffer.append(",COUNTRY\n");
        queryBuffer.append(",STATE\n");
        queryBuffer.append(",CITY\n");
        queryBuffer.append(",STREET\n");
        queryBuffer.append(",POSTAL_CODE\n");
        queryBuffer.append(",PHONE_TYPE\n");
        queryBuffer.append(",COUNTRY_CODE\n");
        queryBuffer.append(",").append(Constants.HYTD_SCHEMA).append(".AES_DECRYPT(PHONE_NUM,'").append(Constants.HYTD_AES_KEY).append("') AS PHONE_NUM\n");
        queryBuffer.append(",PARENT_MEMBER_NUM\n");
        queryBuffer.append(",FAST_CREATE_FLAG\n");
        queryBuffer.append(",FORCE_CREATE_FLAG\n");
        queryBuffer.append(",NO_PRO_SMS_FLAG\n");
        queryBuffer.append(",NO_PRO_EMAIL_FLAG\n");
        queryBuffer.append(",SUBMIT_PERSON\n");
        queryBuffer.append(",CHANNEL_ID\n");
        queryBuffer.append(",CHANNEL_NAME\n");
        queryBuffer.append(",REGISTER_STATUS\n");
        queryBuffer.append(",CRM_CODE\n");
        queryBuffer.append(",CRMR_MSG\n");
        queryBuffer.append(",CREATOR\n");
        queryBuffer.append(",CREATE_DATE\n");
        queryBuffer.append(",REGISTER_CHANNEL\n FROM ").append(Constants.HYTD_SCHEMA).append(".SYS_REGISTER AS sr ");
        queryBuffer.append("WHERE CREATE_DATE BETWEEN '").append(etlDate).append(" 00:00:00.000' AND '").append(etlDate).append(" 23:59:59.999' \n");
        String query = queryBuffer.toString();
        
        String columStr = "ID,CRM_MEMBER_ID,MEMBER_NUMBER,CN_LAST_NAME,CN_FIRST_NAME,LAST_NAME,FIRST_NAME,GENDER,BIRTHDAY,CREDENTIAL_TYPE,CREDENTIAL_NUM,NATIONALITY,LANGUAGE,PIN_QUESTION,PIN_ANSWER,EMAIL_TYPE,EMAIL_ADDR,COMPANY,DEPARTMENT,CARD_STATUS,PIN_STATUS,ADDRESS_TYPE,COUNTRY,STATE,CITY,STREET,POSTAL_CODE,PHONE_TYPE,COUNTRY_CODE,PHONE_NUM,PARENT_MEMBER_NUM,FAST_CREATE_FLAG,FORCE_CREATE_FLAG,NO_PRO_SMS_FLAG,NO_PRO_EMAIL_FLAG,SUBMIT_PERSON,CHANNEL_ID,CHANNEL_NAME,REGISTER_STATUS,CRM_CODE,CRMR_MSG,CREATOR,CREATE_DATE,REGISTER_CHANNEL";
        String[] columnStrs = columStr.split(",");
        List<String> columns = Arrays.stream(columnStrs).collect(Collectors.toList());
        DMDBSourceFunction function = new DMDBSourceFunction(url, user, password, query, columns);
        DataStreamSource<JSONObject> jsonObjectDataStreamSource = env.addSource(function);

        SingleOutputStreamOperator<TOdsHytdSysRegister> tSource = jsonObjectDataStreamSource.map(jsonObject -> {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            TOdsHytdSysRegister tOdsHytdSysRegister = mapper.readValue(JSONObject.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue), TOdsHytdSysRegister.class);
            return tOdsHytdSysRegister;
        });

        Schema schema = Schema.newBuilder()
                .column("ID", DataTypes.BIGINT())
                .column("CRM_MEMBER_ID", DataTypes.VARCHAR(96))
                .column("MEMBER_NUMBER", DataTypes.VARCHAR(96))
                .column("CN_LAST_NAME", DataTypes.VARCHAR(300))
                .column("CN_FIRST_NAME", DataTypes.VARCHAR(300))
                .column("LAST_NAME", DataTypes.VARCHAR(300))
                .column("FIRST_NAME", DataTypes.VARCHAR(300))
                .column("GENDER", DataTypes.VARCHAR(90))
                .column("BIRTHDAY", DataTypes.VARCHAR(36))
                .column("CREDENTIAL_TYPE", DataTypes.VARCHAR(96))
                .column("CREDENTIAL_NUM", DataTypes.VARCHAR(300))
                .column("NATIONALITY", DataTypes.VARCHAR(180))
                .column("LANGUAGE", DataTypes.VARCHAR(300))
                .column("PIN_QUESTION", DataTypes.VARCHAR(300))
                .column("PIN_ANSWER", DataTypes.VARCHAR(300))
                .column("EMAIL_TYPE", DataTypes.VARCHAR(180))
                .column("EMAIL_ADDR", DataTypes.VARCHAR(300))
                .column("COMPANY", DataTypes.VARCHAR(300))
                .column("DEPARTMENT", DataTypes.VARCHAR(300))
                .column("CARD_STATUS", DataTypes.VARCHAR(180))
                .column("PIN_STATUS", DataTypes.VARCHAR(36))
                .column("ADDRESS_TYPE", DataTypes.VARCHAR(180))
                .column("COUNTRY", DataTypes.VARCHAR(180))
                .column("STATE", DataTypes.VARCHAR(96))
                .column("CITY", DataTypes.VARCHAR(300))
                .column("STREET", DataTypes.VARCHAR(1536))
                .column("POSTAL_CODE", DataTypes.VARCHAR(90))
                .column("PHONE_TYPE", DataTypes.VARCHAR(90))
                .column("COUNTRY_CODE", DataTypes.VARCHAR(45))
                .column("PHONE_NUM", DataTypes.VARCHAR(300))
                .column("PARENT_MEMBER_NUM", DataTypes.VARCHAR(90))
                .column("FAST_CREATE_FLAG", DataTypes.VARCHAR(180))
                .column("FORCE_CREATE_FLAG", DataTypes.VARCHAR(180))
                .column("NO_PRO_SMS_FLAG", DataTypes.VARCHAR(36))
                .column("NO_PRO_EMAIL_FLAG", DataTypes.VARCHAR(36))
                .column("SUBMIT_PERSON", DataTypes.VARCHAR(96))
                .column("CHANNEL_ID", DataTypes.VARCHAR(96))
                .column("CHANNEL_NAME", DataTypes.VARCHAR(96))
                .column("REGISTER_STATUS", DataTypes.VARCHAR(18))
                .column("CRM_CODE", DataTypes.VARCHAR(96))
                .column("CRMR_MSG", DataTypes.VARCHAR(3072))
                .column("CREATOR", DataTypes.BIGINT())
                .column("CREATE_DATE", DataTypes.BIGINT())
                .column("REGISTER_CHANNEL", DataTypes.VARCHAR(96))
                .build();
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        // 注册为临时视图
        tEnv.createTemporaryView("SYS_REGISTER", tSource, schema);
        
        //创建Doris目标表
        StringBuffer targetTableSql = new StringBuffer();
        targetTableSql.append("CREATE TABLE T_ODS_HYTD_SYS_REGISTER (\n");
        targetTableSql.append("  `ETL_DATE` DATE,\n");
        targetTableSql.append("  `ID` BIGINT,\n");
        targetTableSql.append("  `CRM_MEMBER_ID` VARCHAR(96),\n");
        targetTableSql.append("  `MEMBER_NUMBER` VARCHAR(96),\n");
        targetTableSql.append("  `CN_LAST_NAME` VARCHAR(300),\n");
        targetTableSql.append("  `CN_FIRST_NAME` VARCHAR(300),\n");
        targetTableSql.append("  `LAST_NAME` VARCHAR(300),\n");
        targetTableSql.append("  `FIRST_NAME` VARCHAR(300),\n");
        targetTableSql.append("  `GENDER` VARCHAR(90),\n");
        targetTableSql.append("  `BIRTHDAY` VARCHAR(36),\n");
        targetTableSql.append("  `CREDENTIAL_TYPE` VARCHAR(96),\n");
        targetTableSql.append("  `CREDENTIAL_NUM` VARCHAR(300),\n");
        targetTableSql.append("  `NATIONALITY` VARCHAR(180),\n");
        targetTableSql.append("  `LANGUAGE` VARCHAR(300),\n");
        targetTableSql.append("  `PIN_QUESTION` VARCHAR(300),\n");
        targetTableSql.append("  `PIN_ANSWER` VARCHAR(300),\n");
        targetTableSql.append("  `EMAIL_TYPE` VARCHAR(180),\n");
        targetTableSql.append("  `EMAIL_ADDR` VARCHAR(300),\n");
        targetTableSql.append("  `COMPANY` VARCHAR(300),\n");
        targetTableSql.append("  `DEPARTMENT` VARCHAR(300),\n");
        targetTableSql.append("  `CARD_STATUS` VARCHAR(180),\n");
        targetTableSql.append("  `PIN_STATUS` VARCHAR(36),\n");
        targetTableSql.append("  `ADDRESS_TYPE` VARCHAR(180),\n");
        targetTableSql.append("  `COUNTRY` VARCHAR(180),\n");
        targetTableSql.append("  `STATE` VARCHAR(96),\n");
        targetTableSql.append("  `CITY` VARCHAR(300),\n");
        targetTableSql.append("  `STREET` VARCHAR(1536),\n");
        targetTableSql.append("  `POSTAL_CODE` VARCHAR(90),\n");
        targetTableSql.append("  `PHONE_TYPE` VARCHAR(90),\n");
        targetTableSql.append("  `COUNTRY_CODE` VARCHAR(45),\n");
        targetTableSql.append("  `PHONE_NUM` VARCHAR(300),\n");
        targetTableSql.append("  `PARENT_MEMBER_NUM` VARCHAR(90),\n");
        targetTableSql.append("  `FAST_CREATE_FLAG` VARCHAR(180),\n");
        targetTableSql.append("  `FORCE_CREATE_FLAG` VARCHAR(180),\n");
        targetTableSql.append("  `NO_PRO_SMS_FLAG` VARCHAR(36),\n");
        targetTableSql.append("  `NO_PRO_EMAIL_FLAG` VARCHAR(36),\n");
        targetTableSql.append("  `SUBMIT_PERSON` VARCHAR(96),\n");
        targetTableSql.append("  `CHANNEL_ID` VARCHAR(96),\n");
        targetTableSql.append("  `CHANNEL_NAME` VARCHAR(96),\n");
        targetTableSql.append("  `REGISTER_STATUS` VARCHAR(18),\n");
        targetTableSql.append("  `CRM_CODE` VARCHAR(96),\n");
        targetTableSql.append("  `CRMR_MSG` VARCHAR(3072),\n");
        targetTableSql.append("  `CREATOR` BIGINT,\n");
        targetTableSql.append("  `CREATE_DATE` TIMESTAMP(6),\n");
        targetTableSql.append("  `REGISTER_CHANNEL` VARCHAR(96)\n");
        targetTableSql.append(") WITH (\n");
        targetTableSql.append(" 'connector' = 'doris',\n");
        targetTableSql.append("'fenodes' = '").append(Constants.DORIS_FE_IP).append(":").append(Constants.DORIS_FE_PORT).append("', -- 替换为 Doris FE 的地址和端口\n");
        targetTableSql.append("'benodes' = '").append(Constants.DORIS_BE_IP).append(":").append(Constants.DORIS_BE_PORT).append("', -- 替换为 Doris FE 的地址和端口\n");
        targetTableSql.append(" 'table.identifier' = '").append(Constants.ODS_DB).append(".T_ODS_HYTD_SYS_REGISTER',\n");
        targetTableSql.append("'sink.label-prefix' = '").append(timestamp).append(uuid).append("',");
        targetTableSql.append(" 'sink.properties.read_json_by_line' = 'true',");
        targetTableSql.append(" 'sink.properties.format' = 'json',");
        targetTableSql.append("    'username' = '").append(Constants.ODS_USER).append("',\n");
        targetTableSql.append("    'password' = '").append(Constants.ODS_PWD).append("'\n");
        targetTableSql.append(")");
        tEnv.executeSql(targetTableSql.toString());
        
        StringBuffer extractSqlBuffer = new StringBuffer();
        extractSqlBuffer.append("INSERT INTO T_ODS_HYTD_SYS_REGISTER(");
        extractSqlBuffer.append("ETL_DATE  ");
        extractSqlBuffer.append(",ID\n");
        extractSqlBuffer.append(",CRM_MEMBER_ID\n");
        extractSqlBuffer.append(",MEMBER_NUMBER\n");
        extractSqlBuffer.append(",CN_LAST_NAME\n");
        extractSqlBuffer.append(",CN_FIRST_NAME\n");
        extractSqlBuffer.append(",LAST_NAME\n");
        extractSqlBuffer.append(",FIRST_NAME\n");
        extractSqlBuffer.append(",GENDER\n");
        extractSqlBuffer.append(",BIRTHDAY\n");
        extractSqlBuffer.append(",CREDENTIAL_TYPE\n");
        extractSqlBuffer.append(",CREDENTIAL_NUM\n");
        extractSqlBuffer.append(",NATIONALITY\n");
        extractSqlBuffer.append(",`LANGUAGE`\n");
        extractSqlBuffer.append(",PIN_QUESTION\n");
        extractSqlBuffer.append(",PIN_ANSWER\n");
        extractSqlBuffer.append(",EMAIL_TYPE\n");
        extractSqlBuffer.append(",EMAIL_ADDR\n");
        extractSqlBuffer.append(",COMPANY\n");
        extractSqlBuffer.append(",DEPARTMENT\n");
        extractSqlBuffer.append(",CARD_STATUS\n");
        extractSqlBuffer.append(",PIN_STATUS\n");
        extractSqlBuffer.append(",ADDRESS_TYPE\n");
        extractSqlBuffer.append(",COUNTRY\n");
        extractSqlBuffer.append(",STATE\n");
        extractSqlBuffer.append(",CITY\n");
        extractSqlBuffer.append(",STREET\n");
        extractSqlBuffer.append(",POSTAL_CODE\n");
        extractSqlBuffer.append(",PHONE_TYPE\n");
        extractSqlBuffer.append(",COUNTRY_CODE\n");
        extractSqlBuffer.append(",PHONE_NUM\n");
        extractSqlBuffer.append(",PARENT_MEMBER_NUM\n");
        extractSqlBuffer.append(",FAST_CREATE_FLAG\n");
        extractSqlBuffer.append(",FORCE_CREATE_FLAG\n");
        extractSqlBuffer.append(",NO_PRO_SMS_FLAG\n");
        extractSqlBuffer.append(",NO_PRO_EMAIL_FLAG\n");
        extractSqlBuffer.append(",SUBMIT_PERSON\n");
        extractSqlBuffer.append(",CHANNEL_ID\n");
        extractSqlBuffer.append(",CHANNEL_NAME\n");
        extractSqlBuffer.append(",REGISTER_STATUS\n");
        extractSqlBuffer.append(",CRM_CODE\n");
        extractSqlBuffer.append(",CRMR_MSG\n");
        extractSqlBuffer.append(",CREATOR\n");
        extractSqlBuffer.append(",CREATE_DATE\n");
        extractSqlBuffer.append(",REGISTER_CHANNEL\n)  ");
        extractSqlBuffer.append("SELECT ");
        extractSqlBuffer.append("CAST('").append(etlDate).append("' AS DATE) ETL_DATE");
        extractSqlBuffer.append(",ID\n");
        extractSqlBuffer.append(",CRM_MEMBER_ID\n");
        extractSqlBuffer.append(",sm4_encrypt (MEMBER_NUMBER,'").append(Constants.SM4_KEY).append("') MEMBER_NUMBER\n");
        extractSqlBuffer.append(",CN_LAST_NAME\n");
        extractSqlBuffer.append(",CN_FIRST_NAME\n");
        extractSqlBuffer.append(",LAST_NAME\n");
        extractSqlBuffer.append(",FIRST_NAME\n");
        extractSqlBuffer.append(",GENDER\n");
        extractSqlBuffer.append(",BIRTHDAY\n");
        extractSqlBuffer.append(",CREDENTIAL_TYPE\n");
        extractSqlBuffer.append(",sm4_encrypt (CREDENTIAL_NUM,'").append(Constants.SM4_KEY).append("') CREDENTIAL_NUM\n");
        extractSqlBuffer.append(",NATIONALITY\n");
        extractSqlBuffer.append(",`LANGUAGE`\n");
        extractSqlBuffer.append(",PIN_QUESTION\n");
        extractSqlBuffer.append(",PIN_ANSWER\n");
        extractSqlBuffer.append(",EMAIL_TYPE\n");
        extractSqlBuffer.append(",sm4_encrypt (EMAIL_ADDR,'").append(Constants.SM4_KEY).append("') EMAIL_ADDR\n");
        extractSqlBuffer.append(",COMPANY\n");
        extractSqlBuffer.append(",DEPARTMENT\n");
        extractSqlBuffer.append(",CARD_STATUS\n");
        extractSqlBuffer.append(",PIN_STATUS\n");
        extractSqlBuffer.append(",ADDRESS_TYPE\n");
        extractSqlBuffer.append(",COUNTRY\n");
        extractSqlBuffer.append(",STATE\n");
        extractSqlBuffer.append(",CITY\n");
        extractSqlBuffer.append(",STREET\n");
        extractSqlBuffer.append(",POSTAL_CODE\n");
        extractSqlBuffer.append(",PHONE_TYPE\n");
        extractSqlBuffer.append(",COUNTRY_CODE\n");
        extractSqlBuffer.append(",sm4_encrypt (PHONE_NUM,'").append(Constants.SM4_KEY).append("') PHONE_NUM \n");
        extractSqlBuffer.append(",sm4_encrypt (PARENT_MEMBER_NUM,'").append(Constants.SM4_KEY).append("') PARENT_MEMBER_NUM\n");
        extractSqlBuffer.append(",FAST_CREATE_FLAG\n");
        extractSqlBuffer.append(",FORCE_CREATE_FLAG\n");
        extractSqlBuffer.append(",NO_PRO_SMS_FLAG\n");
        extractSqlBuffer.append(",NO_PRO_EMAIL_FLAG\n");
        extractSqlBuffer.append(",SUBMIT_PERSON\n");
        extractSqlBuffer.append(",CHANNEL_ID\n");
        extractSqlBuffer.append(",CHANNEL_NAME\n");
        extractSqlBuffer.append(",REGISTER_STATUS\n");
        extractSqlBuffer.append(",CRM_CODE\n");
        extractSqlBuffer.append(",CRMR_MSG\n");
        extractSqlBuffer.append(",CREATOR\n");
        extractSqlBuffer.append(",TO_TIMESTAMP_LTZ(CREATE_DATE, 3) AS CREATE_DATE\n");
        extractSqlBuffer.append(",REGISTER_CHANNEL\n");
        extractSqlBuffer.append(" FROM SYS_REGISTER ");
        String extractSql = extractSqlBuffer.toString();
        
        TableResult result = tEnv.executeSql(extractSql);
        result.print();
    }

}
