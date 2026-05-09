package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.OracleAESDecryptor;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
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
 * @date 2025/7/2 9:31
 */
public class ExtractTOdsZxyhCrmWebNoRegistered {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZxyhCrmWebNoRegistered");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE CRM_WEB_NO_REGISTERED (\n" +
                "ID VARCHAR(60) ,\n" +
                "USERID VARCHAR(150) ,\n" +
                "SEX VARCHAR(3) ,\n" +
                "USERNAME VARCHAR(150) ,\n" +
                "USERPWD VARCHAR(150) ,\n" +
                "REQUESTION VARCHAR(30) ,\n" +
                "REANSWER VARCHAR(300) ,\n" +
                "BORNDATE VARCHAR(30) ,\n" +
                "NATIONALITY VARCHAR(30) ,\n" +
                "FOLK VARCHAR(30) ,\n" +
                "IDETIFYTYPE VARCHAR(6) ,\n" +
                "IDENTIFYNO VARCHAR(300) ,\n" +
                "NATION VARCHAR(60) ,\n" +
                "PROVINCE VARCHAR(300) ,\n" +
                "CITY VARCHAR(300) ,\n" +
                "POSTCODE VARCHAR(60) ,\n" +
                "MOBILEPHONE VARCHAR(60) ,\n" +
                "EMAIL VARCHAR(150) ,\n" +
                "PHONE VARCHAR(60) ,\n" +
                "ADDRESTRING VARCHAR(300) ,\n" +
                "ADDRESTRING2 VARCHAR(300) ,\n" +
                "ADDRESTRING3 VARCHAR(300) ,\n" +
                "COMPANYNAME VARCHAR(150) ,\n" +
                "`POSITION` VARCHAR(60) ,\n" +
                "COMPANYTELEPHONE VARCHAR(60) ,\n" +
                "COMPANYNATION VARCHAR(60) ,\n" +
                "COMPANYPROVINCE VARCHAR(300) ,\n" +
                "COMPANYCITY VARCHAR(300) ,\n" +
                "COMPANYPOSTCODE VARCHAR(60) ,\n" +
                "COMPANYADDRESS VARCHAR(60) ,\n" +
                "COMPANYADDRESS2 VARCHAR(60) ,\n" +
                "COMPANYADDRESS3 VARCHAR(60) ,\n" +
                "CARDTYPE VARCHAR(60) ,\n" +
                "REGDEPARTMENT VARCHAR(60) ,\n" +
                "EDUDG VARCHAR(60) ,\n" +
                "WORK VARCHAR(60) ,\n" +
                "AIRPORT VARCHAR(60) ,\n" +
                "FFPFLAG VARCHAR(60) ,\n" +
                "AUTHORIZE VARCHAR(60) ,\n" +
                "DISABLEREASON VARCHAR(300) ,\n" +
                "ABLEREASON VARCHAR(300) ,\n" +
                "REGTIME VARCHAR(60) ,\n" +
                "UPDATETIME VARCHAR(60) ,\n" +
                "FAMILY_NAME VARCHAR(60) ,\n" +
                "GIVEN_NAME VARCHAR(60) ,\n" +
                "CONTACTTYPE VARCHAR(60) ,\n" +
                "USERSTATUS VARCHAR(60) ,\n" +
                "BINDABLEREASON VARCHAR(300) ,\n" +
                "FFPID VARCHAR(60) ,\n" +
                "NICKNAME VARCHAR(60) ,\n" +
                "UPDATESUCC VARCHAR(60) ,\n" +
                "ACCOUNTYPE VARCHAR(60) ,\n" +
                "BINDEMAIL VARCHAR(150) ,\n" +
                "BINDMOBILE VARCHAR(60) ,\n" +
                "USERTYPE VARCHAR(60) ,\n" +
                "USERFROM VARCHAR(60) ,\n" +
                "USERCATEGORY VARCHAR(60) ,\n" +
                "IS_MANJIAN VARCHAR(60) ,\n" +
                "IS_LIMITED VARCHAR(60) ,\n" +
                "IS_REALNAME VARCHAR(60) ,\n" +
                "DISABLE_TIME VARCHAR(60) ,\n" +
                "DISABLE_OPERATOR VARCHAR(60) ,\n" +
                "IS_WHITELIST VARCHAR(60) ,\n" +
                "CRMID VARCHAR(60) ,\n" +
                "PHONELAND VARCHAR(60) ,\n" +
                "LAST_LOGIN_TIME VARCHAR(60) ,\n" +
                "PINYIN_SURNAME VARCHAR(60) ,\n" +
                "PINYIN_FIRSTNAME VARCHAR(60) ,\n" +
                "BUSINESS_DEPARTMENT VARCHAR(60) ,\n" +
                "TICKET_COUNT VARCHAR(60) ,\n" +
                "SUCCESSFUL_ORDERS_COUNT VARCHAR(60) ,\n" +
                "CONSUMPTION_AMOUNT VARCHAR(60) ,\n" +
                "CREDIT_STATUS VARCHAR(60) ,\n" +
                "CREDIT_SCORE VARCHAR(60) ,\n" +
                "VIP_TYPE VARCHAR(60) ,\n" +
                "CONTACT_NAME VARCHAR(60) ,\n" +
                "CONTACT_MOBILE_PHONE VARCHAR(60) ,\n" +
                "CONTACT_EMAIL VARCHAR(150) ,\n" +
                "CONTACT_PHONE VARCHAR(60) ,\n" +
                "BOOKING_TIME VARCHAR(60) ,\n" +
                "REGISTER_IP VARCHAR(60) ,\n" +
                "LOGIN_COUNT VARCHAR(60) ,\n" +
                "VERIFYMOBILE_TIME VARCHAR(60) ,\n" +
                "DELETE_TIME VARCHAR(60) ,\n" +
                "IS_DELETE CHAR(1) ,\n" +
                "IS_PROFILEREALNAME VARCHAR(3072) ,\n" +
                "REFRESH_FLAG VARCHAR(6) ,\n" +
                "ENABLE CHAR(1) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(96) ,\n" +
                "UPDATE_USER VARCHAR(96),\n" +
                "WEB_IS_LIMITED VARCHAR(30)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.ZXYH_IP+":"+Constants.ZXYH_PORT+"/"+Constants.ZXYH_DB+"',\n" +
                "    'table-name' = '"+Constants.ZXYH_SCHEMA+".CRM_WEB_NO_REGISTERED', \n" +
                "    'username' = '"+Constants.ZXYH_USER+"',\n" +
                "    'password' = '"+Constants.ZXYH_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_ZXYH_CRM_WEB_NO_REGISTERED (\n" +
                "    ETL_DATE DATE,\n" +
                "ID VARCHAR(60) ,\n" +
                "USERID VARCHAR(150) ,\n" +
                "SEX VARCHAR(3) ,\n" +
                "USERNAME VARCHAR(150) ,\n" +
                "USERPWD VARCHAR(768) ,\n" +
                "REQUESTION VARCHAR(30) ,\n" +
                "REANSWER VARCHAR(300) ,\n" +
                "BORNDATE VARCHAR(30) ,\n" +
                "NATIONALITY VARCHAR(30) ,\n" +
                "FOLK VARCHAR(30) ,\n" +
                "IDETIFYTYPE VARCHAR(6) ,\n" +
                "IDENTIFYNO VARCHAR(768) ,\n" +
                "NATION VARCHAR(60) ,\n" +
                "PROVINCE VARCHAR(300) ,\n" +
                "CITY VARCHAR(300) ,\n" +
                "POSTCODE VARCHAR(60) ,\n" +
                "MOBILEPHONE VARCHAR(768) ,\n" +
                "EMAIL VARCHAR(768) ,\n" +
                "PHONE VARCHAR(768) ,\n" +
                "ADDRESTRING VARCHAR(300) ,\n" +
                "ADDRESTRING2 VARCHAR(300) ,\n" +
                "ADDRESTRING3 VARCHAR(300) ,\n" +
                "COMPANYNAME VARCHAR(150) ,\n" +
                "`POSITION` VARCHAR(60) ,\n" +
                "COMPANYTELEPHONE VARCHAR(60) ,\n" +
                "COMPANYNATION VARCHAR(60) ,\n" +
                "COMPANYPROVINCE VARCHAR(300) ,\n" +
                "COMPANYCITY VARCHAR(300) ,\n" +
                "COMPANYPOSTCODE VARCHAR(60) ,\n" +
                "COMPANYADDRESS VARCHAR(60) ,\n" +
                "COMPANYADDRESS2 VARCHAR(60) ,\n" +
                "COMPANYADDRESS3 VARCHAR(60) ,\n" +
                "CARDTYPE VARCHAR(60) ,\n" +
                "REGDEPARTMENT VARCHAR(60) ,\n" +
                "EDUDG VARCHAR(60) ,\n" +
                "WORK VARCHAR(60) ,\n" +
                "AIRPORT VARCHAR(60) ,\n" +
                "FFPFLAG VARCHAR(60) ,\n" +
                "AUTHORIZE VARCHAR(60) ,\n" +
                "DISABLEREASON VARCHAR(300) ,\n" +
                "ABLEREASON VARCHAR(300) ,\n" +
                "REGTIME VARCHAR(60) ,\n" +
                "UPDATETIME VARCHAR(60) ,\n" +
                "FAMILY_NAME VARCHAR(60) ,\n" +
                "GIVEN_NAME VARCHAR(60) ,\n" +
                "CONTACTTYPE VARCHAR(60) ,\n" +
                "USERSTATUS VARCHAR(60) ,\n" +
                "BINDABLEREASON VARCHAR(300) ,\n" +
                "FFPID VARCHAR(60) ,\n" +
                "NICKNAME VARCHAR(60) ,\n" +
                "UPDATESUCC VARCHAR(60) ,\n" +
                "ACCOUNTYPE VARCHAR(60) ,\n" +
                "BINDEMAIL VARCHAR(150) ,\n" +
                "BINDMOBILE VARCHAR(60) ,\n" +
                "USERTYPE VARCHAR(60) ,\n" +
                "USERFROM VARCHAR(60) ,\n" +
                "USERCATEGORY VARCHAR(60) ,\n" +
                "IS_MANJIAN VARCHAR(60) ,\n" +
                "IS_LIMITED VARCHAR(60) ,\n" +
                "IS_REALNAME VARCHAR(60) ,\n" +
                "DISABLE_TIME VARCHAR(60) ,\n" +
                "DISABLE_OPERATOR VARCHAR(60) ,\n" +
                "IS_WHITELIST VARCHAR(60) ,\n" +
                "CRMID VARCHAR(60) ,\n" +
                "PHONELAND VARCHAR(60) ,\n" +
                "LAST_LOGIN_TIME VARCHAR(60) ,\n" +
                "PINYIN_SURNAME VARCHAR(60) ,\n" +
                "PINYIN_FIRSTNAME VARCHAR(60) ,\n" +
                "BUSINESS_DEPARTMENT VARCHAR(60) ,\n" +
                "TICKET_COUNT VARCHAR(60) ,\n" +
                "SUCCESSFUL_ORDERS_COUNT VARCHAR(60) ,\n" +
                "CONSUMPTION_AMOUNT VARCHAR(60) ,\n" +
                "CREDIT_STATUS VARCHAR(60) ,\n" +
                "CREDIT_SCORE VARCHAR(60) ,\n" +
                "VIP_TYPE VARCHAR(60) ,\n" +
                "CONTACT_NAME VARCHAR(60) ,\n" +
                "CONTACT_MOBILE_PHONE VARCHAR(60) ,\n" +
                "CONTACT_EMAIL VARCHAR(150) ,\n" +
                "CONTACT_PHONE VARCHAR(60) ,\n" +
                "BOOKING_TIME VARCHAR(60) ,\n" +
                "REGISTER_IP VARCHAR(60) ,\n" +
                "LOGIN_COUNT VARCHAR(60) ,\n" +
                "VERIFYMOBILE_TIME VARCHAR(60) ,\n" +
                "DELETE_TIME VARCHAR(60) ,\n" +
                "IS_DELETE CHAR(1) ,\n" +
                "IS_PROFILEREALNAME VARCHAR(3072) ,\n" +
                "REFRESH_FLAG VARCHAR(6) ,\n" +
                "ENABLE CHAR(1) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "CREATE_USER VARCHAR(96) ,\n" +
                "UPDATE_USER VARCHAR(96),\n" +
                "WEB_IS_LIMITED VARCHAR(30)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZXYH_CRM_WEB_NO_REGISTERED',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_ZXYH_CRM_WEB_NO_REGISTERED(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "USERID,\n" +
                "SEX,\n" +
                "USERNAME,\n" +
                "USERPWD,\n" +
                "REQUESTION,\n" +
                "REANSWER,\n" +
                "BORNDATE,\n" +
                "NATIONALITY,\n" +
                "FOLK,\n" +
                "IDETIFYTYPE,\n" +
                "IDENTIFYNO,\n" +
                "NATION,\n" +
                "PROVINCE,\n" +
                "CITY,\n" +
                "POSTCODE,\n" +
                "MOBILEPHONE,\n" +
                "EMAIL,\n" +
                "PHONE,\n" +
                "ADDRESTRING,\n" +
                "ADDRESTRING2,\n" +
                "ADDRESTRING3,\n" +
                "COMPANYNAME,\n" +
                "`POSITION`,\n" +
                "COMPANYTELEPHONE,\n" +
                "COMPANYNATION,\n" +
                "COMPANYPROVINCE,\n" +
                "COMPANYCITY,\n" +
                "COMPANYPOSTCODE,\n" +
                "COMPANYADDRESS,\n" +
                "COMPANYADDRESS2,\n" +
                "COMPANYADDRESS3,\n" +
                "CARDTYPE,\n" +
                "REGDEPARTMENT,\n" +
                "EDUDG,\n" +
                "WORK,\n" +
                "AIRPORT,\n" +
                "FFPFLAG,\n" +
                "AUTHORIZE,\n" +
                "DISABLEREASON,\n" +
                "ABLEREASON,\n" +
                "REGTIME,\n" +
                "UPDATETIME,\n" +
                "FAMILY_NAME,\n" +
                "GIVEN_NAME,\n" +
                "CONTACTTYPE,\n" +
                "USERSTATUS,\n" +
                "BINDABLEREASON,\n" +
                "FFPID,\n" +
                "NICKNAME,\n" +
                "UPDATESUCC,\n" +
                "ACCOUNTYPE,\n" +
                "BINDEMAIL,\n" +
                "BINDMOBILE,\n" +
                "USERTYPE,\n" +
                "USERFROM,\n" +
                "USERCATEGORY,\n" +
                "IS_MANJIAN,\n" +
                "IS_LIMITED,\n" +
                "IS_REALNAME,\n" +
                "DISABLE_TIME,\n" +
                "DISABLE_OPERATOR,\n" +
                "IS_WHITELIST,\n" +
                "CRMID,\n" +
                "PHONELAND,\n" +
                "LAST_LOGIN_TIME,\n" +
                "PINYIN_SURNAME,\n" +
                "PINYIN_FIRSTNAME,\n" +
                "BUSINESS_DEPARTMENT,\n" +
                "TICKET_COUNT,\n" +
                "SUCCESSFUL_ORDERS_COUNT,\n" +
                "CONSUMPTION_AMOUNT,\n" +
                "CREDIT_STATUS,\n" +
                "CREDIT_SCORE,\n" +
                "VIP_TYPE,\n" +
                "CONTACT_NAME,\n" +
                "CONTACT_MOBILE_PHONE,\n" +
                "CONTACT_EMAIL,\n" +
                "CONTACT_PHONE,\n" +
                "BOOKING_TIME,\n" +
                "REGISTER_IP,\n" +
                "LOGIN_COUNT,\n" +
                "VERIFYMOBILE_TIME,\n" +
                "DELETE_TIME,\n" +
                "IS_DELETE,\n" +
                "IS_PROFILEREALNAME,\n" +
                "REFRESH_FLAG,\n" +
                "ENABLE,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_USER,\n" +
                "WEB_IS_LIMITED)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "USERID,\n" +
                "SEX,\n" +
                "USERNAME,\n" +
                "  sm4_encrypt(aes_decrypt(USERPWD, '" + Constants.ZXYH_AES_KEY + "'), '" + sm4key + "') USERPWD,\n" +
                "REQUESTION,\n" +
                "REANSWER,\n" +
                "BORNDATE,\n" +
                "NATIONALITY,\n" +
                "FOLK,\n" +
                "IDETIFYTYPE,\n" +
                "  sm4_encrypt(aes_decrypt(IDENTIFYNO, '" + Constants.ZXYH_AES_KEY + "'), '" + sm4key + "') IDENTIFYNO,\n" +
                "NATION,\n" +
                "PROVINCE,\n" +
                "CITY,\n" +
                "POSTCODE,\n" +
                "  sm4_encrypt(aes_decrypt(MOBILEPHONE, '" + Constants.ZXYH_AES_KEY + "'), '" + sm4key + "') MOBILEPHONE,\n" +
                "  sm4_encrypt(aes_decrypt(EMAIL, '" + Constants.ZXYH_AES_KEY + "'), '" + sm4key + "') EMAIL,\n" +
                "  sm4_encrypt(aes_decrypt(PHONE, '" + Constants.ZXYH_AES_KEY + "'), '" + sm4key + "') PHONE,\n" +
                "ADDRESTRING,\n" +
                "ADDRESTRING2,\n" +
                "ADDRESTRING3,\n" +
                "COMPANYNAME,\n" +
                "`POSITION`,\n" +
                "COMPANYTELEPHONE,\n" +
                "COMPANYNATION,\n" +
                "COMPANYPROVINCE,\n" +
                "COMPANYCITY,\n" +
                "COMPANYPOSTCODE,\n" +
                "COMPANYADDRESS,\n" +
                "COMPANYADDRESS2,\n" +
                "COMPANYADDRESS3,\n" +
                "CARDTYPE,\n" +
                "REGDEPARTMENT,\n" +
                "EDUDG,\n" +
                "WORK,\n" +
                "AIRPORT,\n" +
                "FFPFLAG,\n" +
                "AUTHORIZE,\n" +
                "DISABLEREASON,\n" +
                "ABLEREASON,\n" +
                "REGTIME,\n" +
                "UPDATETIME,\n" +
                "FAMILY_NAME,\n" +
                "GIVEN_NAME,\n" +
                "CONTACTTYPE,\n" +
                "USERSTATUS,\n" +
                "BINDABLEREASON,\n" +
                "FFPID,\n" +
                "NICKNAME,\n" +
                "UPDATESUCC,\n" +
                "ACCOUNTYPE,\n" +
                "BINDEMAIL,\n" +
                "BINDMOBILE,\n" +
                "USERTYPE,\n" +
                "USERFROM,\n" +
                "USERCATEGORY,\n" +
                "IS_MANJIAN,\n" +
                "IS_LIMITED,\n" +
                "IS_REALNAME,\n" +
                "DISABLE_TIME,\n" +
                "DISABLE_OPERATOR,\n" +
                "IS_WHITELIST,\n" +
                "CRMID,\n" +
                "PHONELAND,\n" +
                "LAST_LOGIN_TIME,\n" +
                "PINYIN_SURNAME,\n" +
                "PINYIN_FIRSTNAME,\n" +
                "BUSINESS_DEPARTMENT,\n" +
                "TICKET_COUNT,\n" +
                "SUCCESSFUL_ORDERS_COUNT,\n" +
                "CONSUMPTION_AMOUNT,\n" +
                "CREDIT_STATUS,\n" +
                "CREDIT_SCORE,\n" +
                "VIP_TYPE,\n" +
                "CONTACT_NAME,\n" +
                "CONTACT_MOBILE_PHONE,\n" +
                "CONTACT_EMAIL,\n" +
                "CONTACT_PHONE,\n" +
                "BOOKING_TIME,\n" +
                "REGISTER_IP,\n" +
                "LOGIN_COUNT,\n" +
                "VERIFYMOBILE_TIME,\n" +
                "DELETE_TIME,\n" +
                "IS_DELETE,\n" +
                "IS_PROFILEREALNAME,\n" +
                "REFRESH_FLAG,\n" +
                "ENABLE,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_TIME,\n" +
                "CREATE_USER,\n" +
                "UPDATE_USER,\n" +
                "WEB_IS_LIMITED\n" +
                " FROM CRM_WEB_NO_REGISTERED "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
