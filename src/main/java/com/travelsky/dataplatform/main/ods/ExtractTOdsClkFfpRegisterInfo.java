package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
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
public class ExtractTOdsClkFfpRegisterInfo {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
//        String etlDate = "2022-03-27";
        String startDate = etlDate;
        String endDate = etlDate;
        if (args.length > 2) {
            startDate = args[1];
            endDate = args[2];
        }
        String sm4key =  Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsClkFfpRegisterInfo");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();

//创建上游数据源表
        StringBuffer ffpRegisterInfoSql = new StringBuffer();
        ffpRegisterInfoSql.append("CREATE TABLE FFP_REGISTER_INFO (\n");
        ffpRegisterInfoSql.append("ID VARCHAR(192) ,\n");
        ffpRegisterInfoSql.append("FID VARCHAR(765) ,\n");
        ffpRegisterInfoSql.append("FFLTDATE VARCHAR(765) ,\n");
        ffpRegisterInfoSql.append("FNAMECN VARCHAR(765) ,\n");
        ffpRegisterInfoSql.append("F_FIRTST_NAME_CN VARCHAR(765) ,\n");
        ffpRegisterInfoSql.append("F_SECOND_NAME_CN VARCHAR(765) ,\n");
        ffpRegisterInfoSql.append("FNAMEEN VARCHAR(765) ,\n");
        ffpRegisterInfoSql.append("F_FIRST_NAME_EN VARCHAR(765) ,\n");
        ffpRegisterInfoSql.append("F_SECOND_NAME_EN VARCHAR(765) ,\n");
        ffpRegisterInfoSql.append("FSEX VARCHAR(30) ,\n");
        ffpRegisterInfoSql.append("FBIRTHDAY VARCHAR(765) ,\n");
        ffpRegisterInfoSql.append("FCARDTYPE VARCHAR(150) ,\n");
        ffpRegisterInfoSql.append("FIDCARD VARCHAR(150) ,\n");
        ffpRegisterInfoSql.append("FPHONE VARCHAR(270) ,\n");
        ffpRegisterInfoSql.append("FADDRESS VARCHAR(765) ,\n");
        ffpRegisterInfoSql.append("FFLTNUM VARCHAR(150) ,\n");
        ffpRegisterInfoSql.append("FSEATNUM VARCHAR(60) ,\n");
        ffpRegisterInfoSql.append("FCARD VARCHAR(150) ,\n");
        ffpRegisterInfoSql.append("FDEPT VARCHAR(150) ,\n");
        ffpRegisterInfoSql.append("FUSER VARCHAR(150) ,\n");
        ffpRegisterInfoSql.append("FUPDATE VARCHAR(150) ,\n");
        ffpRegisterInfoSql.append("FDOWN VARCHAR(30) ,\n");
        ffpRegisterInfoSql.append("FOPERATOR VARCHAR(300) ,\n");
        ffpRegisterInfoSql.append("FDEMO VARCHAR(765) ,\n");
        ffpRegisterInfoSql.append("BL1 VARCHAR(1500) ,\n");
        ffpRegisterInfoSql.append("EMAIL VARCHAR(150) ,\n");
        ffpRegisterInfoSql.append("CITY VARCHAR(600) ,\n");
        ffpRegisterInfoSql.append("PROVINCE VARCHAR(600) ,\n");
        ffpRegisterInfoSql.append("CARRIER_AIRLINE VARCHAR(30) ,\n");
        ffpRegisterInfoSql.append("PWD VARCHAR(150) ,\n");
        ffpRegisterInfoSql.append("SEND_STATUS VARCHAR(6) ,\n");
        ffpRegisterInfoSql.append("SEND_TIME TIMESTAMP(6) ,\n");
        ffpRegisterInfoSql.append("PARENT_FFP_NUM VARCHAR(150) ,\n");
        ffpRegisterInfoSql.append("OPERATE_DEPT VARCHAR(120) ,\n");
        ffpRegisterInfoSql.append("ORG VARCHAR(30) ,\n");
        ffpRegisterInfoSql.append("DEST VARCHAR(30) ,\n");
        ffpRegisterInfoSql.append("OP_ACCOUNT VARCHAR(150) ,\n");
        ffpRegisterInfoSql.append("OP_NAME VARCHAR(150) ,\n");
        ffpRegisterInfoSql.append("CREW VARCHAR(900) ,\n");
        ffpRegisterInfoSql.append("BOOKING_CLASS VARCHAR(30) ,\n");
        ffpRegisterInfoSql.append("TKT_NUM VARCHAR(60) ,\n");
        ffpRegisterInfoSql.append("BOARDING_NUMBER VARCHAR(60) ,\n");
        ffpRegisterInfoSql.append("RETRO_FLAG VARCHAR(15) ,\n");
        ffpRegisterInfoSql.append("WS_FEED_MSG VARCHAR(1500) ,\n");
        ffpRegisterInfoSql.append("RETRO_FEED_MSG VARCHAR(1500) ,\n");
        ffpRegisterInfoSql.append("DEV_ACCOUNT VARCHAR(150) ,\n");
        ffpRegisterInfoSql.append("DATA_ORIGIN VARCHAR(30) ,\n");
        ffpRegisterInfoSql.append("CREW_BASE VARCHAR(300) ,\n");
        ffpRegisterInfoSql.append("INTEGRAL_FLAG VARCHAR(30) ,\n");
        ffpRegisterInfoSql.append("SEND_FLAG VARCHAR(30) ,\n");
        ffpRegisterInfoSql.append("RETRO_DATE TIMESTAMP(6) ,\n");
        ffpRegisterInfoSql.append("REMARKS VARCHAR(765) ,\n");
        ffpRegisterInfoSql.append("DEL_FLAG CHAR(1) ,\n");
        ffpRegisterInfoSql.append("CREATE_BY VARCHAR(192) ,\n");
        ffpRegisterInfoSql.append("CREATE_DATE TIMESTAMP(6) ,\n");
        ffpRegisterInfoSql.append("UPDATE_BY VARCHAR(192) ,\n");
        ffpRegisterInfoSql.append("UPDATE_DATE TIMESTAMP(6) ,\n");
        ffpRegisterInfoSql.append("STATUS_FLG VARCHAR(30) ,\n");
        ffpRegisterInfoSql.append("ETL_INSERT_DT TIMESTAMP(6) ,\n");
        ffpRegisterInfoSql.append("ETL_UPD_DT TIMESTAMP(6) ,\n");
        ffpRegisterInfoSql.append("HF_FLAG VARCHAR(3) ,\n");
        ffpRegisterInfoSql.append("COUNTRY_CODE VARCHAR(45) ,\n");
        ffpRegisterInfoSql.append("ETL_HANDLE_FLAG VARCHAR(30)\n");
        ffpRegisterInfoSql.append(") WITH (\n");
        ffpRegisterInfoSql.append("    'connector' = 'jdbc',\n");
        ffpRegisterInfoSql.append("    'url' = 'jdbc:oracle:thin:@//").append(Constants.CLK_IP).append(":").append(Constants.CLK_PORT).append("/").append(Constants.CLK_DB).append("',\n");
        ffpRegisterInfoSql.append("    'table-name' = '").append(Constants.CLK_SCHEMA).append(".FFP_REGISTER_INFO', \n");
        ffpRegisterInfoSql.append("    'username' = '").append(Constants.CLK_USER).append("',\n");
        ffpRegisterInfoSql.append("    'password' = '").append(Constants.CLK_PWD).append("'\n");
        ffpRegisterInfoSql.append(",\n").append("  'driver' = '").append(Constants.ORACLE_DRIVER).append("'");
        ffpRegisterInfoSql.append(")");
        tEnv.executeSql(ffpRegisterInfoSql.toString());
        //创建Doris目标表
        tEnv.executeSql(CreateTableSql.CLK_FFP_REGISTER_INFO);
        //数据抽取sql，配置增量字段、数据加密等
        StringBuffer extractSqlBuffer = new StringBuffer();
        extractSqlBuffer.append("INSERT INTO T_ODS_CLK_FFP_REGISTER_INFO(");
        extractSqlBuffer.append("ETL_DATE,\n");
        extractSqlBuffer.append("ID,\n");
        extractSqlBuffer.append("FID,\n");
        extractSqlBuffer.append("FFLTDATE,\n");
        extractSqlBuffer.append("FNAMECN,\n");
        extractSqlBuffer.append("F_FIRTST_NAME_CN,\n");
        extractSqlBuffer.append("F_SECOND_NAME_CN,\n");
        extractSqlBuffer.append("FNAMEEN,\n");
        extractSqlBuffer.append("F_FIRST_NAME_EN,\n");
        extractSqlBuffer.append("F_SECOND_NAME_EN,\n");
        extractSqlBuffer.append("FSEX,\n");
        extractSqlBuffer.append("FBIRTHDAY,\n");
        extractSqlBuffer.append("FCARDTYPE,\n");
        extractSqlBuffer.append("FIDCARD,\n");
        extractSqlBuffer.append("FPHONE,\n");
        extractSqlBuffer.append("FADDRESS,\n");
        extractSqlBuffer.append("FFLTNUM,\n");
        extractSqlBuffer.append("FSEATNUM,\n");
        extractSqlBuffer.append("FCARD,\n");
        extractSqlBuffer.append("FDEPT,\n");
        extractSqlBuffer.append("FUSER,\n");
        extractSqlBuffer.append("FUPDATE,\n");
        extractSqlBuffer.append("FDOWN,\n");
        extractSqlBuffer.append("FOPERATOR,\n");
        extractSqlBuffer.append("FDEMO,\n");
        extractSqlBuffer.append("BL1,\n");
        extractSqlBuffer.append("EMAIL,\n");
        extractSqlBuffer.append("CITY,\n");
        extractSqlBuffer.append("PROVINCE,\n");
        extractSqlBuffer.append("CARRIER_AIRLINE,\n");
        extractSqlBuffer.append("PWD,\n");
        extractSqlBuffer.append("SEND_STATUS,\n");
        extractSqlBuffer.append("SEND_TIME,\n");
        extractSqlBuffer.append("PARENT_FFP_NUM,\n");
        extractSqlBuffer.append("OPERATE_DEPT,\n");
        extractSqlBuffer.append("ORG,\n");
        extractSqlBuffer.append("DEST,\n");
        extractSqlBuffer.append("OP_ACCOUNT,\n");
        extractSqlBuffer.append("OP_NAME,\n");
        extractSqlBuffer.append("CREW,\n");
        extractSqlBuffer.append("BOOKING_CLASS,\n");
        extractSqlBuffer.append("TKT_NUM,\n");
        extractSqlBuffer.append("BOARDING_NUMBER,\n");
        extractSqlBuffer.append("RETRO_FLAG,\n");
        extractSqlBuffer.append("WS_FEED_MSG,\n");
        extractSqlBuffer.append("RETRO_FEED_MSG,\n");
        extractSqlBuffer.append("DEV_ACCOUNT,\n");
        extractSqlBuffer.append("DATA_ORIGIN,\n");
        extractSqlBuffer.append("CREW_BASE,\n");
        extractSqlBuffer.append("INTEGRAL_FLAG,\n");
        extractSqlBuffer.append("SEND_FLAG,\n");
        extractSqlBuffer.append("RETRO_DATE,\n");
        extractSqlBuffer.append("REMARKS,\n");
        extractSqlBuffer.append("DEL_FLAG,\n");
        extractSqlBuffer.append("CREATE_BY,\n");
        extractSqlBuffer.append("CREATE_DATE,\n");
        extractSqlBuffer.append("UPDATE_BY,\n");
        extractSqlBuffer.append("UPDATE_DATE,\n");
        extractSqlBuffer.append("STATUS_FLG,\n");
        extractSqlBuffer.append("ETL_INSERT_DT,\n");
        extractSqlBuffer.append("ETL_UPD_DT,\n");
        extractSqlBuffer.append("HF_FLAG,\n");
        extractSqlBuffer.append("COUNTRY_CODE,\n");
        extractSqlBuffer.append("ETL_HANDLE_FLAG)\n");
        extractSqlBuffer.append("SELECT ");
        extractSqlBuffer.append("CAST('").append(etlDate).append("' AS DATE) ETL_DATE,");
        extractSqlBuffer.append("ID,\n");
        extractSqlBuffer.append("FID,\n");
        extractSqlBuffer.append("FFLTDATE,\n");
        extractSqlBuffer.append("FNAMECN,\n");
        extractSqlBuffer.append("F_FIRTST_NAME_CN,\n");
        extractSqlBuffer.append("F_SECOND_NAME_CN,\n");
        extractSqlBuffer.append("FNAMEEN,\n");
        extractSqlBuffer.append("F_FIRST_NAME_EN,\n");
        extractSqlBuffer.append("F_SECOND_NAME_EN,\n");
        extractSqlBuffer.append("FSEX,\n");
        extractSqlBuffer.append("FBIRTHDAY,\n");
        extractSqlBuffer.append("FCARDTYPE,\n");
        extractSqlBuffer.append("  sm4_encrypt(aes_decrypt(FIDCARD, '").append(Constants.LYGJ_AES_KEY).append("'), '").append(sm4key).append("') FIDCARD,\n");
        extractSqlBuffer.append("  sm4_encrypt(aes_decrypt(FPHONE, '").append(Constants.LYGJ_AES_KEY).append("'), '").append(sm4key).append("') FPHONE,\n");
        extractSqlBuffer.append("FADDRESS,\n");
        extractSqlBuffer.append("FFLTNUM,\n");
        extractSqlBuffer.append("FSEATNUM,\n");
        extractSqlBuffer.append(" sm4_encrypt (FCARD,'").append(sm4key).append("') FCARD, \n");
        extractSqlBuffer.append("FDEPT,\n");
        extractSqlBuffer.append("FUSER,\n");
        extractSqlBuffer.append("FUPDATE,\n");
        extractSqlBuffer.append("FDOWN,\n");
        extractSqlBuffer.append("FOPERATOR,\n");
        extractSqlBuffer.append("FDEMO,\n");
        extractSqlBuffer.append("BL1,\n");
        extractSqlBuffer.append(" sm4_encrypt (EMAIL,'").append(sm4key).append("') EMAIL, \n");
        extractSqlBuffer.append("CITY,\n");
        extractSqlBuffer.append("PROVINCE,\n");
        extractSqlBuffer.append("CARRIER_AIRLINE,\n");
        extractSqlBuffer.append(" PWD,\n");
        extractSqlBuffer.append("SEND_STATUS,\n");
        extractSqlBuffer.append("SEND_TIME,\n");
        extractSqlBuffer.append(" sm4_encrypt (PARENT_FFP_NUM,'").append(sm4key).append("') PARENT_FFP_NUM, \n");
        extractSqlBuffer.append("OPERATE_DEPT,\n");
        extractSqlBuffer.append("ORG,\n");
        extractSqlBuffer.append("DEST,\n");
        extractSqlBuffer.append("OP_ACCOUNT,\n");
        extractSqlBuffer.append("OP_NAME,\n");
        extractSqlBuffer.append("CREW,\n");
        extractSqlBuffer.append("BOOKING_CLASS,\n");
        extractSqlBuffer.append("TKT_NUM,\n");
        extractSqlBuffer.append("BOARDING_NUMBER,\n");
        extractSqlBuffer.append("RETRO_FLAG,\n");
        extractSqlBuffer.append("WS_FEED_MSG,\n");
        extractSqlBuffer.append("RETRO_FEED_MSG,\n");
        extractSqlBuffer.append("DEV_ACCOUNT,\n");
        extractSqlBuffer.append("DATA_ORIGIN,\n");
        extractSqlBuffer.append("CREW_BASE,\n");
        extractSqlBuffer.append("INTEGRAL_FLAG,\n");
        extractSqlBuffer.append("SEND_FLAG,\n");
        extractSqlBuffer.append("RETRO_DATE,\n");
        extractSqlBuffer.append("REMARKS,\n");
        extractSqlBuffer.append("DEL_FLAG,\n");
        extractSqlBuffer.append("CREATE_BY,\n");
        extractSqlBuffer.append("CREATE_DATE,\n");
        extractSqlBuffer.append("UPDATE_BY,\n");
        extractSqlBuffer.append("UPDATE_DATE,\n");
        extractSqlBuffer.append("STATUS_FLG,\n");
        extractSqlBuffer.append("ETL_INSERT_DT,\n");
        extractSqlBuffer.append("ETL_UPD_DT,\n");
        extractSqlBuffer.append("HF_FLAG,\n");
        extractSqlBuffer.append("COUNTRY_CODE,\n");
        extractSqlBuffer.append("ETL_HANDLE_FLAG\n");
        extractSqlBuffer.append(" FROM FFP_REGISTER_INFO ");
        extractSqlBuffer.append(" WHERE  CREATE_DATE BETWEEN TIMESTAMP '").append(startDate).append(" 00:00:00' AND TIMESTAMP '").append(endDate).append(" 23:59:59.999' ");
        extractSqlBuffer.append(" OR  UPDATE_DATE BETWEEN TIMESTAMP '").append(startDate).append(" 00:00:00' AND TIMESTAMP '").append(endDate).append(" 23:59:59.999' ");
        String extractSql = extractSqlBuffer.toString();

        TableResult result = tEnv.executeSql(extractSql);

        result.print();


    }
}
