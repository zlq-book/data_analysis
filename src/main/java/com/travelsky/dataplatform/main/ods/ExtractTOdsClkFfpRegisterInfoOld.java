package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.udf.OracleAESDecryptor;
import com.travelsky.dataplatform.udf.OracleDESDecryptor;
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
public class ExtractTOdsClkFfpRegisterInfoOld {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
//        String etlDate = "2016-04-13";
        String sm4key =  Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsClkFfpRegisterInfoOld");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
//        // 注册AES加密解密UDF
//        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        // 注册DES解密UDF
        tEnv.createTemporarySystemFunction("des_decrypt", OracleDESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();

//创建上游数据源表
        tEnv.executeSql("CREATE TABLE FFP_REGISTER_INFO (\n" +
                "ID VARCHAR(192) ,\n" +
                "FID VARCHAR(765) ,\n" +
                "FFLTDATE VARCHAR(765) ,\n" +
                "FNAMECN VARCHAR(765) ,\n" +
                "F_FIRTST_NAME_CN VARCHAR(765) ,\n" +
                "F_SECOND_NAME_CN VARCHAR(765) ,\n" +
                "FNAMEEN VARCHAR(765) ,\n" +
                "F_FIRST_NAME_EN VARCHAR(765) ,\n" +
                "F_SECOND_NAME_EN VARCHAR(765) ,\n" +
                "FSEX VARCHAR(30) ,\n" +
                "FBIRTHDAY VARCHAR(765) ,\n" +
                "FCARDTYPE VARCHAR(150) ,\n" +
                "FIDCARD VARCHAR(150) ,\n" +
                "FPHONE VARCHAR(270) ,\n" +
                "FADDRESS VARCHAR(765) ,\n" +
                "FFLTNUM VARCHAR(150) ,\n" +
                "FSEATNUM VARCHAR(60) ,\n" +
                "FCARD VARCHAR(150) ,\n" +
                "FDEPT VARCHAR(150) ,\n" +
                "FUSER VARCHAR(150) ,\n" +
                "FUPDATE VARCHAR(150) ,\n" +
                "FDOWN VARCHAR(30) ,\n" +
                "FOPERATOR VARCHAR(300) ,\n" +
                "FDEMO VARCHAR(765) ,\n" +
                "BL1 VARCHAR(1500) ,\n" +
                "EMAIL VARCHAR(150) ,\n" +
                "CITY VARCHAR(600) ,\n" +
                "PROVINCE VARCHAR(600) ,\n" +
                "CARRIER_AIRLINE VARCHAR(30) ,\n" +
                "PWD VARCHAR(150) ,\n" +
                "SEND_STATUS VARCHAR(6) ,\n" +
                "SEND_TIME TIMESTAMP(6) ,\n" +
                "PARENT_FFP_NUM VARCHAR(150) ,\n" +
                "OPERATE_DEPT VARCHAR(120) ,\n" +
                "ORG VARCHAR(30) ,\n" +
                "DEST VARCHAR(30) ,\n" +
                "OP_ACCOUNT VARCHAR(150) ,\n" +
                "OP_NAME VARCHAR(150) ,\n" +
                "CREW VARCHAR(900) ,\n" +
                "BOOKING_CLASS VARCHAR(30) ,\n" +
                "TKT_NUM VARCHAR(60) ,\n" +
                "BOARDING_NUMBER VARCHAR(60) ,\n" +
                "RETRO_FLAG VARCHAR(15) ,\n" +
                "WS_FEED_MSG VARCHAR(1500) ,\n" +
                "RETRO_FEED_MSG VARCHAR(1500) ,\n" +
                "DEV_ACCOUNT VARCHAR(150) ,\n" +
                "DATA_ORIGIN VARCHAR(30) ,\n" +
                "CREW_BASE VARCHAR(300) ,\n" +
                "INTEGRAL_FLAG VARCHAR(30) ,\n" +
                "SEND_FLAG VARCHAR(30) ,\n" +
                "RETRO_DATE TIMESTAMP(6) ,\n" +
                "REMARKS VARCHAR(765) ,\n" +
                "DEL_FLAG CHAR(1) ,\n" +
                "CREATE_BY VARCHAR(192) ,\n" +
                "CREATE_DATE TIMESTAMP(6) ,\n" +
                "UPDATE_BY VARCHAR(192) ,\n" +
                "UPDATE_DATE TIMESTAMP(6) ,\n" +
                "STATUS_FLG VARCHAR(30) ,\n" +
                "ETL_INSERT_DT TIMESTAMP(6) ,\n" +
                "ETL_UPD_DT TIMESTAMP(6) ,\n" +
                "HF_FLAG VARCHAR(3) ,\n" +
                "COUNTRY_CODE VARCHAR(45) ,\n" +
                "ETL_HANDLE_FLAG VARCHAR(30)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.CLK_IP+":"+Constants.CLK_PORT+"/"+Constants.CLK_DB+"',\n" +
                "    'table-name' = '"+Constants.CLK_SCHEMA+".FFP_REGISTER_INFO', \n" +
                "    'username' = '"+Constants.CLK_USER+"',\n" +
                "    'password' = '"+Constants.CLK_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql(CreateTableSql.CLK_FFP_REGISTER_INFO);
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_CLK_FFP_REGISTER_INFO(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "FID,\n" +
                "FFLTDATE,\n" +
                "FNAMECN,\n" +
                "F_FIRTST_NAME_CN,\n" +
                "F_SECOND_NAME_CN,\n" +
                "FNAMEEN,\n" +
                "F_FIRST_NAME_EN,\n" +
                "F_SECOND_NAME_EN,\n" +
                "FSEX,\n" +
                "FBIRTHDAY,\n" +
                "FCARDTYPE,\n" +
                "FIDCARD,\n" +
                "FPHONE,\n" +
                "FADDRESS,\n" +
                "FFLTNUM,\n" +
                "FSEATNUM,\n" +
                "FCARD,\n" +
                "FDEPT,\n" +
                "FUSER,\n" +
                "FUPDATE,\n" +
                "FDOWN,\n" +
                "FOPERATOR,\n" +
                "FDEMO,\n" +
                "BL1,\n" +
                "EMAIL,\n" +
                "CITY,\n" +
                "PROVINCE,\n" +
                "CARRIER_AIRLINE,\n" +
                "PWD,\n" +
                "SEND_STATUS,\n" +
                "SEND_TIME,\n" +
                "PARENT_FFP_NUM,\n" +
                "OPERATE_DEPT,\n" +
                "ORG,\n" +
                "DEST,\n" +
                "OP_ACCOUNT,\n" +
                "OP_NAME,\n" +
                "CREW,\n" +
                "BOOKING_CLASS,\n" +
                "TKT_NUM,\n" +
                "BOARDING_NUMBER,\n" +
                "RETRO_FLAG,\n" +
                "WS_FEED_MSG,\n" +
                "RETRO_FEED_MSG,\n" +
                "DEV_ACCOUNT,\n" +
                "DATA_ORIGIN,\n" +
                "CREW_BASE,\n" +
                "INTEGRAL_FLAG,\n" +
                "SEND_FLAG,\n" +
                "RETRO_DATE,\n" +
                "REMARKS,\n" +
                "DEL_FLAG,\n" +
                "CREATE_BY,\n" +
                "CREATE_DATE,\n" +
                "UPDATE_BY,\n" +
                "UPDATE_DATE,\n" +
                "STATUS_FLG,\n" +
                "ETL_INSERT_DT,\n" +
                "ETL_UPD_DT,\n" +
                "HF_FLAG,\n" +
                "COUNTRY_CODE,\n" +
                "ETL_HANDLE_FLAG)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "FID,\n" +
                "FFLTDATE,\n" +
                "FNAMECN,\n" +
                "F_FIRTST_NAME_CN,\n" +
                "F_SECOND_NAME_CN,\n" +
                "FNAMEEN,\n" +
                "F_FIRST_NAME_EN,\n" +
                "F_SECOND_NAME_EN,\n" +
                "FSEX,\n" +
                "FBIRTHDAY,\n" +
                "FCARDTYPE,\n" +
                "  sm4_encrypt(des_decrypt(FIDCARD, '" + Constants.LYGJ_ZB_AES_KEY + "'), '" + sm4key + "') FIDCARD,\n" +
                "  sm4_encrypt(des_decrypt(FPHONE, '" + Constants.LYGJ_ZB_AES_KEY + "'), '" + sm4key + "') FPHONE,\n" +
                "FADDRESS,\n" +
                "FFLTNUM,\n" +
                "FSEATNUM,\n" +
                " sm4_encrypt (FCARD,'" + sm4key + "') FCARD, \n" +
                "FDEPT,\n" +
                "FUSER,\n" +
                "FUPDATE,\n" +
                "FDOWN,\n" +
                "FOPERATOR,\n" +
                "FDEMO,\n" +
                "BL1,\n" +
                " sm4_encrypt (EMAIL,'" + sm4key + "') EMAIL, \n" +
                "CITY,\n" +
                "PROVINCE,\n" +
                "CARRIER_AIRLINE,\n" +
                "PWD,\n" +
                "SEND_STATUS,\n" +
                "SEND_TIME,\n" +
                " sm4_encrypt (PARENT_FFP_NUM,'" + sm4key + "') PARENT_FFP_NUM, \n" +
                "OPERATE_DEPT,\n" +
                "ORG,\n" +
                "DEST,\n" +
                "OP_ACCOUNT,\n" +
                "OP_NAME,\n" +
                "CREW,\n" +
                "BOOKING_CLASS,\n" +
                "TKT_NUM,\n" +
                "BOARDING_NUMBER,\n" +
                "RETRO_FLAG,\n" +
                "WS_FEED_MSG,\n" +
                "RETRO_FEED_MSG,\n" +
                "DEV_ACCOUNT,\n" +
                "DATA_ORIGIN,\n" +
                "CREW_BASE,\n" +
                "INTEGRAL_FLAG,\n" +
                "SEND_FLAG,\n" +
                "RETRO_DATE,\n" +
                "REMARKS,\n" +
                "DEL_FLAG,\n" +
                "CREATE_BY,\n" +
                "CREATE_DATE,\n" +
                "UPDATE_BY,\n" +
                "UPDATE_DATE,\n" +
                "STATUS_FLG,\n" +
                "ETL_INSERT_DT,\n" +
                "ETL_UPD_DT,\n" +
                "HF_FLAG,\n" +
                "COUNTRY_CODE,\n" +
                "ETL_HANDLE_FLAG\n" +
                " FROM FFP_REGISTER_INFO "
                + " WHERE  CREATE_DATE < TIMESTAMP '2022-01-01 00:00:00' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
