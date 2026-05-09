package com.travelsky.dataplatform.main.ods.v2.clk;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.main.ods.v2.khbp.ExtractTOdsKfbpBusFare;
import com.travelsky.dataplatform.udf.OracleAESDecryptor;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * @author
 * @date 2025/10/11 9:31
 */
public class ExtractTOdsClkFfpFzUserScore {
    public static Logger log = LoggerFactory.getLogger(ExtractTOdsClkFfpFzUserScore.class);
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
        log.info("ExtractTOdsKfbpBusOrder etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);//        String etlDate = "2016-04-13";
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
        tEnv.executeSql("CREATE TABLE `FFP_FZ_USER_SCORE` (\n" +
                "  `ID` VARCHAR(64) NOT NULL,\n" +
                "  `USERCODE` VARCHAR(50),\n" +
                "  `INTEGRAL` INT,\n" +
                "  `ADD_DATE` DATE,\n" +
                "  `DEPT` VARCHAR(100),\n" +
                "  `FFP_SDMEMBER_ATTRS_ID` VARCHAR(64),\n" +
                "  `FLAG` TINYINT,\n" +
                "  `REST_INTEGRAL` DECIMAL(7, 2),\n" +
                "  `REMARKS` VARCHAR(255),\n" +
                "  `CREATE_BY` VARCHAR(64),\n" +
                "  `CREATE_DATE` TIMESTAMP,\n" +
                "  `UPDATE_BY` VARCHAR(64),\n" +
                "  `UPDATE_DATE` TIMESTAMP,\n" +
                "  `DEL_FLAG` VARCHAR(1),\n" +
                "  `EXP_DATE` DATE\n" +

                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.CLK_IP+":"+Constants.CLK_PORT+"/"+Constants.CLK_DB+"',\n" +
                "    'table-name' = '"+Constants.CLK_SCHEMA+".FFP_FZ_USER_SCORE', \n" +
                "    'username' = '"+Constants.CLK_USER+"',\n" +
                "    'password' = '"+Constants.CLK_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql(CreateTableSql.FFP_FZ_USER_SCORE);
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_CLK_FFP_FZ_USER_SCORE\n" +
                "(ID,\n" +
                " USERCODE,\n" +
                " INTEGRAL,\n" +
                " ADD_DATE,\n" +
                " DEPT,\n" +
                " FFP_SDMEMBER_ATTRS_ID,\n" +
                " FLAG,\n" +
                " REST_INTEGRAL,\n" +
                " REMARKS,\n" +
                " CREATE_BY,\n" +
                " CREATE_DATE,\n" +
                " UPDATE_BY,\n" +
                " UPDATE_DATE,\n" +
                " DEL_FLAG,\n" +
                " EXP_DATE,\n" +
                " ETL_CREATE_TIME,\n" +
                " ETL_UPDATE_TIME,\n" +
                " ETL_DATE)\n " +
                "SELECT\n" +
                " ID,\n" +
                " USERCODE,\n" +
                " INTEGRAL,\n" +
                " ADD_DATE,\n" +
                " DEPT,\n" +
                " sm4_encrypt(FFP_SDMEMBER_ATTRS_ID, '" + Constants.SM4_KEY + "') FFP_SDMEMBER_ATTRS_ID,\n" +
                " FLAG,\n" +
                " REST_INTEGRAL,\n" +
                " REMARKS,\n" +
                " CREATE_BY,\n" +
                " CREATE_DATE,\n" +
                " UPDATE_BY,\n" +
                " UPDATE_DATE,\n" +
                " DEL_FLAG,\n" +
                " EXP_DATE,\n" +
                " CURRENT_TIMESTAMP AS ETL_CREATE_TIME,\n" +
                " CURRENT_TIMESTAMP AS ETL_UPDATE_TIME,\n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                " FROM FFP_FZ_USER_SCORE "
                + " WHERE  CREATE_DATE BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  UPDATE_DATE BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql);

        result.print();


    }
}
