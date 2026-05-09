package com.travelsky.dataplatform.main.ods.v2.sc;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.UUID;

/**
 * @author
 * @date 2025/7/2 9:31
 */
public class ExtractTOdsScUploadMonthFinBalance {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
//        String etlDate = "2022-07-11";
        String startDate = etlDate;
        String endDate = etlDate;
        if (args.length > 2) {
            startDate = args[1];
            endDate = args[2];
        }
        System.out.println("ExtractTOdsScUploadMonthFinBalance etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsScUploadMonthFinBalance");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE T_ODS_UPLOAD_MONTH_FIN_BALANCE (\n" +
                "TICKET_AIRLINE VARCHAR(30) ,\n" +
                "TICEKET_NUM VARCHAR(60) ,\n" +
                "FLIGHT_NO VARCHAR(30) ,\n" +
                "DEP_DATE VARCHAR(8) ,\n" +
                "ORIG VARCHAR(30) ,\n" +
                "DEST VARCHAR(30) ,\n" +
                "CABIN VARCHAR(1) ,\n" +
                "SALE_CODE VARCHAR(60) ,\n" +
                "AGENT VARCHAR(600) ,\n" +
                "NET_INCOME DECIMAL(8,2) ,\n" +
                "TARIFF_CODE VARCHAR(90) ,\n" +
                "SHARE_FLAG VARCHAR(1) ,\n" +
                "IS_GROUP VARCHAR(1) ,\n" +
                "AGENT_AMT DECIMAL(8,2) ,\n" +
                "PROMO_AMT DECIMAL(8,2) ,\n" +
                "PROCEDUEE_FEE DECIMAL(8,2) ,\n" +
                "D_OR_I VARCHAR(1) ,\n" +
                "FILE_MONTH VARCHAR(8) ,\n" +
                "TABLE_PARTITION_DATE DATE ,\n" +
                "ETL_INSERT_TIME TIMESTAMP(6) ,\n" +
                "ETL_UPDATE_TIME TIMESTAMP(6) ,\n" +
                "PSG_FLAG VARCHAR(90) ,\n" +
                "PRINT_DATE VARCHAR(8) ,\n" +
                "CLK_NO VARCHAR(90) ,\n" +
                "ET_HC VARCHAR(90) \n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.CLK_DW_IP+":"+Constants.CLK_DW_PORT+"/"+Constants.CLK_DW_DB+"',\n" +
                "    'table-name' = '"+Constants.CLK_ODS_SCHEMA+".T_ODS_UPLOAD_MONTH_FIN_BALANCE', \n" +
                "    'username' = '"+Constants.CLK_DW_USER+"',\n" +
                "    'password' = '"+Constants.CLK_DW_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_SC_UPLOAD_MONTH_FIN_BALANCE (\n" +
                "    ETL_DATE DATE,\n" +
                "ID VARCHAR(500) ,\n" +
                "TICKET_AIRLINE VARCHAR(30) ,\n" +
                "TICEKET_NUM VARCHAR(60) ,\n" +
                "FLIGHT_NO VARCHAR(30) ,\n" +
                "DEP_DATE VARCHAR(8) ,\n" +
                "ORIG VARCHAR(30) ,\n" +
                "DEST VARCHAR(30) ,\n" +
                "CABIN VARCHAR(1) ,\n" +
                "SALE_CODE VARCHAR(60) ,\n" +
                "AGENT VARCHAR(600) ,\n" +
                "NET_INCOME DECIMAL(8,2) ,\n" +
                "TARIFF_CODE VARCHAR(90) ,\n" +
                "SHARE_FLAG VARCHAR(1) ,\n" +
                "IS_GROUP VARCHAR(1) ,\n" +
                "AGENT_AMT DECIMAL(8,2) ,\n" +
                "PROMO_AMT DECIMAL(8,2) ,\n" +
                "PROCEDUEE_FEE DECIMAL(8,2) ,\n" +
                "D_OR_I VARCHAR(1) ,\n" +
                "FILE_MONTH VARCHAR(8) ,\n" +
                "TABLE_PARTITION_DATE DATE ,\n" +
                "ETL_INSERT_TIME TIMESTAMP(6) ,\n" +
                "ETL_UPDATE_TIME TIMESTAMP(6) ,\n" +
                "PSG_FLAG VARCHAR(90) ,\n" +
                "PRINT_DATE VARCHAR(8) ,\n" +
                "CLK_NO VARCHAR(90) ,\n" +
                "ET_HC VARCHAR(90) \n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_SC_UPLOAD_MONTH_FIN_BALANCE',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_SC_UPLOAD_MONTH_FIN_BALANCE(" +
                "ETL_DATE,\n" +
                "ID," +
                "TICKET_AIRLINE,\n" +
                "TICEKET_NUM,\n" +
                "FLIGHT_NO,\n" +
                "DEP_DATE,\n" +
                "ORIG,\n" +
                "DEST,\n" +
                "CABIN,\n" +
                "SALE_CODE,\n" +
                "AGENT,\n" +
                "NET_INCOME,\n" +
                "TARIFF_CODE,\n" +
                "SHARE_FLAG,\n" +
                "IS_GROUP,\n" +
                "AGENT_AMT,\n" +
                "PROMO_AMT,\n" +
                "PROCEDUEE_FEE,\n" +
                "D_OR_I,\n" +
                "FILE_MONTH,\n" +
                "TABLE_PARTITION_DATE,\n" +
                "ETL_INSERT_TIME,\n" +
                "ETL_UPDATE_TIME,\n" +
                "PSG_FLAG,\n" +
                "PRINT_DATE,\n" +
                "CLK_NO,\n" +
                "ET_HC)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "TICKET_AIRLINE||TICEKET_NUM||ORIG||DEP_DATE AS ID,\n" +
                "TICKET_AIRLINE,\n" +
                "TICEKET_NUM,\n" +
                "FLIGHT_NO,\n" +
                "DEP_DATE,\n" +
                "ORIG,\n" +
                "DEST,\n" +
                "CABIN,\n" +
                "SALE_CODE,\n" +
                "AGENT,\n" +
                "NET_INCOME,\n" +
                "TARIFF_CODE,\n" +
                "SHARE_FLAG,\n" +
                "IS_GROUP,\n" +
                "AGENT_AMT,\n" +
                "PROMO_AMT,\n" +
                "PROCEDUEE_FEE,\n" +
                "D_OR_I,\n" +
                "FILE_MONTH,\n" +
                "TABLE_PARTITION_DATE,\n" +
                "ETL_INSERT_TIME,\n" +
                "ETL_UPDATE_TIME,\n" +
                "PSG_FLAG,\n" +
                "PRINT_DATE,\n" +
                "CLK_NO,\n" +
                "ET_HC\n" +
                " FROM T_ODS_UPLOAD_MONTH_FIN_BALANCE "
                + " WHERE  ETL_INSERT_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  ETL_UPDATE_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
