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
@Deprecated
public class ExtractTOdsClkTDwDTicketPrice {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
//        String etlDate = "2016-03-27";
        String startDate = etlDate;
        String endDate = etlDate;
        if (args.length > 2) {
            startDate = args[1];
            endDate = args[2];
        }
        System.out.println("ExtractTOdsClkTDwDTicketPrice etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsClkTDwDTicketPrice");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();

//创建上游数据源表
        tEnv.executeSql("CREATE TABLE T_DW_D_TICKET_PRICE (\n" +
                "ROW_ID BIGINT, \n" +
                "AIR_CODE VARCHAR(30), \n" +
                "FLIGHT_NO VARCHAR(30) , \n" +
                "EX_DATE TIMESTAMP(6), \n" +
                "UP_LOCATION VARCHAR(30), \n" +
                "DIS_LOCATION VARCHAR(30), \n" +
                "PRICE_ONEWAY DECIMAL(8,2), \n" +
                "PRICE_TOWWAY DECIMAL(8,2), \n" +
                "START_DATE TIMESTAMP(6), \n" +
                "END_DATE TIMESTAMP(6), \n" +
                "EXP_FLAG VARCHAR(1), \n" +
                "SEAT_TYPE VARCHAR(1), \n" +
                "DIST BIGINT, \n" +
                "SEG_TYPE VARCHAR(1), \n" +
                "DATA_ACTIVE_FLAG VARCHAR(1), \n" +
                "SOURCE_ID BIGINT, \n" +
                "ETL_INSERT_TIME TIMESTAMP(6), \n" +
                "ETL_UPDATE_TIME TIMESTAMP(6)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.CLK_DW_IP+":"+Constants.CLK_DW_PORT+"/"+Constants.CLK_DW_DB+"',\n" +
                "    'table-name' = '"+Constants.CLK_DW_SCHEMA+".T_DW_D_TICKET_PRICE', \n" +
                "    'username' = '"+Constants.CLK_DW_USER+"',\n" +
                "    'password' = '"+Constants.CLK_DW_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_CLK_T_DW_D_TICKET_PRICE (\n" +
                "    ETL_DATE DATE,\n" +
                "ROW_ID BIGINT, \n" +
                "AIR_CODE VARCHAR(30), \n" +
                "FLIGHT_NO VARCHAR(30) , \n" +
                "EX_DATE TIMESTAMP(6), \n" +
                "UP_LOCATION VARCHAR(30), \n" +
                "DIS_LOCATION VARCHAR(30), \n" +
                "PRICE_ONEWAY DECIMAL(8,2), \n" +
                "PRICE_TOWWAY DECIMAL(8,2), \n" +
                "START_DATE TIMESTAMP(6), \n" +
                "END_DATE TIMESTAMP(6), \n" +
                "EXP_FLAG VARCHAR(1), \n" +
                "SEAT_TYPE VARCHAR(1), \n" +
                "DIST BIGINT, \n" +
                "SEG_TYPE VARCHAR(1), \n" +
                "DATA_ACTIVE_FLAG VARCHAR(1), \n" +
                "SOURCE_ID BIGINT, \n" +
                "ETL_INSERT_TIME TIMESTAMP(6), \n" +
                "ETL_UPDATE_TIME TIMESTAMP(6)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_CLK_T_DW_D_TICKET_PRICE',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_CLK_T_DW_D_TICKET_PRICE(" +
                "ETL_DATE,\n" +
                "ROW_ID,\n" +
                "AIR_CODE,\n" +
                "FLIGHT_NO,\n" +
                "EX_DATE,\n" +
                "UP_LOCATION,\n" +
                "DIS_LOCATION,\n" +
                "PRICE_ONEWAY,\n" +
                "PRICE_TOWWAY,\n" +
                "START_DATE,\n" +
                "END_DATE,\n" +
                "EXP_FLAG,\n" +
                "SEAT_TYPE,\n" +
                "DIST,\n" +
                "SEG_TYPE,\n" +
                "DATA_ACTIVE_FLAG,\n" +
                "SOURCE_ID,\n" +
                "ETL_INSERT_TIME,\n" +
                "ETL_UPDATE_TIME)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ROW_ID,\n" +
                "AIR_CODE,\n" +
                "FLIGHT_NO,\n" +
                "EX_DATE,\n" +
                "UP_LOCATION,\n" +
                "DIS_LOCATION,\n" +
                "PRICE_ONEWAY,\n" +
                "PRICE_TOWWAY,\n" +
                "START_DATE,\n" +
                "END_DATE,\n" +
                "EXP_FLAG,\n" +
                "SEAT_TYPE,\n" +
                "DIST,\n" +
                "SEG_TYPE,\n" +
                "DATA_ACTIVE_FLAG,\n" +
                "SOURCE_ID,\n" +
                "ETL_INSERT_TIME,\n" +
                "ETL_UPDATE_TIME\n" +
                " FROM T_DW_D_TICKET_PRICE "
                + " WHERE  ETL_INSERT_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  ETL_UPDATE_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
