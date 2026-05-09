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
public class ExtractTOdsScUploadB2cOrderInfo {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
//        String etlDate = "2017-04-12";
        String startDate = etlDate;
        String endDate = etlDate;
        if (args.length > 2) {
            startDate = args[1];
            endDate = args[2];
        }
        System.out.println("ExtractTOdsScUploadB2cOrderInfo etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsScUploadB2cOrderInfo");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
//创建上游数据源表
        tEnv.executeSql("CREATE TABLE T_ODS_UPLOAD_B2C_ORDERINFO (\n" +
                "ID BIGINT ,\n" +
                "ORDER_NO VARCHAR(54) ,\n" +
                "BANK_ORDER_NO VARCHAR(210) ,\n" +
                "VOUCHER_NO VARCHAR(30) ,\n" +
                "PNR VARCHAR(18) ,\n" +
                "TICKET_NO VARCHAR(42) ,\n" +
                "FLIGHT_NO VARCHAR(30) ,\n" +
                "UP_LOCATION VARCHAR(60) ,\n" +
                "DEST_LOCATION VARCHAR(60) ,\n" +
                "CABIN VARCHAR(15) ,\n" +
                "TAKEOFF_TIME TIMESTAMP(6) ,\n" +
                "PASS_NAME VARCHAR(108) ,\n" +
                "ID_NO VARCHAR(256) ,\n" +
                "ORIGINAL_FARE DECIMAL(10,2) ,\n" +
                "DISCOUNT_FARE DECIMAL(10,2) ,\n" +
                "CTAX DECIMAL(10,2) ,\n" +
                "FTAX DECIMAL(10,2) ,\n" +
                "FARE_OTHER DECIMAL(10,2) ,\n" +
                "INSURANCE DECIMAL(10,2) ,\n" +
                "FARE_TOTAL DECIMAL(10,2) ,\n" +
                "PRODUCT_NAME VARCHAR(60) ,\n" +
                "ORDER_DATE TIMESTAMP(6) ,\n" +
                "ORDER_STATUS VARCHAR(60) ,\n" +
                "ORDER_SOURCE DECIMAL(1,0) ,\n" +
                "ORDER_USER VARCHAR(150) ,\n" +
                "REGISTER_USER VARCHAR(150) ,\n" +
                "ORDER_MOBILE VARCHAR(60) ,\n" +
                "ORDER_EMAIL VARCHAR(256) ,\n" +
                "BANK_NAME VARCHAR(90) ,\n" +
                "R_CARRIER VARCHAR(12) ,\n" +
                "GUEST_SCODE VARCHAR(300) ,\n" +
                "CONTACT_NAME VARCHAR(90) ,\n" +
                "MEM_NUM VARCHAR(256) ,\n" +
                "FILE_DATE VARCHAR(900) ,\n" +
                "ETL_INSERT_TIME TIMESTAMP(6) ,\n" +
                "ETL_UPDATE_TIME TIMESTAMP(6) ,\n" +
                "FLIGHT_SEGMENT VARCHAR(300) ,\n" +
                "FLIGHT_SEGMENT_CODE VARCHAR(120) ,\n" +
                "TK_TIME TIMESTAMP(6) ,\n" +
                "ORDER_CHANNEL VARCHAR(120) ,\n" +
                "PAY_TIME TIMESTAMP(6) ,\n" +
                "TK_PROCEDURE VARCHAR(90) ,\n" +
                "PASSENGER_TYPE VARCHAR(30) ,\n" +
                "ID_TYPE VARCHAR(90) ,\n" +
                "TRIP_TYPE VARCHAR(30) ,\n" +
                "D_OR_I VARCHAR(30) ,\n" +
                "REDUCTION VARCHAR(30) ,\n" +
                "COUPON_NO VARCHAR(90) ,\n" +
                "CONN_MOBILE VARCHAR(256) ,\n" +
                "SITE VARCHAR(30) ,\n" +
                "`LANGUAGE` VARCHAR(30) ,\n" +
                "CURRENCY_TYPE VARCHAR(15) ,\n" +
                "TEL_PHONE VARCHAR(60) ,\n" +
                "COUPON_NAME VARCHAR(150) ,\n" +
                "COUPON_FARE VARCHAR(30) ,\n" +
                "COUPON_NUM VARCHAR(120) ,\n" +
                "FARE_ZHIJIAN VARCHAR(30) ,\n" +
                "FARE_JTCXZJ VARCHAR(30) ,\n" +
                "IS_JTCXZJ VARCHAR(60) ,\n" +
                "FAREBASIS VARCHAR(120) ,\n" +
                "IS_ZSBX VARCHAR(12) ,\n" +
                "NAME_ZSBX VARCHAR(300) ,\n" +
                "IS_ZSKQ VARCHAR(12) \n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.CLK_DW_IP+":"+Constants.CLK_DW_PORT+"/"+Constants.CLK_DW_DB+"',\n" +
                "    'table-name' = '"+Constants.CLK_ODS_SCHEMA+".T_ODS_UPLOAD_B2C_ORDERINFO', \n" +
                "    'username' = '"+Constants.CLK_DW_USER+"',\n" +
                "    'password' = '"+Constants.CLK_DW_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_SC_UPLOAD_B2C_ORDERINFO (\n" +
                "ROW_ID VARCHAR(100) ,\n" +
                "ID BIGINT ,\n" +
                "ORDER_NO VARCHAR(54) ,\n" +
                "BANK_ORDER_NO VARCHAR(210) ,\n" +
                "VOUCHER_NO VARCHAR(30) ,\n" +
                "PNR VARCHAR(18) ,\n" +
                "TICKET_NO VARCHAR(42) ,\n" +
                "FLIGHT_NO VARCHAR(30) ,\n" +
                "UP_LOCATION VARCHAR(60) ,\n" +
                "DEST_LOCATION VARCHAR(60) ,\n" +
                "CABIN VARCHAR(15) ,\n" +
                "TAKEOFF_TIME TIMESTAMP(6) ,\n" +
                "PASS_NAME VARCHAR(108) ,\n" +
                "ID_NO VARCHAR(256) ,\n" +
                "ORIGINAL_FARE DECIMAL(10,2) ,\n" +
                "DISCOUNT_FARE DECIMAL(10,2) ,\n" +
                "CTAX DECIMAL(10,2) ,\n" +
                "FTAX DECIMAL(10,2) ,\n" +
                "FARE_OTHER DECIMAL(10,2) ,\n" +
                "INSURANCE DECIMAL(10,2) ,\n" +
                "FARE_TOTAL DECIMAL(10,2) ,\n" +
                "PRODUCT_NAME VARCHAR(60) ,\n" +
                "ORDER_DATE TIMESTAMP(6) ,\n" +
                "ORDER_STATUS VARCHAR(60) ,\n" +
                "ORDER_SOURCE DECIMAL(1,0) ,\n" +
                "ORDER_USER VARCHAR(150) ,\n" +
                "REGISTER_USER VARCHAR(150) ,\n" +
                "ORDER_MOBILE VARCHAR(60) ,\n" +
                "ORDER_EMAIL VARCHAR(256) ,\n" +
                "BANK_NAME VARCHAR(90) ,\n" +
                "R_CARRIER VARCHAR(12) ,\n" +
                "GUEST_SCODE VARCHAR(300) ,\n" +
                "CONTACT_NAME VARCHAR(90) ,\n" +
                "MEM_NUM VARCHAR(256) ,\n" +
                "FILE_DATE VARCHAR(900) ,\n" +
                "ETL_INSERT_TIME TIMESTAMP(6) ,\n" +
                "ETL_UPDATE_TIME TIMESTAMP(6) ,\n" +
                "FLIGHT_SEGMENT VARCHAR(300) ,\n" +
                "FLIGHT_SEGMENT_CODE VARCHAR(120) ,\n" +
                "TK_TIME TIMESTAMP(6) ,\n" +
                "ORDER_CHANNEL VARCHAR(120) ,\n" +
                "PAY_TIME TIMESTAMP(6) ,\n" +
                "TK_PROCEDURE VARCHAR(90) ,\n" +
                "PASSENGER_TYPE VARCHAR(30) ,\n" +
                "ID_TYPE VARCHAR(90) ,\n" +
                "TRIP_TYPE VARCHAR(30) ,\n" +
                "D_OR_I VARCHAR(30) ,\n" +
                "REDUCTION VARCHAR(30) ,\n" +
                "COUPON_NO VARCHAR(90) ,\n" +
                "CONN_MOBILE VARCHAR(256) ,\n" +
                "SITE VARCHAR(30) ,\n" +
                "`LANGUAGE` VARCHAR(30) ,\n" +
                "CURRENCY_TYPE VARCHAR(15) ,\n" +
                "TEL_PHONE VARCHAR(60) ,\n" +
                "COUPON_NAME VARCHAR(150) ,\n" +
                "COUPON_FARE VARCHAR(30) ,\n" +
                "COUPON_NUM VARCHAR(120) ,\n" +
                "FARE_ZHIJIAN VARCHAR(30) ,\n" +
                "FARE_JTCXZJ VARCHAR(30) ,\n" +
                "IS_JTCXZJ VARCHAR(60) ,\n" +
                "FAREBASIS VARCHAR(120) ,\n" +
                "IS_ZSBX VARCHAR(12) ,\n" +
                "NAME_ZSBX VARCHAR(300) ,\n" +
                "IS_ZSKQ VARCHAR(12) ,\n" +
                "ETL_DATE DATE  \n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_SC_UPLOAD_B2C_ORDERINFO',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_SC_UPLOAD_B2C_ORDERINFO(" +
                "ETL_DATE,\n" +
                "ROW_ID,\n" +
                "ID,\n" +
                "ORDER_NO,\n" +
                "BANK_ORDER_NO,\n" +
                "VOUCHER_NO,\n" +
                "PNR,\n" +
                "TICKET_NO,\n" +
                "FLIGHT_NO,\n" +
                "UP_LOCATION,\n" +
                "DEST_LOCATION,\n" +
                "CABIN,\n" +
                "TAKEOFF_TIME,\n" +
                "PASS_NAME,\n" +
                "ID_NO,\n" +
                "ORIGINAL_FARE,\n" +
                "DISCOUNT_FARE,\n" +
                "CTAX,\n" +
                "FTAX,\n" +
                "FARE_OTHER,\n" +
                "INSURANCE,\n" +
                "FARE_TOTAL,\n" +
                "PRODUCT_NAME,\n" +
                "ORDER_DATE,\n" +
                "ORDER_STATUS,\n" +
                "ORDER_SOURCE,\n" +
                "ORDER_USER,\n" +
                "REGISTER_USER,\n" +
                "ORDER_MOBILE,\n" +
                "ORDER_EMAIL,\n" +
                "BANK_NAME,\n" +
                "R_CARRIER,\n" +
                "GUEST_SCODE,\n" +
                "CONTACT_NAME,\n" +
                "MEM_NUM,\n" +
                "FILE_DATE,\n" +
                "ETL_INSERT_TIME,\n" +
                "ETL_UPDATE_TIME,\n" +
                "FLIGHT_SEGMENT,\n" +
                "FLIGHT_SEGMENT_CODE,\n" +
                "TK_TIME,\n" +
                "ORDER_CHANNEL,\n" +
                "PAY_TIME,\n" +
                "TK_PROCEDURE,\n" +
                "PASSENGER_TYPE,\n" +
                "ID_TYPE,\n" +
                "TRIP_TYPE,\n" +
                "D_OR_I,\n" +
                "REDUCTION,\n" +
                "COUPON_NO,\n" +
                "CONN_MOBILE,\n" +
                "SITE,\n" +
                "`LANGUAGE`,\n" +
                "CURRENCY_TYPE,\n" +
                "TEL_PHONE,\n" +
                "COUPON_NAME,\n" +
                "COUPON_FARE,\n" +
                "COUPON_NUM,\n" +
                "FARE_ZHIJIAN,\n" +
                "FARE_JTCXZJ,\n" +
                "IS_JTCXZJ,\n" +
                "FAREBASIS,\n" +
                "IS_ZSBX,\n" +
                "NAME_ZSBX,\n" +
                "IS_ZSKQ)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "FILE_DATE||CAST(ID AS STRING) AS ROW_ID," +
                "ID,\n" +
                "ORDER_NO,\n" +
                "BANK_ORDER_NO,\n" +
                "VOUCHER_NO,\n" +
                "PNR,\n" +
                "TICKET_NO,\n" +
                "FLIGHT_NO,\n" +
                "UP_LOCATION,\n" +
                "DEST_LOCATION,\n" +
                "CABIN,\n" +
                "TAKEOFF_TIME,\n" +
                "PASS_NAME,\n" +
                "  sm4_encrypt(ID_NO, '" + sm4key + "') ID_NO,\n" +
                "ORIGINAL_FARE,\n" +
                "DISCOUNT_FARE,\n" +
                "CTAX,\n" +
                "FTAX,\n" +
                "FARE_OTHER,\n" +
                "INSURANCE,\n" +
                "FARE_TOTAL,\n" +
                "PRODUCT_NAME,\n" +
                "ORDER_DATE,\n" +
                "ORDER_STATUS,\n" +
                "ORDER_SOURCE,\n" +
                "  sm4_encrypt(ORDER_USER, '\" + sm4key + \"') ORDER_USER,\n" +
                "REGISTER_USER,\n" +
                "  sm4_encrypt(ORDER_MOBILE, '" + sm4key + "') ORDER_MOBILE,\n" +
                "  sm4_encrypt(ORDER_EMAIL, '" + sm4key + "') ORDER_EMAIL,\n" +
                "BANK_NAME,\n" +
                "R_CARRIER,\n" +
                "GUEST_SCODE,\n" +
                "CONTACT_NAME,\n" +
                "  sm4_encrypt(MEM_NUM, '" + sm4key + "') MEM_NUM,\n" +
                "FILE_DATE,\n" +
                "ETL_INSERT_TIME,\n" +
                "ETL_UPDATE_TIME,\n" +
                "FLIGHT_SEGMENT,\n" +
                "FLIGHT_SEGMENT_CODE,\n" +
                "TK_TIME,\n" +
                "ORDER_CHANNEL,\n" +
                "PAY_TIME,\n" +
                "TK_PROCEDURE,\n" +
                "PASSENGER_TYPE,\n" +
                "ID_TYPE,\n" +
                "TRIP_TYPE,\n" +
                "D_OR_I,\n" +
                "REDUCTION,\n" +
                "COUPON_NO,\n" +
                "  sm4_encrypt(CONN_MOBILE, '" + sm4key + "') CONN_MOBILE,\n" +
                "SITE,\n" +
                "`LANGUAGE`,\n" +
                "CURRENCY_TYPE,\n" +
                "  sm4_encrypt(TEL_PHONE, '\" + sm4key + \"') TEL_PHONE,\n" +
                "COUPON_NAME,\n" +
                "COUPON_FARE,\n" +
                "COUPON_NUM,\n" +
                "FARE_ZHIJIAN,\n" +
                "FARE_JTCXZJ,\n" +
                "IS_JTCXZJ,\n" +
                "FAREBASIS,\n" +
                "IS_ZSBX,\n" +
                "NAME_ZSBX,\n" +
                "IS_ZSKQ\n" +
                " FROM T_ODS_UPLOAD_B2C_ORDERINFO "
                + " WHERE  ETL_INSERT_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  ETL_UPDATE_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
