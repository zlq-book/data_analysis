package com.travelsky.dataplatform.main.ods;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.TOdsLyxOrderPassengerInfo;
import com.travelsky.dataplatform.source.OracleDBSourceFunction;
import com.travelsky.dataplatform.udf.OracleAESDecryptor;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author
 * @date 2025/7/2 9:31
 */
public class ExtractTOdsLyxOrderPassengerInfo {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
//        String etlDate = "2019-12-30";
        String sm4key =  Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLyxOrderPassengerInfo");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();

        String url = "jdbc:oracle:thin:@//" + Constants.LYX_IP + ":" + Constants.LYX_PORT + "/" + Constants.LYX_DB;
        String user = Constants.LYX_USER;
        String password = Constants.LYX_PWD;
        String query =   "SELECT " +
                "ID,\n" +
                "PSG_NAME_CN,\n" +
                "PSG_NAME_EN,\n" +
                "CERT_TYPE,\n" +
                Constants.LYX_SCHEMA + ".decrypt_func(CERT_NUM) CERT_NUM,\n" +
                "PASSENGER_TYPE,\n" +
                "CONTACT_NAME,\n" +
                Constants.LYX_SCHEMA + ".decrypt_func(CONTACT_NO) CONTACT_NO,\n" +
                "FLIGHT_ID,\n" +
                "TICKET_NUM,\n" +
                "PNR_NO,\n" +
                "TICKET_PRICE,\n" +
                "TICKET_STATUS,\n" +
                "TICKET_TIME,\n" +
                "AIRPORT_TAX,\n" +
                "FUEL_TAX,\n" +
                "INSURE_TAX,\n" +
                "INSURE_STATUS,\n" +
                "LY_VALUE,\n" +
                "LY_STATUS,\n" +
                "ACCOMPANY_NAME,\n" +
                "ACCOMPANY_CERT_NO,\n" +
                "ACCOMPANY_TICKET_NUM,\n" +
                "IS_ACCOMPANY,\n" +
                "OLD_PSG_ID,\n" +
                "AGE,\n" +
                "CHANGE_FEE,\n" +
                "COMPENSATION_FEE,\n" +
                "LAST_CHANGE_FEE,\n" +
                "REFUND_NUM,\n" +
                "REFUND_NUM_STATUS,\n" +
                "REFUND_AMOUNT_STATUS,\n" +
                "INSURE_NO,\n" +
                "INSURE_SERIAL_NUMBER,\n" +
                "OPT_STATUS,\n" +
                "CASH,\n" +
                "TICKET_FAIL_REASON,\n" +
                "INSURANCE_FAIL_REASON\n" +
                " FROM " + Constants.LYX_SCHEMA + ".ORDER_PASSENGER_INFO opi "
                ;
        String columStr = "ID,PSG_NAME_CN,PSG_NAME_EN,CERT_TYPE,CERT_NUM,PASSENGER_TYPE,CONTACT_NAME,CONTACT_NO,FLIGHT_ID,TICKET_NUM,PNR_NO,TICKET_PRICE,TICKET_STATUS,TICKET_TIME,AIRPORT_TAX,FUEL_TAX,INSURE_TAX,INSURE_STATUS,LY_VALUE,LY_STATUS,ACCOMPANY_NAME,ACCOMPANY_CERT_NO,ACCOMPANY_TICKET_NUM,IS_ACCOMPANY,OLD_PSG_ID,AGE,CHANGE_FEE,COMPENSATION_FEE,LAST_CHANGE_FEE,REFUND_NUM,REFUND_NUM_STATUS,REFUND_AMOUNT_STATUS,INSURE_NO,INSURE_SERIAL_NUMBER,OPT_STATUS,CASH,TICKET_FAIL_REASON,INSURANCE_FAIL_REASON";
        String[] columnStrs = columStr.split(",");
        List<String> columns = Arrays.stream(columnStrs).collect(Collectors.toList());
        OracleDBSourceFunction function = new OracleDBSourceFunction(url, user, password, query, columns);
        DataStreamSource<JSONObject> jsonObjectDataStreamSource = env.addSource(function);

        SingleOutputStreamOperator<TOdsLyxOrderPassengerInfo> tSource = jsonObjectDataStreamSource.map(jsonObject -> {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            TOdsLyxOrderPassengerInfo tOdsLyxOrderBase = mapper.readValue(JSONObject.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue), TOdsLyxOrderPassengerInfo.class);
            return tOdsLyxOrderBase;
        });
        tSource.print();
        Schema schema = Schema.newBuilder()
                .column("ID", DataTypes.BIGINT())
                .column("PSG_NAME_CN", DataTypes.VARCHAR(100))
                .column("PSG_NAME_EN", DataTypes.VARCHAR(100))
                .column("CERT_TYPE", DataTypes.VARCHAR(6))
                .column("CERT_NUM", DataTypes.VARCHAR(200))
                .column("PASSENGER_TYPE", DataTypes.VARCHAR(6))
                .column("CONTACT_NAME", DataTypes.VARCHAR(50))
                .column("CONTACT_NO", DataTypes.VARCHAR(200))
                .column("FLIGHT_ID", DataTypes.BIGINT())
                .column("TICKET_NUM", DataTypes.VARCHAR(20))
                .column("PNR_NO", DataTypes.VARCHAR(20))
                .column("TICKET_PRICE", DataTypes.VARCHAR(20))
                .column("TICKET_STATUS", DataTypes.VARCHAR(20))
                .column("TICKET_TIME", DataTypes.BIGINT())
                .column("AIRPORT_TAX", DataTypes.VARCHAR(20))
                .column("FUEL_TAX", DataTypes.VARCHAR(20))
                .column("INSURE_TAX", DataTypes.VARCHAR(20))
                .column("INSURE_STATUS", DataTypes.VARCHAR(2))
                .column("LY_VALUE", DataTypes.VARCHAR(20))
                .column("LY_STATUS", DataTypes.VARCHAR(2))
                .column("ACCOMPANY_NAME", DataTypes.VARCHAR(100))
                .column("ACCOMPANY_CERT_NO", DataTypes.VARCHAR(200))
                .column("ACCOMPANY_TICKET_NUM", DataTypes.VARCHAR(20))
                .column("IS_ACCOMPANY", DataTypes.VARCHAR(2))
                .column("OLD_PSG_ID", DataTypes.BIGINT())
                .column("AGE", DataTypes.INT())
                .column("CHANGE_FEE", DataTypes.VARCHAR(20))
                .column("COMPENSATION_FEE", DataTypes.VARCHAR(20))
                .column("LAST_CHANGE_FEE", DataTypes.VARCHAR(20))
                .column("REFUND_NUM", DataTypes.VARCHAR(30))
                .column("REFUND_NUM_STATUS", DataTypes.VARCHAR(2))
                .column("REFUND_AMOUNT_STATUS", DataTypes.VARCHAR(2))
                .column("INSURE_NO", DataTypes.VARCHAR(100))
                .column("INSURE_SERIAL_NUMBER", DataTypes.VARCHAR(100))
                .column("OPT_STATUS", DataTypes.VARCHAR(2))
                .column("CASH", DataTypes.VARCHAR(20))
                .column("TICKET_FAIL_REASON", DataTypes.VARCHAR(1000))
                .column("INSURANCE_FAIL_REASON", DataTypes.VARCHAR(1000))
                .build();
        // 注册为临时视图
        tEnv.createTemporaryView("ORDER_PASSENGER_INFO", tSource, schema);

        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_LYX_ORDER_PASSENGER_INFO (\n" +
                "    ETL_DATE DATE,\n" +
                "ID BIGINT ,\n" +
                "PSG_NAME_CN VARCHAR(300) ,\n" +
                "PSG_NAME_EN VARCHAR(300) ,\n" +
                "CERT_TYPE VARCHAR(18) ,\n" +
                "CERT_NUM VARCHAR(600) ,\n" +
                "PASSENGER_TYPE VARCHAR(18) ,\n" +
                "CONTACT_NAME VARCHAR(150) ,\n" +
                "CONTACT_NO VARCHAR(600) ,\n" +
                "FLIGHT_ID BIGINT ,\n" +
                "TICKET_NUM VARCHAR(60) ,\n" +
                "PNR_NO VARCHAR(60) ,\n" +
                "TICKET_PRICE VARCHAR(60) ,\n" +
                "TICKET_STATUS VARCHAR(60) ,\n" +
                "TICKET_TIME TIMESTAMP(6) ,\n" +
                "AIRPORT_TAX VARCHAR(60) ,\n" +
                "FUEL_TAX VARCHAR(60) ,\n" +
                "INSURE_TAX VARCHAR(60) ,\n" +
                "INSURE_STATUS VARCHAR(6) ,\n" +
                "LY_VALUE VARCHAR(60) ,\n" +
                "LY_STATUS VARCHAR(6) ,\n" +
                "ACCOMPANY_NAME VARCHAR(300) ,\n" +
                "ACCOMPANY_CERT_NO VARCHAR(600) ,\n" +
                "ACCOMPANY_TICKET_NUM VARCHAR(60) ,\n" +
                "IS_ACCOMPANY VARCHAR(6) ,\n" +
                "OLD_PSG_ID BIGINT ,\n" +
                "AGE INT ,\n" +
                "CHANGE_FEE VARCHAR(60) ,\n" +
                "COMPENSATION_FEE VARCHAR(60) ,\n" +
                "LAST_CHANGE_FEE VARCHAR(60) ,\n" +
                "REFUND_NUM VARCHAR(90) ,\n" +
                "REFUND_NUM_STATUS VARCHAR(6) ,\n" +
                "REFUND_AMOUNT_STATUS VARCHAR(6) ,\n" +
                "INSURE_NO VARCHAR(300) ,\n" +
                "INSURE_SERIAL_NUMBER VARCHAR(300) ,\n" +
                "OPT_STATUS VARCHAR(6) ,\n" +
                "CASH VARCHAR(60) ,\n" +
                "TICKET_FAIL_REASON VARCHAR(3000) ,\n" +
                "INSURANCE_FAIL_REASON VARCHAR(3000) \n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYX_ORDER_PASSENGER_INFO',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql,配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_LYX_ORDER_PASSENGER_INFO(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "PSG_NAME_CN,\n" +
                "PSG_NAME_EN,\n" +
                "CERT_TYPE,\n" +
                "CERT_NUM,\n" +
                "PASSENGER_TYPE,\n" +
                "CONTACT_NAME,\n" +
                "CONTACT_NO,\n" +
                "FLIGHT_ID,\n" +
                "TICKET_NUM,\n" +
                "PNR_NO,\n" +
                "TICKET_PRICE,\n" +
                "TICKET_STATUS,\n" +
                "TICKET_TIME,\n" +
                "AIRPORT_TAX,\n" +
                "FUEL_TAX,\n" +
                "INSURE_TAX,\n" +
                "INSURE_STATUS,\n" +
                "LY_VALUE,\n" +
                "LY_STATUS,\n" +
                "ACCOMPANY_NAME,\n" +
                "ACCOMPANY_CERT_NO,\n" +
                "ACCOMPANY_TICKET_NUM,\n" +
                "IS_ACCOMPANY,\n" +
                "OLD_PSG_ID,\n" +
                "AGE,\n" +
                "CHANGE_FEE,\n" +
                "COMPENSATION_FEE,\n" +
                "LAST_CHANGE_FEE,\n" +
                "REFUND_NUM,\n" +
                "REFUND_NUM_STATUS,\n" +
                "REFUND_AMOUNT_STATUS,\n" +
                "INSURE_NO,\n" +
                "INSURE_SERIAL_NUMBER,\n" +
                "OPT_STATUS,\n" +
                "CASH,\n" +
                "TICKET_FAIL_REASON,\n" +
                "INSURANCE_FAIL_REASON)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "PSG_NAME_CN,\n" +
                "PSG_NAME_EN,\n" +
                "CERT_TYPE,\n" +
                "  sm4_encrypt(CERT_NUM, '" + sm4key + "') CERT_NUM,\n" +
                "PASSENGER_TYPE,\n" +
                "CONTACT_NAME,\n" +
                "  sm4_encrypt(CONTACT_NO, '" + sm4key + "') CONTACT_NO,\n" +
                "FLIGHT_ID,\n" +
                "TICKET_NUM,\n" +
                "PNR_NO,\n" +
                "TICKET_PRICE,\n" +
                "TICKET_STATUS,\n" +
                "TO_TIMESTAMP_LTZ(TICKET_TIME, 3) TICKET_TIME,\n" +
                "AIRPORT_TAX,\n" +
                "FUEL_TAX,\n" +
                "INSURE_TAX,\n" +
                "INSURE_STATUS,\n" +
                "LY_VALUE,\n" +
                "LY_STATUS,\n" +
                "ACCOMPANY_NAME,\n" +
                "ACCOMPANY_CERT_NO,\n" +
                "ACCOMPANY_TICKET_NUM,\n" +
                "IS_ACCOMPANY,\n" +
                "OLD_PSG_ID,\n" +
                "AGE,\n" +
                "CHANGE_FEE,\n" +
                "COMPENSATION_FEE,\n" +
                "LAST_CHANGE_FEE,\n" +
                "REFUND_NUM,\n" +
                "REFUND_NUM_STATUS,\n" +
                "REFUND_AMOUNT_STATUS,\n" +
                "INSURE_NO,\n" +
                "INSURE_SERIAL_NUMBER,\n" +
                "OPT_STATUS,\n" +
                "CASH,\n" +
                "TICKET_FAIL_REASON,\n" +
                "INSURANCE_FAIL_REASON\n" +
                " FROM ORDER_PASSENGER_INFO ";

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
