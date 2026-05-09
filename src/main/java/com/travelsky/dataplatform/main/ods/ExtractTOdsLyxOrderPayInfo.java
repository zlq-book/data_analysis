package com.travelsky.dataplatform.main.ods;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.TOdsLyxOrderPayInfo;
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
public class ExtractTOdsLyxOrderPayInfo {
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
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLyxOrderPayInfo");
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
                "LY_VALUE_STATUS,\n" +
                "LY_SUM,\n" +
                "LY_PAY_TIME,\n" +
                "CASH_PAY_STATUS,\n" +
                "PAY_WAY,\n" +
                "CASH_SUM,\n" +
                "CASH_PAY_NO,\n" +
                Constants.LYX_SCHEMA + ".decrypt_func(CASH_PAY_MOBILE) CASH_PAY_MOBILE,\n" +
                "CASH_PAY_TIME,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_TIME,\n" +
                "PAY_SOURCE,\n" +
                "CASH_FAIL_REASON,\n" +
                "LY_FAIL_REASON,\n" +
                "ORDER_TYPE,\n" +
                "CASHPAY_STARTTIME,\n" +
                "ORIGIN_TYPE\n" +
                " FROM " + Constants.LYX_SCHEMA + ".ORDER_PAY_INFO opi"
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        String columStr = "ID,LY_VALUE_STATUS,LY_SUM,LY_PAY_TIME,CASH_PAY_STATUS,PAY_WAY,CASH_SUM,CASH_PAY_NO,CASH_PAY_MOBILE,CASH_PAY_TIME,CREATE_TIME,UPDATE_TIME,PAY_SOURCE,CASH_FAIL_REASON,LY_FAIL_REASON,ORDER_TYPE,CASHPAY_STARTTIME,ORIGIN_TYPE";
        String[] columnStrs = columStr.split(",");
        List<String> columns = Arrays.stream(columnStrs).collect(Collectors.toList());
        OracleDBSourceFunction function = new OracleDBSourceFunction(url, user, password, query, columns);
        DataStreamSource<JSONObject> jsonObjectDataStreamSource = env.addSource(function);

        SingleOutputStreamOperator<TOdsLyxOrderPayInfo> tSource = jsonObjectDataStreamSource.map(jsonObject -> {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            TOdsLyxOrderPayInfo tOdsLyxOrderBase = mapper.readValue(JSONObject.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue), TOdsLyxOrderPayInfo.class);
            return tOdsLyxOrderBase;
        });
        tSource.print();
        Schema schema = Schema.newBuilder()
                .column("ID", DataTypes.BIGINT())
                .column("LY_VALUE_STATUS", DataTypes.VARCHAR(2))
                .column("LY_SUM", DataTypes.VARCHAR(20))
                .column("LY_PAY_TIME", DataTypes.BIGINT())
                .column("CASH_PAY_STATUS", DataTypes.VARCHAR(2))
                .column("PAY_WAY", DataTypes.VARCHAR(2))
                .column("CASH_SUM", DataTypes.VARCHAR(20))
                .column("CASH_PAY_NO", DataTypes.VARCHAR(50))
                .column("CASH_PAY_MOBILE", DataTypes.VARCHAR(200))
                .column("CASH_PAY_TIME", DataTypes.BIGINT())
                .column("CREATE_TIME", DataTypes.BIGINT())
                .column("UPDATE_TIME", DataTypes.BIGINT())
                .column("PAY_SOURCE", DataTypes.VARCHAR(2))
                .column("CASH_FAIL_REASON", DataTypes.VARCHAR(1000))
                .column("LY_FAIL_REASON", DataTypes.VARCHAR(1000))
                .column("ORDER_TYPE", DataTypes.VARCHAR(2))
                .column("CASHPAY_STARTTIME", DataTypes.BIGINT())
                .column("ORIGIN_TYPE", DataTypes.VARCHAR(20))
                .build();
        // 注册为临时视图
        tEnv.createTemporaryView("ORDER_PAY_INFO", tSource, schema);

        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_LYX_ORDER_PAY_INFO (\n" +
                "    ETL_DATE DATE,\n" +
                "ID BIGINT ,\n" +
                "LY_VALUE_STATUS VARCHAR(6) ,\n" +
                "LY_SUM VARCHAR(60) ,\n" +
                "LY_PAY_TIME TIMESTAMP(6) ,\n" +
                "CASH_PAY_STATUS VARCHAR(6) ,\n" +
                "PAY_WAY VARCHAR(6) ,\n" +
                "CASH_SUM VARCHAR(60) ,\n" +
                "CASH_PAY_NO VARCHAR(150) ,\n" +
                "CASH_PAY_MOBILE VARCHAR(600) ,\n" +
                "CASH_PAY_TIME TIMESTAMP(6) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "PAY_SOURCE VARCHAR(6) ,\n" +
                "CASH_FAIL_REASON VARCHAR(3000) ,\n" +
                "LY_FAIL_REASON VARCHAR(3000) ,\n" +
                "ORDER_TYPE VARCHAR(6) ,\n" +
                "CASHPAY_STARTTIME TIMESTAMP(6) ,\n" +
                "ORIGIN_TYPE VARCHAR(60)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYX_ORDER_PAY_INFO',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_LYX_ORDER_PAY_INFO(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "LY_VALUE_STATUS,\n" +
                "LY_SUM,\n" +
                "LY_PAY_TIME,\n" +
                "CASH_PAY_STATUS,\n" +
                "PAY_WAY,\n" +
                "CASH_SUM,\n" +
                "CASH_PAY_NO,\n" +
                "CASH_PAY_MOBILE,\n" +
                "CASH_PAY_TIME,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_TIME,\n" +
                "PAY_SOURCE,\n" +
                "CASH_FAIL_REASON,\n" +
                "LY_FAIL_REASON,\n" +
                "ORDER_TYPE,\n" +
                "CASHPAY_STARTTIME,\n" +
                "ORIGIN_TYPE)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "LY_VALUE_STATUS,\n" +
                "LY_SUM,\n" +
                "TO_TIMESTAMP_LTZ(LY_PAY_TIME, 3) LY_PAY_TIME,\n" +
                "CASH_PAY_STATUS,\n" +
                "PAY_WAY,\n" +
                "CASH_SUM,\n" +
                "CASH_PAY_NO,\n" +
                "  sm4_encrypt(CASH_PAY_MOBILE, '" + sm4key + "') CASH_PAY_MOBILE,\n" +
                "TO_TIMESTAMP_LTZ(CASH_PAY_TIME, 3) CASH_PAY_TIME,\n" +
                "TO_TIMESTAMP_LTZ(CREATE_TIME, 3) CREATE_TIME,\n" +
                "TO_TIMESTAMP_LTZ(UPDATE_TIME, 3) UPDATE_TIME,\n" +
                "PAY_SOURCE,\n" +
                "CASH_FAIL_REASON,\n" +
                "LY_FAIL_REASON,\n" +
                "ORDER_TYPE,\n" +
                "TO_TIMESTAMP_LTZ(CASHPAY_STARTTIME, 3) CASHPAY_STARTTIME,\n" +
                "ORIGIN_TYPE\n" +
                " FROM ORDER_PAY_INFO "
//                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
//                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
