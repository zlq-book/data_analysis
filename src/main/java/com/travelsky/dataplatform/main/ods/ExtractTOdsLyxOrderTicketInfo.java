package com.travelsky.dataplatform.main.ods;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.TOdsLyxOrderTicketInfo;
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
public class ExtractTOdsLyxOrderTicketInfo {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
//        String etlDate = "2019-12-30";
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLyxOrderTicketInfo");
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
        String query =    "SELECT " +
                "ID,\n" +
                "ORDER_NO,\n" +
                "ORDER_SOURCE,\n" +
                "ORDER_STATUS,\n" +
                "LY_NAME,\n" +
                "LY_CARD, \n" +
                "LY_LEVEL,\n" +
                Constants.LYX_SCHEMA + ".decrypt_func(LY_MOBILE) LY_MOBILE,\n" +
                "ORDER_PERSON,\n" +
                "CREATE_TIME,\n" +
                "UPDATOR,\n" +
                "UPDATE_TIME,\n" +
                "TOTAL_PRICE,\n" +
                "TOTAL_TAX,\n" +
                "TOTAL_EXTRA,\n" +
                "PAY_ID,\n" +
                "USED_LEFT_FLAG,\n" +
                "ERROR_STATUS,\n" +
                "ERROR_SOLUTION\n" +
                " FROM " + Constants.LYX_SCHEMA + ".ORDER_TICKET_INFO "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        String columStr = "ID,ORDER_NO,ORDER_SOURCE,ORDER_STATUS,LY_NAME,LY_CARD,LY_LEVEL,LY_MOBILE,ORDER_PERSON,CREATE_TIME,UPDATOR,UPDATE_TIME,TOTAL_PRICE,TOTAL_TAX,TOTAL_EXTRA,PAY_ID,USED_LEFT_FLAG,ERROR_STATUS,ERROR_SOLUTION";
        String[] columnStrs = columStr.split(",");
        List<String> columns = Arrays.stream(columnStrs).collect(Collectors.toList());
        OracleDBSourceFunction function = new OracleDBSourceFunction(url, user, password, query, columns);
        DataStreamSource<JSONObject> jsonObjectDataStreamSource = env.addSource(function);

        SingleOutputStreamOperator<TOdsLyxOrderTicketInfo> tSource = jsonObjectDataStreamSource.map(jsonObject -> {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            TOdsLyxOrderTicketInfo tOdsLyxOrderBase = mapper.readValue(JSONObject.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue), TOdsLyxOrderTicketInfo.class);
            return tOdsLyxOrderBase;
        });
        tSource.print();
        Schema schema = Schema.newBuilder()
                .column("ID", DataTypes.BIGINT())
                .column("ORDER_NO", DataTypes.VARCHAR(100))
                .column("ORDER_SOURCE", DataTypes.VARCHAR(2))
                .column("ORDER_STATUS", DataTypes.VARCHAR(2))
                .column("LY_NAME", DataTypes.VARCHAR(50))
                .column("LY_CARD", DataTypes.VARCHAR(100))
                .column("LY_LEVEL", DataTypes.VARCHAR(10))
                .column("LY_MOBILE", DataTypes.VARCHAR(200))
                .column("ORDER_PERSON", DataTypes.VARCHAR(50))
                .column("CREATE_TIME", DataTypes.BIGINT())
                .column("UPDATOR", DataTypes.VARCHAR(50))
                .column("UPDATE_TIME", DataTypes.BIGINT())
                .column("TOTAL_PRICE", DataTypes.VARCHAR(20))
                .column("TOTAL_TAX", DataTypes.VARCHAR(20))
                .column("TOTAL_EXTRA", DataTypes.VARCHAR(20))
                .column("PAY_ID", DataTypes.BIGINT())
                .column("USED_LEFT_FLAG", DataTypes.VARCHAR(2))
                .column("ERROR_STATUS", DataTypes.VARCHAR(2))
                .column("ERROR_SOLUTION", DataTypes.VARCHAR(2))
                .build();
        // 注册为临时视图
        tEnv.createTemporaryView("ORDER_TICKET_INFO", tSource, schema);

        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_LYX_ORDER_TICKET_INFO (\n" +
                "    ETL_DATE DATE,\n" +
                "ID BIGINT ,\n" +
                "ORDER_NO VARCHAR(300) ,\n" +
                "ORDER_SOURCE VARCHAR(6) ,\n" +
                "ORDER_STATUS VARCHAR(6) ,\n" +
                "LY_NAME VARCHAR(150) ,\n" +
                "LY_CARD VARCHAR(300) ,\n" +
                "LY_LEVEL VARCHAR(30) ,\n" +
                "LY_MOBILE VARCHAR(600) ,\n" +
                "ORDER_PERSON VARCHAR(150) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATOR VARCHAR(150) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "TOTAL_PRICE VARCHAR(60) ,\n" +
                "TOTAL_TAX VARCHAR(60) ,\n" +
                "TOTAL_EXTRA VARCHAR(60) ,\n" +
                "PAY_ID BIGINT ,\n" +
                "USED_LEFT_FLAG VARCHAR(6) ,\n" +
                "ERROR_STATUS VARCHAR(6) ,\n" +
                "ERROR_SOLUTION VARCHAR(6)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYX_ORDER_TICKET_INFO',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_LYX_ORDER_TICKET_INFO(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "ORDER_NO,\n" +
                "ORDER_SOURCE,\n" +
                "ORDER_STATUS,\n" +
                "LY_NAME,\n" +
                "LY_CARD,\n" +
                "LY_LEVEL,\n" +
                "LY_MOBILE,\n" +
                "ORDER_PERSON,\n" +
                "CREATE_TIME,\n" +
                "UPDATOR,\n" +
                "UPDATE_TIME,\n" +
                "TOTAL_PRICE,\n" +
                "TOTAL_TAX,\n" +
                "TOTAL_EXTRA,\n" +
                "PAY_ID,\n" +
                "USED_LEFT_FLAG,\n" +
                "ERROR_STATUS,\n" +
                "ERROR_SOLUTION)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "ORDER_NO,\n" +
                "ORDER_SOURCE,\n" +
                "ORDER_STATUS,\n" +
                "LY_NAME,\n" +
                " sm4_encrypt (LY_CARD,'" + sm4key + "') LY_CARD, \n" +
                "LY_LEVEL,\n" +
                "  sm4_encrypt(LY_MOBILE, '" + sm4key + "') LY_MOBILE,\n" +
                "ORDER_PERSON,\n" +
                "TO_TIMESTAMP_LTZ(CREATE_TIME, 3) CREATE_TIME,\n" +
                "UPDATOR,\n" +
                "TO_TIMESTAMP_LTZ(UPDATE_TIME, 3) UPDATE_TIME,\n" +
                "TOTAL_PRICE,\n" +
                "TOTAL_TAX,\n" +
                "TOTAL_EXTRA,\n" +
                "PAY_ID,\n" +
                "USED_LEFT_FLAG,\n" +
                "ERROR_STATUS,\n" +
                "ERROR_SOLUTION\n" +
                " FROM ORDER_TICKET_INFO "
//                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
//                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
