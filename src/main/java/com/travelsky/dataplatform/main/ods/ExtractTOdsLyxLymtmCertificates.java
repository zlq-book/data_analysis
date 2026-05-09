package com.travelsky.dataplatform.main.ods;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.TOdsLyxLymtmCertificates;
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
public class ExtractTOdsLyxLymtmCertificates {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key =  Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLyxLymtmCertificates");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();


        String url = "jdbc:oracle:thin:@//" + Constants.LYX_IP + ":" + Constants.LYX_PORT + "/" + Constants.LYX_DB;
        String user = Constants.LYX_USER;
        String password = Constants.LYX_PWD;
        String query =  "SELECT " +
                "ID,\n" +
                "MTM_CARD_NUM, \n" +
                "ID_TYPE,\n" +
                Constants.LYX_SCHEMA + ".decrypt_func(ID_NUM) ID_NUM,\n" +
                "MTM_ID_FLG,\n" +
                "ID_STATUS,\n" +
                "EFFECTIVE_TIME,\n" +
                "EXPIRES_TIME,\n" +
                "DOC_PHOTO1,\n" +
                "DOC_PHOTO2,\n" +
                "DOC_PHOTO3,\n" +
                "DOC_PHOTO4,\n" +
                "DOC_PHOTO5,\n" +
                "EXPIRES_OPERATOR,\n" +
                "VERSION,\n" +
                "DEL_FLG,\n" +
                "CREATE_ID,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_ID,\n" +
                "UPDATE_TIME,\n" +
                Constants.LYX_SCHEMA + ".decrypt_func(ID_NUM_LYGJ) ID_NUM_LYGJ\n" +
                " FROM " + Constants.LYX_SCHEMA + ".LYMTM_CERTIFICATES lc "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        String columStr = "ID,MTM_CARD_NUM,ID_TYPE,ID_NUM,MTM_ID_FLG,ID_STATUS,EFFECTIVE_TIME,EXPIRES_TIME,DOC_PHOTO1,DOC_PHOTO2,DOC_PHOTO3,DOC_PHOTO4,DOC_PHOTO5,EXPIRES_OPERATOR,VERSION,DEL_FLG,CREATE_ID,CREATE_TIME,UPDATE_ID,UPDATE_TIME,ID_NUM_LYGJ";
        String[] columnStrs = columStr.split(",");
        List<String> columns = Arrays.stream(columnStrs).collect(Collectors.toList());
        OracleDBSourceFunction function = new OracleDBSourceFunction(url, user, password, query, columns);
        DataStreamSource<JSONObject> jsonObjectDataStreamSource = env.addSource(function);

        SingleOutputStreamOperator<TOdsLyxLymtmCertificates> tSource = jsonObjectDataStreamSource.map(jsonObject -> {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            TOdsLyxLymtmCertificates tOdsLyxOrderBase = mapper.readValue(JSONObject.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue), TOdsLyxLymtmCertificates.class);
            return tOdsLyxOrderBase;
        });
        tSource.print();
        Schema schema = Schema.newBuilder()
                .column("ID", DataTypes.BIGINT())
                .column("MTM_CARD_NUM", DataTypes.VARCHAR(256))
                .column("ID_TYPE", DataTypes.VARCHAR(4))
                .column("ID_NUM", DataTypes.VARCHAR(256))
                .column("MTM_ID_FLG", DataTypes.VARCHAR(2))
                .column("ID_STATUS", DataTypes.VARCHAR(2))
                .column("EFFECTIVE_TIME", DataTypes.BIGINT())
                .column("EXPIRES_TIME", DataTypes.BIGINT())
                .column("DOC_PHOTO1", DataTypes.VARCHAR(500))
                .column("DOC_PHOTO2", DataTypes.VARCHAR(500))
                .column("DOC_PHOTO3", DataTypes.VARCHAR(500))
                .column("DOC_PHOTO4", DataTypes.VARCHAR(500))
                .column("DOC_PHOTO5", DataTypes.VARCHAR(500))
                .column("EXPIRES_OPERATOR", DataTypes.VARCHAR(60))
                .column("VERSION", DataTypes.BIGINT())
                .column("DEL_FLG", DataTypes.VARCHAR(2))
                .column("CREATE_ID", DataTypes.VARCHAR(60))
                .column("CREATE_TIME", DataTypes.BIGINT())
                .column("UPDATE_ID", DataTypes.VARCHAR(60))
                .column("UPDATE_TIME", DataTypes.BIGINT())
                .column("ID_NUM_LYGJ", DataTypes.VARCHAR(256))
                .build();
        // 注册为临时视图
        tEnv.createTemporaryView("LYMTM_CERTIFICATES", tSource, schema);

        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_LYX_LYMTM_CERTIFICATES (\n" +
                "    ETL_DATE DATE,\n" +
                "ID BIGINT ,\n" +
                "MTM_CARD_NUM VARCHAR(300) ,\n" +
                "ID_TYPE VARCHAR(12) ,\n" +
                "ID_NUM VARCHAR(300) ,\n" +
                "MTM_ID_FLG VARCHAR(6) ,\n" +
                "ID_STATUS VARCHAR(6) ,\n" +
                "EFFECTIVE_TIME TIMESTAMP(6) ,\n" +
                "EXPIRES_TIME TIMESTAMP(6) ,\n" +
                "DOC_PHOTO1 VARCHAR(1500) ,\n" +
                "DOC_PHOTO2 VARCHAR(1500) ,\n" +
                "DOC_PHOTO3 VARCHAR(1500) ,\n" +
                "DOC_PHOTO4 VARCHAR(1500) ,\n" +
                "DOC_PHOTO5 VARCHAR(1500) ,\n" +
                "EXPIRES_OPERATOR VARCHAR(60) ,\n" +
                "VERSION BIGINT ,\n" +
                "DEL_FLG VARCHAR(6) ,\n" +
                "CREATE_ID VARCHAR(60) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_ID VARCHAR(60) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "ID_NUM_LYGJ VARCHAR(768)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYX_LYMTM_CERTIFICATES',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_LYX_LYMTM_CERTIFICATES(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "MTM_CARD_NUM,\n" +
                "ID_TYPE,\n" +
                "ID_NUM,\n" +
                "MTM_ID_FLG,\n" +
                "ID_STATUS,\n" +
                "EFFECTIVE_TIME,\n" +
                "EXPIRES_TIME,\n" +
                "DOC_PHOTO1,\n" +
                "DOC_PHOTO2,\n" +
                "DOC_PHOTO3,\n" +
                "DOC_PHOTO4,\n" +
                "DOC_PHOTO5,\n" +
                "EXPIRES_OPERATOR,\n" +
                "VERSION,\n" +
                "DEL_FLG,\n" +
                "CREATE_ID,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_ID,\n" +
                "UPDATE_TIME,\n" +
                "ID_NUM_LYGJ)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "  sm4_encrypt(MTM_CARD_NUM, '" + sm4key + "') MTM_CARD_NUM,\n" +
                "ID_TYPE,\n" +
                "  sm4_encrypt(ID_NUM, '" + sm4key + "') ID_NUM,\n" +
                "MTM_ID_FLG,\n" +
                "ID_STATUS,\n" +
                "TO_TIMESTAMP_LTZ(EFFECTIVE_TIME, 3) EFFECTIVE_TIME,\n" +
                "TO_TIMESTAMP_LTZ(EXPIRES_TIME, 3) EXPIRES_TIME,\n" +
                "DOC_PHOTO1,\n" +
                "DOC_PHOTO2,\n" +
                "DOC_PHOTO3,\n" +
                "DOC_PHOTO4,\n" +
                "DOC_PHOTO5,\n" +
                "EXPIRES_OPERATOR,\n" +
                "VERSION,\n" +
                "DEL_FLG,\n" +
                "CREATE_ID,\n" +
                "TO_TIMESTAMP_LTZ(CREATE_TIME, 3)CREATE_TIME,\n" +
                "UPDATE_ID,\n" +
                "TO_TIMESTAMP_LTZ(UPDATE_TIME, 3) UPDATE_TIME,\n" +
                "  sm4_encrypt(ID_NUM_LYGJ, '" + sm4key + "') ID_NUM_LYGJ\n" +
                " FROM LYMTM_CERTIFICATES "
//                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
//                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
