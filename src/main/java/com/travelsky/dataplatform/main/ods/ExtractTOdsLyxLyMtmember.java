package com.travelsky.dataplatform.main.ods;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.TOdsLyxLyMtmember;
import com.travelsky.dataplatform.source.OracleDBSourceFunction;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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
public class ExtractTOdsLyxLyMtmember {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
//        String etlDate = "2019-08-01";
        String sm4key =  Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLyxLyMtmember");
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
                "MTM_STATUS,\n" +
                "MTM_PASS,\n" +
                "MTM_SURNAME_CN,\n" +
                "MTM_NAME_CN,\n" +
                "MTM_SURNAME_EN,\n" +
                "MTM_NAME_EN,\n" +
                "MTM_HIPPOCRATES,\n" +
                "MTM_NATIONALITY,\n" +
                "MTM_BIRTHDAY,\n" +
                "MTM_REGISTER_TIME,\n" +
                "PASS_ACTIVE_STATE,\n" +
                "REALNAME_ATTESTATION,\n" +
                "ATTESTATION_MODE,\n" +
                "ATTESTATION_TIME,\n" +
                Constants.LYX_SCHEMA + ".decrypt_func(MTM_MOBILE) MTM_MOBILE,\n" +
                "MAIN_ID_TYPE,\n" +
                Constants.LYX_SCHEMA + ".decrypt_func(MAIN_ID_NUM) MAIN_ID_NUM,\n" +
                "VERSION,\n" +
                "DEL_FLG,\n" +
                "CREATE_ID,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_ID,\n" +
                "UPDATE_TIME,\n" +
                "LINK_ID,\n" +
                "LINK_SOURCE,\n" +
                "GESTURE_STATUS,\n" +
                "GESTURE_PASS,\n" +
                "GESTURE_TIP,\n" +
                "LIP_ATTESTATION,\n" +
                "TIMING_TASK_STATUS,\n" +
                "ORI_LEVEL,\n" +
                "LEVEL_UPDATE_TIME,\n" +
                "REGISTER_STATUS,\n" +
                "PRIVATE_POLICY_STATUS,\n" +
                "PRIVATE_POLICY_SEND_TIME,\n" +
                "PRIVATE_POLICY_CONFIRM_TIME,\n" +
                "UPPWD_DATE,\n" +
                "DIRECT_SELL_USER_ID\n" +
                " FROM " + Constants.LYX_SCHEMA + ".LY_MTMEMBER lm"
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        String columStr = "ID,MTM_CARD_NUM,MTM_STATUS,MTM_PASS,MTM_SURNAME_CN,MTM_NAME_CN,MTM_SURNAME_EN,MTM_NAME_EN,MTM_HIPPOCRATES,MTM_NATIONALITY,MTM_BIRTHDAY,MTM_REGISTER_TIME,PASS_ACTIVE_STATE,REALNAME_ATTESTATION,ATTESTATION_MODE,ATTESTATION_TIME,MTM_MOBILE,MAIN_ID_TYPE,MAIN_ID_NUM,VERSION,DEL_FLG,CREATE_ID,CREATE_TIME,UPDATE_ID,UPDATE_TIME,LINK_ID,LINK_SOURCE,GESTURE_STATUS,GESTURE_PASS,GESTURE_TIP,TIMING_TASK_STATUS,LIP_ATTESTATION,REGISTER_STATUS,PRIVATE_POLICY_STATUS,PRIVATE_POLICY_SEND_TIME,PRIVATE_POLICY_CONFIRM_TIME,LEVEL_UPDATE_TIME,ORI_LEVEL,UPPWD_DATE,DIRECT_SELL_USER_ID";
        String[] columnStrs = columStr.split(",");
        List<String> columns = Arrays.stream(columnStrs).collect(Collectors.toList());
        OracleDBSourceFunction function = new OracleDBSourceFunction(url, user, password, query, columns);
        DataStreamSource<JSONObject> jsonObjectDataStreamSource = env.addSource(function);

        SingleOutputStreamOperator<TOdsLyxLyMtmember> tSource = jsonObjectDataStreamSource.map(jsonObject -> {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            TOdsLyxLyMtmember tOdsLyxOrderBase = mapper.readValue(JSONObject.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue), TOdsLyxLyMtmember.class);
            return tOdsLyxOrderBase;
        });
        tSource.print();
        Schema schema = Schema.newBuilder()
                .column("ID", DataTypes.BIGINT())
                .column("MTM_CARD_NUM", DataTypes.VARCHAR(20))
                .column("MTM_STATUS", DataTypes.VARCHAR(4))
                .column("MTM_PASS", DataTypes.VARCHAR(100))
                .column("MTM_SURNAME_CN", DataTypes.VARCHAR(50))
                .column("MTM_NAME_CN", DataTypes.VARCHAR(50))
                .column("MTM_SURNAME_EN", DataTypes.VARCHAR(50))
                .column("MTM_NAME_EN", DataTypes.VARCHAR(50))
                .column("MTM_HIPPOCRATES", DataTypes.VARCHAR(20))
                .column("MTM_NATIONALITY", DataTypes.VARCHAR(20))
                .column("MTM_BIRTHDAY", DataTypes.BIGINT())
                .column("MTM_REGISTER_TIME", DataTypes.BIGINT())
                .column("PASS_ACTIVE_STATE", DataTypes.VARCHAR(4))
                .column("REALNAME_ATTESTATION", DataTypes.VARCHAR(4))
                .column("ATTESTATION_MODE", DataTypes.VARCHAR(4))
                .column("ATTESTATION_TIME", DataTypes.BIGINT())
                .column("MTM_MOBILE", DataTypes.VARCHAR(100))
                .column("MAIN_ID_TYPE", DataTypes.VARCHAR(20))
                .column("MAIN_ID_NUM", DataTypes.VARCHAR(100))
                .column("VERSION", DataTypes.BIGINT())
                .column("DEL_FLG", DataTypes.VARCHAR(20))
                .column("CREATE_ID", DataTypes.VARCHAR(20))
                .column("CREATE_TIME", DataTypes.BIGINT())
                .column("UPDATE_ID", DataTypes.VARCHAR(20))
                .column("UPDATE_TIME", DataTypes.BIGINT())
                .column("LINK_ID", DataTypes.VARCHAR(20))
                .column("LINK_SOURCE", DataTypes.VARCHAR(20))
                .column("GESTURE_STATUS", DataTypes.VARCHAR(20))
                .column("GESTURE_PASS", DataTypes.VARCHAR(100))
                .column("GESTURE_TIP", DataTypes.VARCHAR(20))
                .column("TIMING_TASK_STATUS", DataTypes.VARCHAR(2))
                .column("LIP_ATTESTATION", DataTypes.VARCHAR(2))
                .column("REGISTER_STATUS", DataTypes.VARCHAR(5))
                .column("PRIVATE_POLICY_STATUS", DataTypes.VARCHAR(5))
                .column("PRIVATE_POLICY_SEND_TIME", DataTypes.BIGINT())
                .column("PRIVATE_POLICY_CONFIRM_TIME", DataTypes.BIGINT())
                .column("LEVEL_UPDATE_TIME", DataTypes.BIGINT())
                .column("ORI_LEVEL", DataTypes.VARCHAR(100))
                .column("UPPWD_DATE", DataTypes.BIGINT())
                .column("DIRECT_SELL_USER_ID", DataTypes.VARCHAR(100))
                .build();
        // 注册为临时视图
        tEnv.createTemporaryView("LY_MTMEMBER", tSource, schema);

        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_LYX_LY_MTMEMBER (\n" +
                "    ETL_DATE DATE,\n" +
                "ID BIGINT ,\n" +
                "MTM_CARD_NUM VARCHAR(300) ,\n" +
                "MTM_STATUS VARCHAR(12) ,\n" +
                "MTM_PASS VARCHAR(300) ,\n" +
                "MTM_SURNAME_CN VARCHAR(150) ,\n" +
                "MTM_NAME_CN VARCHAR(150) ,\n" +
                "MTM_SURNAME_EN VARCHAR(150) ,\n" +
                "MTM_NAME_EN VARCHAR(150) ,\n" +
                "MTM_HIPPOCRATES VARCHAR(60) ,\n" +
                "MTM_NATIONALITY VARCHAR(60) ,\n" +
                "MTM_BIRTHDAY TIMESTAMP(6) ,\n" +
                "MTM_REGISTER_TIME TIMESTAMP(6) ,\n" +
                "PASS_ACTIVE_STATE VARCHAR(12) ,\n" +
                "REALNAME_ATTESTATION VARCHAR(12) ,\n" +
                "ATTESTATION_MODE VARCHAR(12) ,\n" +
                "ATTESTATION_TIME TIMESTAMP(6) ,\n" +
                "MTM_MOBILE VARCHAR(300) ,\n" +
                "MAIN_ID_TYPE VARCHAR(60) ,\n" +
                "MAIN_ID_NUM VARCHAR(300) ,\n" +
                "VERSION BIGINT ,\n" +
                "DEL_FLG VARCHAR(60) ,\n" +
                "CREATE_ID VARCHAR(60) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATE_ID VARCHAR(60) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "LINK_ID VARCHAR(60) ,\n" +
                "LINK_SOURCE VARCHAR(60) ,\n" +
                "GESTURE_STATUS VARCHAR(60) ,\n" +
                "GESTURE_PASS VARCHAR(300) ,\n" +
                "GESTURE_TIP VARCHAR(60) ,\n" +
                "LIP_ATTESTATION VARCHAR(6) ,\n" +
                "TIMING_TASK_STATUS VARCHAR(6) ,\n" +
                "ORI_LEVEL VARCHAR(300) ,\n" +
                "LEVEL_UPDATE_TIME TIMESTAMP(6) ,\n" +
                "REGISTER_STATUS VARCHAR(15) ,\n" +
                "PRIVATE_POLICY_STATUS VARCHAR(15) ,\n" +
                "PRIVATE_POLICY_SEND_TIME TIMESTAMP(6) ,\n" +
                "PRIVATE_POLICY_CONFIRM_TIME TIMESTAMP(6) ,\n" +
                "UPPWD_DATE TIMESTAMP(6) ,\n" +
                "DIRECT_SELL_USER_ID VARCHAR(300)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYX_LY_MTMEMBER',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");

        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_LYX_LY_MTMEMBER(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "MTM_CARD_NUM,\n" +
                "MTM_STATUS,\n" +
                "MTM_PASS,\n" +
                "MTM_SURNAME_CN,\n" +
                "MTM_NAME_CN,\n" +
                "MTM_SURNAME_EN,\n" +
                "MTM_NAME_EN,\n" +
                "MTM_HIPPOCRATES,\n" +
                "MTM_NATIONALITY,\n" +
                "MTM_BIRTHDAY,\n" +
                "MTM_REGISTER_TIME,\n" +
                "PASS_ACTIVE_STATE,\n" +
                "REALNAME_ATTESTATION,\n" +
                "ATTESTATION_MODE,\n" +
                "ATTESTATION_TIME,\n" +
                "MTM_MOBILE,\n" +
                "MAIN_ID_TYPE,\n" +
                "MAIN_ID_NUM,\n" +
                "VERSION,\n" +
                "DEL_FLG,\n" +
                "CREATE_ID,\n" +
                "CREATE_TIME,\n" +
                "UPDATE_ID,\n" +
                "UPDATE_TIME,\n" +
                "LINK_ID,\n" +
                "LINK_SOURCE,\n" +
                "GESTURE_STATUS,\n" +
                "GESTURE_PASS,\n" +
                "GESTURE_TIP,\n" +
                "LIP_ATTESTATION,\n" +
                "TIMING_TASK_STATUS,\n" +
                "ORI_LEVEL,\n" +
                "LEVEL_UPDATE_TIME,\n" +
                "REGISTER_STATUS,\n" +
                "PRIVATE_POLICY_STATUS,\n" +
                "PRIVATE_POLICY_SEND_TIME,\n" +
                "PRIVATE_POLICY_CONFIRM_TIME,\n" +
                "UPPWD_DATE,\n" +
                "DIRECT_SELL_USER_ID)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                " sm4_encrypt(MTM_CARD_NUM,'" + sm4key + "') MTM_CARD_NUM, \n" +
                "MTM_STATUS,\n" +
                "MTM_PASS,\n" +
                "MTM_SURNAME_CN,\n" +
                "MTM_NAME_CN,\n" +
                "MTM_SURNAME_EN,\n" +
                "MTM_NAME_EN,\n" +
                "MTM_HIPPOCRATES,\n" +
                "MTM_NATIONALITY,\n" +
                "TO_TIMESTAMP_LTZ(MTM_BIRTHDAY, 3) MTM_BIRTHDAY,\n" +
                "TO_TIMESTAMP_LTZ(MTM_REGISTER_TIME, 3) MTM_REGISTER_TIME,\n" +
                "PASS_ACTIVE_STATE,\n" +
                "REALNAME_ATTESTATION,\n" +
                "ATTESTATION_MODE,\n" +
                "TO_TIMESTAMP_LTZ(ATTESTATION_TIME, 3) ATTESTATION_TIME,\n" +
                "  sm4_encrypt(MTM_MOBILE, '" + sm4key + "') MTM_MOBILE,\n" +
                "MAIN_ID_TYPE,\n" +
                "  sm4_encrypt(MAIN_ID_NUM, '" + sm4key + "') MAIN_ID_NUM,\n" +
                "VERSION,\n" +
                "DEL_FLG,\n" +
                "CREATE_ID,\n" +
                "TO_TIMESTAMP_LTZ(CREATE_TIME, 3) CREATE_TIME,\n" +
                "UPDATE_ID,\n" +
                "TO_TIMESTAMP_LTZ(UPDATE_TIME, 3) UPDATE_TIME,\n" +
                "LINK_ID,\n" +
                "LINK_SOURCE,\n" +
                "GESTURE_STATUS,\n" +
                "GESTURE_PASS,\n" +
                "GESTURE_TIP,\n" +
                "LIP_ATTESTATION,\n" +
                "TIMING_TASK_STATUS,\n" +
                "ORI_LEVEL,\n" +
                "TO_TIMESTAMP_LTZ(LEVEL_UPDATE_TIME, 3) LEVEL_UPDATE_TIME,\n" +
                "REGISTER_STATUS,\n" +
                "PRIVATE_POLICY_STATUS,\n" +
                "TO_TIMESTAMP_LTZ(PRIVATE_POLICY_SEND_TIME, 3) PRIVATE_POLICY_SEND_TIME,\n" +
                "TO_TIMESTAMP_LTZ(PRIVATE_POLICY_CONFIRM_TIME, 3) PRIVATE_POLICY_CONFIRM_TIME,\n" +
                "TO_TIMESTAMP_LTZ(UPPWD_DATE, 3) UPPWD_DATE,\n" +
                "DIRECT_SELL_USER_ID\n" +
                " FROM LY_MTMEMBER "
//                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
//                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
