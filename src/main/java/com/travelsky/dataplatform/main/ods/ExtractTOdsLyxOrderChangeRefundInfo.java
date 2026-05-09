package com.travelsky.dataplatform.main.ods;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.TOdsLyxOrderChangeRefundInfo;
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
public class ExtractTOdsLyxOrderChangeRefundInfo {
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
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLyxOrderChangeRefundInfo");
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
                "ORDER_NO,\n" +
                "TOTAL_REFUND_STATUS,\n" +
                "ORDER_TYPE,\n" +
                "OLD_ORDER_NO,\n" +
                "ORIGINAL_ORDER_NO,\n" +
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
                "CHANGE_FEE_TOTAL,\n" +
                "COMPENSATION_FEE_TOTAL,\n" +
                "LY_SUM,\n" +
                "CASH_SUM,\n" +
                "CHANGE_PERPSON,\n" +
                "CHANGE_TIME,\n" +
                "REFUND_PERPSON,\n" +
                "REFUND_TIME,\n" +
                "REFUND_LY_VALUE,\n" +
                "REFUND_LY_STATUS,\n" +
                "LY_EXPIRES_TIME,\n" +
                "REFUND_CASH,\n" +
                "REFUND_CASH_STATUS,\n" +
                "AIRPORT_TAX_TOTAL,\n" +
                "FUEL_TAX_TOTAL,\n" +
                "INSURE_FEE_TOTAL,\n" +
                "CHANGE_SUM_FEE,\n" +
                "LAST_CHANGE_FEE,\n" +
                "REFUND_TYPE,\n" +
                "NOVOLUNTEER_REASON,\n" +
                "REFUND_EXPLAIN,\n" +
                "REFUND_EVIDENCE,\n" +
                "REFUND_EVIDENCE_IMAGE,\n" +
                "APPLY_PERSON,\n" +
                "APPLY_TIME,\n" +
                "AUDIT_PERSON,\n" +
                "AUDIT_TIME,\n" +
                "PAY_ID,\n" +
                "USED_LEFT_FLAG,\n" +
                "IS_ABNORMAL,\n" +
                "ERROR_STATUS,\n" +
                "AUDIT_STATUS,\n" +
                "COMPLETE_STATUS,\n" +
                "AUDIT_REMARK,\n" +
                "AUDIT_RESULT,\n" +
                "AUDIT_SECOND_PERSON,\n" +
                "AUDIT_SECOND_TIME,\n" +
                "AUDIT_SECOND_RESULT,\n" +
                "AUDIT_SECOND_REMARK,\n" +
                "CHANGE_TYPE,\n" +
                "CHANGE_EXPLAIN,\n" +
                "CHANGE_NOVOLUNTEER_REASON,\n" +
                "CHANGE_EVIDENCE,\n" +
                "CHANGE_EVIDENCE_IMAGE,\n" +
                "REFUND_LY_VALUE_METHOD,\n" +
                "EFFECTIVE_DATE,\n" +
                "ERROR_SOLUTION\n" +
                " FROM " + Constants.LYX_SCHEMA + ".ORDER_CHANGE_REFUND_INFO ocri"
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        String columStr = "ID,ORDER_NO,TOTAL_REFUND_STATUS,ORDER_TYPE,OLD_ORDER_NO,ORIGINAL_ORDER_NO,ORDER_SOURCE,ORDER_STATUS,LY_NAME,LY_CARD,LY_LEVEL,LY_MOBILE,ORDER_PERSON,CREATE_TIME,UPDATOR,UPDATE_TIME,CHANGE_FEE_TOTAL,COMPENSATION_FEE_TOTAL,LY_SUM,CASH_SUM,CHANGE_PERPSON,CHANGE_TIME,REFUND_PERPSON,REFUND_TIME,REFUND_LY_VALUE,REFUND_LY_STATUS,LY_EXPIRES_TIME,REFUND_CASH,REFUND_CASH_STATUS,AIRPORT_TAX_TOTAL,FUEL_TAX_TOTAL,INSURE_FEE_TOTAL,CHANGE_SUM_FEE,LAST_CHANGE_FEE,REFUND_TYPE,NOVOLUNTEER_REASON,REFUND_EXPLAIN,REFUND_EVIDENCE,REFUND_EVIDENCE_IMAGE,APPLY_PERSON,APPLY_TIME,AUDIT_PERSON,AUDIT_TIME,PAY_ID,USED_LEFT_FLAG,IS_ABNORMAL,ERROR_STATUS,AUDIT_STATUS,COMPLETE_STATUS,AUDIT_REMARK,AUDIT_RESULT,AUDIT_SECOND_PERSON,AUDIT_SECOND_TIME,AUDIT_SECOND_RESULT,AUDIT_SECOND_REMARK,CHANGE_TYPE,CHANGE_EXPLAIN,CHANGE_NOVOLUNTEER_REASON,CHANGE_EVIDENCE,CHANGE_EVIDENCE_IMAGE,REFUND_LY_VALUE_METHOD,EFFECTIVE_DATE,ERROR_SOLUTION";
        String[] columnStrs = columStr.split(",");
        List<String> columns = Arrays.stream(columnStrs).collect(Collectors.toList());
        OracleDBSourceFunction function = new OracleDBSourceFunction(url, user, password, query, columns);
        DataStreamSource<JSONObject> jsonObjectDataStreamSource = env.addSource(function);

        SingleOutputStreamOperator<TOdsLyxOrderChangeRefundInfo> tSource = jsonObjectDataStreamSource.map(jsonObject -> {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            TOdsLyxOrderChangeRefundInfo tOdsLyxOrderBase = mapper.readValue(JSONObject.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue), TOdsLyxOrderChangeRefundInfo.class);
            return tOdsLyxOrderBase;
        });
        tSource.print();
        Schema schema = Schema.newBuilder()
                .column("ID", DataTypes.BIGINT())
                .column("ORDER_NO", DataTypes.VARCHAR(100))
                .column("TOTAL_REFUND_STATUS", DataTypes.VARCHAR(2))
                .column("ORDER_TYPE", DataTypes.VARCHAR(2))
                .column("OLD_ORDER_NO", DataTypes.VARCHAR(100))
                .column("ORIGINAL_ORDER_NO", DataTypes.VARCHAR(100))
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
                .column("CHANGE_FEE_TOTAL", DataTypes.VARCHAR(20))
                .column("COMPENSATION_FEE_TOTAL", DataTypes.VARCHAR(20))
                .column("LY_SUM", DataTypes.VARCHAR(20))
                .column("CASH_SUM", DataTypes.VARCHAR(20))
                .column("CHANGE_PERPSON", DataTypes.VARCHAR(50))
                .column("CHANGE_TIME", DataTypes.BIGINT())
                .column("REFUND_PERPSON", DataTypes.VARCHAR(50))
                .column("REFUND_TIME", DataTypes.BIGINT())
                .column("REFUND_LY_VALUE", DataTypes.VARCHAR(20))
                .column("REFUND_LY_STATUS", DataTypes.VARCHAR(2))
                .column("LY_EXPIRES_TIME", DataTypes.BIGINT())
                .column("REFUND_CASH", DataTypes.VARCHAR(20))
                .column("REFUND_CASH_STATUS", DataTypes.VARCHAR(2))
                .column("AIRPORT_TAX_TOTAL", DataTypes.VARCHAR(20))
                .column("FUEL_TAX_TOTAL", DataTypes.VARCHAR(20))
                .column("INSURE_FEE_TOTAL", DataTypes.VARCHAR(20))
                .column("CHANGE_SUM_FEE", DataTypes.VARCHAR(20))
                .column("LAST_CHANGE_FEE", DataTypes.VARCHAR(20))
                .column("REFUND_TYPE", DataTypes.VARCHAR(2))
                .column("NOVOLUNTEER_REASON", DataTypes.VARCHAR(100))
                .column("REFUND_EXPLAIN", DataTypes.VARCHAR(100))
                .column("REFUND_EVIDENCE", DataTypes.VARCHAR(50))
                .column("REFUND_EVIDENCE_IMAGE", DataTypes.VARCHAR(500))
                .column("APPLY_PERSON", DataTypes.VARCHAR(150))
                .column("APPLY_TIME", DataTypes.BIGINT())
                .column("AUDIT_PERSON", DataTypes.VARCHAR(50))
                .column("AUDIT_TIME", DataTypes.BIGINT())
                .column("PAY_ID", DataTypes.BIGINT())
                .column("USED_LEFT_FLAG", DataTypes.VARCHAR(2))
                .column("IS_ABNORMAL", DataTypes.VARCHAR(2))
                .column("ERROR_STATUS", DataTypes.VARCHAR(2))
                .column("AUDIT_STATUS", DataTypes.VARCHAR(2))
                .column("COMPLETE_STATUS", DataTypes.VARCHAR(2))
                .column("AUDIT_REMARK", DataTypes.VARCHAR(255))
                .column("AUDIT_RESULT", DataTypes.VARCHAR(255))
                .column("AUDIT_SECOND_PERSON", DataTypes.VARCHAR(255))
                .column("AUDIT_SECOND_TIME", DataTypes.BIGINT())
                .column("AUDIT_SECOND_RESULT", DataTypes.VARCHAR(255))
                .column("AUDIT_SECOND_REMARK", DataTypes.VARCHAR(255))
                .column("CHANGE_TYPE", DataTypes.VARCHAR(2))
                .column("CHANGE_EXPLAIN", DataTypes.VARCHAR(100))
                .column("CHANGE_NOVOLUNTEER_REASON", DataTypes.VARCHAR(255))
                .column("CHANGE_EVIDENCE", DataTypes.VARCHAR(50))
                .column("CHANGE_EVIDENCE_IMAGE", DataTypes.VARCHAR(300))
                .column("REFUND_LY_VALUE_METHOD", DataTypes.VARCHAR(2))
                .column("EFFECTIVE_DATE", DataTypes.BIGINT())
                .column("ERROR_SOLUTION", DataTypes.VARCHAR(2))
                .build();
        // 注册为临时视图
        tEnv.createTemporaryView("ORDER_CHANGE_REFUND_INFO", tSource, schema);

        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_LYX_ORDER_CHANGE_REFUND_INFO (\n" +
                "    ETL_DATE DATE ,\n" +
                "ID BIGINT ,\n" +
                "ORDER_NO VARCHAR(300) ,\n" +
                "TOTAL_REFUND_STATUS VARCHAR(6) ,\n" +
                "ORDER_TYPE VARCHAR(6) ,\n" +
                "OLD_ORDER_NO VARCHAR(300) ,\n" +
                "ORIGINAL_ORDER_NO VARCHAR(300) ,\n" +
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
                "CHANGE_FEE_TOTAL VARCHAR(60) ,\n" +
                "COMPENSATION_FEE_TOTAL VARCHAR(60) ,\n" +
                "LY_SUM VARCHAR(60) ,\n" +
                "CASH_SUM VARCHAR(60) ,\n" +
                "CHANGE_PERPSON VARCHAR(150) ,\n" +
                "CHANGE_TIME TIMESTAMP(6) ,\n" +
                "REFUND_PERPSON VARCHAR(150) ,\n" +
                "REFUND_TIME TIMESTAMP(6) ,\n" +
                "REFUND_LY_VALUE VARCHAR(60) ,\n" +
                "REFUND_LY_STATUS VARCHAR(6) ,\n" +
                "LY_EXPIRES_TIME DATE ,\n" +
                "REFUND_CASH VARCHAR(60) ,\n" +
                "REFUND_CASH_STATUS VARCHAR(6) ,\n" +
                "AIRPORT_TAX_TOTAL VARCHAR(60) ,\n" +
                "FUEL_TAX_TOTAL VARCHAR(60) ,\n" +
                "INSURE_FEE_TOTAL VARCHAR(60) ,\n" +
                "CHANGE_SUM_FEE VARCHAR(60) ,\n" +
                "LAST_CHANGE_FEE VARCHAR(60) ,\n" +
                "REFUND_TYPE VARCHAR(6) ,\n" +
                "NOVOLUNTEER_REASON VARCHAR(300) ,\n" +
                "REFUND_EXPLAIN VARCHAR(300) ,\n" +
                "REFUND_EVIDENCE VARCHAR(150) ,\n" +
                "REFUND_EVIDENCE_IMAGE VARCHAR(1500) ,\n" +
                "APPLY_PERSON VARCHAR(450) ,\n" +
                "APPLY_TIME TIMESTAMP(6) ,\n" +
                "AUDIT_PERSON VARCHAR(150) ,\n" +
                "AUDIT_TIME TIMESTAMP(6) ,\n" +
                "PAY_ID BIGINT ,\n" +
                "USED_LEFT_FLAG VARCHAR(6) ,\n" +
                "IS_ABNORMAL VARCHAR(6) ,\n" +
                "ERROR_STATUS VARCHAR(6) ,\n" +
                "AUDIT_STATUS VARCHAR(6) ,\n" +
                "COMPLETE_STATUS VARCHAR(6) ,\n" +
                "AUDIT_REMARK VARCHAR(765) ,\n" +
                "AUDIT_RESULT VARCHAR(765) ,\n" +
                "AUDIT_SECOND_PERSON VARCHAR(765) ,\n" +
                "AUDIT_SECOND_TIME TIMESTAMP(6) ,\n" +
                "AUDIT_SECOND_RESULT VARCHAR(765) ,\n" +
                "AUDIT_SECOND_REMARK VARCHAR(765) ,\n" +
                "CHANGE_TYPE VARCHAR(6) ,\n" +
                "CHANGE_EXPLAIN VARCHAR(300) ,\n" +
                "CHANGE_NOVOLUNTEER_REASON VARCHAR(765) ,\n" +
                "CHANGE_EVIDENCE VARCHAR(150) ,\n" +
                "CHANGE_EVIDENCE_IMAGE VARCHAR(900) ,\n" +
                "REFUND_LY_VALUE_METHOD VARCHAR(6) ,\n" +
                "EFFECTIVE_DATE DATE ,\n" +
                "ERROR_SOLUTION VARCHAR(6)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYX_ORDER_CHANGE_REFUND_INFO',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_LYX_ORDER_CHANGE_REFUND_INFO(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "ORDER_NO,\n" +
                "TOTAL_REFUND_STATUS,\n" +
                "ORDER_TYPE,\n" +
                "OLD_ORDER_NO,\n" +
                "ORIGINAL_ORDER_NO,\n" +
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
                "CHANGE_FEE_TOTAL,\n" +
                "COMPENSATION_FEE_TOTAL,\n" +
                "LY_SUM,\n" +
                "CASH_SUM,\n" +
                "CHANGE_PERPSON,\n" +
                "CHANGE_TIME,\n" +
                "REFUND_PERPSON,\n" +
                "REFUND_TIME,\n" +
                "REFUND_LY_VALUE,\n" +
                "REFUND_LY_STATUS,\n" +
                "LY_EXPIRES_TIME,\n" +
                "REFUND_CASH,\n" +
                "REFUND_CASH_STATUS,\n" +
                "AIRPORT_TAX_TOTAL,\n" +
                "FUEL_TAX_TOTAL,\n" +
                "INSURE_FEE_TOTAL,\n" +
                "CHANGE_SUM_FEE,\n" +
                "LAST_CHANGE_FEE,\n" +
                "REFUND_TYPE,\n" +
                "NOVOLUNTEER_REASON,\n" +
                "REFUND_EXPLAIN,\n" +
                "REFUND_EVIDENCE,\n" +
                "REFUND_EVIDENCE_IMAGE,\n" +
                "APPLY_PERSON,\n" +
                "APPLY_TIME,\n" +
                "AUDIT_PERSON,\n" +
                "AUDIT_TIME,\n" +
                "PAY_ID,\n" +
                "USED_LEFT_FLAG,\n" +
                "IS_ABNORMAL,\n" +
                "ERROR_STATUS,\n" +
                "AUDIT_STATUS,\n" +
                "COMPLETE_STATUS,\n" +
                "AUDIT_REMARK,\n" +
                "AUDIT_RESULT,\n" +
                "AUDIT_SECOND_PERSON,\n" +
                "AUDIT_SECOND_TIME,\n" +
                "AUDIT_SECOND_RESULT,\n" +
                "AUDIT_SECOND_REMARK,\n" +
                "CHANGE_TYPE,\n" +
                "CHANGE_EXPLAIN,\n" +
                "CHANGE_NOVOLUNTEER_REASON,\n" +
                "CHANGE_EVIDENCE,\n" +
                "CHANGE_EVIDENCE_IMAGE,\n" +
                "REFUND_LY_VALUE_METHOD,\n" +
                "EFFECTIVE_DATE,\n" +
                "ERROR_SOLUTION)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "ORDER_NO,\n" +
                "TOTAL_REFUND_STATUS,\n" +
                "ORDER_TYPE,\n" +
                "OLD_ORDER_NO,\n" +
                "ORIGINAL_ORDER_NO,\n" +
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
                "CHANGE_FEE_TOTAL,\n" +
                "COMPENSATION_FEE_TOTAL,\n" +
                "LY_SUM,\n" +
                "CASH_SUM,\n" +
                "CHANGE_PERPSON,\n" +
                "TO_TIMESTAMP_LTZ(CHANGE_TIME, 3) CHANGE_TIME,\n" +
                "REFUND_PERPSON,\n" +
                "TO_TIMESTAMP_LTZ(REFUND_TIME, 3) REFUND_TIME,\n" +
                "REFUND_LY_VALUE,\n" +
                "REFUND_LY_STATUS,\n" +
                "CAST(TO_TIMESTAMP_LTZ(LY_EXPIRES_TIME, 3) AS DATE) LY_EXPIRES_TIME,\n" +
                "REFUND_CASH,\n" +
                "REFUND_CASH_STATUS,\n" +
                "AIRPORT_TAX_TOTAL,\n" +
                "FUEL_TAX_TOTAL,\n" +
                "INSURE_FEE_TOTAL,\n" +
                "CHANGE_SUM_FEE,\n" +
                "LAST_CHANGE_FEE,\n" +
                "REFUND_TYPE,\n" +
                "NOVOLUNTEER_REASON,\n" +
                "REFUND_EXPLAIN,\n" +
                "REFUND_EVIDENCE,\n" +
                "REFUND_EVIDENCE_IMAGE,\n" +
                "APPLY_PERSON,\n" +
                "TO_TIMESTAMP_LTZ(APPLY_TIME, 3) APPLY_TIME,\n" +
                "AUDIT_PERSON,\n" +
                "TO_TIMESTAMP_LTZ(AUDIT_TIME, 3) AUDIT_TIME,\n" +
                "PAY_ID,\n" +
                "USED_LEFT_FLAG,\n" +
                "IS_ABNORMAL,\n" +
                "ERROR_STATUS,\n" +
                "AUDIT_STATUS,\n" +
                "COMPLETE_STATUS,\n" +
                "AUDIT_REMARK,\n" +
                "AUDIT_RESULT,\n" +
                "AUDIT_SECOND_PERSON,\n" +
                "TO_TIMESTAMP_LTZ(AUDIT_SECOND_TIME, 3) AUDIT_SECOND_TIME,\n" +
                "AUDIT_SECOND_RESULT,\n" +
                "AUDIT_SECOND_REMARK,\n" +
                "CHANGE_TYPE,\n" +
                "CHANGE_EXPLAIN,\n" +
                "CHANGE_NOVOLUNTEER_REASON,\n" +
                "CHANGE_EVIDENCE,\n" +
                "CHANGE_EVIDENCE_IMAGE,\n" +
                "REFUND_LY_VALUE_METHOD,\n" +
                "CAST(TO_TIMESTAMP_LTZ(EFFECTIVE_DATE, 3) AS DATE) EFFECTIVE_DATE,\n" +
                "ERROR_SOLUTION\n" +
                " FROM ORDER_CHANGE_REFUND_INFO "
//                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
//                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
