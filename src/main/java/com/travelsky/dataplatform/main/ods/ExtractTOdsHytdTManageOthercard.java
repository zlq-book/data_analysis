package com.travelsky.dataplatform.main.ods;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.TOdsHytdSysRegister;
import com.travelsky.dataplatform.module.ods.TOdsHytdTManageOthercard;
import com.travelsky.dataplatform.source.DMDBSourceFunction;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.KerberosAuthUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.checkerframework.checker.units.qual.C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author kuangaihua
 * @date 2025/6/26 16:29
 */
public class ExtractTOdsHytdTManageOthercard {
    private static final Logger logger = LoggerFactory.getLogger(ExtractTOdsHytdTManageOthercard.class);
    public static void main(String[] args) throws Exception {
        System.out.println("ExtractTOdsHytdTManageOthercard data transfer start");
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        System.out.println("ExtractTOdsHytdTManageOthercard etl_date:" + etlDate);
        System.out.println("ExtractTOdsHytdTManageOthercard getExecutionEnvironment start");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsHytdTManageOthercard");

        env.setParallelism(1);
        System.out.println("ExtractTOdsHytdTManageOthercard getExecutionEnvironment end");
        System.out.println("ExtractTOdsHytdTManageOthercard StreamTableEnvironment.create(env) start");
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        System.out.println("ExtractTOdsHytdTManageOthercard StreamTableEnvironment.create(env) end");
        // 注册SM4加密UDF
        System.out.println("ExtractTOdsHytdTManageOthercard register SM4 encrypt start");
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        System.out.println("ExtractTOdsHytdTManageOthercard register SM4 encrypt end");
        System.out.println("ExtractTOdsHytdTManageOthercard executeSql create table for source start");
        StringBuffer urlBuffer = new StringBuffer();
        urlBuffer.append("jdbc:dm://").append(Constants.HYTD_IP).append(":").append(Constants.HYTD_PORT).append("/").append(Constants.HYTD_DB);
        String url = urlBuffer.toString();
        String user = Constants.HYTD_USER;
        String password = Constants.HYTD_PWD;
        
        StringBuffer queryBuffer = new StringBuffer();
        queryBuffer.append("SELECT ");
        queryBuffer.append("ID\n");
        queryBuffer.append(",CRM_CARDNO\n");
        queryBuffer.append(",CARD_NO\n");
        queryBuffer.append(",NAME\n");
        queryBuffer.append(",NAME_EN\n");
        queryBuffer.append(",CERT_TYPE\n");
        queryBuffer.append(",").append(Constants.HYTD_SCHEMA).append(".AES_DECRYPT(CERT_NO,'").append(Constants.HYTD_AES_KEY).append("') AS CERT_NO\n");
        queryBuffer.append(",END_TIME\n FROM ").append(Constants.HYTD_SCHEMA).append(".T_MANAGE_OTHERCARD AS sr");
        String query = queryBuffer.toString();
        
        String columStr = "ID,CRM_CARDNO,CARD_NO,NAME,NAME_EN,CERT_TYPE,CERT_NO,END_TIME";
        String[] columnStrs = columStr.split(",");
        List<String> columns = Arrays.stream(columnStrs).collect(Collectors.toList());
        DMDBSourceFunction function = new DMDBSourceFunction(url, user, password, query, columns);
        DataStreamSource<JSONObject> jsonObjectDataStreamSource = env.addSource(function);
        SingleOutputStreamOperator<TOdsHytdTManageOthercard> tSource = jsonObjectDataStreamSource.map(jsonObject -> {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            TOdsHytdTManageOthercard tOdsHytdTManageOthercard = mapper.readValue(JSONObject.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue), TOdsHytdTManageOthercard.class);
            return tOdsHytdTManageOthercard;
        });
        Schema schema = Schema.newBuilder()
                .column("ID", DataTypes.STRING())
                .column("CRM_CARDNO", DataTypes.STRING())
                .column("CARD_NO", DataTypes.STRING())
                .column("NAME", DataTypes.STRING())
                .column("NAME_EN", DataTypes.STRING())
                .column("CERT_TYPE", DataTypes.STRING())
                .column("CERT_NO", DataTypes.STRING())
                .column("END_TIME", DataTypes.BIGINT())
                .build();
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        // 注册为临时视图
        tEnv.createTemporaryView("T_MANAGE_OTHERCARD", tSource, schema);
        System.out.println("ExtractTOdsHytdTManageOthercard executeSql create table for source end");
        //创建Doris目标表
        System.out.println("ExtractTOdsHytdTManageOthercard executeSql create table for doris start");
        
        StringBuffer targetTableSql = new StringBuffer();
        targetTableSql.append("CREATE TABLE T_ODS_HYTD_T_MANAGE_OTHERCARD (\n");
        targetTableSql.append("  `ETL_DATE` DATE,\n");
        targetTableSql.append("    `ID` STRING COMMENT '唯一标识',\n");
        targetTableSql.append("    `CRM_CARDNO` STRING COMMENT 'CRM系统卡号',\n");
        targetTableSql.append("    `CARD_NO` STRING COMMENT '物理卡号',\n");
        targetTableSql.append("    `NAME` STRING COMMENT '姓名(中文)',\n");
        targetTableSql.append("    `NAME_EN` STRING COMMENT '姓名(英文)',\n");
        targetTableSql.append("    `CERT_TYPE` STRING COMMENT '证件类型',\n");
        targetTableSql.append("    `CERT_NO` STRING COMMENT '证件号码',\n");
        targetTableSql.append("    `END_TIME` TIMESTAMP(6) COMMENT '有效期截止时间'\n");
        targetTableSql.append(") WITH (\n");
        targetTableSql.append(" 'connector' = 'doris',\n");
        targetTableSql.append("'fenodes' = '").append(Constants.DORIS_FE_IP).append(":").append(Constants.DORIS_FE_PORT).append("', -- 替换为 Doris FE 的地址和端口\n");
        targetTableSql.append("'benodes' = '").append(Constants.DORIS_BE_IP).append(":").append(Constants.DORIS_BE_PORT).append("', -- 替换为 Doris FE 的地址和端口\n");
        targetTableSql.append("  'table.identifier' = '").append(Constants.ODS_DB).append(".T_ODS_HYTD_T_MANAGE_OTHERCARD',\n");
        targetTableSql.append("'sink.label-prefix' = '").append(timestamp).append(uuid).append("',");
        targetTableSql.append(" 'sink.properties.read_json_by_line' = 'true',");
        targetTableSql.append(" 'sink.properties.format' = 'json',");
        targetTableSql.append("    'username' = '").append(Constants.ODS_USER).append("',\n");
        targetTableSql.append("    'password' = '").append(Constants.ODS_PWD).append("'\n");
        targetTableSql.append(")");
        tEnv.executeSql(targetTableSql.toString());
        
        System.out.println("ExtractTOdsHytdTManageOthercard executeSql create table for doris end");
        
        StringBuffer extractSqlBuffer = new StringBuffer();
        extractSqlBuffer.append("INSERT INTO T_ODS_HYTD_T_MANAGE_OTHERCARD(");
        extractSqlBuffer.append("ETL_DATE  ");
        extractSqlBuffer.append(",ID\n");
        extractSqlBuffer.append(",CRM_CARDNO\n");
        extractSqlBuffer.append(",CARD_NO\n");
        extractSqlBuffer.append(",NAME\n");
        extractSqlBuffer.append(",NAME_EN\n");
        extractSqlBuffer.append(",CERT_TYPE\n");
        extractSqlBuffer.append(",CERT_NO\n");
        extractSqlBuffer.append(",END_TIME\n)  ");
        extractSqlBuffer.append("SELECT ");
        extractSqlBuffer.append("CAST('").append(etlDate).append("' AS DATE) ETL_DATE");
        extractSqlBuffer.append(",ID\n");
        extractSqlBuffer.append(",sm4_encrypt (CRM_CARDNO,'").append(Constants.SM4_KEY).append("') CRM_CARDNO\n");
        extractSqlBuffer.append(",CARD_NO\n");
        extractSqlBuffer.append(",NAME\n");
        extractSqlBuffer.append(",NAME_EN\n");
        extractSqlBuffer.append(",CERT_TYPE\n");
        extractSqlBuffer.append(",sm4_encrypt (CERT_NO,'").append(Constants.SM4_KEY).append("') CERT_NO\n");
        extractSqlBuffer.append(",TO_TIMESTAMP_LTZ(END_TIME, 3) END_TIME\n");
        extractSqlBuffer.append(" FROM T_MANAGE_OTHERCARD ");
        String extractSql = extractSqlBuffer.toString();
        System.out.println(extractSql);
        System.out.println("ExtractTOdsHytdTManageOthercard executeSql extract start");
        TableResult result = tEnv.executeSql(extractSql);
        System.out.println("ExtractTOdsHytdTManageOthercard executeSql extract end");
        result.print();
        System.out.println("ExtractTOdsHytdTManageOthercard data transfer end");


    }

}
