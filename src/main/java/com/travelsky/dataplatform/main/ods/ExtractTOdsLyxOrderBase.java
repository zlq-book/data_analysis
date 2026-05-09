package com.travelsky.dataplatform.main.ods;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.TOdsLyxOrderBase;
import com.travelsky.dataplatform.source.OracleDBSourceFunction;
import com.travelsky.dataplatform.udf.OracleAESDecryptor;
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
public class ExtractTOdsLyxOrderBase {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
//        String etlDate = "2019-08-01";
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env);
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
        String query = "SELECT " +
                "ID,\n" +
                "ORDER_NO,\n" +
                "ORDER_STATUS,\n" +
                "ORDER_TYPE,\n" +
                "PRODUCT_NAME,\n" +
                "ORDER_TIME,\n" +
                "ORDER_ORIGIN,\n" +
                "CURRENT_DEALER,\n" +
                "DEAL_TIME,\n" +
                "PSG_NAME,\n" +
                "VIP_IF,\n" +
                "VIP_LEVEL,\n" +
                "VIP_TYPE,\n" +
                "CERTIFY_TYPE,\n" +
                Constants.LYX_SCHEMA + ".decrypt_func(CERTIFY_NUM) CERTIFY_NUM,\n" +
                Constants.LYX_SCHEMA + ".decrypt_func(PHONE_NUM) PHONE_NUM,\n" +
                "TICKET_NUM,\n" +
                "TICKET_STATUS,\n" +
                "ISSUE_OFFICE,\n" +
                "BOOK_OFFICE,\n" +
                "FLT_NUM,\n" +
                "FLT_DATE,\n" +
                "ORIG,\n" +
                "DEST,\n" +
                "LAUNCH_TIME,\n" +
                "ARRIVE_TIME,\n" +
                "FLT_STATUS,\n" +
                "TICKET_PRICE,\n" +
                "CABIN_CLASS,\n" +
                "DISCUSS_STATUS,\n" +
                "DISCUSS_CONTENT,\n" +
                "DISCUSS_LEVEL,\n" +
                "DISCUSS_TIME,\n" +
                "CREATOR,\n" +
                "CREATE_TIME,\n" +
                "UPDATOR,\n" +
                "UPDATE_TIME,\n" +
                "TICKET_FLAG,\n" +
                "ORDER_OLD_NO,\n" +
                "PRODUCT_CODE,\n" +
                "PRODUCT_ID,\n" +
                "ORIG_NAME,\n" +
                "DEST_NAME,\n" +
                "PSG_ID,\n" +
                "ORIG_TICKET_NUM,\n" +
                "ORIG_TICKET_PRICE,\n" +
                "ORIG_FLT_NUM,\n" +
                "ORIG_FLT_DATE,\n" +
                "ORIG_ORIG,\n" +
                "ORIG_DEST,\n" +
                "ORIG_LAUNCH_TIME,\n" +
                "ORIG_ARRIVE_TIME,\n" +
                "ORIG_CABIN_CLASS,\n" +
                "GROUP_ID,\n" +
                "PSG_CARDNO,\n" +
                "OUT_CLASS,\n" +
                "OUT_PRICE,\n" +
                "ADDPRICE_INCOME,\n" +
                "LINK_ID,\n" +
                "LINK_SOURCE,\n" +
                "PNR_ICS,\n" +
                "PNR_CRS,\n" +
                "LINK_FLAG,\n" +
                "IS_REVALIDATION,\n" +
                "PAY_OPERATE_REASON,\n" +
                "PAY_OPERATE_TYPE,\n" +
                "REMIND_FLAG,\n" +
                "IATA_NO,\n" +
                "AGENT_NAME,\n" +
                "SERVICE_COST,\n" +
                "PAY_STATUS,\n" +
                "SALES_DEPT,\n" +
                "B2B_OFFICE,\n" +
                "BI_DEPTNAME,\n" +
                "BI_SERVICE_COST,\n" +
                "BI_PROMOTION_COST,\n" +
                "SERVICE_LEVEL,\n" +
                "ISSUE_TIME,\n" +
                "BI_MARKETING_INCENTIVES_PRICE\n" +
                " FROM " + Constants.LYX_SCHEMA + ".ORDER_BASE "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        String columStr = "ID,ORDER_NO,ORDER_STATUS,ORDER_TYPE,PRODUCT_NAME,ORDER_TIME,ORDER_ORIGIN,CURRENT_DEALER,DEAL_TIME,PSG_NAME,VIP_IF,VIP_LEVEL,VIP_TYPE,CERTIFY_TYPE,CERTIFY_NUM,PHONE_NUM,TICKET_NUM,TICKET_STATUS,ISSUE_OFFICE,BOOK_OFFICE,FLT_NUM,FLT_DATE,ORIG,DEST,LAUNCH_TIME,ARRIVE_TIME,FLT_STATUS,TICKET_PRICE,CABIN_CLASS,DISCUSS_STATUS,DISCUSS_CONTENT,DISCUSS_LEVEL,DISCUSS_TIME,CREATOR,CREATE_TIME,UPDATOR,UPDATE_TIME,TICKET_FLAG,ORDER_OLD_NO,PRODUCT_CODE,PRODUCT_ID,ORIG_NAME,DEST_NAME,PSG_ID,ORIG_TICKET_NUM,ORIG_TICKET_PRICE,ORIG_FLT_NUM,ORIG_FLT_DATE,ORIG_ORIG,ORIG_DEST,ORIG_LAUNCH_TIME,ORIG_ARRIVE_TIME,ORIG_CABIN_CLASS,GROUP_ID,PSG_CARDNO,OUT_CLASS,OUT_PRICE,ADDPRICE_INCOME,LINK_ID,LINK_SOURCE,PNR_ICS,PNR_CRS,LINK_FLAG,IS_REVALIDATION,PAY_OPERATE_REASON,PAY_OPERATE_TYPE,REMIND_FLAG,IATA_NO,AGENT_NAME,SERVICE_COST,PAY_STATUS,SALES_DEPT,B2B_OFFICE,BI_DEPTNAME,BI_SERVICE_COST,BI_PROMOTION_COST,SERVICE_LEVEL,ISSUE_TIME,BI_MARKETING_INCENTIVES_PRICE";
        String[] columnStrs = columStr.split(",");
        List<String> columns = Arrays.stream(columnStrs).collect(Collectors.toList());
        OracleDBSourceFunction function = new OracleDBSourceFunction(url, user, password, query, columns);
        DataStreamSource<JSONObject> jsonObjectDataStreamSource = env.addSource(function);

        SingleOutputStreamOperator<TOdsLyxOrderBase> tSource = jsonObjectDataStreamSource.map(jsonObject -> {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            TOdsLyxOrderBase tOdsLyxOrderBase = mapper.readValue(JSONObject.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue), TOdsLyxOrderBase.class);
            return tOdsLyxOrderBase;
        });
        tSource.print();
        Schema schema = Schema.newBuilder()
                .column("ID", DataTypes.BIGINT())
                .column("ORDER_NO", DataTypes.VARCHAR(100))
                .column("ORDER_STATUS", DataTypes.VARCHAR(4))
                .column("ORDER_TYPE", DataTypes.VARCHAR(10))
                .column("PRODUCT_NAME", DataTypes.VARCHAR(50))
                .column("ORDER_TIME",DataTypes.BIGINT())
                .column("ORDER_ORIGIN", DataTypes.VARCHAR(20))
                .column("CURRENT_DEALER", DataTypes.VARCHAR(20))
                .column("DEAL_TIME",DataTypes.BIGINT())
                .column("PSG_NAME", DataTypes.VARCHAR(50))
                .column("VIP_IF", DataTypes.VARCHAR(2))
                .column("VIP_LEVEL", DataTypes.VARCHAR(4))
                .column("VIP_TYPE", DataTypes.VARCHAR(20))
                .column("CERTIFY_TYPE", DataTypes.VARCHAR(6))
                .column("CERTIFY_NUM", DataTypes.VARCHAR(100))
                .column("PHONE_NUM", DataTypes.VARCHAR(100))
                .column("TICKET_NUM", DataTypes.VARCHAR(20))
                .column("TICKET_STATUS", DataTypes.VARCHAR(20))
                .column("ISSUE_OFFICE", DataTypes.VARCHAR(50))
                .column("BOOK_OFFICE", DataTypes.VARCHAR(50))
                .column("FLT_NUM", DataTypes.VARCHAR(8))
                .column("FLT_DATE", DataTypes.BIGINT())
                .column("ORIG", DataTypes.VARCHAR(6))
                .column("DEST", DataTypes.VARCHAR(6))
                .column("LAUNCH_TIME",DataTypes.BIGINT())
                .column("ARRIVE_TIME", DataTypes.BIGINT())
                .column("FLT_STATUS", DataTypes.VARCHAR(4))
                .column("TICKET_PRICE", DataTypes.VARCHAR(10))
                .column("CABIN_CLASS", DataTypes.VARCHAR(2))
                .column("DISCUSS_STATUS", DataTypes.VARCHAR(4))
                .column("DISCUSS_CONTENT", DataTypes.VARCHAR(500))
                .column("DISCUSS_LEVEL", DataTypes.VARCHAR(2))
                .column("DISCUSS_TIME", DataTypes.BIGINT())
                .column("CREATOR", DataTypes.VARCHAR(20))
                .column("CREATE_TIME", DataTypes.BIGINT())
                .column("UPDATOR", DataTypes.VARCHAR(20))
                .column("UPDATE_TIME", DataTypes.BIGINT())
                .column("TICKET_FLAG", DataTypes.VARCHAR(20))
                .column("ORDER_OLD_NO", DataTypes.VARCHAR(100))
                .column("PRODUCT_CODE", DataTypes.VARCHAR(50))
                .column("PRODUCT_ID", DataTypes.BIGINT())
                .column("ORIG_NAME", DataTypes.VARCHAR(20))
                .column("DEST_NAME", DataTypes.VARCHAR(20))
                .column("PSG_ID", DataTypes.BIGINT())
                .column("ORIG_TICKET_NUM", DataTypes.VARCHAR(20))
                .column("ORIG_TICKET_PRICE", DataTypes.VARCHAR(10))
                .column("ORIG_FLT_NUM", DataTypes.VARCHAR(8))
                .column("ORIG_FLT_DATE", DataTypes.BIGINT())
                .column("ORIG_ORIG", DataTypes.VARCHAR(6))
                .column("ORIG_DEST", DataTypes.VARCHAR(6))
                .column("ORIG_LAUNCH_TIME", DataTypes.BIGINT())
                .column("ORIG_ARRIVE_TIME",DataTypes.BIGINT())
                .column("ORIG_CABIN_CLASS", DataTypes.VARCHAR(2))
                .column("GROUP_ID", DataTypes.BIGINT())
                .column("PSG_CARDNO", DataTypes.VARCHAR(20))
                .column("OUT_CLASS", DataTypes.VARCHAR(4))
                .column("OUT_PRICE", DataTypes.VARCHAR(20))
                .column("ADDPRICE_INCOME", DataTypes.VARCHAR(20))
                .column("LINK_ID", DataTypes.VARCHAR(20))
                .column("LINK_SOURCE", DataTypes.VARCHAR(20))
                .column("PNR_ICS", DataTypes.VARCHAR(20))
                .column("PNR_CRS", DataTypes.VARCHAR(20))
                .column("LINK_FLAG", DataTypes.VARCHAR(4))
                .column("IS_REVALIDATION", DataTypes.VARCHAR(3))
                .column("PAY_OPERATE_REASON", DataTypes.VARCHAR(100))
                .column("PAY_OPERATE_TYPE", DataTypes.VARCHAR(100))
                .column("REMIND_FLAG", DataTypes.VARCHAR(5))
                .column("IATA_NO", DataTypes.VARCHAR(10))
                .column("AGENT_NAME", DataTypes.VARCHAR(50))
                .column("SERVICE_COST", DataTypes.VARCHAR(10))
                .column("PAY_STATUS", DataTypes.VARCHAR(4))
                .column("SALES_DEPT", DataTypes.VARCHAR(100))
                .column("B2B_OFFICE", DataTypes.VARCHAR(100))
                .column("BI_DEPTNAME", DataTypes.VARCHAR(200))
                .column("BI_SERVICE_COST", DataTypes.VARCHAR(10))
                .column("BI_PROMOTION_COST", DataTypes.VARCHAR(10))
                .column("SERVICE_LEVEL", DataTypes.VARCHAR(10))
                .column("ISSUE_TIME", DataTypes.BIGINT())
                .column("BI_MARKETING_INCENTIVES_PRICE", DataTypes.VARCHAR(500))
                .build();
        // 注册为临时视图
        tEnv.createTemporaryView("ORDER_BASE", tSource, schema);

        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_LYX_ORDER_BASE (\n" +
                "    ETL_DATE DATE,\n" +
                "ID BIGINT ,\n" +
                "ORDER_NO VARCHAR(300) ,\n" +
                "ORDER_STATUS VARCHAR(12) ,\n" +
                "ORDER_TYPE VARCHAR(30) ,\n" +
                "PRODUCT_NAME VARCHAR(150) ,\n" +
                "ORDER_TIME TIMESTAMP(6) ,\n" +
                "ORDER_ORIGIN VARCHAR(60) ,\n" +
                "CURRENT_DEALER VARCHAR(60) ,\n" +
                "DEAL_TIME TIMESTAMP(6) ,\n" +
                "PSG_NAME VARCHAR(150) ,\n" +
                "VIP_IF VARCHAR(6) ,\n" +
                "VIP_LEVEL VARCHAR(12) ,\n" +
                "VIP_TYPE VARCHAR(60) ,\n" +
                "CERTIFY_TYPE VARCHAR(18) ,\n" +
                "CERTIFY_NUM VARCHAR(300) ,\n" +
                "PHONE_NUM VARCHAR(300) ,\n" +
                "TICKET_NUM VARCHAR(60) ,\n" +
                "TICKET_STATUS VARCHAR(60) ,\n" +
                "ISSUE_OFFICE VARCHAR(150) ,\n" +
                "BOOK_OFFICE VARCHAR(150) ,\n" +
                "FLT_NUM VARCHAR(24) ,\n" +
                "FLT_DATE DATE ,\n" +
                "ORIG VARCHAR(18) ,\n" +
                "DEST VARCHAR(18) ,\n" +
                "LAUNCH_TIME TIMESTAMP(6) ,\n" +
                "ARRIVE_TIME TIMESTAMP(6) ,\n" +
                "FLT_STATUS VARCHAR(12) ,\n" +
                "TICKET_PRICE VARCHAR(30) ,\n" +
                "CABIN_CLASS VARCHAR(6) ,\n" +
                "DISCUSS_STATUS VARCHAR(12) ,\n" +
                "DISCUSS_CONTENT VARCHAR(1500) ,\n" +
                "DISCUSS_LEVEL VARCHAR(6) ,\n" +
                "DISCUSS_TIME TIMESTAMP(6) ,\n" +
                "CREATOR VARCHAR(60) ,\n" +
                "CREATE_TIME TIMESTAMP(6) ,\n" +
                "UPDATOR VARCHAR(60) ,\n" +
                "UPDATE_TIME TIMESTAMP(6) ,\n" +
                "TICKET_FLAG VARCHAR(60) ,\n" +
                "ORDER_OLD_NO VARCHAR(300) ,\n" +
                "PRODUCT_CODE VARCHAR(150) ,\n" +
                "PRODUCT_ID BIGINT ,\n" +
                "ORIG_NAME VARCHAR(60) ,\n" +
                "DEST_NAME VARCHAR(60) ,\n" +
                "PSG_ID BIGINT ,\n" +
                "ORIG_TICKET_NUM VARCHAR(60) ,\n" +
                "ORIG_TICKET_PRICE VARCHAR(30) ,\n" +
                "ORIG_FLT_NUM VARCHAR(24) ,\n" +
                "ORIG_FLT_DATE DATE ,\n" +
                "ORIG_ORIG VARCHAR(18) ,\n" +
                "ORIG_DEST VARCHAR(18) ,\n" +
                "ORIG_LAUNCH_TIME TIMESTAMP(6) ,\n" +
                "ORIG_ARRIVE_TIME TIMESTAMP(6) ,\n" +
                "ORIG_CABIN_CLASS VARCHAR(6) ,\n" +
                "GROUP_ID BIGINT ,\n" +
                "PSG_CARDNO VARCHAR(60) ,\n" +
                "OUT_CLASS VARCHAR(12) ,\n" +
                "OUT_PRICE VARCHAR(60) ,\n" +
                "ADDPRICE_INCOME VARCHAR(60) ,\n" +
                "LINK_ID VARCHAR(60) ,\n" +
                "LINK_SOURCE VARCHAR(60) ,\n" +
                "PNR_ICS VARCHAR(60) ,\n" +
                "PNR_CRS VARCHAR(60) ,\n" +
                "LINK_FLAG VARCHAR(12) ,\n" +
                "IS_REVALIDATION VARCHAR(9) ,\n" +
                "PAY_OPERATE_REASON VARCHAR(300) ,\n" +
                "PAY_OPERATE_TYPE VARCHAR(300) ,\n" +
                "REMIND_FLAG VARCHAR(15) ,\n" +
                "IATA_NO VARCHAR(30) ,\n" +
                "AGENT_NAME VARCHAR(150) ,\n" +
                "SERVICE_COST VARCHAR(30) ,\n" +
                "PAY_STATUS VARCHAR(12) ,\n" +
                "SALES_DEPT VARCHAR(300) ,\n" +
                "B2B_OFFICE VARCHAR(300) ,\n" +
                "BI_DEPTNAME VARCHAR(600) ,\n" +
                "BI_SERVICE_COST VARCHAR(30) ,\n" +
                "BI_PROMOTION_COST VARCHAR(30) ,\n" +
                "SERVICE_LEVEL VARCHAR(30) ,\n" +
                "ISSUE_TIME TIMESTAMP(6) ,\n" +
                "BI_MARKETING_INCENTIVES_PRICE VARCHAR(1500)\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYX_ORDER_BASE',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_LYX_ORDER_BASE(" +
                "ETL_DATE,\n" +
                "ID,\n" +
                "ORDER_NO,\n" +
                "ORDER_STATUS,\n" +
                "ORDER_TYPE,\n" +
                "PRODUCT_NAME,\n" +
                "ORDER_TIME,\n" +
                "ORDER_ORIGIN,\n" +
                "CURRENT_DEALER,\n" +
                "DEAL_TIME,\n" +
                "PSG_NAME,\n" +
                "VIP_IF,\n" +
                "VIP_LEVEL,\n" +
                "VIP_TYPE,\n" +
                "CERTIFY_TYPE,\n" +
                "CERTIFY_NUM,\n" +
                "PHONE_NUM,\n" +
                "TICKET_NUM,\n" +
                "TICKET_STATUS,\n" +
                "ISSUE_OFFICE,\n" +
                "BOOK_OFFICE,\n" +
                "FLT_NUM,\n" +
                "FLT_DATE,\n" +
                "ORIG,\n" +
                "DEST,\n" +
                "LAUNCH_TIME,\n" +
                "ARRIVE_TIME,\n" +
                "FLT_STATUS,\n" +
                "TICKET_PRICE,\n" +
                "CABIN_CLASS,\n" +
                "DISCUSS_STATUS,\n" +
                "DISCUSS_CONTENT,\n" +
                "DISCUSS_LEVEL,\n" +
                "DISCUSS_TIME,\n" +
                "CREATOR,\n" +
                "CREATE_TIME,\n" +
                "UPDATOR,\n" +
                "UPDATE_TIME,\n" +
                "TICKET_FLAG,\n" +
                "ORDER_OLD_NO,\n" +
                "PRODUCT_CODE,\n" +
                "PRODUCT_ID,\n" +
                "ORIG_NAME,\n" +
                "DEST_NAME,\n" +
                "PSG_ID,\n" +
                "ORIG_TICKET_NUM,\n" +
                "ORIG_TICKET_PRICE,\n" +
                "ORIG_FLT_NUM,\n" +
                "ORIG_FLT_DATE,\n" +
                "ORIG_ORIG,\n" +
                "ORIG_DEST,\n" +
                "ORIG_LAUNCH_TIME,\n" +
                "ORIG_ARRIVE_TIME,\n" +
                "ORIG_CABIN_CLASS,\n" +
                "GROUP_ID,\n" +
                "PSG_CARDNO,\n" +
                "OUT_CLASS,\n" +
                "OUT_PRICE,\n" +
                "ADDPRICE_INCOME,\n" +
                "LINK_ID,\n" +
                "LINK_SOURCE,\n" +
                "PNR_ICS,\n" +
                "PNR_CRS,\n" +
                "LINK_FLAG,\n" +
                "IS_REVALIDATION,\n" +
                "PAY_OPERATE_REASON,\n" +
                "PAY_OPERATE_TYPE,\n" +
                "REMIND_FLAG,\n" +
                "IATA_NO,\n" +
                "AGENT_NAME,\n" +
                "SERVICE_COST,\n" +
                "PAY_STATUS,\n" +
                "SALES_DEPT,\n" +
                "B2B_OFFICE,\n" +
                "BI_DEPTNAME,\n" +
                "BI_SERVICE_COST,\n" +
                "BI_PROMOTION_COST,\n" +
                "SERVICE_LEVEL,\n" +
                "ISSUE_TIME,\n" +
                "BI_MARKETING_INCENTIVES_PRICE)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "ID,\n" +
                "ORDER_NO,\n" +
                "ORDER_STATUS,\n" +
                "ORDER_TYPE,\n" +
                "PRODUCT_NAME,\n" +
                "TO_TIMESTAMP_LTZ(ORDER_TIME, 3) ORDER_TIME,\n" +
                "ORDER_ORIGIN,\n" +
                "CURRENT_DEALER,\n" +
                "TO_TIMESTAMP_LTZ(DEAL_TIME, 3) DEAL_TIME,\n" +
                "PSG_NAME,\n" +
                "VIP_IF,\n" +
                "VIP_LEVEL,\n" +
                "VIP_TYPE,\n" +
                "CERTIFY_TYPE,\n" +
                "  sm4_encrypt(CERTIFY_NUM, '" + sm4key + "') CERTIFY_NUM,\n" +
                "  sm4_encrypt(PHONE_NUM, '" + sm4key + "') PHONE_NUM,\n" +
                "TICKET_NUM,\n" +
                "TICKET_STATUS,\n" +
                "ISSUE_OFFICE,\n" +
                "BOOK_OFFICE,\n" +
                "FLT_NUM,\n" +
                "CAST(TO_TIMESTAMP_LTZ(FLT_DATE, 3) AS DATE) FLT_DATE,\n" +
                "ORIG,\n" +
                "DEST,\n" +
                "TO_TIMESTAMP_LTZ(LAUNCH_TIME, 3) LAUNCH_TIME,\n" +
                "TO_TIMESTAMP_LTZ(ARRIVE_TIME, 3) ARRIVE_TIME,\n" +
                "FLT_STATUS,\n" +
                "TICKET_PRICE,\n" +
                "CABIN_CLASS,\n" +
                "DISCUSS_STATUS,\n" +
                "DISCUSS_CONTENT,\n" +
                "DISCUSS_LEVEL,\n" +
                "TO_TIMESTAMP_LTZ(DISCUSS_TIME, 3) DISCUSS_TIME,\n" +
                "CREATOR,\n" +
                "TO_TIMESTAMP_LTZ(CREATE_TIME, 3) CREATE_TIME,\n" +
                "UPDATOR,\n" +
                "TO_TIMESTAMP_LTZ(UPDATE_TIME, 3) UPDATE_TIME,\n" +
                "TICKET_FLAG,\n" +
                "ORDER_OLD_NO,\n" +
                "PRODUCT_CODE,\n" +
                "PRODUCT_ID,\n" +
                "ORIG_NAME,\n" +
                "DEST_NAME,\n" +
                "PSG_ID,\n" +
                "ORIG_TICKET_NUM,\n" +
                "ORIG_TICKET_PRICE,\n" +
                "ORIG_FLT_NUM,\n" +
                "CAST(TO_TIMESTAMP_LTZ(ORIG_FLT_DATE, 3) AS DATE) ORIG_FLT_DATE,\n" +
                "ORIG_ORIG,\n" +
                "ORIG_DEST,\n" +
                "TO_TIMESTAMP_LTZ(ORIG_LAUNCH_TIME, 3) ORIG_LAUNCH_TIME,\n" +
                "TO_TIMESTAMP_LTZ(ORIG_ARRIVE_TIME, 3) ORIG_ARRIVE_TIME,\n" +
                "ORIG_CABIN_CLASS,\n" +
                "GROUP_ID,\n" +
                "PSG_CARDNO,\n" +
                "OUT_CLASS,\n" +
                "OUT_PRICE,\n" +
                "ADDPRICE_INCOME,\n" +
                "LINK_ID,\n" +
                "LINK_SOURCE,\n" +
                "PNR_ICS,\n" +
                "PNR_CRS,\n" +
                "LINK_FLAG,\n" +
                "IS_REVALIDATION,\n" +
                "PAY_OPERATE_REASON,\n" +
                "PAY_OPERATE_TYPE,\n" +
                "REMIND_FLAG,\n" +
                "IATA_NO,\n" +
                "AGENT_NAME,\n" +
                "SERVICE_COST,\n" +
                "PAY_STATUS,\n" +
                "SALES_DEPT,\n" +
                "B2B_OFFICE,\n" +
                "BI_DEPTNAME,\n" +
                "BI_SERVICE_COST,\n" +
                "BI_PROMOTION_COST,\n" +
                "SERVICE_LEVEL,\n" +
                "TO_TIMESTAMP_LTZ(ISSUE_TIME, 3) ISSUE_TIME,\n" +
                "BI_MARKETING_INCENTIVES_PRICE\n" +
                " FROM ORDER_BASE "
//                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
//                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;

        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();
//        env.execute();

    }
}
