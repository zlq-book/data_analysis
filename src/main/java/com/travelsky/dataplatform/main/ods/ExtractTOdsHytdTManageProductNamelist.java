package com.travelsky.dataplatform.main.ods;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.TOdsHytdTManageOthercard;
import com.travelsky.dataplatform.module.ods.TOdsHytdTManageProductNamelist;
import com.travelsky.dataplatform.source.DMDBSourceFunction;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.KerberosAuthUtils;
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
 * @author kuangaihua
 * @date 2025/6/26 16:29
 */
public class ExtractTOdsHytdTManageProductNamelist {
    public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsHytdTManageProductNamelist");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        StringBuffer urlBuffer = new StringBuffer();
        urlBuffer.append("jdbc:dm://").append(Constants.HYTD_IP).append(":").append(Constants.HYTD_PORT).append("/").append(Constants.HYTD_DB);
        String url = urlBuffer.toString();
        String user = Constants.HYTD_USER;
        String password = Constants.HYTD_PWD;
        
        StringBuffer queryBuffer = new StringBuffer();
        queryBuffer.append("SELECT ");
        queryBuffer.append("ID\n");
        queryBuffer.append(",PRODUCT_ID\n");
        queryBuffer.append(",MEMBER_ID\n");
        queryBuffer.append(",CREATED_TIME\n");
        queryBuffer.append(",MODIFIED_TIME\n");
        queryBuffer.append(",MEMBER_CARD\n");
        queryBuffer.append(",").append(Constants.HYTD_SCHEMA).append(".AES_DECRYPT(CRED_CODE,'").append(Constants.HYTD_AES_KEY).append("') AS CRED_CODE\n");
        queryBuffer.append(",").append(Constants.HYTD_SCHEMA).append(".AES_DECRYPT(MOBILE_NUMBER,'").append(Constants.HYTD_AES_KEY).append("') AS MOBILE_NUMBER\n");
        queryBuffer.append(",ATTRIBUTE\n");
        queryBuffer.append(",LEVEL_NAME\n");
        queryBuffer.append(",CN_LAST_NAME\n");
        queryBuffer.append(",CN_FIRST_NAME\n");
        queryBuffer.append(",LAST_NAME\n");
        queryBuffer.append(",FIRST_NAME\n");
        queryBuffer.append(",DATEOF_BIRTH\n");
        queryBuffer.append(",PROMO_CODE\n");
        queryBuffer.append(",CHANNEL_ID\n");
        queryBuffer.append(",CHANNEL_NAME\n FROM ").append(Constants.HYTD_SCHEMA).append(".T_MANAGE_PRODUCT_NAMELIST \n");
        queryBuffer.append("WHERE CREATED_TIME BETWEEN '").append(etlDate).append(" 00:00:00.000' AND '").append(etlDate).append(" 23:59:59.999' \n");
        queryBuffer.append("OR MODIFIED_TIME BETWEEN '").append(etlDate).append(" 00:00:00.000' AND '").append(etlDate).append(" 23:59:59.999'");
        String query = queryBuffer.toString();
        
        String columStr="ID,PRODUCT_ID,MEMBER_ID,CREATED_TIME,MODIFIED_TIME,MEMBER_CARD,CRED_CODE,MOBILE_NUMBER,ATTRIBUTE,LEVEL_NAME,CN_LAST_NAME,CN_FIRST_NAME,LAST_NAME,FIRST_NAME,DATEOF_BIRTH,PROMO_CODE,CHANNEL_ID,CHANNEL_NAME";
        String[] columnStrs = columStr.split(",");
        List<String> columns = Arrays.stream(columnStrs).collect(Collectors.toList());
        DMDBSourceFunction function = new DMDBSourceFunction(url, user, password, query,columns);
        DataStreamSource<JSONObject> jsonObjectDataStreamSource = env.addSource(function);
        jsonObjectDataStreamSource.print();
        SingleOutputStreamOperator<TOdsHytdTManageProductNamelist> tSource = jsonObjectDataStreamSource.map(jsonObject -> {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            TOdsHytdTManageProductNamelist tOdsHytdTManageProductNamelist = mapper.readValue(JSONObject.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue), TOdsHytdTManageProductNamelist.class);
            return tOdsHytdTManageProductNamelist;
        });
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        Schema schema = Schema.newBuilder()
                .column("ID", DataTypes.VARCHAR(150))
                .column("PRODUCT_ID", DataTypes.VARCHAR(150))
                .column("MEMBER_ID", DataTypes.VARCHAR(150))
                .column("CREATED_TIME", DataTypes.BIGINT())
                .column("MODIFIED_TIME", DataTypes.BIGINT())
                .column("MEMBER_CARD", DataTypes.VARCHAR(768))
                .column("CRED_CODE", DataTypes.VARCHAR(768))
                .column("MOBILE_NUMBER", DataTypes.VARCHAR(768))
                .column("ATTRIBUTE", DataTypes.VARCHAR(300))
                .column("LEVEL_NAME", DataTypes.VARCHAR(300))
                .column("CN_LAST_NAME", DataTypes.VARCHAR(600))
                .column("CN_FIRST_NAME", DataTypes.VARCHAR(600))
                .column("LAST_NAME", DataTypes.VARCHAR(600))
                .column("FIRST_NAME", DataTypes.VARCHAR(600))
                .column("DATEOF_BIRTH", DataTypes.BIGINT())
                .column("PROMO_CODE", DataTypes.VARCHAR(300))
                .column("CHANNEL_ID", DataTypes.VARCHAR(300))
                .column("CHANNEL_NAME", DataTypes.VARCHAR(300))
                .build();
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        // 注册为临时视图
        tEnv.createTemporaryView("T_MANAGE_PRODUCT_NAMELIST", tSource, schema);
        //创建Doris目标表
        
        StringBuffer targetTableSql = new StringBuffer();
        targetTableSql.append("CREATE TABLE T_ODS_HYTD_T_MANAGE_PRODUCT_NAMELIST (\n");
        targetTableSql.append("  `ETL_DATE` DATE,\n");
        targetTableSql.append("    ID VARCHAR(150) NOT NULL COMMENT '主键',\n");
        targetTableSql.append("    PRODUCT_ID VARCHAR(150) COMMENT '产品ID (产品名称，中英文均可能，对内)',\n");
        targetTableSql.append("    MEMBER_ID VARCHAR(150) COMMENT '会员ID (国航的数据，非卡号)',\n");
        targetTableSql.append("    CREATED_TIME TIMESTAMP(6) COMMENT '报名时间',\n");
        targetTableSql.append("    MODIFIED_TIME TIMESTAMP(6) COMMENT '修改时间（同报名时间）',\n");
        targetTableSql.append("    MEMBER_CARD VARCHAR(768) COMMENT '会员卡号',\n");
        targetTableSql.append("    CRED_CODE VARCHAR(768) COMMENT '证件号',\n");
        targetTableSql.append("    MOBILE_NUMBER VARCHAR(768) COMMENT '手机号',\n");
        targetTableSql.append("    ATTRIBUTE VARCHAR(300) COMMENT '属性（不关心）',\n");
        targetTableSql.append("    LEVEL_NAME VARCHAR(300) COMMENT '会员级别',\n");
        targetTableSql.append("    CN_LAST_NAME VARCHAR(600) COMMENT '中文姓',\n");
        targetTableSql.append("    CN_FIRST_NAME VARCHAR(600) COMMENT '中文名',\n");
        targetTableSql.append("    LAST_NAME VARCHAR(600) COMMENT '英文姓',\n");
        targetTableSql.append("    FIRST_NAME VARCHAR(600) COMMENT '英文名',\n");
        targetTableSql.append("    DATEOF_BIRTH TIMESTAMP(6) COMMENT '生日',\n");
        targetTableSql.append("    PROMO_CODE VARCHAR(300) COMMENT '促销代码（只有部分产品有，产品里程上传相关）',\n");
        targetTableSql.append("    CHANNEL_ID VARCHAR(300) COMMENT '渠道ID（从哪里报名的，APP、网站、小程序等）--需要渠道号及枚举值，以及与会员发展渠道的关系',\n");
        targetTableSql.append("    CHANNEL_NAME VARCHAR(300) COMMENT '渠道名称'");
        targetTableSql.append(") WITH (\n");
        targetTableSql.append(" 'connector' = 'doris',\n");
        targetTableSql.append("'fenodes' = '").append(Constants.DORIS_FE_IP).append(":").append(Constants.DORIS_FE_PORT).append("', -- 替换为 Doris FE 的地址和端口\n");
        targetTableSql.append("'benodes' = '").append(Constants.DORIS_BE_IP).append(":").append(Constants.DORIS_BE_PORT).append("', -- 替换为 Doris FE 的地址和端口\n");
        targetTableSql.append(" 'table.identifier' = '").append(Constants.ODS_DB).append(".T_ODS_HYTD_T_MANAGE_PRODUCT_NAMELIST',\n");
        targetTableSql.append("'sink.label-prefix' = '").append(timestamp).append(uuid).append("',");
        targetTableSql.append(" 'sink.properties.read_json_by_line' = 'true',");
        targetTableSql.append(" 'sink.properties.format' = 'json',");
        targetTableSql.append("    'username' = '").append(Constants.ODS_USER).append("',\n");
        targetTableSql.append("    'password' = '").append(Constants.ODS_PWD).append("'\n");
        targetTableSql.append(")");
        tEnv.executeSql(targetTableSql.toString());
        
        StringBuffer extractSqlBuffer = new StringBuffer();
        extractSqlBuffer.append("INSERT INTO T_ODS_HYTD_T_MANAGE_PRODUCT_NAMELIST(");
        extractSqlBuffer.append("ETL_DATE  ");
        extractSqlBuffer.append(",ID\n");
        extractSqlBuffer.append(",PRODUCT_ID\n");
        extractSqlBuffer.append(",MEMBER_ID\n");
        extractSqlBuffer.append(",CREATED_TIME\n");
        extractSqlBuffer.append(",MODIFIED_TIME\n");
        extractSqlBuffer.append(",MEMBER_CARD\n");
        extractSqlBuffer.append(",CRED_CODE\n");
        extractSqlBuffer.append(",MOBILE_NUMBER\n");
        extractSqlBuffer.append(",ATTRIBUTE\n");
        extractSqlBuffer.append(",LEVEL_NAME\n");
        extractSqlBuffer.append(",CN_LAST_NAME\n");
        extractSqlBuffer.append(",CN_FIRST_NAME\n");
        extractSqlBuffer.append(",LAST_NAME\n");
        extractSqlBuffer.append(",FIRST_NAME\n");
        extractSqlBuffer.append(",DATEOF_BIRTH\n");
        extractSqlBuffer.append(",PROMO_CODE\n");
        extractSqlBuffer.append(",CHANNEL_ID\n");
        extractSqlBuffer.append(",CHANNEL_NAME\n)  ");
        extractSqlBuffer.append("SELECT ");
        extractSqlBuffer.append("CAST('").append(etlDate).append("' AS DATE) ETL_DATE");
        extractSqlBuffer.append(",ID\n");
        extractSqlBuffer.append(",PRODUCT_ID\n");
        extractSqlBuffer.append(",MEMBER_ID\n");
        extractSqlBuffer.append(",TO_TIMESTAMP_LTZ(CREATED_TIME, 3) CREATED_TIME\n");
        extractSqlBuffer.append(",TO_TIMESTAMP_LTZ(MODIFIED_TIME, 3) MODIFIED_TIME\n");
        extractSqlBuffer.append(",sm4_encrypt (MEMBER_CARD,'").append(Constants.SM4_KEY).append("') MEMBER_CARD\n");
        extractSqlBuffer.append(",sm4_encrypt (CRED_CODE,'").append(Constants.SM4_KEY).append("') CRED_CODE\n");
        extractSqlBuffer.append(",sm4_encrypt (MOBILE_NUMBER,'").append(Constants.SM4_KEY).append("') MOBILE_NUMBER\n");
        extractSqlBuffer.append(",ATTRIBUTE\n");
        extractSqlBuffer.append(",LEVEL_NAME\n");
        extractSqlBuffer.append(",CN_LAST_NAME\n");
        extractSqlBuffer.append(",CN_FIRST_NAME\n");
        extractSqlBuffer.append(",LAST_NAME\n");
        extractSqlBuffer.append(",FIRST_NAME\n");
        extractSqlBuffer.append(",TO_TIMESTAMP_LTZ(DATEOF_BIRTH, 3) DATEOF_BIRTH\n");
        extractSqlBuffer.append(",PROMO_CODE\n");
        extractSqlBuffer.append(",CHANNEL_ID\n");
        extractSqlBuffer.append(",CHANNEL_NAME\n");
        extractSqlBuffer.append(" FROM T_MANAGE_PRODUCT_NAMELIST ");
        String extractSql = extractSqlBuffer.toString();
        
        TableResult result = tEnv.executeSql(extractSql);
        result.print();
    }

}
