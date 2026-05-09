package com.travelsky.dataplatform.main.ods;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.TOdsHytdSysDictData;
import com.travelsky.dataplatform.source.DMDBSourceFunction;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class ExtractTOdsHytdSysDictData {
    private static final Logger logger = LoggerFactory.getLogger(ExtractTOdsHytdSysDictData.class);

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            logger.error("etl_date参数为空");
            System.exit(0);
        }
        String etlDate = args[0];
        logger.info("etl_date:" + etlDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsHytdSysDictData");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        StringBuffer urlBuffer = new StringBuffer();
        urlBuffer.append("jdbc:dm://").append(Constants.HYTD_IP).append(":").append(Constants.HYTD_PORT).append("/").append(Constants.HYTD_DB);
        String url = urlBuffer.toString();
        String user = Constants.HYTD_USER;
        String password = Constants.HYTD_PWD;
        
        StringBuffer queryBuffer = new StringBuffer();
        queryBuffer.append("SELECT \n");
        queryBuffer.append(" ID\n");
        queryBuffer.append(",DICT_TYPE_ID\n");
        queryBuffer.append(",DICT_LABEL\n");
        queryBuffer.append(",DICT_VALUE\n");
        queryBuffer.append(",REMARK\n");
        queryBuffer.append(",SORT\n");
        queryBuffer.append(",CREATOR\n");
        queryBuffer.append(",CREATE_DATE\n");
        queryBuffer.append(",UPDATER\n");
        queryBuffer.append(",UPDATE_DATE \n");
        queryBuffer.append("FROM ").append(Constants.HYTD_SCHEMA).append(".SYS_DICT_DATA AS sdt ");
        queryBuffer.append("WHERE CREATE_DATE BETWEEN '").append(etlDate).append(" 00:00:00.000' AND '").append(etlDate).append(" 23:59:59.999' \n");
        String query = queryBuffer.toString();
        
        String columStr = "ID,DICT_TYPE_ID,DICT_LABEL,DICT_VALUE,REMARK,SORT,CREATOR,CREATE_DATE,UPDATER,UPDATE_DATE";
        String[] columnStrs = columStr.split(",");
        List<String> columns = Arrays.stream(columnStrs).collect(Collectors.toList());
        DMDBSourceFunction function = new DMDBSourceFunction(url, user, password, query, columns);
        DataStreamSource<JSONObject> jsonObjectDataStreamSource = env.addSource(function);

        SingleOutputStreamOperator<TOdsHytdSysDictData> tSource = jsonObjectDataStreamSource.map(jsonObject -> {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            TOdsHytdSysDictData tOdsHytdSysDictData = mapper.readValue(JSONObject.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue), TOdsHytdSysDictData.class);
            return tOdsHytdSysDictData;
        });
        Schema schema = Schema.newBuilder()
                .column("ID", DataTypes.BIGINT())
                .column("DICT_TYPE_ID", DataTypes.BIGINT())
                .column("DICT_LABEL", DataTypes.VARCHAR(765))
                .column("DICT_VALUE", DataTypes.VARCHAR(765))
                .column("REMARK", DataTypes.VARCHAR(765))
                .column("SORT", DataTypes.BIGINT())
                .column("CREATOR", DataTypes.BIGINT())
                .column("CREATE_DATE", DataTypes.BIGINT())
                .column("UPDATER", DataTypes.BIGINT())
                .column("UPDATE_DATE", DataTypes.BIGINT())
                .build();
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        // 注册为临时视图
        tEnv.createTemporaryView("SYS_DICT_DATA", tSource, schema);
        
        //创建Doris目标表
        StringBuffer targetTableSql = new StringBuffer();
        targetTableSql.append("CREATE TABLE T_ODS_HYTD_SYS_DICT_DATA (\n");
        targetTableSql.append("    ID bigint,\n");
        targetTableSql.append("    DICT_TYPE_ID bigint  ,\n");
        targetTableSql.append("    DICT_LABEL VARCHAR(765)  ,\n");
        targetTableSql.append("    DICT_VALUE VARCHAR(765)  ,\n");
        targetTableSql.append("    REMARK VARCHAR(765)  ,\n");
        targetTableSql.append("    SORT bigint  ,\n");
        targetTableSql.append("    CREATOR bigint  ,\n");
        targetTableSql.append("    CREATE_DATE TIMESTAMP(6)  ,\n");
        targetTableSql.append("    UPDATER bigint  ,\n");
        targetTableSql.append("    UPDATE_DATE TIMESTAMP(6)  ,\n");
        targetTableSql.append("    ETL_CREATE_TIME TIMESTAMP(6),\n");
        targetTableSql.append("    ETL_UPDATE_TIME TIMESTAMP(6) ,\n");
        targetTableSql.append("    ETL_DATE date \n");
        targetTableSql.append(") WITH (\n");
        targetTableSql.append(" 'connector' = 'doris',\n");
        targetTableSql.append("'fenodes' = '").append(Constants.DORIS_FE_IP).append(":").append(Constants.DORIS_FE_PORT).append("', -- 替换为 Doris FE 的地址和端口\n");
        targetTableSql.append("'benodes' = '").append(Constants.DORIS_BE_IP).append(":").append(Constants.DORIS_BE_PORT).append("', -- 替换为 Doris FE 的地址和端口\n");
        targetTableSql.append(" 'table.identifier' = '").append(Constants.ODS_DB).append(".T_ODS_HYTD_SYS_DICT_DATA',\n");
        targetTableSql.append("'sink.label-prefix' = '").append(timestamp).append(uuid).append("',");
        targetTableSql.append(" 'sink.properties.read_json_by_line' = 'true',");
        targetTableSql.append(" 'sink.properties.format' = 'json',");
        targetTableSql.append("    'username' = '").append(Constants.ODS_USER).append("',\n");
        targetTableSql.append("    'password' = '").append(Constants.ODS_PWD).append("'\n");
        targetTableSql.append(")");
        tEnv.executeSql(targetTableSql.toString());
        
        StringBuffer extractSqlBuffer = new StringBuffer();
        extractSqlBuffer.append("INSERT INTO T_ODS_HYTD_SYS_DICT_DATA(");
        extractSqlBuffer.append("ID\n");
        extractSqlBuffer.append(",DICT_TYPE_ID\n");
        extractSqlBuffer.append(",DICT_LABEL\n");
        extractSqlBuffer.append(",DICT_VALUE\n");
        extractSqlBuffer.append(",REMARK\n");
        extractSqlBuffer.append(",SORT\n");
        extractSqlBuffer.append(",CREATOR\n");
        extractSqlBuffer.append(",CREATE_DATE\n");
        extractSqlBuffer.append(",UPDATER\n");
        extractSqlBuffer.append(",UPDATE_DATE\n");
        extractSqlBuffer.append(",ETL_CREATE_TIME\n");
        extractSqlBuffer.append(",ETL_UPDATE_TIME\n");
        extractSqlBuffer.append(",ETL_DATE\n)  ");
        extractSqlBuffer.append("SELECT ");
        extractSqlBuffer.append("ID\n");
        extractSqlBuffer.append(",DICT_TYPE_ID\n");
        extractSqlBuffer.append(",DICT_LABEL\n");
        extractSqlBuffer.append(",DICT_VALUE\n");
        extractSqlBuffer.append(",REMARK\n");
        extractSqlBuffer.append(",SORT\n");
        extractSqlBuffer.append(",CREATOR\n");
        extractSqlBuffer.append(",TO_TIMESTAMP_LTZ(CREATE_DATE, 3) AS  CREATE_DATE\n");
        extractSqlBuffer.append(",UPDATER\n");
        extractSqlBuffer.append(",TO_TIMESTAMP_LTZ(UPDATE_DATE, 3) AS UPDATE_DATE\n");
        extractSqlBuffer.append(",CURRENT_TIMESTAMP AS ETL_CREATE_TIME\n");
        extractSqlBuffer.append(",CURRENT_TIMESTAMP AS ETL_UPDATE_TIME\n");
        extractSqlBuffer.append(",CAST('").append(etlDate).append("' AS DATE) ETL_DATE\n");
        extractSqlBuffer.append(" FROM SYS_DICT_DATA ");
        String extractSql = extractSqlBuffer.toString();
        
        TableResult result = tEnv.executeSql(extractSql);
        result.print();
    }

}
