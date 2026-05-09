package com.travelsky.dataplatform.main.ods.v2.hytd;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.TOdsHytdSysRegister;
import com.travelsky.dataplatform.module.ods.TOdsHytdTManageProduct;
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

/**
 * @author
 * @date 2025/10/11 15:30
 */
public class ExtractTOdsHytdTManageProduct {
    private static final Logger logger = LoggerFactory.getLogger(ExtractTOdsHytdTManageProduct.class);

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            logger.error("etl_date参数为空");
            System.exit(0);
        }
        String etlDate = args[0];
        String startDate = etlDate;
        String endDate = etlDate;
        if (args.length > 2) {
            startDate = args[1];
            endDate = args[2];
        }
        logger.info("ExtractTOdsHytdTManageProduct etl_date:" + etlDate+", startDate:"+startDate+", endDate:"+endDate);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsHytdTManageProduct");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        String url = "jdbc:dm://" + Constants.HYTD_IP + ":" + Constants.HYTD_PORT + "/" + Constants.HYTD_DB;
        String user = Constants.HYTD_USER;
        String password = Constants.HYTD_PWD;
        String query = "SELECT\n" +
                " ID,\n" +
                " NAME,\n" +
                " START_TIME,\n" +
                " END_TIME,\n" +
                " STATUS,\n" +
                " IMG_URL,\n" +
                " CONTENT,\n" +
                " CREATED_TIME,\n" +
                " MODIFIED_TIME,\n" +
                " PROMO_CODE,\n" +
                " PAGE_TYPE,\n" +
                " PAGE_URL,\n" +
                " PAGE_URL_PC,\n" +
                " IMG_URL_HEIGHT,\n" +
                " IMG_URL_LIST,\n" +
                " IMG_URL_PC,\n" +
                " AIR_CHINA_END_TIME,\n" +
                " CLICK_VOLUME,\n" +
                " CONTENT_PC,\n" +
                " NEED_SIGN\n" +
                " FROM "+ Constants.HYTD_SCHEMA + ".T_MANAGE_PRODUCT AS mp "
                +" WHERE  CREATED_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                +" OR  MODIFIED_TIME BETWEEN TIMESTAMP '" + startDate + " 00:00:00' AND TIMESTAMP '" + endDate + " 23:59:59.999' "
                ;
        String columStr = "ID,NAME,START_TIME,END_TIME,STATUS,IMG_URL,CONTENT,CREATED_TIME,MODIFIED_TIME,PROMO_CODE,PAGE_TYPE,PAGE_URL,PAGE_URL_PC,IMG_URL_HEIGHT,IMG_URL_LIST,IMG_URL_PC,AIR_CHINA_END_TIME,CLICK_VOLUME,CONTENT_PC,NEED_SIGN";
        String[] columnStrs = columStr.split(",");
        List<String> columns = Arrays.stream(columnStrs).collect(Collectors.toList());
        DMDBSourceFunction function = new DMDBSourceFunction(url, user, password, query, columns);
        DataStreamSource<JSONObject> jsonObjectDataStreamSource = env.addSource(function);

        SingleOutputStreamOperator<TOdsHytdTManageProduct> tSource = jsonObjectDataStreamSource.map(jsonObject -> {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            TOdsHytdTManageProduct tOdsHytdTManageProduct = mapper.readValue(JSONObject.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue), TOdsHytdTManageProduct.class);
            return tOdsHytdTManageProduct;
        });
        Schema schema = Schema.newBuilder()
                .column("ID", DataTypes.STRING())
                .column("NAME", DataTypes.STRING())
                .column("START_TIME", DataTypes.BIGINT())
                .column("END_TIME", DataTypes.BIGINT())
                .column("STATUS", DataTypes.STRING())
                .column("IMG_URL", DataTypes.STRING())
                .column("CONTENT", DataTypes.STRING())
                .column("CREATED_TIME", DataTypes.BIGINT())
                .column("MODIFIED_TIME", DataTypes.BIGINT())
                .column("PROMO_CODE", DataTypes.STRING())
                .column("PAGE_TYPE", DataTypes.STRING())
                .column("PAGE_URL", DataTypes.STRING())
                .column("PAGE_URL_PC", DataTypes.STRING())
                .column("IMG_URL_HEIGHT", DataTypes.STRING())
                .column("IMG_URL_LIST", DataTypes.STRING())
                .column("IMG_URL_PC", DataTypes.STRING())
                .column("AIR_CHINA_END_TIME", DataTypes.BIGINT())
                .column("CLICK_VOLUME", DataTypes.INT())
                .column("CONTENT_PC", DataTypes.STRING())
                .column("NEED_SIGN", DataTypes.STRING())
                .build();
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        // 注册为临时视图
        tEnv.createTemporaryView("T_MANAGE_PRODUCT", tSource, schema);
        //创建Doris目标表
        TableResult tableResult = tEnv.executeSql("CREATE TABLE `T_ODS_HYTD_T_MANAGE_PRODUCT` (\n" +
                "  `ID` STRING NOT NULL COMMENT '主键',\n" +
                "  `NAME` STRING COMMENT '产品名称',\n" +
                "  `START_TIME` TIMESTAMP(6) COMMENT '生效时间',\n" +
                "  `END_TIME` TIMESTAMP(6) COMMENT '失效时间',\n" +
                "  `STATUS` STRING COMMENT '生效状态',\n" +
                "  `IMG_URL` STRING COMMENT '产品封面',\n" +
                "  `CONTENT` STRING COMMENT '产品描述',\n" +
                "  `CREATED_TIME` TIMESTAMP(6) COMMENT '创建时间',\n" +
                "  `MODIFIED_TIME` TIMESTAMP(6) COMMENT '修改时间',\n" +
                "  `PROMO_CODE` STRING COMMENT '国航促销活动Code',\n" +
                "  `PAGE_TYPE` STRING COMMENT '宣传页类型',\n" +
                "  `PAGE_URL` STRING COMMENT 'H5定制页面地址',\n" +
                "  `PAGE_URL_PC` STRING COMMENT 'PC端定制页面地址',\n" +
                "  `IMG_URL_HEIGHT` STRING COMMENT '封面图片',\n" +
                "  `IMG_URL_LIST` STRING COMMENT '封面图片',\n" +
                "  `IMG_URL_PC` STRING COMMENT '封面图片',\n" +
                "  `AIR_CHINA_END_TIME` TIMESTAMP(6) COMMENT '国航平台系统设置结束时间',\n" +
                "  `CLICK_VOLUME` INT COMMENT '点击次数',\n" +
                "  `CONTENT_PC` STRING COMMENT 'PC端内容',\n" +
                "  `NEED_SIGN` STRING,\n" +
                "  `ETL_CREATE_TIME` TIMESTAMP(6) COMMENT '数据入仓时间',\n" +
                "  `ETL_UPDATE_TIME` TIMESTAMP(6) COMMENT '数据在数仓更新时间时间',\n" +
                "  `ETL_DATE` DATE COMMENT '数据ETL日期'\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_HYTD_T_MANAGE_PRODUCT',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        String extractSql = "INSERT INTO T_ODS_HYTD_T_MANAGE_PRODUCT(\n" +
                " ID,\n" +
                " NAME,\n" +
                " START_TIME,\n" +
                " END_TIME,\n" +
                " STATUS,\n" +
                " IMG_URL,\n" +
                " CONTENT,\n" +
                " CREATED_TIME,\n" +
                " MODIFIED_TIME,\n" +
                " PROMO_CODE,\n" +
                " PAGE_TYPE,\n" +
                " PAGE_URL,\n" +
                " PAGE_URL_PC,\n" +
                " IMG_URL_HEIGHT,\n" +
                " IMG_URL_LIST,\n" +
                " IMG_URL_PC,\n" +
                " AIR_CHINA_END_TIME,\n" +
                " CLICK_VOLUME,\n" +
                " CONTENT_PC,\n" +
                " NEED_SIGN, \n" +
                " `ETL_CREATE_TIME`, \n" +
                " `ETL_UPDATE_TIME`, \n" +
                " `ETL_DATE`) " +

                " SELECT\n" +
                " ID,\n" +
                " NAME,\n" +
                " TO_TIMESTAMP_LTZ(START_TIME, 3) START_TIME,\n" +
                " TO_TIMESTAMP_LTZ(END_TIME, 3) END_TIME,\n" +
                " STATUS,\n" +
                " IMG_URL,\n" +
                " CONTENT,\n" +
                " TO_TIMESTAMP_LTZ(CREATED_TIME, 3) CREATED_TIME,\n" +
                " TO_TIMESTAMP_LTZ(MODIFIED_TIME, 3) MODIFIED_TIME,\n" +
                " PROMO_CODE,\n" +
                " PAGE_TYPE,\n" +
                " PAGE_URL,\n" +
                " PAGE_URL_PC,\n" +
                " IMG_URL_HEIGHT,\n" +
                " IMG_URL_LIST,\n" +
                " IMG_URL_PC,\n" +
                " TO_TIMESTAMP_LTZ(AIR_CHINA_END_TIME, 3) AIR_CHINA_END_TIME,\n" +
                " CLICK_VOLUME,\n" +
                " CONTENT_PC,\n" +
                " NEED_SIGN,\n" +
                " CURRENT_TIMESTAMP AS ETL_CREATE_TIME,\n" +
                " CURRENT_TIMESTAMP AS ETL_UPDATE_TIME,\n" +
                " CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                " FROM T_MANAGE_PRODUCT ";
        TableResult result = tEnv.executeSql(extractSql);
        result.print();
    }

}
