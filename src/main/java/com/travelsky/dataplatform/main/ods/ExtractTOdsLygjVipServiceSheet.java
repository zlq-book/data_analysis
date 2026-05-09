package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.OracleAESDecryptor;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.UUID;

/**
 * @author kuangaihua
 * @date 2025/6/26 16:29
 */
public class ExtractTOdsLygjVipServiceSheet {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLygjVipServiceSheet");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE VIP_SERVICE_SHEET (\n" +
                "  ID BIGINT NOT NULL COMMENT '主键ID',\n" +
                "  PASSENGER_ID BIGINT COMMENT '旅客ID',\n" +
                "  VIP_INFO_ID BIGINT COMMENT 'VIP信息ID',\n" +
                "  VIP_TYPE_ID BIGINT COMMENT 'VIP类型ID',\n" +
                "  FLIGHT_NUM VARCHAR(24) COMMENT '航班号',\n" +
                "  FLIGHT_DATE TIMESTAMP(6) COMMENT '航班日期',\n" +
                "  NAME VARCHAR(300) COMMENT '姓名',\n" +
                "  GENDER VARCHAR(48) COMMENT '性别',\n" +
                "  ID_CARD VARCHAR(12000) COMMENT '身份证件号',\n" +
                "  DUTY VARCHAR(960) COMMENT '职务',\n" +
                "  BOOK_NUMB VARCHAR(192) COMMENT '预订号码',\n" +
                "  CABIN_CLASS VARCHAR(6) COMMENT '舱位等级',\n" +
                "  RETINUE_NAME VARCHAR(900) COMMENT '随行人员姓名',\n" +
                "  RETINUE_NUM BIGINT COMMENT '随行人员数量',\n" +
                "  RETINUE_CABIN_CLASS VARCHAR(3) COMMENT '随行人员舱位等级',\n" +
                "  BOOK_TICKET VARCHAR(3) COMMENT '订票标识',\n" +
                "  PROVIDE_FOOD VARCHAR(3) COMMENT '提供餐饮标识',\n" +
                "  SENT_CAR VARCHAR(3) COMMENT '派车标识',\n" +
                "  BOARD_CHECK VARCHAR(3) COMMENT '登机检查标识',\n" +
                "  RECEIVED_MAN VARCHAR(3) COMMENT '接待人员标识',\n" +
                "  VISITANT_ROOM VARCHAR(3) COMMENT '贵宾室标识',\n" +
                "  CABIN_SERVICE VARCHAR(3) COMMENT '客舱服务标识',\n" +
                "  RECEIVED_CAR VARCHAR(3) COMMENT '接车标识',\n" +
                "  RECEIVED_OFFICE VARCHAR(3) COMMENT '接待办公室标识',\n" +
                "  OTHER VARCHAR(3) COMMENT '其他服务标识',\n" +
                "  SERVICE_HINT VARCHAR(3072) COMMENT '服务提示',\n" +
                "  FOOD_DESIRE VARCHAR(3072) COMMENT '餐饮需求',\n" +
                "  SERVICE_DESIRE VARCHAR(3072) COMMENT '服务需求',\n" +
                "  OPERATOR BIGINT COMMENT '操作员',\n" +
                "  OPERATE_DATE TIMESTAMP(6) COMMENT '操作日期',\n" +
                "  CARD_NUM VARCHAR(192) COMMENT '卡号',\n" +
                "  MOBILE VARCHAR(12000) COMMENT '手机号',\n" +
                "  DEPARTURE_AIRPORT VARCHAR(60) COMMENT '出发机场',\n" +
                "  ARRIVAL_AIRPORT VARCHAR(60) COMMENT '到达机场',\n" +
                "  APPROVED VARCHAR(6) COMMENT '批准状态',\n" +
                "  SUGGESTION VARCHAR(3072) COMMENT '建议',\n" +
                "  APPROVED_MAN VARCHAR(192) COMMENT '批准人',\n" +
                "  APPROVED_DATE TIMESTAMP(6) COMMENT '批准日期',\n" +
                "  FLIGHT_INFO_ID BIGINT COMMENT '航班信息ID',\n" +
                "  PROC_INST_ID BIGINT COMMENT '流程实例ID',\n" +
                "  BAGGAGE_LEVEL VARCHAR(192) COMMENT '行李等级',\n" +
                "  BAGGAGE_AMOUNT BIGINT COMMENT '行李数量',\n" +
                "  BAGGAGE_BRAND_NUM VARCHAR(6000) COMMENT '行李品牌编号',\n" +
                "  CERTIFICATE_TYPE VARCHAR(192) COMMENT '证件类型',\n" +
                "  CARD_TYPE VARCHAR(192) COMMENT '卡类型',\n" +
                "  IS_GREEN_CHANNEL BIGINT COMMENT '绿色通道标识',\n" +
                "  BAGGAGE_REMARK VARCHAR(3072) COMMENT '行李备注',\n" +
                "  AUTO_MATCH_TYPE BIGINT COMMENT '自动匹配类型',\n" +
                "  IS_CANCEL VARCHAR(3) COMMENT '取消标识',\n" +
                "  FROM_SYSTEM VARCHAR(30) COMMENT '来源系统',\n" +
                "  CIP_FLAG VARCHAR(3) COMMENT 'CIP标识',\n" +
                "  RESV1 VARCHAR(180) COMMENT '保留字段1',\n" +
                "  RESV2 VARCHAR(180) COMMENT '保留字段2',\n" +
                "  RESV3 VARCHAR(180) COMMENT '保留字段3',\n" +
                "  RESV4 VARCHAR(60) COMMENT '保留字段4',\n" +
                "  RESV5 VARCHAR(60) COMMENT '保留字段5',\n" +
                "  RESV6 VARCHAR(60) COMMENT '保留字段6',\n" +
                "  RESV7 VARCHAR(60) COMMENT '保留字段7',\n" +
                "  RESV8 VARCHAR(60) COMMENT '保留字段8',\n" +
                "  COMPANY VARCHAR(600) COMMENT '公司',\n" +
                "  MEMO VARCHAR(1200) COMMENT '备注',\n" +
                "  SMS_WEATHER VARCHAR(3) COMMENT '短信天气标识',\n" +
                "  LAYER_TYPE_ID VARCHAR(60) COMMENT '层级类型ID',\n" +
                "  REMARK VARCHAR(3000) COMMENT '备注说明'" +


                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.LYGJ_IP + ":" + Constants.LYGJ_PORT + "/" + Constants.LYGJ_DB + "',\n" +
                "    'table-name' = '" + Constants.LYGJ_SCHEMA + ".VIP_SERVICE_SHEET', \n" +
                "    'username' = '" + Constants.LYGJ_USER + "',\n" +
                "    'password' = '" + Constants.LYGJ_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_LYGJ_VIP_SERVICE_SHEET (\n" +
                "  ETL_DATE DATE NOT NULL  COMMENT '数据ETL日期',\n" +
                "  ID BIGINT NOT NULL COMMENT '主键ID',\n" +
                "  PASSENGER_ID BIGINT COMMENT '旅客ID',\n" +
                "  VIP_INFO_ID BIGINT COMMENT 'VIP信息ID',\n" +
                "  VIP_TYPE_ID BIGINT COMMENT 'VIP类型ID',\n" +
                "  FLIGHT_NUM VARCHAR(24) COMMENT '航班号',\n" +
                "  FLIGHT_DATE TIMESTAMP(6) COMMENT '航班日期',\n" +
                "  NAME VARCHAR(300) COMMENT '姓名',\n" +
                "  GENDER VARCHAR(48) COMMENT '性别',\n" +
                "  ID_CARD VARCHAR(12000) COMMENT '身份证件号',\n" +
                "  DUTY VARCHAR(960) COMMENT '职务',\n" +
                "  BOOK_NUMB VARCHAR(192) COMMENT '预订号码',\n" +
                "  CABIN_CLASS VARCHAR(6) COMMENT '舱位等级',\n" +
                "  RETINUE_NAME VARCHAR(900) COMMENT '随行人员姓名',\n" +
                "  RETINUE_NUM BIGINT COMMENT '随行人员数量',\n" +
                "  RETINUE_CABIN_CLASS VARCHAR(3) COMMENT '随行人员舱位等级',\n" +
                "  BOOK_TICKET VARCHAR(3) COMMENT '订票标识',\n" +
                "  PROVIDE_FOOD VARCHAR(3) COMMENT '提供餐饮标识',\n" +
                "  SENT_CAR VARCHAR(3) COMMENT '派车标识',\n" +
                "  BOARD_CHECK VARCHAR(3) COMMENT '登机检查标识',\n" +
                "  RECEIVED_MAN VARCHAR(3) COMMENT '接待人员标识',\n" +
                "  VISITANT_ROOM VARCHAR(3) COMMENT '贵宾室标识',\n" +
                "  CABIN_SERVICE VARCHAR(3) COMMENT '客舱服务标识',\n" +
                "  RECEIVED_CAR VARCHAR(3) COMMENT '接车标识',\n" +
                "  RECEIVED_OFFICE VARCHAR(3) COMMENT '接待办公室标识',\n" +
                "  OTHER VARCHAR(3) COMMENT '其他服务标识',\n" +
                "  SERVICE_HINT VARCHAR(3072) COMMENT '服务提示',\n" +
                "  FOOD_DESIRE VARCHAR(3072) COMMENT '餐饮需求',\n" +
                "  SERVICE_DESIRE VARCHAR(3072) COMMENT '服务需求',\n" +
                "  OPERATOR BIGINT COMMENT '操作员',\n" +
                "  OPERATE_DATE TIMESTAMP(6) COMMENT '操作日期',\n" +
                "  CARD_NUM VARCHAR(192) COMMENT '卡号',\n" +
                "  MOBILE VARCHAR(12000) COMMENT '手机号',\n" +
                "  DEPARTURE_AIRPORT VARCHAR(60) COMMENT '出发机场',\n" +
                "  ARRIVAL_AIRPORT VARCHAR(60) COMMENT '到达机场',\n" +
                "  APPROVED VARCHAR(6) COMMENT '批准状态',\n" +
                "  SUGGESTION VARCHAR(3072) COMMENT '建议',\n" +
                "  APPROVED_MAN VARCHAR(192) COMMENT '批准人',\n" +
                "  APPROVED_DATE TIMESTAMP(6) COMMENT '批准日期',\n" +
                "  FLIGHT_INFO_ID BIGINT COMMENT '航班信息ID',\n" +
                "  PROC_INST_ID BIGINT COMMENT '流程实例ID',\n" +
                "  BAGGAGE_LEVEL VARCHAR(192) COMMENT '行李等级',\n" +
                "  BAGGAGE_AMOUNT BIGINT COMMENT '行李数量',\n" +
                "  BAGGAGE_BRAND_NUM VARCHAR(6000) COMMENT '行李品牌编号',\n" +
                "  CERTIFICATE_TYPE VARCHAR(192) COMMENT '证件类型',\n" +
                "  CARD_TYPE VARCHAR(192) COMMENT '卡类型',\n" +
                "  IS_GREEN_CHANNEL BIGINT COMMENT '绿色通道标识',\n" +
                "  BAGGAGE_REMARK VARCHAR(3072) COMMENT '行李备注',\n" +
                "  AUTO_MATCH_TYPE BIGINT COMMENT '自动匹配类型',\n" +
                "  IS_CANCEL VARCHAR(3) COMMENT '取消标识',\n" +
                "  FROM_SYSTEM VARCHAR(30) COMMENT '来源系统',\n" +
                "  CIP_FLAG VARCHAR(3) COMMENT 'CIP标识',\n" +
                "  RESV1 VARCHAR(180) COMMENT '保留字段1',\n" +
                "  RESV2 VARCHAR(180) COMMENT '保留字段2',\n" +
                "  RESV3 VARCHAR(180) COMMENT '保留字段3',\n" +
                "  RESV4 VARCHAR(60) COMMENT '保留字段4',\n" +
                "  RESV5 VARCHAR(60) COMMENT '保留字段5',\n" +
                "  RESV6 VARCHAR(60) COMMENT '保留字段6',\n" +
                "  RESV7 VARCHAR(60) COMMENT '保留字段7',\n" +
                "  RESV8 VARCHAR(60) COMMENT '保留字段8',\n" +
                "  COMPANY VARCHAR(600) COMMENT '公司',\n" +
                "  MEMO VARCHAR(1200) COMMENT '备注',\n" +
                "  SMS_WEATHER VARCHAR(3) COMMENT '短信天气标识',\n" +
                "  LAYER_TYPE_ID VARCHAR(60) COMMENT '层级类型ID',\n" +
                "  REMARK VARCHAR(3000) COMMENT '备注说明'" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYGJ_VIP_SERVICE_SHEET',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
                //") WITH (\n" +
                //"    'connector' = 'jdbc',\n" +
                //"    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
                //"    'table-name' = 'T_ODS_LYGJ_VIP_SERVICE_SHEET', -- 替换为实际的表名\n" +
                //"    'username' = '" + Constants.ODS_USER + "',\n" +
                //"    'password' = '" + Constants.ODS_PWD + "'\n" +
                //",\n" + "  'driver' = '" + Constants.MYSQL_DRIVER + "'" +
                //")");
        String extractSql = "INSERT INTO T_ODS_LYGJ_VIP_SERVICE_SHEET(" +
                "  ETL_DATE,\n" +
                "  ID,\n" +
                "  PASSENGER_ID,\n" +
                "  VIP_INFO_ID,\n" +
                "  VIP_TYPE_ID,\n" +
                "  FLIGHT_NUM,\n" +
                "  FLIGHT_DATE,\n" +
                "  NAME,\n" +
                "  GENDER,\n" +
                "  ID_CARD,\n" +
                "  DUTY,\n" +
                "  BOOK_NUMB,\n" +
                "  CABIN_CLASS,\n" +
                "  RETINUE_NAME,\n" +
                "  RETINUE_NUM,\n" +
                "  RETINUE_CABIN_CLASS,\n" +
                "  BOOK_TICKET,\n" +
                "  PROVIDE_FOOD,\n" +
                "  SENT_CAR,\n" +
                "  BOARD_CHECK,\n" +
                "  RECEIVED_MAN,\n" +
                "  VISITANT_ROOM,\n" +
                "  CABIN_SERVICE,\n" +
                "  RECEIVED_CAR,\n" +
                "  RECEIVED_OFFICE,\n" +
                "  OTHER,\n" +
                "  SERVICE_HINT,\n" +
                "  FOOD_DESIRE,\n" +
                "  SERVICE_DESIRE,\n" +
                "  OPERATOR,\n" +
                "  OPERATE_DATE,\n" +
                "  CARD_NUM,\n" +
                "  MOBILE,\n" +
                "  DEPARTURE_AIRPORT,\n" +
                "  ARRIVAL_AIRPORT,\n" +
                "  APPROVED,\n" +
                "  SUGGESTION,\n" +
                "  APPROVED_MAN,\n" +
                "  APPROVED_DATE,\n" +
                "  FLIGHT_INFO_ID,\n" +
                "  PROC_INST_ID,\n" +
                "  BAGGAGE_LEVEL,\n" +
                "  BAGGAGE_AMOUNT,\n" +
                "  BAGGAGE_BRAND_NUM,\n" +
                "  CERTIFICATE_TYPE,\n" +
                "  CARD_TYPE,\n" +
                "  IS_GREEN_CHANNEL,\n" +
                "  BAGGAGE_REMARK,\n" +
                "  AUTO_MATCH_TYPE,\n" +
                "  IS_CANCEL,\n" +
                "  FROM_SYSTEM,\n" +
                "  CIP_FLAG,\n" +
                "  RESV1,\n" +
                "  RESV2,\n" +
                "  RESV3,\n" +
                "  RESV4,\n" +
                "  RESV5,\n" +
                "  RESV6,\n" +
                "  RESV7,\n" +
                "  RESV8,\n" +
                "  COMPANY,\n" +
                "  MEMO,\n" +
                "  SMS_WEATHER,\n" +
                "  LAYER_TYPE_ID,\n" +
                "  REMARK\n)  " +
                "SELECT \n" +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE" +
                ",ID,\n" +
                "  PASSENGER_ID,\n" +
                "  VIP_INFO_ID,\n" +
                "  VIP_TYPE_ID,\n" +
                "  FLIGHT_NUM,\n" +
                "  FLIGHT_DATE,\n" +
                "  NAME,\n" +
                "  GENDER,\n" +
                "  sm4_encrypt(aes_decrypt(ID_CARD, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') ID_CARD,\n" +
                "  sm4_encrypt(aes_decrypt(DUTY, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') DUTY,\n" +
                "  BOOK_NUMB,\n" +
                "  CABIN_CLASS,\n" +
                "  RETINUE_NAME,\n" +
                "  RETINUE_NUM,\n" +
                "  RETINUE_CABIN_CLASS,\n" +
                "  BOOK_TICKET,\n" +
                "  PROVIDE_FOOD,\n" +
                "  SENT_CAR,\n" +
                "  BOARD_CHECK,\n" +
                "  RECEIVED_MAN,\n" +
                "  VISITANT_ROOM,\n" +
                "  CABIN_SERVICE,\n" +
                "  RECEIVED_CAR,\n" +
                "  RECEIVED_OFFICE,\n" +
                "  OTHER,\n" +
                "  SERVICE_HINT,\n" +
                "  FOOD_DESIRE,\n" +
                "  SERVICE_DESIRE,\n" +
                "  OPERATOR,\n" +
                "  OPERATE_DATE,\n" +
                "  sm4_encrypt (CARD_NUM,'" + Constants.SM4_KEY + "') CARD_NUM, \n" +
                "  sm4_encrypt(aes_decrypt(MOBILE, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') MOBILE,\n" +
                "  DEPARTURE_AIRPORT,\n" +
                "  ARRIVAL_AIRPORT,\n" +
                "  APPROVED,\n" +
                "  SUGGESTION,\n" +
                "  APPROVED_MAN,\n" +
                "  APPROVED_DATE,\n" +
                "  FLIGHT_INFO_ID,\n" +
                "  PROC_INST_ID,\n" +
                "  BAGGAGE_LEVEL,\n" +
                "  BAGGAGE_AMOUNT,\n" +
                "  BAGGAGE_BRAND_NUM,\n" +
                "  CERTIFICATE_TYPE,\n" +
                "  CARD_TYPE,\n" +
                "  IS_GREEN_CHANNEL,\n" +
                "  BAGGAGE_REMARK,\n" +
                "  AUTO_MATCH_TYPE,\n" +
                "  IS_CANCEL,\n" +
                "  FROM_SYSTEM,\n" +
                "  CIP_FLAG,\n" +
                "  RESV1,\n" +
                "  RESV2,\n" +
                "  RESV3,\n" +
                "  RESV4,\n" +
                "  RESV5,\n" +
                "  RESV6,\n" +
                "  RESV7,\n" +
                "  RESV8,\n" +
                "  COMPANY,\n" +
                "  MEMO,\n" +
                "  SMS_WEATHER,\n" +
                "  LAYER_TYPE_ID,\n" +
                "  REMARK" +
                " FROM VIP_SERVICE_SHEET "
                + " WHERE  OPERATE_DATE BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        TableResult result = tEnv.executeSql(extractSql);

        result.print();


    }

}
