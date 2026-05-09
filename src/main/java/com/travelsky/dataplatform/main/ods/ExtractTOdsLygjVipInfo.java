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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.UUID;

import static com.travelsky.trp.usercenter.data.analysis.utils.TransUtils.readLastId;

/**
 * @author kuangaihua
 * @date 2025/6/26 16:29
 */
public class ExtractTOdsLygjVipInfo {

    static final Logger logger = LoggerFactory.getLogger(ExtractTOdsLygjVipInfo.class);

    // ID存储文件路径
    private static final String ID_FILE = "vip_info_last_id.txt";
    // 每次查询的数据量
    private static final int BATCH_SIZE = 100000;

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];

        // 从文件读取上次的ID
        long lastId = readLastId(ID_FILE);
        logger.info("ExtractTOdsLygjVipInfo开始执行：{}, 日期： {}", lastId, etlDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLygjVipInfo");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE VIP_INFO (\n" +
                "  ID BIGINT NOT NULL COMMENT '主键ID',\n" +
                "  VIP_TYPE_ID BIGINT COMMENT 'VIP类型ID',\n" +
                "  NAME VARCHAR(300) COMMENT '姓名',\n" +
                "  GENDER VARCHAR(48) COMMENT '性别',\n" +
                "  NATIONALITY VARCHAR(60) COMMENT '国籍',\n" +
                "  ID_CARD VARCHAR(12000) COMMENT '身份证件号',\n" +
                "  DUTY VARCHAR(960) COMMENT '职务',\n" +
                "  CARD_TYPE VARCHAR(192) COMMENT '卡类型',\n" +
                "  CARD_NUM VARCHAR(288) COMMENT '卡号',\n" +
                "  MOBILE VARCHAR(12000) COMMENT '手机号',\n" +
                "  PHOTO VARCHAR(192) COMMENT '照片',\n" +
                "  FAVORITE VARCHAR(3072) COMMENT '喜好',\n" +
                "  SERVICE_DESC VARCHAR(3072) COMMENT '服务描述',\n" +
                "  MEMO VARCHAR(3072) COMMENT '备注',\n" +
                "  CERTIFICATE_TYPE VARCHAR(30) COMMENT '证件类型',\n" +
                "  FOOD_DESC VARCHAR(3072) COMMENT '餐饮描述',\n" +
                "  SEAT_LOVE VARCHAR(3072) COMMENT '座位偏好',\n" +
                "  OTHER VARCHAR(3072) COMMENT '其他信息',\n" +
                "  PAXID BIGINT COMMENT '旅客ID',\n" +
                "  GRP_ID VARCHAR(60) COMMENT '团队ID',\n" +
                "  GRP_CLASS VARCHAR(60) COMMENT '团队等级',\n" +
                "  UPD_TIME TIMESTAMP(6) COMMENT '更新时间',\n" +
                "  UPD_USER VARCHAR(150) COMMENT '更新用户',\n" +
                "  BL1 VARCHAR(180) COMMENT '保留字段1',\n" +
                "  BL2 VARCHAR(180) COMMENT '保留字段2',\n" +
                "  BL3 VARCHAR(180) COMMENT '保留字段3',\n" +
                "  BL4 VARCHAR(60) COMMENT '保留字段4',\n" +
                "  BL5 VARCHAR(60) COMMENT '保留字段5',\n" +
                "  BL6 VARCHAR(60) COMMENT '保留字段6',\n" +
                "  BL7 VARCHAR(60) COMMENT '保留字段7',\n" +
                "  BL8 VARCHAR(60) COMMENT '保留字段8',\n" +
                "  FOOD_DESC_TEMP VARCHAR(3072) COMMENT '餐饮描述临时',\n" +
                "  SEAT_LOVE_TEMP VARCHAR(3072) COMMENT '座位偏好临时',\n" +
                "  OTHER_TEMP VARCHAR(3072) COMMENT '其他信息临时',\n" +
                "  COMPANY VARCHAR(600) COMMENT '公司',\n" +
                "  VIP_APPEAL VARCHAR(6000) COMMENT 'VIP诉求',\n" +
                "  NAME_MEMO VARCHAR(150) COMMENT '姓名备注',\n" +
                "  VIP_FOOD_LOVE_IDS VARCHAR(1500) COMMENT '喜爱食品ID',\n" +
                "  VIP_FOOD_LOVE_NAME VARCHAR(1500) COMMENT '喜爱食品名称',\n" +
                "  VIP_FOOD_LOVE_MEMO VARCHAR(1500) COMMENT '食品备注',\n" +
                "  VIP_DRINK_LOVE_IDS VARCHAR(1500) COMMENT '喜爱饮品ID',\n" +
                "  VIP_DRINK_LOVE_NAMES VARCHAR(1500) COMMENT '喜爱饮品名称',\n" +
                "  VIP_DRINK_LOVE_MEMO VARCHAR(1500) COMMENT '饮品备注',\n" +
                "  VIP_SEAT_LOVE_IDS VARCHAR(1500) COMMENT '座位偏好ID',\n" +
                "  VIP_SEAT_LOVE_NAMES VARCHAR(1500) COMMENT '座位偏好名称',\n" +
                "  VIP_SEAT_LOVE_MEMO VARCHAR(1500) COMMENT '座位备注',\n" +
                "  VIP_LOUNGE_LOVE_IDS VARCHAR(1500) COMMENT '休息室偏好ID',\n" +
                "  VIP_LOUNGE_LOVE_NAMES VARCHAR(1500) COMMENT '休息室偏好名称',\n" +
                "  VIP_LOUNGE_LOVE_MEMO VARCHAR(1500) COMMENT '休息室备注',\n" +
                "  BIRTHDAY VARCHAR(30) COMMENT '生日',\n" +
                "  UPDATE_TIME TIMESTAMP(6) COMMENT '更新时间',\n" +
                "  CREATE_TIME TIMESTAMP(6) COMMENT '创建时间',\n" +
                "  VIP_TYPE_NAMES VARCHAR(300) COMMENT 'VIP类型名称',\n" +
                "  CREATER VARCHAR(300) COMMENT '创建人',\n" +
                "  UPDATE_PERSON VARCHAR(300) COMMENT '更新人',\n" +
                "  GRADE VARCHAR(300) COMMENT '等级',\n" +
                "  APPLICANTDEPT BIGINT COMMENT '申请部门',\n" +
                "  APPLICANT BIGINT COMMENT '申请人',\n" +
                "  CREATORDEPT BIGINT COMMENT '创建部门',\n" +
                "  LAYER_TYPE_ID VARCHAR(60) COMMENT '层级类型ID',\n" +
                "  LAYER_SOURCE VARCHAR(300) COMMENT '层级来源',\n" +
                "  LAYER_BASE VARCHAR(300) COMMENT '基础层级',\n" +
                "  LAYER_CREATE_TM TIMESTAMP(6) COMMENT '层级创建时间',\n" +
                "  LAYER_UPDATE_TM TIMESTAMP(6) COMMENT '层级更新时间',\n" +
                "  LAYER_VALIDITY TIMESTAMP(6) COMMENT '层级有效期',\n" +
                "  LAYER_LONG_VALIDITY VARCHAR(18) COMMENT '长期有效标识'\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.LYGJ_IP + ":" + Constants.LYGJ_PORT + "/" + Constants.LYGJ_DB + "',\n" +
                "    'table-name' = '" + Constants.LYGJ_SCHEMA + ".VIP_INFO', \n" +
                "    'username' = '" + Constants.LYGJ_USER + "',\n" +
                "    'password' = '" + Constants.LYGJ_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_LYGJ_VIP_INFO (\n" +
                "ETL_DATE DATE NOT NULL  COMMENT '数据ETL日期',\n" +
                "  ID BIGINT NOT NULL COMMENT '主键ID',\n" +
                "  VIP_TYPE_ID BIGINT COMMENT 'VIP类型ID',\n" +
                "  NAME VARCHAR(300) COMMENT '姓名',\n" +
                "  GENDER VARCHAR(48) COMMENT '性别',\n" +
                "  NATIONALITY VARCHAR(60) COMMENT '国籍',\n" +
                "  ID_CARD VARCHAR(12000) COMMENT '身份证件号',\n" +
                "  DUTY VARCHAR(960) COMMENT '职务',\n" +
                "  CARD_TYPE VARCHAR(192) COMMENT '卡类型',\n" +
                "  CARD_NUM VARCHAR(288) COMMENT '卡号',\n" +
                "  MOBILE VARCHAR(12000) COMMENT '手机号',\n" +
                "  PHOTO VARCHAR(192) COMMENT '照片',\n" +
                "  FAVORITE VARCHAR(3072) COMMENT '喜好',\n" +
                "  SERVICE_DESC VARCHAR(3072) COMMENT '服务描述',\n" +
                "  MEMO VARCHAR(3072) COMMENT '备注',\n" +
                "  CERTIFICATE_TYPE VARCHAR(30) COMMENT '证件类型',\n" +
                "  FOOD_DESC VARCHAR(3072) COMMENT '餐饮描述',\n" +
                "  SEAT_LOVE VARCHAR(3072) COMMENT '座位偏好',\n" +
                "  OTHER VARCHAR(3072) COMMENT '其他信息',\n" +
                "  PAXID BIGINT COMMENT '旅客ID',\n" +
                "  GRP_ID VARCHAR(60) COMMENT '团队ID',\n" +
                "  GRP_CLASS VARCHAR(60) COMMENT '团队等级',\n" +
                "  UPD_TIME TIMESTAMP(6) COMMENT '更新时间',\n" +
                "  UPD_USER VARCHAR(150) COMMENT '更新用户',\n" +
                "  BL1 VARCHAR(180) COMMENT '保留字段1',\n" +
                "  BL2 VARCHAR(180) COMMENT '保留字段2',\n" +
                "  BL3 VARCHAR(180) COMMENT '保留字段3',\n" +
                "  BL4 VARCHAR(60) COMMENT '保留字段4',\n" +
                "  BL5 VARCHAR(60) COMMENT '保留字段5',\n" +
                "  BL6 VARCHAR(60) COMMENT '保留字段6',\n" +
                "  BL7 VARCHAR(60) COMMENT '保留字段7',\n" +
                "  BL8 VARCHAR(60) COMMENT '保留字段8',\n" +
                "  FOOD_DESC_TEMP VARCHAR(3072) COMMENT '餐饮描述临时',\n" +
                "  SEAT_LOVE_TEMP VARCHAR(3072) COMMENT '座位偏好临时',\n" +
                "  OTHER_TEMP VARCHAR(3072) COMMENT '其他信息临时',\n" +
                "  COMPANY VARCHAR(600) COMMENT '公司',\n" +
                "  VIP_APPEAL VARCHAR(6000) COMMENT 'VIP诉求',\n" +
                "  NAME_MEMO VARCHAR(150) COMMENT '姓名备注',\n" +
                "  VIP_FOOD_LOVE_IDS VARCHAR(1500) COMMENT '喜爱食品ID',\n" +
                "  VIP_FOOD_LOVE_NAME VARCHAR(1500) COMMENT '喜爱食品名称',\n" +
                "  VIP_FOOD_LOVE_MEMO VARCHAR(1500) COMMENT '食品备注',\n" +
                "  VIP_DRINK_LOVE_IDS VARCHAR(1500) COMMENT '喜爱饮品ID',\n" +
                "  VIP_DRINK_LOVE_NAMES VARCHAR(1500) COMMENT '喜爱饮品名称',\n" +
                "  VIP_DRINK_LOVE_MEMO VARCHAR(1500) COMMENT '饮品备注',\n" +
                "  VIP_SEAT_LOVE_IDS VARCHAR(1500) COMMENT '座位偏好ID',\n" +
                "  VIP_SEAT_LOVE_NAMES VARCHAR(1500) COMMENT '座位偏好名称',\n" +
                "  VIP_SEAT_LOVE_MEMO VARCHAR(1500) COMMENT '座位备注',\n" +
                "  VIP_LOUNGE_LOVE_IDS VARCHAR(1500) COMMENT '休息室偏好ID',\n" +
                "  VIP_LOUNGE_LOVE_NAMES VARCHAR(1500) COMMENT '休息室偏好名称',\n" +
                "  VIP_LOUNGE_LOVE_MEMO VARCHAR(1500) COMMENT '休息室备注',\n" +
                "  BIRTHDAY VARCHAR(30) COMMENT '生日',\n" +
                "  UPDATE_TIME TIMESTAMP(6) COMMENT '更新时间',\n" +
                "  CREATE_TIME TIMESTAMP(6) COMMENT '创建时间',\n" +
                "  VIP_TYPE_NAMES VARCHAR(300) COMMENT 'VIP类型名称',\n" +
                "  CREATER VARCHAR(300) COMMENT '创建人',\n" +
                "  UPDATE_PERSON VARCHAR(300) COMMENT '更新人',\n" +
                "  GRADE VARCHAR(300) COMMENT '等级',\n" +
                "  APPLICANTDEPT BIGINT COMMENT '申请部门',\n" +
                "  APPLICANT BIGINT COMMENT '申请人',\n" +
                "  CREATORDEPT BIGINT COMMENT '创建部门',\n" +
                "  LAYER_TYPE_ID VARCHAR(60) COMMENT '层级类型ID',\n" +
                "  LAYER_SOURCE VARCHAR(300) COMMENT '层级来源',\n" +
                "  LAYER_BASE VARCHAR(300) COMMENT '基础层级',\n" +
                "  LAYER_CREATE_TM TIMESTAMP(6) COMMENT '层级创建时间',\n" +
                "  LAYER_UPDATE_TM TIMESTAMP(6) COMMENT '层级更新时间',\n" +
                "  LAYER_VALIDITY TIMESTAMP(6) COMMENT '层级有效期',\n" +
                "  LAYER_LONG_VALIDITY VARCHAR(18) COMMENT '长期有效标识'\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYGJ_VIP_INFO',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
                //") WITH (\n" +
                //"    'connector' = 'jdbc',\n" +
                //"    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
                //"    'table-name' = 'T_ODS_LYGJ_VIP_INFO', -- 替换为实际的表名\n" +
                //"    'username' = '" + Constants.ODS_USER + "',\n" +
                //"    'password' = '" + Constants.ODS_PWD + "'\n" +
                //",\n" + "  'driver' = '" + Constants.MYSQL_DRIVER + "'" +
                //")");
        // 构建抽取SQL，日期条件用TO_DATE转换，避免Oracle日期格式不匹配问题
        String extractSql = "INSERT INTO T_ODS_LYGJ_VIP_INFO(" +
                "  ETL_DATE,\n" +
                "  ID,\n" +
                "  VIP_TYPE_ID,\n" +
                "  NAME,\n" +
                "  GENDER,\n" +
                "  NATIONALITY,\n" +
                "  ID_CARD,\n" +
                "  DUTY,\n" +
                "  CARD_TYPE,\n" +
                "  CARD_NUM,\n" +
                "  MOBILE,\n" +
                "  PHOTO,\n" +
                "  FAVORITE,\n" +
                "  SERVICE_DESC,\n" +
                "  MEMO,\n" +
                "  CERTIFICATE_TYPE,\n" +
                "  FOOD_DESC,\n" +
                "  SEAT_LOVE,\n" +
                "  OTHER,\n" +
                "  PAXID,\n" +
                "  GRP_ID,\n" +
                "  GRP_CLASS,\n" +
                "  UPD_TIME,\n" +
                "  UPD_USER,\n" +
                "  BL1,\n" +
                "  BL2,\n" +
                "  BL3,\n" +
                "  BL4,\n" +
                "  BL5,\n" +
                "  BL6,\n" +
                "  BL7,\n" +
                "  BL8,\n" +
                "  FOOD_DESC_TEMP,\n" +
                "  SEAT_LOVE_TEMP,\n" +
                "  OTHER_TEMP,\n" +
                "  COMPANY,\n" +
                "  VIP_APPEAL,\n" +
                "  NAME_MEMO,\n" +
                "  VIP_FOOD_LOVE_IDS,\n" +
                "  VIP_FOOD_LOVE_NAME,\n" +
                "  VIP_FOOD_LOVE_MEMO,\n" +
                "  VIP_DRINK_LOVE_IDS,\n" +
                "  VIP_DRINK_LOVE_NAMES,\n" +
                "  VIP_DRINK_LOVE_MEMO,\n" +
                "  VIP_SEAT_LOVE_IDS,\n" +
                "  VIP_SEAT_LOVE_NAMES,\n" +
                "  VIP_SEAT_LOVE_MEMO,\n" +
                "  VIP_LOUNGE_LOVE_IDS,\n" +
                "  VIP_LOUNGE_LOVE_NAMES,\n" +
                "  VIP_LOUNGE_LOVE_MEMO,\n" +
                "  BIRTHDAY,\n" +
                "  UPDATE_TIME,\n" +
                "  CREATE_TIME,\n" +
                "  VIP_TYPE_NAMES,\n" +
                "  CREATER,\n" +
                "  UPDATE_PERSON,\n" +
                "  GRADE,\n" +
                "  APPLICANTDEPT,\n" +
                "  APPLICANT,\n" +
                "  CREATORDEPT,\n" +
                "  LAYER_TYPE_ID,\n" +
                "  LAYER_SOURCE,\n" +
                "  LAYER_BASE,\n" +
                "  LAYER_CREATE_TM,\n" +
                "  LAYER_UPDATE_TM,\n" +
                "  LAYER_VALIDITY,\n" +
                "  LAYER_LONG_VALIDITY\n)  " +
                "SELECT \n" +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE" +
                ",ID,\n" +
                "  VIP_TYPE_ID,\n" +
                "  NAME,\n" +
                "  GENDER,\n" +
                "  NATIONALITY,\n" +
                "  sm4_encrypt(aes_decrypt(ID_CARD, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') ID_CARD,\n" +
                "  sm4_encrypt(aes_decrypt(DUTY, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') DUTY,\n" +
                "  CARD_TYPE,\n" +
                "  sm4_encrypt(CARD_NUM,'" + Constants.SM4_KEY + "') CARD_NUM, \n" +
                "  sm4_encrypt(aes_decrypt(MOBILE, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') MOBILE,\n" +
                "  PHOTO,\n" +
                "  FAVORITE,\n" +
                "  SERVICE_DESC,\n" +
                "  MEMO,\n" +
                "  CERTIFICATE_TYPE,\n" +
                "  FOOD_DESC,\n" +
                "  SEAT_LOVE,\n" +
                "  OTHER,\n" +
                "  PAXID,\n" +
                "  GRP_ID,\n" +
                "  GRP_CLASS,\n" +
                "  UPD_TIME,\n" +
                "  UPD_USER,\n" +
                "  BL1,\n" +
                "  BL2,\n" +
                "  BL3,\n" +
                "  BL4,\n" +
                "  BL5,\n" +
                "  BL6,\n" +
                "  BL7,\n" +
                "  BL8,\n" +
                "  FOOD_DESC_TEMP,\n" +
                "  SEAT_LOVE_TEMP,\n" +
                "  OTHER_TEMP,\n" +
                "  COMPANY,\n" +
                "  VIP_APPEAL,\n" +
                "  NAME_MEMO,\n" +
                "  VIP_FOOD_LOVE_IDS,\n" +
                "  VIP_FOOD_LOVE_NAME,\n" +
                "  VIP_FOOD_LOVE_MEMO,\n" +
                "  VIP_DRINK_LOVE_IDS,\n" +
                "  VIP_DRINK_LOVE_NAMES,\n" +
                "  VIP_DRINK_LOVE_MEMO,\n" +
                "  VIP_SEAT_LOVE_IDS,\n" +
                "  VIP_SEAT_LOVE_NAMES,\n" +
                "  VIP_SEAT_LOVE_MEMO,\n" +
                "  VIP_LOUNGE_LOVE_IDS,\n" +
                "  VIP_LOUNGE_LOVE_NAMES,\n" +
                "  VIP_LOUNGE_LOVE_MEMO,\n" +
                "  BIRTHDAY,\n" +
                "  UPDATE_TIME,\n" +
                "  CREATE_TIME,\n" +
                "  VIP_TYPE_NAMES,\n" +
                "  CREATER,\n" +
                "  UPDATE_PERSON,\n" +
                "  GRADE,\n" +
                "  APPLICANTDEPT,\n" +
                "  APPLICANT,\n" +
                "  CREATORDEPT,\n" +
                "  LAYER_TYPE_ID,\n" +
                "  LAYER_SOURCE,\n" +
                "  LAYER_BASE,\n" +
                "  LAYER_CREATE_TM,\n" +
                "  LAYER_UPDATE_TM,\n" +
                "  LAYER_VALIDITY,\n" +
                "  LAYER_LONG_VALIDITY" +
                " FROM VIP_INFO "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                +" OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }

}
