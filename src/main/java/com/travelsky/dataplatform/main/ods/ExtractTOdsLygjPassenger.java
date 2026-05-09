package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.OracleAESDecryptor;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.DorisUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * @author kuangaihua
 * @date 2025/6/26 16:29
 */
public class    ExtractTOdsLygjPassenger {
    static final Logger logger = LoggerFactory.getLogger(ExtractTOdsLygjPassenger.class);
    private static final String ID_FILE_PATH = "file/ods_lygj_passenger_current_id.txt";
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        // Read current ID from resource file
        //long currentId = readCurrentId();
        //logger.info("ExtractTOdsLygjPassenger开始执行：{}, 日期： {}", currentId, etlDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLygjPassenger");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册AES加密解密UDF
        tEnv.createTemporarySystemFunction("aes_decrypt", OracleAESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE PASSENGER (\n" +
                "  ID BIGINT NOT NULL COMMENT '主键ID',\n" +
                "  ZWXM VARCHAR(180) COMMENT '中文姓名',\n" +
                "  YWXM VARCHAR(300) COMMENT '英文姓名',\n" +
                "  XB CHAR(1) COMMENT '性别',\n" +
                "  ZJ1 VARCHAR(12000) COMMENT '证件号1',\n" +
                "  ZJ2 VARCHAR(12000) COMMENT '证件号2',\n" +
                "  ZJ3 VARCHAR(12000) COMMENT '证件号3',\n" +
                "  ZJ4 VARCHAR(12000) COMMENT '证件号4',\n" +
                "  ZJ5 VARCHAR(12000) COMMENT '证件号5',\n" +
                "  ZJLX5 VARCHAR(150) COMMENT '证件类型5',\n" +
                "  B2CYH VARCHAR(60) COMMENT '不常用信息字段',\n" +
                "  BGDH VARCHAR(12000) COMMENT '办公电话',\n" +
                "  BGSJ VARCHAR(12000) COMMENT '办公手机',\n" +
                "  JTDH VARCHAR(12000) COMMENT '家庭电话',\n" +
                "  JTSJ VARCHAR(12000) COMMENT '家庭手机',\n" +
                "  DZYX VARCHAR(300) COMMENT '电子邮箱',\n" +
                "  BGDZ VARCHAR(1440) COMMENT '办公地址',\n" +
                "  BGYB VARCHAR(60) COMMENT '办公邮编',\n" +
                "  JTDZ VARCHAR(600) COMMENT '家庭地址',\n" +
                "  JTYB VARCHAR(60) COMMENT '家庭邮编',\n" +
                "  CZHM VARCHAR(150) COMMENT '常旅客号/会员号',\n" +
                "  PNRLXZ VARCHAR(12000) COMMENT 'PNR联系组信息',\n" +
                "  ZJSZD VARCHAR(180) COMMENT '证件所在地',\n" +
                "  RZDD VARCHAR(1440) COMMENT '入住地点',\n" +
                "  BGSJD VARCHAR(180) COMMENT '办公时间段',\n" +
                "  JTSJD VARCHAR(180) COMMENT '家庭时间段',\n" +
                "  JTCS VARCHAR(180) COMMENT '家庭城市',\n" +
                "  BGCS VARCHAR(180) COMMENT '办公城市',\n" +
                "  LVSR TIMESTAMP(6) COMMENT '旅行日期',\n" +
                "  BL1 VARCHAR(180) COMMENT '保留字段1',\n" +
                "  BL2 VARCHAR(180) COMMENT '保留字段2',\n" +
                "  BL3 VARCHAR(180) COMMENT '保留字段3',\n" +
                "  BL4 VARCHAR(60) COMMENT '保留字段4',\n" +
                "  BL5 VARCHAR(60) COMMENT '保留字段5',\n" +
                "  BL6 VARCHAR(60) COMMENT '保留字段6',\n" +
                "  BL7 VARCHAR(60) COMMENT '保留字段7',\n" +
                "  BL8 VARCHAR(60) COMMENT '保留字段8',\n" +
                "  UPDATE_TIME TIMESTAMP(6) COMMENT '更新时间',\n" +
                "  UPDATE_PERSON VARCHAR(300) COMMENT '更新人',\n" +
                "  MOBILE_PHONE VARCHAR(12000) COMMENT '手机号码',\n" +
                "  MPHONE_FLG BIGINT COMMENT '手机标志',\n" +
                "  MPHONE_FLG_DATE TIMESTAMP(6) COMMENT '手机标志日期'" +

                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.LYGJ_IP + ":" + Constants.LYGJ_PORT + "/" + Constants.LYGJ_DB + "',\n" +
                "    'table-name' = '" + Constants.LYGJ_SCHEMA + ".PASSENGER', \n" +
                "    'username' = '" + Constants.LYGJ_USER + "',\n" +
                "    'password' = '" + Constants.LYGJ_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_LYGJ_PASSENGER (\n" +
                "ETL_DATE DATE NOT NULL  COMMENT '数据ETL日期',\n" +
                        "  ID BIGINT NOT NULL COMMENT '主键ID',\n" +
                        "  ZWXM VARCHAR(180) COMMENT '中文姓名',\n" +
                        "  YWXM VARCHAR(300) COMMENT '英文姓名',\n" +
                        "  XB CHAR(1) COMMENT '性别',\n" +
                        "  ZJ1 VARCHAR(12000) COMMENT '证件号1',\n" +
                        "  ZJ2 VARCHAR(12000) COMMENT '证件号2',\n" +
                        "  ZJ3 VARCHAR(12000) COMMENT '证件号3',\n" +
                        "  ZJ4 VARCHAR(12000) COMMENT '证件号4',\n" +
                        "  ZJ5 VARCHAR(12000) COMMENT '证件号5',\n" +
                        "  ZJLX5 VARCHAR(150) COMMENT '证件类型5',\n" +
                        "  B2CYH VARCHAR(60) COMMENT '不常用信息字段',\n" +
                        "  BGDH VARCHAR(12000) COMMENT '办公电话',\n" +
                        "  BGSJ VARCHAR(12000) COMMENT '办公手机',\n" +
                        "  JTDH VARCHAR(12000) COMMENT '家庭电话',\n" +
                        "  JTSJ VARCHAR(12000) COMMENT '家庭手机',\n" +
                        "  DZYX VARCHAR(300) COMMENT '电子邮箱',\n" +
                        "  BGDZ VARCHAR(1440) COMMENT '办公地址',\n" +
                        "  BGYB VARCHAR(60) COMMENT '办公邮编',\n" +
                        "  JTDZ VARCHAR(600) COMMENT '家庭地址',\n" +
                        "  JTYB VARCHAR(60) COMMENT '家庭邮编',\n" +
                        "  CZHM VARCHAR(150) COMMENT '常旅客号/会员号',\n" +
                        "  PNRLXZ VARCHAR(12000) COMMENT 'PNR联系组信息',\n" +
                        "  ZJSZD VARCHAR(180) COMMENT '证件所在地',\n" +
                        "  RZDD VARCHAR(1440) COMMENT '入住地点',\n" +
                        "  BGSJD VARCHAR(180) COMMENT '办公时间段',\n" +
                        "  JTSJD VARCHAR(180) COMMENT '家庭时间段',\n" +
                        "  JTCS VARCHAR(180) COMMENT '家庭城市',\n" +
                        "  BGCS VARCHAR(180) COMMENT '办公城市',\n" +
                        "  LVSR TIMESTAMP(6) COMMENT '旅行日期',\n" +
                        "  BL1 VARCHAR(180) COMMENT '保留字段1',\n" +
                        "  BL2 VARCHAR(180) COMMENT '保留字段2',\n" +
                        "  BL3 VARCHAR(180) COMMENT '保留字段3',\n" +
                        "  BL4 VARCHAR(60) COMMENT '保留字段4',\n" +
                        "  BL5 VARCHAR(60) COMMENT '保留字段5',\n" +
                        "  BL6 VARCHAR(60) COMMENT '保留字段6',\n" +
                        "  BL7 VARCHAR(60) COMMENT '保留字段7',\n" +
                        "  BL8 VARCHAR(60) COMMENT '保留字段8',\n" +
                        "  UPDATE_TIME TIMESTAMP(6) COMMENT '更新时间',\n" +
                        "  UPDATE_PERSON VARCHAR(300) COMMENT '更新人',\n" +
                        "  MOBILE_PHONE VARCHAR(12000) COMMENT '手机号码',\n" +
                        "  MPHONE_FLG BIGINT COMMENT '手机标志',\n" +
                        "  MPHONE_FLG_DATE TIMESTAMP(6) COMMENT '手机标志日期'" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYGJ_PASSENGER',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
                //") WITH (\n" +
                //"    'connector' = 'jdbc',\n" +
                //"    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
                //"    'table-name' = 'T_ODS_LYGJ_PASSENGER', -- 替换为实际的表名\n" +
                //"    'username' = '" + Constants.ODS_USER + "',\n" +
                //"    'password' = '" + Constants.ODS_PWD + "'\n" +
                //",\n" + "  'driver' = '" + Constants.MYSQL_DRIVER + "'" +
                //")");

        String extractSql = "INSERT INTO T_ODS_LYGJ_PASSENGER(" +
                "  ETL_DATE,\n" +
                "  ID,\n" +
                "  ZWXM,\n" +
                "  YWXM,\n" +
                "  XB,\n" +
                "  ZJ1,\n" +
                "  ZJ2,\n" +
                "  ZJ3,\n" +
                "  ZJ4,\n" +
                "  ZJ5,\n" +
                "  ZJLX5,\n" +
                "  B2CYH,\n" +
                "  BGDH,\n" +
                "  BGSJ,\n" +
                "  JTDH,\n" +
                "  JTSJ,\n" +
                "  DZYX,\n" +
                "  BGDZ,\n" +
                "  BGYB,\n" +
                "  JTDZ,\n" +
                "  JTYB,\n" +
                "  CZHM,\n" +
                "  PNRLXZ,\n" +
                "  ZJSZD,\n" +
                "  RZDD,\n" +
                "  BGSJD,\n" +
                "  JTSJD,\n" +
                "  JTCS,\n" +
                "  BGCS,\n" +
                "  LVSR,\n" +
                "  BL1,\n" +
                "  BL2,\n" +
                "  BL3,\n" +
                "  BL4,\n" +
                "  BL5,\n" +
                "  BL6,\n" +
                "  BL7,\n" +
                "  BL8,\n" +
                "  UPDATE_TIME,\n" +
                "  UPDATE_PERSON,\n" +
                "  MOBILE_PHONE,\n" +
                "  MPHONE_FLG,\n" +
                "  MPHONE_FLG_DATE\n)  " +
                " SELECT \n" +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE" +
                "  ,ID,\n" +
                "  ZWXM,\n" +
                "  YWXM,\n" +
                "  XB,\n" +
                "  sm4_encrypt(aes_decrypt(ZJ1, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') ZJ1,\n" +
                "  sm4_encrypt(aes_decrypt(ZJ2, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') ZJ2,\n" +
                "  sm4_encrypt(aes_decrypt(ZJ3, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') ZJ3,\n" +
                "  sm4_encrypt(aes_decrypt(ZJ4, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') ZJ4,\n" +
                "  sm4_encrypt(aes_decrypt(ZJ5, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') ZJ5,\n" +
                "  ZJLX5,\n" +
                "  B2CYH,\n" +
                "  sm4_encrypt(aes_decrypt(BGDH, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') BGDH,\n" +
                "  sm4_encrypt(aes_decrypt(BGSJ, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') BGSJ,\n" +
                "  sm4_encrypt(aes_decrypt(JTDH, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') JTDH,\n" +
                "  sm4_encrypt(aes_decrypt(JTSJ, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') JTSJ,\n" +
                "  sm4_encrypt(aes_decrypt(DZYX, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') DZYX,\n" +
                "  BGDZ,\n" +
                "  BGYB,\n" +
                "  JTDZ,\n" +
                "  JTYB,\n" +
                "  sm4_encrypt(aes_decrypt(CZHM, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') CZHM,\n" +
                "  sm4_encrypt(aes_decrypt(PNRLXZ, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') PNRLXZ,\n" +
                "  ZJSZD,\n" +
                "  RZDD,\n" +
                "  BGSJD,\n" +
                "  JTSJD,\n" +
                "  JTCS,\n" +
                "  BGCS,\n" +
                "  LVSR,\n" +
                "  BL1,\n" +
                "  BL2,\n" +
                "  BL3,\n" +
                "  BL4,\n" +
                "  BL5,\n" +
                "  BL6,\n" +
                "  BL7,\n" +
                "  BL8,\n" +
                "  UPDATE_TIME,\n" +
                "  UPDATE_PERSON,\n" +
                "  sm4_encrypt(aes_decrypt(MOBILE_PHONE, '" + Constants.LYGJ_AES_KEY + "'), '" + Constants.SM4_KEY + "') MOBILE_PHONE,\n" +
                "  MPHONE_FLG,\n" +
                "  MPHONE_FLG_DATE" +
                " FROM PASSENGER ";
                //" FROM PASSENGER WHERE ID > " + currentId;
        TableResult result = tEnv.executeSql(extractSql);
        //try {
        //    result.await();
        //} catch (InterruptedException e) {
        //    e.printStackTrace();
        //} catch (ExecutionException e) {
        //    e.printStackTrace();
        //}
        //long newMaxId = getNewMaxId(tEnv);
        //System.out.println("New maximum ID: " + newMaxId);
        //
        //if (newMaxId > currentId) {
        //    updateCurrentId(newMaxId);
        //}

    }
    private static long readCurrentId() throws IOException {
        Path externalFile = Paths.get(ID_FILE_PATH);
        if (Files.exists(externalFile)) {
            return Long.parseLong(Files.readAllLines(externalFile).get(0));
        } else {
            logger.error("文件未找到: " + ID_FILE_PATH);
            throw new IOException("文件未找到");
        }
    }

    private static long getNewMaxId(StreamTableEnvironment tEnv) {

        // 查询强id对应的tid>2的数据，并查找出tid被挂载的所有强id
        Connection conn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.ODS_USER, Constants.ODS_PWD);
        Statement stmt = DorisUtils.getStatement(conn);
        ResultSet rs = DorisUtils.getDorisResult(stmt, "SELECT MAX(ID) AS ID FROM "+Constants.ODS_DB+".T_ODS_LYGJ_PASSENGER");
        long id=0L;
        try {
            if (rs.next()) {
                id = rs.getLong("ID");

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        DorisUtils.close(conn, stmt, rs);
        return id;
    }


    private static void updateCurrentId(long newId) throws IOException {
        Path idFile = Paths.get(ID_FILE_PATH);
        Files.write(idFile, String.valueOf(newId).getBytes(StandardCharsets.UTF_8));
    }

}
