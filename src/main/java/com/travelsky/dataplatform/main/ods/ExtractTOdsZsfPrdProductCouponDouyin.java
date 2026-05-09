package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.CreateTableSql;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.UUID;

public class ExtractTOdsZsfPrdProductCouponDouyin {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key= Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsZsfPrdProductCouponDouyin");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);

        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE PRD_PRODUCT_COUPON_DOUYIN (\n" +
                "    ID VARCHAR(108) NOT NULL,\n" +
                "    ORDER_ID VARCHAR(108),\n" +
                "    `COUNT` INT,\n" +
                "    START_TIME INT,\n" +
                "    EXPIRE_TIME INT,\n" +
                "    SKU_NAME VARCHAR(96),\n" +
                "    SKU_ID VARCHAR(96),\n" +
                "    CARD_CODE VARCHAR(96),\n" +
                "    COUPON_CODE VARCHAR(96),\n" +
                "    PHONE VARCHAR(96),\n" +
                "    STATUS CHAR(1),\n" +
                "    VERIFICATION_TIME TIMESTAMP(6),\n" +
                "    REFUND_TYPE INT,\n" +
                "    AFTER_SALE_ID VARCHAR(96),\n" +
                "    CERTIFICATE_ID VARCHAR(96),\n" +
                "    REFUND_APPLY_TIME INT,\n" +
                "    REFUND_AUDIT_TIME INT,\n" +
                "    REFUND_AUDIT_RESULT INT,\n" +
                "    ENABLE CHAR(1),\n" +
                "    CREATE_TIME TIMESTAMP(6),\n" +
                "    CREATE_USER VARCHAR(384),\n" +
                "    UPDATE_TIME TIMESTAMP(6),\n" +
                "    UPDATE_USER VARCHAR(384)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://"+ Constants.ZSF_IP+":"+Constants.ZSF_PORT+"/"+Constants.ZSF_DB+"',\n" +
                "    'table-name' = 'PRD_PRODUCT_COUPON_DOUYIN', \n" +
                "    'username' = '"+Constants.ZSF_USER+"',\n" +
                "    'password' = '"+Constants.ZSF_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
                ")");
        //创建Doris目标表
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        String ZSF_PRD_PRODUCT_COUPON_DOUYIN = "CREATE TABLE T_ODS_ZSF_PRD_PRODUCT_COUPON_DOUYIN (\n" +
                "    ETL_DATE DATE NOT NULL,\n" +
                "    ID VARCHAR(108) NOT NULL,\n" +
                "    ORDER_ID VARCHAR(108),\n" +
                "    `COUNT` INT,\n" +
                "    START_TIME INT,\n" +
                "    EXPIRE_TIME INT,\n" +
                "    SKU_NAME VARCHAR(96),\n" +
                "    SKU_ID VARCHAR(96),\n" +
                "    CARD_CODE VARCHAR(96),\n" +
                "    COUPON_CODE VARCHAR(96),\n" +
                "    PHONE VARCHAR(96),\n" +
                "    STATUS CHAR(1),\n" +
                "    VERIFICATION_TIME TIMESTAMP(6),\n" +
                "    REFUND_TYPE INT,\n" +
                "    AFTER_SALE_ID VARCHAR(96),\n" +
                "    CERTIFICATE_ID VARCHAR(96),\n" +
                "    REFUND_APPLY_TIME INT,\n" +
                "    REFUND_AUDIT_TIME INT,\n" +
                "    REFUND_AUDIT_RESULT INT,\n" +
                "    ENABLE CHAR(1),\n" +
                "    CREATE_TIME TIMESTAMP(6),\n" +
                "    CREATE_USER VARCHAR(384),\n" +
                "    UPDATE_TIME TIMESTAMP(6),\n" +
                "    UPDATE_USER VARCHAR(384)" +
//                ") WITH (\n" +
//                "    'connector' = 'jdbc',\n" +
//                "    'url' = 'jdbc:mysql://"+ Constants.DORIS_IP +":"+Constants.DORIS_PORT+"/"+Constants.ODS_DB+"',\n" +
//                "    'table-name' = 'T_ODS_ZSF_PRD_PRODUCT_COUPON_DOUYIN', -- 替换为实际的表名\n" +
//                "    'username' = '"+Constants.ODS_USER+"',\n" +
//                "    'password' = '"+Constants.ODS_PWD+"'\n" +
//                ")";
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_ZSF_PRD_PRODUCT_COUPON_DOUYIN',\n" +
                "'sink.label-prefix' = '" + timestamp+uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")";
        tEnv.executeSql(ZSF_PRD_PRODUCT_COUPON_DOUYIN);

        String extractSql = "INSERT INTO T_ODS_ZSF_PRD_PRODUCT_COUPON_DOUYIN(" +
                "ETL_DATE\n" +
                ",ID\n" +
                ",ORDER_ID\n" +
                ",`COUNT`\n" +
                ",START_TIME\n" +
                ",EXPIRE_TIME\n" +
                ",SKU_NAME\n" +
                ",SKU_ID\n" +
                ",CARD_CODE\n" +
                ",COUPON_CODE\n" +
                ",PHONE\n" +
                ",STATUS\n" +
                ",VERIFICATION_TIME\n" +
                ",REFUND_TYPE\n" +
                ",AFTER_SALE_ID\n" +
                ",CERTIFICATE_ID\n" +
                ",REFUND_APPLY_TIME\n" +
                ",REFUND_AUDIT_TIME\n" +
                ",REFUND_AUDIT_RESULT\n" +
                ",ENABLE\n" +
                ",CREATE_TIME\n" +
                ",CREATE_USER\n" +
                ",UPDATE_TIME\n" +
                ",UPDATE_USER\n" +
                ") " +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE \n" +
                ",ID\n" +
                ",ORDER_ID\n" +
                ",`COUNT`\n" +
                ",START_TIME\n" +
                ",EXPIRE_TIME\n" +
                ",SKU_NAME\n" +
                ",SKU_ID\n" +
                ",CARD_CODE\n" +
                ",COUPON_CODE\n" +
                ",sm4_encrypt (PHONE,'" + sm4key + "') PHONE\n" +
                ",STATUS\n" +
                ",VERIFICATION_TIME\n" +
                ",REFUND_TYPE\n" +
                ",AFTER_SALE_ID\n" +
                ",CERTIFICATE_ID\n" +
                ",REFUND_APPLY_TIME\n" +
                ",REFUND_AUDIT_TIME\n" +
                ",REFUND_AUDIT_RESULT\n" +
                ",ENABLE\n" +
                ",CREATE_TIME\n" +
                ",CREATE_USER\n" +
                ",UPDATE_TIME\n" +
                ",UPDATE_USER" +
                " FROM PRD_PRODUCT_COUPON_DOUYIN "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' " +
                " OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        TableResult result = tEnv.executeSql(extractSql
        );

        result.print();


    }
}
