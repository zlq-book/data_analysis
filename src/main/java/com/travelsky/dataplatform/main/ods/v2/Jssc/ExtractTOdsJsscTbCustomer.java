package com.travelsky.dataplatform.main.ods.v2.Jssc;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.OracleAESDecryptor;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.DorisUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

/**
 * * 第一次全取，后续根据ID自增序列取
 */
public class ExtractTOdsJsscTbCustomer {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsJsscTbCustomer");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE TB_CUSTOMER (\n" +
                "    ID                BIGINT,\n" +
                "    CERTNO            VARCHAR(90),\n" +
                "    CERTTYPE          BIGINT,\n" +
                "    MAILADDRESS       VARCHAR(900),\n" +
                "    PHONENUMBER       VARCHAR(60),\n" +
                "    NAME              VARCHAR(64),\n" +
                "    COMPANYNAME       VARCHAR(300),\n" +
                "    REMARK            VARCHAR(900),\n" +
                "    CALLNUMBER        VARCHAR(60),\n" +
                "    ORGANIZE_ID       BIGINT,\n" +
                "    MAIL              VARCHAR(100),\n" +
                "    POSTALCODE        VARCHAR(255),\n" +
                "    ADDRESS           BIGINT,\n" +
                "    JSONDATA          VARCHAR(3000),\n" +
                "    BILLNEWADDRESS    VARCHAR(900),\n" +
                "    INVOICETTITLE     VARCHAR(255),\n" +
                "    NEEDBILL          BIGINT,\n" +
                "    EMAIL             VARCHAR(900),\n" +
                "    EMAILPHONENUMBER  VARCHAR(255),\n" +
                "    BILLPHONENUMBER   VARCHAR(255),\n" +
                "    BILLNAME          VARCHAR(255),\n" +
                "    BOOKPURCHASERNAME VARCHAR(255)" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.JSSC_IP + ":" + Constants.JSSC_PORT + "/" + Constants.JSSC_DB + "',\n" +
                "    'table-name' = '" + Constants.JSSC_SCHEMA + ".TB_CUSTOMER', \n" +
                "    'username' = '" + Constants.JSSC_USER + "',\n" +
                "    'password' = '" + Constants.JSSC_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_JSSC_TB_CUSTOMER (\n" +
                "    ID                BIGINT,\n" +
                "    CERTNO            VARCHAR(90),\n" +
                "    CERTTYPE          BIGINT,\n" +
                "    MAILADDRESS       VARCHAR(900),\n" +
                "    PHONENUMBER       VARCHAR(60),\n" +
                "    NAME              VARCHAR(64),\n" +
                "    COMPANYNAME       VARCHAR(300),\n" +
                "    REMARK            VARCHAR(900),\n" +
                "    CALLNUMBER        VARCHAR(60),\n" +
                "    ORGANIZE_ID       BIGINT,\n" +
                "    MAIL              VARCHAR(100),\n" +
                "    POSTALCODE        VARCHAR(255),\n" +
                "    ADDRESS           BIGINT,\n" +
                "    JSONDATA          VARCHAR(3000),\n" +
                "    BILLNEWADDRESS    VARCHAR(900),\n" +
                "    INVOICETTITLE     VARCHAR(255),\n" +
                "    NEEDBILL          BIGINT,\n" +
                "    EMAIL             VARCHAR(900),\n" +
                "    EMAILPHONENUMBER  VARCHAR(255),\n" +
                "    BILLPHONENUMBER   VARCHAR(255),\n" +
                "    BILLNAME          VARCHAR(255),\n" +
                "    BOOKPURCHASERNAME VARCHAR(255)," +
                "    ETL_DATE DATE" +
                //") WITH (\n" +
                //"    'connector' = 'jdbc',\n" +
                //"    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
                //"    'table-name' = 'T_ODS_JSSC_TB_CUSTOMER',\n" +
                //"    'username' = '" + Constants.ODS_USER + "',\n" +
                //"    'password' = '" + Constants.ODS_PWD + "'\n" +
                //",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
                //")");
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "  'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "',\n" +
                "  'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "',\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_JSSC_TB_CUSTOMER',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        // 获取最新ID
        long newMaxId = getNewMaxId();
        String extractSql = "INSERT INTO T_ODS_JSSC_TB_CUSTOMER (" +
                "    ETL_DATE,\n" +
                "    ID,\n" +
                "    CERTNO,\n" +
                "    CERTTYPE,\n" +
                "    MAILADDRESS,\n" +
                "    PHONENUMBER,\n" +
                "    NAME,\n" +
                "    COMPANYNAME,\n" +
                "    REMARK,\n" +
                "    CALLNUMBER,\n" +
                "    ORGANIZE_ID,\n" +
                "    MAIL,\n" +
                "    POSTALCODE,\n" +
                "    ADDRESS,\n" +
                "    JSONDATA,\n" +
                "    BILLNEWADDRESS,\n" +
                "    INVOICETTITLE,\n" +
                "    NEEDBILL,\n" +
                "    EMAIL,\n" +
                "    EMAILPHONENUMBER,\n" +
                "    BILLPHONENUMBER,\n" +
                "    BILLNAME,\n" +
                "    BOOKPURCHASERNAME)  " +
                "    SELECT \n" +
                "    CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "    ID,\n" +
                "    sm4_encrypt(CERTNO, '" + Constants.SM4_KEY + "') CERTNO, \n" +
                "    CERTTYPE,\n" +
                "    MAILADDRESS,\n" +
                "    sm4_encrypt(PHONENUMBER, '" + Constants.SM4_KEY + "') PHONENUMBER, \n" +
                "    NAME,\n" +
                "    COMPANYNAME,\n" +
                "    REMARK,\n" +
                "    CALLNUMBER,\n" +
                "    ORGANIZE_ID,\n" +
                "    sm4_encrypt(MAIL, '" + Constants.SM4_KEY + "') MAIL, \n" +
                "    POSTALCODE,\n" +
                "    ADDRESS,\n" +
                "    JSONDATA,\n" +
                "    sm4_encrypt(BILLNEWADDRESS, '" + Constants.SM4_KEY + "') BILLNEWADDRESS, \n" +
                "    INVOICETTITLE,\n" +
                "    NEEDBILL,\n" +
                "    sm4_encrypt(EMAIL, '" + Constants.SM4_KEY + "') EMAIL, \n" +
                "    sm4_encrypt(EMAILPHONENUMBER, '" + Constants.SM4_KEY + "') EMAILPHONENUMBER, \n" +
                "    sm4_encrypt(BILLPHONENUMBER, '" + Constants.SM4_KEY + "') BILLPHONENUMBER, \n" +
                "    BILLNAME,\n" +
                "    BOOKPURCHASERNAME " +
                "    FROM TB_CUSTOMER" +
                "    WHERE ID > " + newMaxId
                ;
        TableResult result = tEnv.executeSql(extractSql);

        result.print();

    }

    private static long getNewMaxId() {

        // 查询强id对应的tid>2的数据，并查找出tid被挂载的所有强id
        Connection conn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.ODS_USER, Constants.ODS_PWD);
        Statement stmt = DorisUtils.getStatement(conn);
        ResultSet rs = DorisUtils.getDorisResult(stmt, "SELECT MAX(ID) AS ID FROM "+Constants.ODS_DB+ ".T_ODS_JSSC_TB_CUSTOMER");
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
}
