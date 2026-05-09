package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
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
public class ExtractTOdsWxapCheckinTrpOrderPassenger {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        String sm4key = Constants.SM4_KEY;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsWxapCheckinTrpOrderPassenger");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        DATAStream 抽取方式先不考虑
/*        // 配置Oracle连接参数
        String driver = "oracle.jdbc.driver.OracleDriver";
        String url = "jdbc:oracle:thin:@//10.31.0.5:50081/weixin";
        String username = "scwxh5_etl";
        String password = "";
        RowTypeInfo rowType = new RowTypeInfo(
                Types.BIG_DEC,    // id字段
                Types.STRING  // order_no字段
        );*/
        /*// 创建JDBC输入格式
        JdbcInputFormat inputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername(driver)
                .setDBUrl(url)
                .setUsername(username)
                .setPassword(password)
                .setRowTypeInfo(rowType)
                .setQuery("select ID,ORDER_NO from scwxh5.CHECKIN_TRP_ORDER cto ")
                .finish();
        DataStreamSource<Row> source = env.createInput(inputFormat);
//        source.print();
*//*        SingleOutputStreamOperator<Row> map = source.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) throws Exception {
                int ID = (int) (double) value.getField("ID");
                String ORDER_NO = (String) value.getField("ORDER_NO");
//                Row row = new Row();
//
//                jsonObject.put("ID",ID);
//                jsonObject.put("ORDER_NO",ORDER_NO);
                return null;
            }
        });*//*
        tEnv.createTemporaryView("CHECKIN_TRP_ORDER", source, $("ID"), $("ORDER_NO"));*/
// 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);

//创建上游数据源表
        tEnv.executeSql("CREATE TABLE CHECKIN_TRP_ORDER_PASSENGER  (\n" +
                "ID BIGINT, " +
                "ORDER_NO VARCHAR(96), " +
                "FLIGHT_NO VARCHAR(36), " +
                "FLIGHT_DATE VARCHAR(30), " +
                "DEP VARCHAR(15), " +
                "ARR VARCHAR(15), " +
                "TICKET_NO VARCHAR(96), " +
                "SEAT_NO VARCHAR(24), " +
                "PSR_NAME VARCHAR(192), " +
                "SEAT_TYPE TINYINT, " +
                "SEAT_AMOUNT BIGINT, " +
                "DEP_TIME VARCHAR(24), " +
                "ARR_TIME VARCHAR(24), " +
                "EMD_NO VARCHAR(96), " +
                "SEAT_STATUS TINYINT, " +
                "PNR VARCHAR(36), " +
                "TRP_ORDER_NO VARCHAR(96), " +
                "SHARE_FLIGHT_NO VARCHAR(36), " +
                "ASR_SEAT_NO VARCHAR(24), " +
                "CREATE_TIME TIMESTAMP(6), " +
                "UPDATE_TIME TIMESTAMP(6), " +
                "PSR_TYPE VARCHAR(48), " +
                "ARR_DATE VARCHAR(30), " +
                "DURATION BIGINT" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.WXAP_IP + ":" + Constants.WXAP_PORT + "/" + Constants.WXAP_DB + "',\n" +
                "    'table-name' = '" + Constants.WXAP_SCHEMA + ".CHECKIN_TRP_ORDER_PASSENGER ', \n" +
                "    'username' = '" + Constants.WXAP_USER + "',\n" +
                "    'password' = '" + Constants.WXAP_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_WXAP_CHECKIN_TRP_ORDER_PASSENGER  (\n" +
                "ETL_DATE DATE, " +
                "ID BIGINT, " +
                "ORDER_NO VARCHAR(96), " +
                "FLIGHT_NO VARCHAR(36), " +
                "FLIGHT_DATE VARCHAR(30), " +
                "DEP VARCHAR(15), " +
                "ARR VARCHAR(15), " +
                "TICKET_NO VARCHAR(96), " +
                "SEAT_NO VARCHAR(24), " +
                "PSR_NAME VARCHAR(192), " +
                "SEAT_TYPE TINYINT, " +
                "SEAT_AMOUNT BIGINT, " +
                "DEP_TIME VARCHAR(24), " +
                "ARR_TIME VARCHAR(24), " +
                "EMD_NO VARCHAR(96), " +
                "SEAT_STATUS TINYINT, " +
                "PNR VARCHAR(36), " +
                "TRP_ORDER_NO VARCHAR(96), " +
                "SHARE_FLIGHT_NO VARCHAR(36), " +
                "ASR_SEAT_NO VARCHAR(24), " +
                "CREATE_TIME TIMESTAMP(6), " +
                "UPDATE_TIME TIMESTAMP(6), " +
                "PSR_TYPE VARCHAR(48), " +
                "ARR_DATE VARCHAR(30), " +
                "DURATION BIGINT" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_WXAP_CHECKIN_TRP_ORDER_PASSENGER',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        String extractSql = "INSERT INTO T_ODS_WXAP_CHECKIN_TRP_ORDER_PASSENGER(" +
                "ETL_DATE\n" +
                ",ID\n" +
                ",ORDER_NO\n" +
                ",FLIGHT_NO\n" +
                ",FLIGHT_DATE\n" +
                ",DEP\n" +
                ",ARR\n" +
                ",TICKET_NO\n" +
                ",SEAT_NO\n" +
                ",PSR_NAME\n" +
                ",SEAT_TYPE\n" +
                ",SEAT_AMOUNT\n" +
                ",DEP_TIME\n" +
                ",ARR_TIME\n" +
                ",EMD_NO\n" +
                ",SEAT_STATUS\n" +
                ",PNR\n" +
                ",TRP_ORDER_NO\n" +
                ",SHARE_FLIGHT_NO\n" +
                ",ASR_SEAT_NO\n" +
                ",CREATE_TIME\n" +
                ",UPDATE_TIME\n" +
                ",PSR_TYPE\n" +
                ",ARR_DATE\n" +
                ",DURATION\n)  " +
                "SELECT CAST('" + etlDate + "' AS DATE) ETL_DATE\n" +
                ",ID\n" +
                ",ORDER_NO\n" +
                ",FLIGHT_NO\n" +
                ",FLIGHT_DATE\n" +
                ",DEP\n" +
                ",ARR\n" +
                ",TICKET_NO\n" +
                ",SEAT_NO\n" +
                ",PSR_NAME\n" +
                ",SEAT_TYPE\n" +
                ",SEAT_AMOUNT\n" +
                ",DEP_TIME\n" +
                ",ARR_TIME\n" +
                ",EMD_NO\n" +
                ",SEAT_STATUS\n" +
                ",PNR\n" +
                ",TRP_ORDER_NO\n" +
                ",SHARE_FLIGHT_NO\n" +
                ",ASR_SEAT_NO\n" +
                ",CREATE_TIME\n" +
                ",UPDATE_TIME\n" +
                ",PSR_TYPE\n" +
                ",ARR_DATE\n" +
                ",DURATION\n" +
                " FROM CHECKIN_TRP_ORDER_PASSENGER "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' " +
                " OR  UPDATE_TIME BETWEEN TIMESTAMP '" + etlDate + " 00:00:00' AND TIMESTAMP '" + etlDate + " 23:59:59.999' "
                ;
        TableResult result = tEnv.executeSql(extractSql);

        result.print();


    }

}
