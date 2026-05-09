package com.travelsky.dataplatform.main.ods.v2.sc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.TOdsScTbTicketPriceBase;
import com.travelsky.dataplatform.source.OracleDBSourceByGbkFunction;
import com.travelsky.dataplatform.source.OracleDBSourceFunction;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.DorisUtils;
import com.travelsky.trp.usercenter.data.analysis.transform.hsd.CommonOutputTags;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author
 * @date 2025/7/2 9:31
 */
public class ExtractTOdsScTbTicketPriceBase {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
//        String etlDate = "2025-11-22";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsScTbTicketPriceBase");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();

        String url = "jdbc:oracle:thin:@//" + Constants.SDEXT_IP + ":" + Constants.SDEXT_PORT + "/" + Constants.SDEXT_DB;
        String user = Constants.SDEXT_USER;
        String password = Constants.SDEXT_PWD;
        String query =  "SELECT \n" +
                "AIR_CODE,\n" +
                "EX_DATE,\n" +
                "UP_LOCATION,\n" +
                "DIS_LOCATION,\n" +
                "PRICE_ONEWAY,\n" +
                "PRICE_TOWWAY,\n" +
                "START_DATE,\n" +
                "END_DATE,\n" +
                "SEAT_TYPE,\n" +
                "OUT_LINE , \n" +
                "CMD , \n" +
                "DIST\n" +
                " FROM " + Constants.SDEXT_SCHEMA + ".TB_TICKET_PRICE_BASE ttpb "
                ;
        String columStr = "AIR_CODE,EX_DATE,UP_LOCATION,DIS_LOCATION,PRICE_ONEWAY,PRICE_TOWWAY,START_DATE,END_DATE,SEAT_TYPE,OUT_LINE,CMD,DIST";
        String[] columnStrs = columStr.split(",");
        List<String> columns = Arrays.stream(columnStrs).collect(Collectors.toList());
        OracleDBSourceByGbkFunction function = new OracleDBSourceByGbkFunction(url, user, password, query, columns);
        DataStreamSource<JSONObject> jsonObjectDataStreamSource = env.addSource(function);

        SingleOutputStreamOperator<TOdsScTbTicketPriceBase> tSource = jsonObjectDataStreamSource.map(jsonObject -> {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            TOdsScTbTicketPriceBase tOdsLyxOrderBase = mapper.readValue(JSONObject.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue), TOdsScTbTicketPriceBase.class);
            return tOdsLyxOrderBase;
        });
        tSource.print();
        Schema schema = Schema.newBuilder()
                .column("AIR_CODE", DataTypes.VARCHAR(30))
                .column("EX_DATE", DataTypes.VARCHAR(10))
                .column("UP_LOCATION", DataTypes.VARCHAR(10))
                .column("DIS_LOCATION", DataTypes.VARCHAR(10))
                .column("PRICE_ONEWAY", DataTypes.DOUBLE())
                .column("PRICE_TOWWAY", DataTypes.DOUBLE())
                .column("START_DATE", DataTypes.VARCHAR(10))
                .column("END_DATE", DataTypes.VARCHAR(10))
                .column("SEAT_TYPE", DataTypes.VARCHAR(1))
                .column("OUT_LINE", DataTypes.VARCHAR(240))
                .column("CMD", DataTypes.VARCHAR(12))
                .column("DIST", DataTypes.BIGINT())
                .build();
        // 注册为临时视图
        tEnv.createTemporaryView("TB_TICKET_PRICE_BASE", tSource, schema);


//创建上游数据源表
        tEnv.executeSql("CREATE TABLE TB_TICKET_PRICE_BASE (\n" +
                "AIR_CODE VARCHAR(30), \n" +
                "EX_DATE VARCHAR(10), \n" +
                "UP_LOCATION VARCHAR(30), \n" +
                "DIS_LOCATION VARCHAR(30), \n" +
                "PRICE_ONEWAY DECIMAL(8,2), \n" +
                "PRICE_TOWWAY DECIMAL(8,2), \n" +
                "START_DATE VARCHAR(10), \n" +
                "END_DATE VARCHAR(10), \n" +
                "SEAT_TYPE VARCHAR(1), \n" +
                "OUT_LINE VARCHAR(240), \n" +
                "CMD VARCHAR(12), \n" +
                "DIST BIGINT \n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//"+Constants.SDEXT_IP+":"+Constants.SDEXT_PORT+"/"+Constants.SDEXT_DB+"',\n" +
                "    'table-name' = '"+Constants.SDEXT_SCHEMA+".TB_TICKET_PRICE_BASE', \n" +
                "    'username' = '"+Constants.SDEXT_USER+"',\n" +
                "    'password' = '"+Constants.SDEXT_PWD+"'\n" +
                ",\n" + "  'driver' = '"+Constants.ORACLE_DRIVER+"'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_SC_TB_TICKET_PRICE_ORDER (\n" +
                "    ETL_DATE DATE,\n" +
                "AIR_CODE VARCHAR(30), \n" +
                "EX_DATE VARCHAR(10), \n" +
                "UP_LOCATION VARCHAR(30), \n" +
                "DIS_LOCATION VARCHAR(30), \n" +
                "PRICE_ONEWAY DECIMAL(8,2), \n" +
                "PRICE_TOWWAY DECIMAL(8,2), \n" +
                "START_DATE VARCHAR(10), \n" +
                "END_DATE VARCHAR(10), \n" +
                "SEAT_TYPE VARCHAR(1), \n" +
                "OUT_LINE VARCHAR(240), \n" +
                "CMD VARCHAR(12), \n" +
                "DIST BIGINT \n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_SC_TB_TICKET_PRICE_ORDER',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        //数据抽取sql，配置增量字段、数据加密等
        String extractSql = "INSERT INTO T_ODS_SC_TB_TICKET_PRICE_ORDER(" +
                "ETL_DATE,\n" +
                "AIR_CODE,\n" +
                "EX_DATE,\n" +
                "UP_LOCATION,\n" +
                "DIS_LOCATION,\n" +
                "PRICE_ONEWAY,\n" +
                "PRICE_TOWWAY,\n" +
                "START_DATE,\n" +
                "END_DATE,\n" +
                "SEAT_TYPE,\n" +
                "OUT_LINE , \n" +
                "CMD , \n" +
                "DIST)\n" +
                "SELECT " +
                "CAST('" + etlDate + "' AS DATE) ETL_DATE," +
                "AIR_CODE,\n" +
                "EX_DATE,\n" +
                "UP_LOCATION,\n" +
                "DIS_LOCATION,\n" +
                "PRICE_ONEWAY,\n" +
                "PRICE_TOWWAY,\n" +
                "START_DATE,\n" +
                "END_DATE,\n" +
                "SEAT_TYPE,\n" +
                "OUT_LINE , \n" +
                "CMD , \n" +
                "DIST\n" +
                " FROM TB_TICKET_PRICE_BASE "
                ;

        TableResult result = tEnv.executeSql(extractSql);

        result.await();

        deleteOld(tEnv, etlDate);


    }

    private static void deleteOld(StreamTableEnvironment tEnv,String etlDate) throws Exception {
        // 删除之前日期
        // 从 Doris 表中读取数据
        Connection conn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.ODS_USER, Constants.ODS_PWD);
        Statement stmt = DorisUtils.getStatement(conn);
        String querySql =  "SELECT DISTINCT ETL_DATE from " + Constants.ODS_DB + ".T_ODS_SC_TB_TICKET_PRICE_ORDER ORDER BY ETL_DATE DESC";
        ResultSet rs = DorisUtils.getDorisResult(stmt, querySql);
        boolean flag = false;
        try {
            while (rs.next()) {
                String date = rs.getString("ETL_DATE");
                if (etlDate.equals(date)) {
                    flag = true;
                } else {
                    if (flag) {
                        stmt = DorisUtils.getStatement(conn);
                        String deleteSql = "DELETE FROM " + Constants.ODS_DB + ".T_ODS_SC_TB_TICKET_PRICE_ORDER" +
                                " WHERE ETL_DATE <'"+ etlDate +"'";
                        DorisUtils.excuteDorisInsert(stmt, deleteSql);
                    }
                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        DorisUtils.close(conn, stmt, rs);


    }
}
