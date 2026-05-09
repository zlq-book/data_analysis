package com.travelsky.dataplatform.source;


import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;
import dm.jdbc.driver.DmdbTimestamp;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.*;
import java.util.List;

/**
 * @author kuangaihua
 * @date 2025/7/4 16:44
 */
public class OracleDBSourceFunction extends RichParallelSourceFunction<JSONObject> {
    private volatile boolean isRunning = true;
    private String url;
    private String user;
    private String password;
    private String query;
    private List<String> columns;
    private Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    public OracleDBSourceFunction(String url, String user, String password, String query, List<String> columns) {
        this.isRunning = isRunning;
        this.url = url;
        this.user = user;
        this.password = password;
        this.query = query;
        this.columns = columns;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(Constants.ORACLE_DRIVER); //  JDBC 驱动
        conn = DriverManager.getConnection(url, user, password);
    }

    public OracleDBSourceFunction(String url, String user, String password, String query) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.query = query;
    }

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        super.setRuntimeContext(t);
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return super.getRuntimeContext();
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        return super.getIterationRuntimeContext();
    }

    @Override
    public void close() throws Exception {
        closeResources(conn, stmt, rs); // 关闭资源

    }

    @Override
    public void run(SourceContext<JSONObject> ctx) throws Exception {

        try {

            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                JSONObject jsonObj = new JSONObject();
                for (int i = 1; i <= columnCount; i++) {
                    String colName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);

                    // 特殊处理 时间戳类型
                    if (value instanceof Timestamp) {
                        value = ((Timestamp) value).getTime();
                    }
                    jsonObj.put(colName, value);

                }
                ctx.collect(jsonObj);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "DMDBSourceFunction1{" +
                "isRunning=" + isRunning +
                ", url='" + url + '\'' +
                ", user='" + user + '\'' +
                ", password='" + password + '\'' +
                ", query='" + query + '\'' +
                ", columns=" + columns +
                '}';
    }

    @Override
    public void cancel() {
        isRunning = false; // 停止数据读取
        closeResources(conn, stmt, rs); // 关闭资源
    }

    private void closeResources(Connection conn, PreparedStatement stmt, ResultSet rs) {
        try {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
            if (conn != null) conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
