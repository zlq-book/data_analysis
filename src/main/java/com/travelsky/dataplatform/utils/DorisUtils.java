package com.travelsky.dataplatform.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.constans.Constants;

import java.sql.*;

/**
 * @author kuangaihua
 * @date 2025/7/28 9:56
 */
public class DorisUtils {
    public static void main(String[] args) throws ClassNotFoundException {
        Connection conn = getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
        Statement stmt = getStatement(conn);
        ResultSet rs = getDorisResult(stmt, "SELECT ERROR_TID,SAME_STRONGID,ETL_DATE,CREATE_TIME,UPDATE_TIME FROM DIM_TEST.T_DIM_IDMAPPING_ERROR ");
        try {
            while (rs.next()) {
                System.out.println("ERROR_TID:" + rs.getString("ERROR_TID"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        close(conn, stmt, rs);
    }

    public static Connection getConnection(String url, String user, String password) {
        Connection conn = null;
        try {
            Class.forName(Constants.MYSQL_DRIVER);
            conn = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static Statement getStatement(Connection conn) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stmt;
    }

    public static PreparedStatement prepareSql(Connection conn ,String sql) {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return preparedStatement;
    }

    public static void executeBatch(Connection conn ,PreparedStatement preparedStatement) {

        try {
            preparedStatement.executeBatch();
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ResultSet getDorisResult(Statement stmt, String querySql) {
        try {
            return stmt.executeQuery(querySql);
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                if (stmt.isClosed() || stmt.getConnection() == null || stmt.getConnection().isClosed()) {
                    Connection connin = getConnection(Constants.DORIS_JDBC_URL, Constants.ODS_USER, Constants.ODS_PWD);
                    Statement stmtin = getStatement(connin);
                    ResultSet result = stmtin.executeQuery(querySql);
                    close(connin, stmtin, null);
                    return result;
                }
            } catch (SQLException ex) {
                e.printStackTrace();
            }
            return null;
        }

    }

    public static ResultSet getDorisResultByType(Statement stmt, String querySql, String type) {
        try {
            return stmt.executeQuery(querySql);
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                // 检查原始连接是否有效
                if (stmt.isClosed() || stmt.getConnection() == null || stmt.getConnection().isClosed()) {
                    Connection connin = null;
                    Statement stmtin = null;

                    // 根据type类型选择不同的用户和密码
                    if (Constants.ODS_DB.equalsIgnoreCase(type)) {
                        connin = getConnection(Constants.DORIS_JDBC_URL, Constants.ODS_USER, Constants.ODS_PWD);
                    } else if (Constants.DIM_DB.equalsIgnoreCase(type)) {
                        connin = getConnection(Constants.DORIS_JDBC_URL, Constants.DIM_USER, Constants.DIM_PWD);
                    } else if (Constants.DWD_DB.equalsIgnoreCase(type)) {
                        connin = getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
                    } else {
                        return null;
                    }

                    if (connin != null) {
                        stmtin = getStatement(connin);
                        if (stmtin != null) {
                            ResultSet result = stmtin.executeQuery(querySql);
                            close(connin, stmtin, null);
                            return result;
                        }
                    }
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            return null;
        }
    }
/**
 * 查询doris结果，并封装到JSONArray
 * */
    public static JSONArray getDorisResultWithJsonArrayByType(Statement stmt, String querySql, String type) {
        try {
            ResultSet resultSet = stmt.executeQuery(querySql);
            JSONArray jsonArray = new JSONArray();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                JSONObject jsonObject = new JSONObject();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = resultSet.getObject(i);
                    jsonObject.put(columnName, value);
                }
                jsonArray.add(jsonObject);
            }
            resultSet.close();
            return jsonArray;

        } catch (SQLException e) {
            e.printStackTrace();
            try {
                // 检查原始连接是否有效
                if (stmt.isClosed() || stmt.getConnection() == null || stmt.getConnection().isClosed()) {
                    Connection connin = null;
                    Statement stmtin = null;

                    // 根据type类型选择不同的用户和密码
                    if (Constants.ODS_DB.equalsIgnoreCase(type)) {
                        connin = getConnection(Constants.DORIS_JDBC_URL, Constants.ODS_USER, Constants.ODS_PWD);
                    } else if (Constants.DIM_DB.equalsIgnoreCase(type)) {
                        connin = getConnection(Constants.DORIS_JDBC_URL, Constants.DIM_USER, Constants.DIM_PWD);
                    } else if (Constants.DWD_DB.equalsIgnoreCase(type)) {
                        connin = getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
                    } else {
                        return null;
                    }

                    if (connin != null) {
                        stmtin = getStatement(connin);
                        if (stmtin != null) {
                            ResultSet resultSet = stmt.executeQuery(querySql);
                            JSONArray jsonArray = new JSONArray();
                            ResultSetMetaData metaData = resultSet.getMetaData();
                            int columnCount = metaData.getColumnCount();

                            while (resultSet.next()) {
                                JSONObject jsonObject = new JSONObject();
                                for (int i = 1; i <= columnCount; i++) {
                                    String columnName = metaData.getColumnName(i);
                                    Object value = resultSet.getObject(i);
                                    jsonObject.put(columnName, value);
                                }
                                jsonArray.add(jsonObject);
                            }
                            close(connin, stmtin, resultSet);
                            return jsonArray;
                        }
                    }
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            return null;
        }
    }

    public static int excuteDorisInsert(Statement stmt, String excuteSql) {
        try {
            stmt.executeUpdate(excuteSql);
            return 0;
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                if (stmt.isClosed() || stmt.getConnection() == null || stmt.getConnection().isClosed()) {
                    Connection connin = getConnection(Constants.DORIS_JDBC_URL, Constants.ODS_USER, Constants.ODS_PWD);
                    Statement stmtin = getStatement(connin);
                    int result = stmtin.executeUpdate(excuteSql);
                    close(connin, stmtin, null);
                    return result;
                }
            } catch (SQLException ex) {
                e.printStackTrace();
            }
            return 0;
        }

    }

    public static void close(Connection conn, Statement stmt, ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 查询指定 Doris 表中指定列的最大值（Long 类型）
     * @param tableName 表名（需确保合法，防止 SQL 注入）
     * @param columnName 列名，默认为 "ID"，也可自定义
     */
    public static long queryMaxIdFromDoris(String tableName, String columnName) throws SQLException {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("表名不能为空");
        }
        if (columnName == null || columnName.trim().isEmpty()) {
            columnName = "ID"; // 默认列名
        }

        String url = "jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB;
        String sql = "SELECT MAX(" + columnName + ") AS max_id FROM " + tableName;

        try (Connection conn = DriverManager.getConnection(url, Constants.ODS_USER, Constants.ODS_PWD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                long maxId = rs.getLong("max_id");
                return rs.wasNull() ? 0 : maxId;
            }
        }
        return 0;
    }

    // 重载方法：默认查询 ID 列
    public static long queryMaxIdFromDoris(String tableName) throws SQLException {
        return queryMaxIdFromDoris(tableName, "ID");
    }

}
