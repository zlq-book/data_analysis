package com.travelsky.dataplatform.utils;

import com.travelsky.dataplatform.constans.Constants;

import java.sql.*;

public class DmDbUtils {
    public static Connection getConnection(String url, String user, String password) {
        Connection conn = null;
        try {
            Class.forName(Constants.DM_DRIVER);
            conn = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static ResultSet getDmResult(Statement stmt, String querySql) {
        try {
            return stmt.executeQuery(querySql);
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                if (stmt.isClosed() || stmt.getConnection() == null || stmt.getConnection().isClosed()) {
                    Connection connin = getConnection(Constants.DM_JDBC_URL, Constants.DM_USER, Constants.DM_PWD);
                    Statement stmtin = getStatement(connin);
                    ResultSet result = stmtin.executeQuery(querySql);
                    DorisUtils.close(connin, stmtin, null);
                    return result;
                }
            } catch (SQLException ex) {
                e.printStackTrace();
            }
            return null;
        }
    }

    public static int executeUpdate(Statement stmt, String sql) {
        try {
            stmt.executeUpdate(sql);
            return 0;
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                if (stmt.isClosed() || stmt.getConnection() == null || stmt.getConnection().isClosed()) {
                    Connection connin = getConnection(Constants.DM_JDBC_URL, Constants.DM_USER, Constants.DM_PWD);
                    Statement stmtin = getStatement(connin);
                    int result = stmtin.executeUpdate(sql);
                    DorisUtils.close(connin, stmtin, null);
                    return result;
                }
            } catch (SQLException ex) {
                e.printStackTrace();
            }
            return 0;
        }

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
}
