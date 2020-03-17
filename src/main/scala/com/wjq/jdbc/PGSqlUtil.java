package com.wjq.jdbc;

import java.sql.*;

/**
 * PostgreatSql
 */
public class PGSqlUtil {
    private Connection conn = null;
    private Statement stmt = null;
    private String url;
    private String username;
    private String password;
    {
        try {

            conn = DriverManager.getConnection(url, username, password);
            conn.setAutoCommit(false);
            // 读取百万，千万级别的数据量
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.FETCH_FORWARD);
            stmt.setFetchSize(100000);
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        PGSqlUtil pg = new PGSqlUtil();
        try {
            // 执行sql
            ResultSet rs = pg.stmt.executeQuery("sql");
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
