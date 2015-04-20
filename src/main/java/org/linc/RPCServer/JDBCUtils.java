package org.linc.RPCServer;

import java.sql.*;

/**
 * 本类用于连接远程虎踞库
 *
 * @author xwc
 * @version v1
 */
public class JDBCUtils {
    /**
     * 注册驱动
     *
     * @param driverClass 驱动所需要的类
     */
    public static void loadDriver(String driverClass) {
        try {
            System.out.println(Thread.currentThread().getName() + ": registering driver");
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("some problems with the driver");
        }
    }

    /**
     * 获取连接对象
     *
     * @param url      远程数据库地址
     * @param username 数据库用户名
     * @param password 数据库密码
     * @return 连接远程数据库的 JDBC 对象
     */
    public Connection getConnection(String url, String username, String password) {
        Connection conn = null;
        try {
            System.out.println(Thread.currentThread().getName() + ": getting connection");
            conn = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }


    /**
     * 关闭 statement
     *
     * @param stmt smtm 对象
     */
    public static void stmtRelease(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
                System.out.println("released stmt");
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                stmt = null;
            }
        }
    }

    /**
     * 关闭连接
     *
     * @param conn 连接远程数据库的 JDBC 对象
     */
    public static void connRelease(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
                System.out.println("released connection");
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                conn = null;
            }
        }
    }

    /**
     * 销毁结果集
     *
     * @param rs 结果集
     */
    public static void rsRelease(ResultSet rs) {
        if (rs != null)
            try {
                rs.close();
                System.out.print("released stmt");
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                rs = null;
            }
    }

    /**
     * 释放所有的连接与连接对象
     */
    public static void releaseAll() {
        // rsRelease(rs);
        // stmtRelease(stmt);
        // connRelease(conn);
    }
}
