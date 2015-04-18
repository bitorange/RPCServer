package org.linc.RPCServer;

import java.sql.*;
import java.util.ResourceBundle;

/**
 *
 * This class is used to connect the db.
 * @author xwc
 * @version v1
 *
 * Created by xwc on 2015/3/16.
 */
public class JDBCUtils {
    /**
     * 1、db 驱动
     * 2、jdbc 连接
     * 3、释放 stmt
     * 4、释放 conn
     * 5、释放 resultset
    **/
   /* public static String driverClass;
    public static String url;
    public static String username;
    public static String password;
    static Statement stmt = null;
    static ResultSet rs = null;*/

   /* static{
        driverClass = ResourceBundle.getBundle("db").getString("driverClass");
        url = ResourceBundle.getBundle("db").getString("url");
        username = ResourceBundle.getBundle("db").getString("username");
        password = ResourceBundle.getBundle("db").getString("password");
    }*/

    /**
     * register the driver
     *
     * */
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
     * get the connection object
     * @return connection object
     * */
    public  Connection getConnection(String url,String username,String password){
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
     * close the statement.
     * @param stmt stmt is the stmt object
     */

    public static void stmtRelease(Statement stmt ){
        if(stmt!=null) {
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
     * close the connection
     * @param conn  conn is the connection object
     * */
    public static void connRelease(Connection conn ){
        if(conn!=null) {
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
     * close the resultset
     * @param rs  rs is the result of executed sql
     * */
    public static void rsRelease(ResultSet rs  ){
        if(rs!=null)
            try{
                rs.close();
                System.out.print("released stmt");
            }
            catch(SQLException e){
                e.printStackTrace();
            }finally {
                rs = null;
            }
    }

    /**
     * release all connection including rs, stmt, conn.
     * */
    public static void releaseAll(){
        // rsRelease(rs);
        // stmtRelease(stmt);
        // connRelease(conn);
    }
}
