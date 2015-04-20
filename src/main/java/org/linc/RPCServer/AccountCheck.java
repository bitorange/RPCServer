package org.linc.RPCServer;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 本类用于检查用户名和密码的正确性
 *
 * @author xwc
 * @version v1
 */
public class AccountCheck {

    /**
     * 检测账号密码的正确性
     *
     * @param username 用户名
     * @param password 密码
     * @param con      到远程服务器的 JDBC 连接
     * @return 返回检测结果
     */
    public static String checkAccount(String username, String password, java.sql.Connection con) {
        String result = "";

        if (username == "" || password == "") { // 先判断用户名密码错误情况
            System.out.println("please enter valid username or password");
            return "请输入有效账号与密码";
        }

        String checkSql = "select username,password from user"; // 如果用户名密码格式正确，则扫描判断。
        ConnectJDBC conn = new ConnectJDBC();
        ResultSet rs;   // 执行SQL语句
        try {
            rs = conn.getAndExecuteSQL(checkSql, con);
        } catch (Exception e) {
            return e.getMessage();
        }


        Boolean flag = false;   // 设置标签flag，默认不存在（false）
        try {
            while (rs.next()) {
                if (username.equals(rs.getString("username")) && password.equals(rs.getString("password"))) {
                    flag = true;
                    break;
                }
            }
            if (flag == true) // 根据flag判断
                result = "ok";
            else
                result = "no";
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtils.releaseAll();
        }
        return result;
    }
}
