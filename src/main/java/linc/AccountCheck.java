package linc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * this class will check the username and password .
 * @author xwc
 * @version v1
 * Created by admin on 2015/3/18.
 */
public class AccountCheck {

    public static String checkAccount(String username,String password,java.sql.Connection con){
        String result="";

        //先判断用户名密码错误情况
        if(username=="" || password=="" ){
         System.out.println("please enter valid username or password");
            return "请输入有效账号与密码";
        }

        //如果用户名密码格式正确，则扫描判断。
        String checkSql = "select username,password from user";
        ConnectJDBC conn = new ConnectJDBC();
        //执行SQL语句
        ResultSet rs = conn.getAndExucuteSQL(checkSql,con);

        //设置标签flag，默认不存在（false）
        Boolean flag =false;
        try {
            while(rs.next()){
                if(username.equals(rs.getString("username")) && password.equals(rs.getString("password"))) {
                    flag = true;
                    break;
                }
            }
            //根据flag判断
            if(flag == true)
                result="ok";
            else
                result = "no";
        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
            JDBCUtils.releaseAll();
        }
        return result;
    }
}
