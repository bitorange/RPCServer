package linc;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.*;

/**
 * this class is used to connect the jdbc and execute SQL command.
 * @author  xwc
 * @version v1
 * Created by admin on 2015/3/16.
 */
public class ConnectJDBC {


     private String msg;
     private int code;

    /**
     * the method include register driver,get connection and execute sql, then return resultset
     *@param sql
     *@return resultset the Resultset of execute sql command
     **/
    public  ResultSet getAndExucuteSQL(String sql,Connection conn){
        Statement stmt = null;
        ResultSet rs = null;
        try {
            // 注册获得连接 getconnection
//            conn = JDBCUtils.getConnection();
            System.out.println(Thread.currentThread().getName() + ": creating statement...");
            stmt = conn.createStatement();
        } catch (SQLException e) {
            code = 1;
            msg = "there are some problems with the driver.";
        }

        //执行sql，获取结果集
        try {
            rs = stmt.executeQuery(sql);
            System.out.println(Thread.currentThread().getName() + " executed statement...");
        } catch (SQLException e) {
            msg = "";
            //只取错误信息的后面一段,把冒号分割的第一段省略
            String msgArray[] = e.getMessage().split(":");
            for(int i = 1; i <= msgArray.length - 1; i++){
                msg += msgArray[i];
            }
            code = 1 ;
        }finally {
            JDBCUtils.releaseAll();
        }
        return rs;
    }


    /**
     * this method transform the resultset into json object ,and return a jsonArray.
     * @param rs  the resultset
     * @return JSONArray
     * */
    public  JSONObject transformToJsonArray(ResultSet rs) throws JSONException {

        //建立jsonArray数组封装所有的resultset信息
        JSONArray array = new JSONArray();
        /*  //wholeArray封装所有包含时间，大小，返回码，返回信息的Json
        JSONArray wholeArray = null;*/
        JSONObject wholeJsonObj = null;
        ResultSetMetaData metaData = null;
        if(rs !=null) {
            try {
                //定义列数
                int columnCount = 0;
                //获取列数
                metaData = rs.getMetaData();
                columnCount = metaData.getColumnCount();

                //遍历每条数据
                while (rs.next()) {
                    //创建json存放resultset
                    JSONObject jsonObj = new JSONObject();
                    // 遍历每一列
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnLabel(i);
                        //获得columnName对应的值
                        String value = rs.getString(columnName);
                        jsonObj.put(columnName, value);
                    }
                    array.put(jsonObj);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        //wholeJsonObj用于存放最终返回数据
        wholeJsonObj = new  JSONObject();
        //code为0则正常输出，为10000则异常输出
        if(code == 1)
            wholeJsonObj.put("code",code).put("msg",msg);
            //这里将result,time,code,msg,size数据封装进wholeArray
        else
            msg = "success";
        wholeJsonObj.put("result", array).put("time","20s").put("size","2M").put("code",code).put("msg",msg);
        return wholeJsonObj;
    }
}
