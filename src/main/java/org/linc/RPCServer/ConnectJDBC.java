package org.linc.RPCServer;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.*;
import java.util.ArrayList;

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
    public  ResultSet getAndExucuteSQL(String sql,Connection conn) throws Exception {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            System.out.println(Thread.currentThread().getName() + ": creating statement...");                   // 注册获得连接 getconnection
            stmt = conn.createStatement();
        } catch (SQLException e) {
            code = 1;
            msg = "there are some problems with the driver.";
            throw new Exception(msg);
        }

        try {
            rs = stmt.executeQuery(sql);                //执行sql，获取结果集
            System.out.println(Thread.currentThread().getName() + " executed statement...");
        } catch (SQLException e) {
            msg = "";
            String msgArray[] = e.getMessage().split(":");                  //只取错误信息的后面一段,把冒号分割的第一段省略
            for(int i = 1; i <= msgArray.length - 1; i++){
                msg += msgArray[i];
            }
            code = 1 ;
            System.out.println("msg:"+msg+code);
            throw new Exception(msg);
        }finally {
            JDBCUtils.releaseAll();
        }
        System.out.println(rs.toString());

        return rs;
    }


    /**
     * this method transform the resultset into json object ,and return a jsonArray.
     * @param rs  the resultset
     * @return JSONArray
     * */
   /* public  JSONObject transformToJsonArray(ResultSet rs) throws JSONException {

        JSONArray array = new JSONArray();              //建立jsonArray数组封装所有的resultset信息
        JSONObject wholeJsonObj = null;     //wholeArray封装所有包含时间，大小，返回码，返回信息的Json
        ResultSetMetaData metaData = null;
        if(rs !=null) {
            try {

                int columnCount = 0;            //定义列数
                metaData = rs.getMetaData();        //获取列数
                columnCount = metaData.getColumnCount();

                while (rs.next()) {     //遍历每条数据
                    JSONObject jsonObj = new JSONObject();      //创建json存放resultset
                    for (int i = 1; i <= columnCount; i++) {        // 遍历每一列
                        String columnName = metaData.getColumnLabel(i);     //获得columnName对应的值
                        String value = rs.getString(columnName);
                        jsonObj.put(columnName, value);
                    }
                    array.put(jsonObj);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        wholeJsonObj = new JSONObject();               //wholeJsonObj用于存放最终返回数据

        GetSQLTimeAndInput getSQLInfo = new GetSQLTimeAndInput();       //获取sqlJob的time和input,返回json数据
        JSONArray sqlJobinfo = getSQLInfo.getLastJobInfo(LOGPATH);     //获取job日志
        JSONObject timeAndInputInfo = getSQLInfo.getJobTimeAndInput(sqlJobinfo);        //从日志中解析出time和input返回json数据
        Object time = timeAndInputInfo.get("time");
        Object size = timeAndInputInfo.get("size");

        if(code == 1)       //code为0则正常输出，为10000则异常输出
            wholeJsonObj.put("code",code).put("msg",msg);
            //这里将result,time,code,msg,size数据封装进wholeArray
        else
            msg = "success";
        wholeJsonObj.put("result", array).put("time",time.toString()).put("size",size.toString()).put("code",code).put("msg",msg);
        return wholeJsonObj;
    }*/

    /**
     * 将规则转换后的数据再次转换成 JSON Object
     * @param resultList
     * @return
     */
    public JSONObject convertArrayListToJsonObject(ArrayList<ArrayList<String>> resultList,final String logPath) throws Exception {
        JSONArray jsonArray = new JSONArray();
        for(ArrayList<String> dataRow: resultList){
            JSONObject jsonObj = new JSONObject();
            for(int i = 0; i < dataRow.size(); ++i){
                String resultOfOneRow = dataRow.get(i);
                String columnName = resultOfOneRow.split("\t")[0];
                String value = resultOfOneRow.split("\t").length == 2 ? resultOfOneRow.split("\t")[1] : "";

                // TODO: 同名列的处理
                int sameColumnsNum = 0;
                for(int j = 0; j < i; ++j){
                    String anothercolumnName = dataRow.get(j).split("\t")[0];
                    if(anothercolumnName.equals(columnName)){
                        sameColumnsNum ++;
                    }
                }
                if(sameColumnsNum > 0){
                    columnName = columnName + "_" + sameColumnsNum;
                }

                try {
                    jsonObj.put(columnName, value);
                } catch (JSONException e) {
                    throw new Exception("字段转换成 JSON 数据失败");
                }
            }
            jsonArray.put(jsonObj);
        }

        // Add field into JSON data
        JSONObject finalJsonObject = new JSONObject();
        if(code == 1) {
            try {
                finalJsonObject.put("code", code).put("msg", msg);
            } catch (JSONException e) {
                System.err.println("Err: 字段转换成 JSON 数据失败");
                return null;
            }
        }
        else {
            msg = "success";
        }
        try {
            GetSQLTimeAndInput getSQLInfo = new GetSQLTimeAndInput();       //获取sqlJob的time和input,返回json数据
            JSONArray sqlJobInfo = getSQLInfo.getLastJobInfo(logPath);     //获取job日志
            JSONObject timeAndInputInfo = getSQLInfo.getJobTimeAndInput(sqlJobInfo);        //从日志中解析出time和input返回json数据
            Object time = timeAndInputInfo.get("time");
            Object size = timeAndInputInfo.get("size");
            finalJsonObject.put("result", jsonArray).put("time", time.toString()).put("size", size.toString()).put("code", code).put("msg", msg);
        } catch (JSONException e) {
            System.err.println("Err: 构造 JSONObject 失败");
            return null;
        }
        return finalJsonObject;
    }
}
