package org.linc.RPCServer;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * 本类通过 JDBC 连接远程服务器，并提供接口来执行 SQL 命令
 *
 * @author xwc
 * @version v1
 */
public class ConnectJDBC {
    private String msg;
    private int code;

    /**
     * 注册 JDBC，连接远程服务器，执行 SQL 语句，返回执行结果集
     *
     * @param sql  执行的 SQL 命令
     * @param conn 到远程服务器的 JDBC 连接
     * @return 执行结果集
     */
    public ResultSet getAndExecuteSQL(String sql, Connection conn) throws Exception {
        Statement stmt;
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
            for (int i = 1; i <= msgArray.length - 1; i++) {
                msg += msgArray[i];
            }
            code = 1;
            System.out.println("msg:" + msg + code);
            throw new Exception(msg);
        } finally {
            JDBCUtils.releaseAll();
        }
        System.out.println(rs.toString());

        return rs;
    }

    /**
     * 将规则转换后的数据再次转换成 JSON Object
     *
     * @param resultList SQL 执行结果集经过规则转换得到的新数据
     * @return 转换结果对应的 JSON 数据
     */
    public JSONObject convertArrayListToJsonObject(ArrayList<ArrayList<String>> resultList, final String logPath) throws Exception {
        JSONArray jsonArray = new JSONArray();
        for (ArrayList<String> dataRow : resultList) {
            JSONObject jsonObj = new JSONObject();
            LinkedHashMap linkedHashMap = new LinkedHashMap();
            for (int i = 0; i < dataRow.size(); ++i) {
                String resultOfOneRow = dataRow.get(i);
                String columnName = resultOfOneRow.split("\t")[0];
                String value = resultOfOneRow.split("\t").length == 2 ? resultOfOneRow.split("\t")[1] : "";

                int sameColumnsNum = 0;
                for (int j = 0; j < i; ++j) {
                    String anothercolumnName = dataRow.get(j).split("\t")[0];
                    if (anothercolumnName.equals(columnName)) {
                        sameColumnsNum++;
                    }
                }
                if (sameColumnsNum > 0) {
                    columnName = columnName + "_" + sameColumnsNum;
                }

                // jsonObj.put(columnName, value);
                linkedHashMap.put(columnName, value);
                jsonObj = new JSONObject(linkedHashMap);
            }
            jsonArray.put(jsonObj);
        }

        // Add field into JSON data
        JSONObject finalJsonObject = new JSONObject();
        if (code == 1) {
            try {
                finalJsonObject.put("code", code).put("msg", msg);
            } catch (JSONException e) {
                System.err.println("Err: 字段转换成 JSON 数据失败");
                return null;
            }
        } else {
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
