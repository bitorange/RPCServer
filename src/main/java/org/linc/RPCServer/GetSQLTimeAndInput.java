package org.linc.RPCServer;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * 获取某一 SQL 语句对应 HDFS 读写大小和 SQL 执行时间
 * Created by xwc on 2015/3/27.
 */
public class GetSQLTimeAndInput {
    /**
     * 获取 JobInfo 并将其转换成 JsonObject 对象
     *
     * @param path 日志文件路径
     * @return JSONObject JobInfo 转换而成的 JsonObject 对象
     */
    public JSONArray getLastJobInfo(String path) {
        JSONObject jsonObject;
        JSONArray jsonArray = new JSONArray();
        RandomAccessFile rf = null;
        try {
            rf = new RandomAccessFile(path, "r");
            long len = rf.length();
            long start = rf.getFilePointer();
            long nextEnd = start + len - 1;
            String line;
            rf.seek(nextEnd);
            int c;
            while (nextEnd > start) {
                c = rf.read();
                if (c == '\n' || c == '\r') {
                    line = rf.readLine();
                    if (line == null) {   // 处理文件末尾是空行这种情况
                        nextEnd--;
                        rf.seek(nextEnd);
                        continue;
                    }
                    jsonObject = new JSONObject(line);
                    jsonArray.put(jsonObject);
                    if (line.contains("SparkListenerJobStart")) {
                        break;
                    }
                    //  System.out.println(line);
                    nextEnd--;
                }
                nextEnd--;
                rf.seek(nextEnd);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            System.out.println("JSON 有问题");
        } finally {
            try {
                assert rf != null;
                rf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return jsonArray;
    }

    /**
     * 获取任务读写 HDFS 的大小
     *
     * @param jsonArray JobInfo
     * @return 包含读写 HDFS 大小的 JSONObject 对象
     */
    public JSONObject getJobTimeAndInput(JSONArray jsonArray) {
        JSONObject jsonObject;
        Long totalTime = 0L;
        Long totalSize = 0L;
        for (int i = 0; i < jsonArray.length(); i++) {       //循环，拿出size和time信息
            Long executeTime;
            try {
                jsonObject = jsonArray.getJSONObject(i);   //从头获取jsonArray中的对象
                Object obj = jsonObject.get("Event");
                if (obj.toString().equals("SparkListenerTaskEnd")) {       //如果取出的数据是SparkListenerTaskEnd，则继续获取其中的其他信息
                    JSONObject taskInfo = (JSONObject) jsonObject.get("Task Info");      //获取单个task的运行时间time
                    Long startTime = (Long) taskInfo.get("Launch Time");
                    Long stopTime = (Long) taskInfo.get("Finish Time");
                    executeTime = stopTime - startTime;
                    totalTime = totalTime + executeTime;    //将每个task的执行时间加入到总时间中

                    JSONObject taskMetrics = (JSONObject) jsonObject.get("Task Metrics");      //获取单个task的运行吞吐量size
                    if (!taskMetrics.isNull("Input Metrics")) {       //判断Input Metrics是否存在，因为在有些情况如describe table情况就不存在该key
                        JSONObject inputMetrics = (JSONObject) taskMetrics.get("Input Metrics");
                        Integer inputSize = (Integer) inputMetrics.get("Bytes Read");
                        totalSize = totalSize + inputSize;      //将每个 task 的 inputsize 加入到总吞吐量中
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        JSONObject jsonTimeAndInput = new JSONObject();
        try {
            jsonTimeAndInput.put("time", totalTime + "ms");
            jsonTimeAndInput.put("size", totalSize + "B");
        } catch (JSONException e) {
            System.out.print("JSON 装入总时间和总大小出问题");
        }
        return jsonTimeAndInput;
    }
}
