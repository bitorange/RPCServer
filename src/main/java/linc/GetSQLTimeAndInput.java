package linc;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * this class can get the input and execute time of the job
 * Created by xwc on 2015/3/27.
 */
public class GetSQLTimeAndInput {
    /**
     * this method will get the jobInfo and save it as a jsonObject
     * @param path path is the file path
     * @return JSONObject
     * */
    public JSONArray getLastJobInfo(String path){
        JSONObject jsonObject = null;
        JSONArray jsonArray = new JSONArray();
        RandomAccessFile rf = null;
        try {
            rf = new RandomAccessFile(path,"r");
            long len = rf.length();
            long start = rf.getFilePointer();
            long nextEnd = start+len-1;
            String line;
            rf.seek(nextEnd);
            int c = -1;
            while(nextEnd > start){
                c = rf.read();
                if(c == '\n'||c == '\r' ){
                    line = rf.readLine();
                    if(line == null){   // 处理文件末尾是空行这种情况
                        nextEnd--;
                        rf.seek(nextEnd);
                        continue;
                    }
                    jsonObject = new JSONObject(line);
                    jsonArray.put(jsonObject);
                    if(line.contains("SparkListenerJobStart")) {
                        break;
                    }
                    System.out.println(line);
                    nextEnd--;
                }
                nextEnd--;
                rf.seek(nextEnd);
                if(nextEnd == 0){//当文件指针退至文件开始处，输出第一行
                    System.out.println(rf.readLine());
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }catch(JSONException e){
            System.out.println("JSON 有问题");
        }
        finally{
            try {
                rf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return jsonArray;
    }

    /**
     * this method will get the input and size of ever task(core) and then add them all
     * @param jsonArray jsonArray is the jobInfo
     * @return JSONObject
     * */
    public JSONObject getJobTimeAndInput(JSONArray jsonArray){
        JSONObject  jsonObject = null;
        Long totalTime = 0L;
        Long totalSize = 0L;
        for(int i = 0; i < jsonArray.length(); i++){       //循环，拿出size和time信息
             Long executeTime = 0L;
            try {
                jsonObject =  jsonArray.getJSONObject(i);   //从头获取jsonArray中的对象
                Object obj = jsonObject.get("Event");
                if(obj.toString().equals("SparkListenerTaskEnd")){       //如果取出的数据是SparkListenerTaskEnd，则继续获取其中的其他信息
                    JSONObject taskInfo = (JSONObject)jsonObject.get("Task Info");      //获取单个task的运行时间time
                    Long startTime = (Long)taskInfo.get("Launch Time");
                    Long stopTime = (Long)taskInfo.get("Finish Time");
                    executeTime = stopTime - startTime;
                    totalTime = totalTime + executeTime;    //将每个task的执行时间加入到总时间中

                    JSONObject taskMetrics = (JSONObject)jsonObject.get("Task Metrics");      //获取单个task的运行吞吐量size
                    if(!taskMetrics.isNull("Input Metrics")) {       //判断Input Metrics是否存在，因为在有些情况如describe table情况就不存在该key
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
            jsonTimeAndInput.put("time", totalTime+"ms");
            jsonTimeAndInput.put("size", totalSize+"B");
        }catch(JSONException e){
            System.out.print("JSON 装入总时间和总大小出问题");
        }
        return jsonTimeAndInput;
    }
}
