package service;
/**
 * Created by orange on 2015/3/25.
 */

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.AMQP.BasicProperties;
import linc.AccountCheck;
import linc.ConnectJDBC;
import linc.JDBCUtils;
import linc.fieldsconverter.HQLFieldsConverter;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RPCServer extends Thread{

    private static final String RPC_QUEUE_NAME = "rpc_queue";
    public static ConnectionFactory factory;
    public static Connection connection = null;
    public static Channel channel = null;
    public static QueueingConsumer consumer;
    // private static List<Thread> runningThreads = new ArrayList<Thread>();
    public QueueingConsumer.Delivery delivery;
    public java.sql.Connection con;
    public static int JDBCConnections;
    private HQLFieldsConverter fieldsConverter;    // A converter used to convert the result of SQL command
    public static String path;
    private static HashMap<String,String> keyValueMap;      //配置文件的内容信息

    public RPCServer(QueueingConsumer.Delivery delivery,java.sql.Connection con){
        this.delivery = delivery;
        this.con = con;
    }

    public RPCServer(){

    }

    public void run()
    {
        connectToHive(this.delivery, this.con);
    }

    /**
     * 调用函数对用户名密码进行检验
     * @param name 用户名
     * @param password 密码
     * @param con JDBC连接
     * @return json数据，msg：ok代表登录成功，msg：no代表登录失败，msg：其它代表错误
     */
    private String check(String name,String password,java.sql.Connection con){
        String result = AccountCheck.checkAccount(name, password,con);
        return "{\"code\": \"10\",\"msg\":\"" + result + "\"}";
    }

    /**
     * 对SQL语句进行执行并返回结果
     * @param sql SQL语句
     * @param con JDBC连接
     * @return json数据格式的数据
     */
    private String sqlExecute(String sql,java.sql.Connection con) {

        this.fieldsConverter = new HQLFieldsConverter(con);        //path为jar包当前路径
        ConnectJDBC conn = new ConnectJDBC();
        ResultSet rs = null;
        String response = null;
        ArrayList<ArrayList<String>> result = null;
        try {
            rs = conn.getAndExucuteSQL(sql,con);
            result = fieldsConverter.parseCommand(sql, rs);
            if(result == null){
                throw new Exception("字段转换发生错误");
            }
        }catch (Exception e){
            String msg = e.getMessage();
            response = "{\"code\": \"10\",\"msg\": \"" + msg + "\"}";
            System.out.println("111::"+response);
            return response;
        }

        /* 将查询出来的结果转换为json数据格式 */
        JSONObject jsonObject;
        try {
            // jsonObject = conn.transformToJsonArray(rs);
            jsonObject = conn.convertArrayListToJsonObject(result,keyValueMap.get("logPath"));
            response = jsonObject.toString();
        } catch (Exception e) {
            // e.printStackTrace();
            String msg = e.getMessage();
            response = "{\"code\": \"10\",\"msg\": \"" + msg + "\"}";
        }finally{
        //    JDBCUtils.releaseAll();
        }

        System.out.println("222::"+response);
        return response;
    }

    /**
     * 获取从服务器消息队列中得到的消息信息，处理并向返回消息队列中返回消息
     * @param delivery 消息队列中的一个消息
     * @param con JDBC连接
     */
    public void connectToHive(QueueingConsumer.Delivery delivery,java.sql.Connection con){

        String response = null;
        try {
            // 获取消息中的信息
            BasicProperties props = delivery.getProperties();
            BasicProperties replyProps = new BasicProperties
                    .Builder()
                    .correlationId(props.getCorrelationId())
                    .build();

            try {
                String message = new String(delivery.getBody(), "UTF-8");
                JSONObject jsonObject = new JSONObject(message);
                String service = jsonObject.getString("service");

                if (service.equals("check")) {
                    /* 密码校验 */
                    String name = jsonObject.getString("name");
                    String password = jsonObject.getString("password");

                    RPCServer myServer = new RPCServer();
                    response = myServer.check(name, password,con);

                } else if (service.equals("sqlExecute")) {
                    /* SQL 语句执行 */
                    String sql = jsonObject.getString("sql");
                    RPCServer myServer = new RPCServer();

                    System.out.println("1");
                    response = myServer.sqlExecute(sql,con);
                    System.out.println("2");

                } else {
                    // TODO: 相应处理
                    /* 请求服务出错 */
                }
            } catch (Exception e) {
                e.printStackTrace();
                String msg = "解析出错等错误";
                response = "{\"code\": \"10\",\"msg\": \"" + msg + "\"}";
            } finally {
                // 将消息返回给信息请求者
                assert response != null;
                channel.basicPublish("", props.getReplyTo(), replyProps, response.getBytes("UTF-8"));
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {

        Properties properties = System.getProperties();     //获取配置文件路径
        path = properties.getProperty("user.dir");
        System.out.println("path***=== " + path);
        HashMap<String,String> keyValueMap = RPCServer.readConfFile(path);      //keyValueMap是配置文件的键值对
        JDBCConnections = Integer.valueOf(keyValueMap.get("JDBCConnections"));
        ExecutorService pool=null;
        List<java.sql.Connection> connectionList = null;
        try {
            /* 设置消息队列监控连接 */
            factory = new ConnectionFactory();
            factory.setHost(keyValueMap.get("conHost"));
            factory.setUsername(keyValueMap.get("conUsername"));
            factory.setPassword(keyValueMap.get("conPassword"));

            connection = factory.newConnection();
            channel = connection.createChannel();

            // 在服务端重启之后，先删除监控队列中在服务器端开启前收到的无用消息
            channel.queueDelete(RPC_QUEUE_NAME);

            // 声明所监控的消息队列RPC_QUEUE_NAME
            channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);

            // 1代表公平转发，不要在同一时间给消费者超过一条信息
            channel.basicQos(1);

            consumer = new QueueingConsumer(channel);
            channel.basicConsume(RPC_QUEUE_NAME, false, consumer);

            System.out.println(" Start Server: Monitor Queue");

            // 在创建JDBC连接之前注册driver
            JDBCUtils.loadDriver(keyValueMap.get("driverClass"));

            // 创建JDBCConnections个JDBC连接
            connectionList=new ArrayList<java.sql.Connection>();
            for(int ii=0;ii<JDBCConnections;ii++) {
                JDBCUtils jd = new JDBCUtils();
                java.sql.Connection con = jd.getConnection(keyValueMap.get("url"),keyValueMap.get("username"),keyValueMap.get("password"));
                connectionList.add(con);
            }

            // 创建一个可重用固定线程数的线程池
            pool = Executors.newFixedThreadPool(JDBCConnections);
            int i=0;
            while (true) {
                // 消费者阻塞监听队列
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                i = i % JDBCConnections;

                // 创建实现多线程
                Thread t = new RPCServer(delivery,connectionList.get(i));
                i++;

                // 将线程放入池中进行执行
                pool.execute(t);
            }

        }
        catch  (Exception e) {
            e.printStackTrace();
        }
        finally {
            /* release创建的JDBC，线程池，Connection */
            for(int ii = 0; ii < JDBCConnections; ii++) {
                JDBCUtils.connRelease(connectionList.get(ii));
            }

            if(pool!=null) {
                pool.shutdown();
            }
            if (connection != null) {
                try {
                    connection.close();
                }
                catch (Exception ignore) {}
            }
        }
    }

    /**
     * this method will read the confFile at the root directory
     * @param path
     * @return HashMap
     * */
    public static HashMap<String,String> readConfFile(String path){
        try{
            // path+"/RPCServerConf.properties"  "F:\\software\\IntelliJ IDEA\\IDEA Project\\ZTE_section2\\RPCServerConf.properties"
            File file = new File(path+"/RPCServerConf.properties");
            BufferedReader bfr = new BufferedReader(new FileReader(file));      //获取输入流
            String lines;
            keyValueMap = new HashMap<String, String>(9);
            while((lines = bfr.readLine()) != null){
                if(lines.startsWith("#")){
                    continue;
                }
                String keyValuePair[] = lines.split("=");
                keyValueMap.put(keyValuePair[0],keyValuePair[1]);
            }}catch (Exception e){
            System.out.println("系统配置文件读取错误");
            System.out.println("path : "+path+"/RPCServerConf.properties");
        }
        return keyValueMap;
    }
}
