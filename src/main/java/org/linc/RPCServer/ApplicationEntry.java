package org.linc.RPCServer;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import org.json.JSONObject;
import org.linc.RPCServer.fieldsconverter.HQLFieldsConverter;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 程序入口
 */
public class ApplicationEntry extends Thread {
    /* RPC Server 相关 */
    private static final String RPC_QUEUE_NAME = "rpc_queue";   // RPC 队列名
    private static String conHost = "localhost";    // RabbitMQ Server 地址
    private static String conUsername = "guest";    // RabbitMQ Server 用户名
    private static String conPassword = "guest";    // RabbitMQ Server 密码
    private static ConnectionFactory factory;
    private static Connection connection = null;
    private static Channel channel = null;
    private static QueueingConsumer consumer;
    private QueueingConsumer.Delivery delivery;

    /* JDBC 连接相关 */
    private java.sql.Connection con;    // JDBC 连接
    private static int JDBCConnections; // JDBC 连接数
    private static String JDBCDriver;   // JDBC 驱动
    private static String dbHost;       // 数据库地址
    private static String dbUsername;   // 数据库用户名
    private static String dbPassword;   // 数据库密码

    /* 字段转换相关 */
    private HQLFieldsConverter fieldsConverter;    // HQL 字段转换器

    /* Spark 相关 */
    private static String sparkLogPath; // Spark 日志路径

    public ApplicationEntry(QueueingConsumer.Delivery delivery, java.sql.Connection con) {
        this.delivery = delivery;
        this.con = con;
    }

    public ApplicationEntry() {

    }

    public void run() {
        connectToHive(this.delivery, this.con);
    }

    /**
     * 调用函数对用户名密码进行检验
     *
     * @param name     用户名
     * @param password 密码
     * @param con      到远程服务器的 JDBC 连接
     * @return JSON 数据格式的返回数据，msg：ok 代表登录成功，msg：no 代表登录失败，msg：其它代表错误
     */
    private String check(String name, String password, java.sql.Connection con) {
        String result = AccountCheck.checkAccount(name, password, con);
        return "{\"code\": \"10\",\"msg\":\"" + result + "\"}";
    }

    /**
     * 对SQL语句进行执行并返回结果
     *
     * @param sql SQL语句
     * @param con 到远程服务器的 JDBC 连接
     * @return JSON 数据格式的返回数据
     */
    private String sqlExecute(String sql, java.sql.Connection con) {

        this.fieldsConverter = new HQLFieldsConverter(con);        // path 为 jar 包当前路径
        ConnectJDBC conn = new ConnectJDBC();
        ResultSet rs;
        String response = null;
        ArrayList<ArrayList<String>> result;
        try {
            rs = conn.getAndExecuteSQL(sql, con);
            result = fieldsConverter.parseCommand(sql, rs);
            if (result == null) {
                throw new Exception("字段转换发生错误");
            }
        } catch (Exception e) {
            String msg = e.getMessage();
            response = "{\"code\": \"10\",\"msg\": \"" + msg + "\"}";
            System.out.println("111::" + response);
            return response;
        }

        /* 将查询出来的结果转换为json数据格式 */
        JSONObject jsonObject;
        try {
            // jsonObject = conn.transformToJsonArray(rs);
            jsonObject = conn.convertArrayListToJsonObject(result, ApplicationEntry.sparkLogPath);
            response = jsonObject.toString();
        } catch (Exception e) {
            // e.printStackTrace();
            String msg = e.getMessage();
            response = "{\"code\": \"10\",\"msg\": \"" + msg + "\"}";
        } finally {
            //    JDBCUtils.releaseAll();
        }

        System.out.println("222::" + response);
        return response;
    }

    /**
     * 获取从服务器消息队列中得到的消息信息，处理并向返回消息队列中返回消息
     *
     * @param delivery 消息队列中的一个消息
     * @param con      到远程服务器的 JDBC 连接
     */
    public void connectToHive(QueueingConsumer.Delivery delivery, java.sql.Connection con) {

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

                    ApplicationEntry myServer = new ApplicationEntry();
                    response = myServer.check(name, password, con);

                } else if (service.equals("sqlExecute")) {
                    /* SQL 语句执行 */
                    String sql = jsonObject.getString("sql");
                    ApplicationEntry myServer = new ApplicationEntry();

                    System.out.println("1");
                    response = myServer.sqlExecute(sql, con);
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 解析程序运行参数，读取配置文件
     *
     * @param args 程序参数
     */
    private static void readConfigureFile(String[] args) {
        // 解析参数，读取配置文件
        GlobalVar.parseArgs(args);

        /* RPC 相关参数 */
        ApplicationEntry.conHost = GlobalVar.configMap.get("rpc.server");
        ApplicationEntry.conUsername = GlobalVar.configMap.get("rpc.username");
        ApplicationEntry.conPassword = GlobalVar.configMap.get("rpc.password");

        /* JDBC 相关参数 */
        ApplicationEntry.JDBCDriver = GlobalVar.configMap.get("db.driver");
        ApplicationEntry.JDBCConnections = Integer.valueOf(GlobalVar.configMap.get("db.jdbc.connections"));
        ApplicationEntry.dbHost = GlobalVar.configMap.get("db.url");
        ApplicationEntry.dbUsername = GlobalVar.configMap.get("db.username");
        ApplicationEntry.dbPassword = GlobalVar.configMap.get("db.password");

        /* Spark 相关 */
        ApplicationEntry.sparkLogPath = GlobalVar.configMap.get("spark.log.path");
    }

    /**
     * 主函数
     *
     * @param args 程序参数
     */
    public static void main(String[] args) {
        // 解析程序运行参数，读取配置文件
        ApplicationEntry.readConfigureFile(args);

        ExecutorService pool = null;
        List<java.sql.Connection> connectionList = null;

        /* 设置消息队列监控连接 */
        try {
            factory = new ConnectionFactory();
            factory.setHost(ApplicationEntry.conHost);
            factory.setUsername(ApplicationEntry.conUsername);
            factory.setPassword(ApplicationEntry.conPassword);

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
            JDBCUtils.loadDriver(ApplicationEntry.JDBCDriver);

            // 创建JDBCConnections个JDBC连接
            connectionList = new ArrayList<java.sql.Connection>();
            for (int ii = 0; ii < JDBCConnections; ii++) {
                JDBCUtils jd = new JDBCUtils();
                java.sql.Connection con = jd.getConnection(ApplicationEntry.dbHost,
                        ApplicationEntry.dbUsername, ApplicationEntry.dbPassword);
                connectionList.add(con);
            }

            // 创建一个可重用固定线程数的线程池
            pool = Executors.newFixedThreadPool(JDBCConnections);
            int i = 0;
            while (true) {
                // 消费者阻塞监听队列
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                i = i % JDBCConnections;

                // 创建实现多线程
                Thread t = new ApplicationEntry(delivery, connectionList.get(i));
                i++;

                // 将线程放入池中进行执行
                pool.execute(t);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            /* release创建的JDBC，线程池，Connection */
            for (int ii = 0; ii < JDBCConnections; ii++) {
                JDBCUtils.connRelease(connectionList.get(ii));
            }

            if (pool != null) {
                pool.shutdown();
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception ignore) {
                }
            }
        }
    }
}
