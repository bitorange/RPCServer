package org.linc.RPCServer;
/**
 * Created by orange on 2015/3/25.
 */

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.AMQP.BasicProperties;
import org.linc.RPCServer.fieldsconverter.HQLFieldsConverter;
import org.json.JSONObject;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * RPC 服务器端
 */
public class ApplicationEntry extends Thread {

    private static final String RPC_QUEUE_NAME = "rpc_queue";
    public static ConnectionFactory factory;
    public static Connection connection = null;
    public static Channel channel = null;
    public static QueueingConsumer consumer;
    public QueueingConsumer.Delivery delivery;
    public java.sql.Connection con;
    public static int JDBCConnections;
    private static final String conHost = "10.4.21.103";
    private static final String conUsername = "wucheng";
    private static final String conPassword = "123456";


    static {
        JDBCConnections = Integer.valueOf(ResourceBundle.getBundle("db").getString("JDBCConnections"));
    }

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
     * @param con      JDBC连接
     * @return json数据，msg：ok代表登录成功，msg：no代表登录失败，msg：其它代表错误
     */
    private String check(String name, String password, java.sql.Connection con) {
        String result = AccountCheck.checkAccount(name, password, con);
        return "{\"code\": \"10\",\"msg\":\"" + result + "\"}";
    }

    /**
     * 对SQL语句进行执行并返回结果
     *
     * @param sql SQL语句
     * @param con JDBC连接
     * @return json数据格式的数据
     */
    private String sqlExecute(String sql, java.sql.Connection con) {
        HQLFieldsConverter fieldsConverter = new HQLFieldsConverter(con);   // HQL 字段转换器
        ConnectJDBC conn = new ConnectJDBC();
        ResultSet rs;
        String response = null;
        ArrayList<ArrayList<String>> result;
        try {
            rs = conn.getAndExucuteSQL(sql, con);
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
            jsonObject = conn.convertArrayListToJsonObject(result);
            response = jsonObject.toString();
        } catch (Exception e) {
            // e.printStackTrace();
            String msg = e.getMessage();
            response = "{\"code\": \"10\",\"msg\": \"" + msg + "\"}";
        } finally {
            JDBCUtils.releaseAll();
        }

        System.out.println("222::" + response);
        return response;
    }

    /**
     * 获取从服务器消息队列中得到的消息信息，处理并向返回消息队列中返回消息
     *
     * @param delivery 消息队列中的一个消息
     * @param con      JDBC连接
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
                String service = jsonObject.getString("org/linc/RPCServer/service");

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


    public static void main(String[] args) {
        ExecutorService pool = null;
        List<java.sql.Connection> connectionList = null;
        try {
            /* 设置消息队列监控连接 */
            factory = new ConnectionFactory();
            factory.setHost(conHost);
            factory.setUsername(conUsername);
            factory.setPassword(conPassword);

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
            JDBCUtils.loadDriver();

            // 创建JDBCConnections个JDBC连接
            connectionList = new ArrayList<java.sql.Connection>();
            for (int ii = 0; ii < JDBCConnections; ii++) {
                JDBCUtils jd = new JDBCUtils();
                java.sql.Connection con = jd.getConnection();
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
                assert connectionList != null;
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
