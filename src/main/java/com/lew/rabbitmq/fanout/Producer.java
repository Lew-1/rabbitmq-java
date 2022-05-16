package com.lew.rabbitmq.fanout;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 发布订阅 fanout 模式
 *
 * <br>
 * <img  src="https://www.rabbitmq.com/img/tutorials/python-three.png" alt="">
 * <br>
 * 生产者
 * @author luzhonghe
 * @date 2022/5/13 11:26 AM
 */
public class Producer {

    public static void main(String[] args) {
        // 创建连接工程
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("admin");
        connectionFactory.setVirtualHost("/");
        Connection connection = null;
        Channel channel = null;
        try {
            // 创建连接connection
            connection = connectionFactory.newConnection("生产者");
            // 通过连接获取通道
            channel = connection.createChannel();
            String queueName = "fanout_queue";
            // 通过通道创建交换机，声明队列，绑定关系，路由key，发送和接收消息
            /**
             * @param durable 是否持久化，是则在队列重启后还保留
             * @param exclusive 排他性，是否独占
             * @param autoDelete 自动删除， 队列消费完后自动删除
             * @param arguments 附属额外参数
             */
            channel.queueDeclare(queueName, true, false, false, null);
            // 准备消息
            String message = "66";
            // 准备交换机
            String exchangeName = "fanout_exchange";
            // 定义路由key，没有意义，fanout是广播
            String routeKey = "";
            // 指定交换机类型
            String exchangeType = "fanout";
            // 发送消息
            channel.basicPublish(exchangeName, routeKey, null, message.getBytes());
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        } finally {
            if (channel != null && channel.isOpen()) {
                try {
                    channel.close();
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null && connection.isOpen()) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            
        }


    }
    
}
