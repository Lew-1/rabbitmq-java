package com.lew.rabbitmq.work.fair;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 工作模式
 * <br>
 * 公平分发
 * <br>
 * <img  src="https://www.rabbitmq.com/img/tutorials/python-two.png" alt="">
 * <br>
 * 生产者
 * @author luzhonghe
 * @date 2022/5/13 11:23 AM
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
            String queueName = "queue1";
            // 通过通道创建交换机，声明队列，绑定关系，路由key，发送和接收消息
            /**
             * @param durable 是否持久化，是则在队列重启后还保留
             * @param exclusive 排他性，是否独占
             * @param autoDelete 自动删除， 队列消费完后自动删除
             * @param arguments 附属额外参数
             */
            channel.queueDeclare(queueName, true, false, false, null);
            // 准备消息
            for (int i = 0; i < 20; i++) {
                String message = "轮询消息" + i;
                // 发送消息
                channel.basicPublish("", queueName, null, message.getBytes());
            }
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
