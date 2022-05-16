package com.lew.rabbitmq.work.fair;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 工作模式
 * <br>
 * 公平分发
 * <br>
 * <img  src="https://www.rabbitmq.com/img/tutorials/python-two.png" alt="">
 * <br>
 * 消费者
 * @author luzhonghe
 * @date 2022/5/13 11:26 AM
 */
public class Worker1 {

    public static void main(String[] args) throws Exception {
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
            connection = connectionFactory.newConnection("消费者");
            // 通过连接获取通道
            channel = connection.createChannel();
            String queueName = "queue1";
            Channel finalChannel = channel;
            finalChannel.basicQos(1);
            // 消费消息
            finalChannel.basicConsume(queueName, false, new DeliverCallback() {
                @Override
                public void handle(String consumerTag, Delivery message) throws IOException {
                    try {
                        Thread.sleep(1000);
                        System.out.println("收到消息" + new String(message.getBody(), "UTF-8"));
                        finalChannel.basicAck(message.getEnvelope().getDeliveryTag(), false);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }, new CancelCallback() {
                @Override
                public void handle(String consumerTag) throws IOException {
                    System.out.println("接收失败");
                }
            });
            System.out.println("开始接收");
            System.in.read();
        } catch (Exception e) {
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
