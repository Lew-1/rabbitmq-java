package com.lew.rabbitmq.fanout;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 发布订阅 fanout 模式
 *
 * <br>
 * <img  src="https://www.rabbitmq.com/img/tutorials/python-three.png" alt="">
 * <br>
 * 消费者
 * @author luzhonghe
 * @date 2022/5/13 11:26 AM
 */
public class Consumer {
    
    private static Runnable runnable = new Runnable() {
        @Override
        public void run() {
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
                String queueName = Thread.currentThread().getName();

                // 消费消息
                channel.basicConsume(queueName, true, new DeliverCallback() {
                    @Override
                    public void handle(String consumerTag, Delivery message) throws IOException {
                        System.out.println(queueName + "收到消息" + new String(message.getBody(), "UTF-8"));
                    }
                }, new CancelCallback() {
                    @Override
                    public void handle(String consumerTag) throws IOException {
                        System.out.println("接收失败");
                    }
                });
                System.out.println("开始接收");
                System.in.read();
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
    };

    public static void main(String[] args) {
        new Thread(runnable, "queue1").start();
        new Thread(runnable, "queue2").start();
        new Thread(runnable, "queue3").start();
    }
    
}
