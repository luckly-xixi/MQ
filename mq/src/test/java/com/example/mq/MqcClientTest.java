package com.example.mq;

import com.example.mq.common.Consumer;
import com.example.mq.common.MqException;
import com.example.mq.mqclient.Channel;
import com.example.mq.mqclient.Connection;
import com.example.mq.mqclient.ConnectionFactory;
import com.example.mq.mqserver.BrokerServer;
import com.example.mq.mqserver.core.BasicProperties;
import com.example.mq.mqserver.core.ExchangeType;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;

import java.io.File;
import java.io.IOException;

public class MqcClientTest {

    private BrokerServer brokerServer = null;
    private ConnectionFactory connectionFactory = null;
    private Thread t = null;

    @BeforeEach
    public void setUp() throws IOException {
        // 启动服务器
        DemoApplication.context = SpringApplication.run(DemoApplication.class);
        brokerServer = new BrokerServer(9090);
        t = new Thread(() -> {
            try {
                brokerServer.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        t.start();

        // 配置 ConnectionFactory
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(9090);
    }


    @AfterEach
    public void tearDown() throws IOException {
        brokerServer.stop();

        DemoApplication.context.close();

        File file = new File("./data");
        FileUtils.deleteDirectory(file);

        connectionFactory = null;
    }


    @Test
    public void testConnection() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
    }


    @Test
    public void testChannel() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);
    }

    @Test
    public void testExchange() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);

        boolean ok = channel.exchangeDeclare("testExchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(ok);

        ok = channel.exchangeDelete("testExchange");
        Assertions.assertTrue(ok);

        channel.close();
        connection.close();
    }

    @Test
    public void testQueue() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);

        boolean ok = channel.queueDeclare("testQueue", true, false, false, null);
        Assertions.assertTrue(ok);

        ok = channel.queueDelete("testQueue");
        Assertions.assertTrue(ok);

        channel.close();
        connection.close();
    }


    @Test
    public void testBinding() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);

        boolean ok = channel.exchangeDeclare("testExchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(ok);
        ok = channel.queueDeclare("testQueue", true, false, false, null);
        Assertions.assertTrue(ok);

        ok = channel.queueBind("testQueue", "testExchange", "testBindingKey");
        Assertions.assertTrue(ok);
        ok = channel.queueUnbind("testQueue", "testExchange");
        Assertions.assertTrue(ok);

        channel.close();
        connection.close();
    }


    @Test
    public void testMessage() throws IOException, MqException, InterruptedException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);

        boolean ok = channel.exchangeDeclare("testExchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(ok);
        ok = channel.queueDeclare("testQueue", true, false, false, null);
        Assertions.assertTrue(ok);

        byte[] requestBody = "hello".getBytes();
        ok = channel.basicPublish("testExchange", "testQueue", null, requestBody);
        Assertions.assertTrue(ok);

        ok = channel.basicConsume("testQueue", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) throws MqException, IOException {
                System.out.println("[消费数据] 开始！");
                System.out.println("consumerTag=" + consumerTag);
                System.out.println("basicProperties=" + basicProperties);
                Assertions.assertArrayEquals(requestBody, body);
                System.out.println("[消费数据] 结束！");
            }
        });
        Assertions.assertTrue(ok);
        Thread.sleep(500);

        channel.close();
        connection.close();
    }


}
