package com.example.mq.mqclient;

import com.example.mq.common.*;
import com.example.mq.mqserver.core.BasicProperties;
import com.example.mq.mqserver.core.ExchangeType;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class Channel {

    private String channelId;

    private Connection connection;

    // 用来存储后续客户端收到的服务器响应
    private ConcurrentHashMap<String, BasicReturns> basicReturnsMap = new ConcurrentHashMap<>();

    private Consumer consumer = null;


    public Channel(String channelId, Connection connection) {
        this.channelId = channelId;
        this.connection = connection;
    }

    // 和服务器交互，进行 channel 的同步
    public boolean createChannel() throws IOException {
        BasicArguments basicArguments = new BasicArguments();
        basicArguments.setChannelId(channelId);
        basicArguments.setRid(generateRid());
        byte[] payload = BinaryTool.toBytes(basicArguments);

        Request request = new Request();
        request.setType(0x1);
        request.setLength(payload.length);
        request.setPayload(payload);

        // 发送构造的数据
        connection.writeRequest(request);
        // 等待服务器响应
        BasicReturns basicReturns = waitResult(basicArguments.getRid());
        return basicReturns.isOk();
    }


    // 阻塞等待服务器返回的数据
    private BasicReturns waitResult(String rid) {
        BasicReturns basicReturns = null;

        while ((basicReturns = basicReturnsMap.get(rid)) == null) {
            synchronized(this) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        basicReturnsMap.remove(rid);
        return basicReturns;
    }

    public void putReturns(BasicReturns basicReturns) {
        basicReturnsMap.put(basicReturns.getRid(), basicReturns);
        synchronized (this) {
            notifyAll();
        }
    }


    public String generateRid() {
        return "R-" + UUID.randomUUID().toString();
    }


    // 关闭 channel
    public boolean close() throws IOException {
        BasicArguments basicArguments = new BasicArguments();
        basicArguments.setRid(generateRid());
        basicArguments.setChannelId(channelId);
        byte[] payload = BinaryTool.toBytes(basicArguments);

        Request request = new Request();
        request.setType(0x2);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(basicArguments.getRid());
        return basicReturns.isOk();
    }



    // 创建交换机
    public boolean exchangeDeclare(String exchangeName, ExchangeType exchangeType, boolean durable,
                                   boolean autoDelete, Map<String, Object> arguments) throws IOException {
        ExchangeDeclareArguments exchangeDeclareArguments = new ExchangeDeclareArguments();
        exchangeDeclareArguments.setRid(generateRid());
        exchangeDeclareArguments.setChannelId(channelId);
        exchangeDeclareArguments.setExchangeName(exchangeName);
        exchangeDeclareArguments.setExchangeType(exchangeType);
        exchangeDeclareArguments.setDurable(durable);
        exchangeDeclareArguments.setAutoDelete(autoDelete);
        exchangeDeclareArguments.setArguments(arguments);
        byte[] payload = BinaryTool.toBytes(exchangeDeclareArguments);

        Request request = new Request();
        request.setType(0x3);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(exchangeDeclareArguments.getRid());
        return basicReturns.isOk();
    }

    // 删除交换机
    public boolean exchangeDelete(String exchangeName) throws IOException {
        ExchangeDeleteArguments arguments = new ExchangeDeleteArguments();
        arguments.setRid(generateRid());
        arguments.setChannelId(channelId);
        arguments.setExchangeName(exchangeName);
        byte[] payload = BinaryTool.toBytes(arguments);

        Request request = new Request();
        request.setType(0x4);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(arguments.getRid());
        return basicReturns.isOk();
    }



    // 创建队列
    public boolean queueDeclare(String queueName, boolean durable, boolean exclusive, boolean autoDelete,
                                Map<String, Object> arguments) throws IOException {
        QueueDeclareArguments queueDeclareArguments = new QueueDeclareArguments();
        queueDeclareArguments.setRid(generateRid());
        queueDeclareArguments.setChannelId(channelId);
        queueDeclareArguments.setQueueName(queueName);
        queueDeclareArguments.setDurable(durable);
        queueDeclareArguments.setExclusive(exclusive);
        queueDeclareArguments.setAutoDelete(autoDelete);
        queueDeclareArguments.setArguments(arguments);
        byte[] payload = BinaryTool.toBytes(queueDeclareArguments);

        Request request = new Request();
        request.setType(0x5);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(queueDeclareArguments.getRid());
        return basicReturns.isOk();
    }

    // 删除队列
    public boolean queueDelete(String queueName) throws IOException {
        QueueDeleteArguments arguments = new QueueDeleteArguments();
        arguments.setRid(generateRid());
        arguments.setChannelId(channelId);
        arguments.setQueueName(queueName);
        byte[] payload = BinaryTool.toBytes(arguments);

        Request request = new Request();
        request.setType(0x6);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(arguments.getRid());
        return basicReturns.isOk();
    }



    // 创建绑定
    public boolean queueBind(String queueName, String exchangeName, String bindingKey) throws IOException {
        QueueBindArguments arguments = new QueueBindArguments();
        arguments.setRid(generateRid());
        arguments.setChannelId(channelId);
        arguments.setQueueName(queueName);
        arguments.setExchangeName(exchangeName);
        arguments.setBindingKey(bindingKey);
        byte[] payload = BinaryTool.toBytes(arguments);

        Request request = new Request();
        request.setType(0x7);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(arguments.getRid());
        return basicReturns.isOk();
    }

    // 解除绑定
    public boolean queueUnbind(String queueName, String exchangeName) throws IOException {
        QueueUnbindArguments arguments = new QueueUnbindArguments();
        arguments.setRid(generateRid());
        arguments.setChannelId(channelId);
        arguments.setQueueName(queueName);
        arguments.setExchangeName(exchangeName);
        byte[] payload = BinaryTool.toBytes(arguments);

        Request request = new Request();
        request.setType(0x8);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(arguments.getRid());
        return basicReturns.isOk();
    }



    // 发送消息
    public boolean basicPublish(String exchangeName, String routingKey, BasicProperties basicProperties, byte[] body) throws IOException {
        BasicPublishArguments arguments = new BasicPublishArguments();
        arguments.setRid(generateRid());
        arguments.setChannelId(channelId);
        arguments.setExchangeName(exchangeName);
        arguments.setRoutingKey(routingKey);
        arguments.setBasicProperties(basicProperties);
        arguments.setBody(body);
        byte[] payload = BinaryTool.toBytes(arguments);

        Request request = new Request();
        request.setType(0x9);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(arguments.getRid());
        return basicReturns.isOk();
    }

    // 订阅消息
    public boolean basicConsume(String queueName, boolean autoAck, Consumer consumer) throws MqException, IOException {
        if(this.consumer != null) {
            throw new MqException("该 channel 已经设置过消费消息的回调，不能重复设置！");
        }
        this.consumer = consumer;

        BasicConsumeArguments arguments = new BasicConsumeArguments();
        arguments.setRid(generateRid());
        arguments.setChannelId(channelId);
        arguments.setConsumerTag(channelId);
        arguments.setQueueName(queueName);
        arguments.setAutoAck(autoAck);
        byte[] payload = BinaryTool.toBytes(arguments);

        Request request = new Request();
        request.setType(0xa);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(arguments.getRid());
        return basicReturns.isOk();
    }


    // 确认消息
    public boolean basicAck(String queueName, String messageId) throws IOException {
        BasicAckArguments arguments = new BasicAckArguments();
        arguments.setRid(generateRid());
        arguments.setChannelId(channelId);
        arguments.setQueueName(queueName);
        arguments.setMessageId(messageId);
        byte[] payload = BinaryTool.toBytes(arguments);

        Request request = new Request();
        request.setType(0xb);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(arguments.getRid());
        return basicReturns.isOk();
    }


    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public ConcurrentHashMap<String, BasicReturns> getBasicReturnsMap() {
        return basicReturnsMap;
    }

    public void setBasicReturnsMap(ConcurrentHashMap<String, BasicReturns> basicReturnsMap) {
        this.basicReturnsMap = basicReturnsMap;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }


}
