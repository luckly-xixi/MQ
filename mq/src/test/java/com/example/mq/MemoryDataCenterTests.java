package com.example.mq;


import com.example.mq.common.MqException;
import com.example.mq.mqserver.core.*;
import com.example.mq.mqserver.datacenter.DiskDataCenter;
import com.example.mq.mqserver.datacenter.MemoryDataCenter;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@SpringBootTest
public class MemoryDataCenterTests {
    private MemoryDataCenter memoryDataCenter = null;

    @BeforeEach
    public void setUp() {
        memoryDataCenter = new MemoryDataCenter();
    }

    @AfterEach
    public void tearDown() {
        memoryDataCenter = null;
    }


    private Exchange createTestExchange(String exchangeName) {
        Exchange exchange = new Exchange();
        exchange.setName(exchangeName);
        exchange.setType(ExchangeType.DIRECT);
        exchange.setAutoDelete(false);
        exchange.setDurable(true);
        return exchange;
    }


    private MSGQueue createTestQueue(String queueName) {
        MSGQueue queue = new MSGQueue();
        queue.setName(queueName);
        queue.setDurable(true);
        queue.setAutoDelete(false);
        queue.setExclusive(false);
        return queue;
    }

    // 交换机测试
    @Test
    public void testExchange()  {

        Exchange expectedExchange = createTestExchange("testExchange");
        memoryDataCenter.insertExchange(expectedExchange);
        Exchange actualExchange = memoryDataCenter.getExchange("testExchange");
        // 如果相同应该引用的是同一个对象
        Assertions.assertEquals(expectedExchange, actualExchange);

        memoryDataCenter.deleteExchange("testExchange");

        actualExchange = memoryDataCenter.getExchange("testExchange");
        Assertions.assertNull(actualExchange);
    }


    // 队列测试
    @Test
    public void testQueue()  {
        MSGQueue expectedQueue = createTestQueue("testQueue");
        memoryDataCenter.insertQueue(expectedQueue);
        MSGQueue actualQueue = memoryDataCenter.getQueue("testQueue");
        Assertions.assertEquals(expectedQueue, actualQueue);
        memoryDataCenter.deleteQueue("testQueue");
        actualQueue = memoryDataCenter.getQueue("testQueue");
        Assertions.assertNull(actualQueue);
    }


    // 绑定测试
    @Test
    public void testBinding() throws MqException {
        Binding expectedBinding = new Binding();
        expectedBinding.setExchangeName("testExchange");
        expectedBinding.setQueueName("testQueue");
        expectedBinding.setBindingKey("testBindingKey");
        memoryDataCenter.insertBinding(expectedBinding);

        Binding actualBinding = memoryDataCenter.getBinding("testExchange", "testQueue");
        Assertions.assertEquals(expectedBinding, actualBinding);

        ConcurrentHashMap<String, Binding> bindingMap = memoryDataCenter.getBindings("testExchange");
        Assertions.assertEquals(1, bindingMap.size());
        Assertions.assertEquals(expectedBinding, bindingMap.get("testQueue"));

        memoryDataCenter.deleteBinding(expectedBinding);

        actualBinding = memoryDataCenter.getBinding("testExchange", "testQueue");
        Assertions.assertNull(actualBinding);
    }




    // 消息测试

    public Message createTestMessage(String content) {
        Message message = Message.createMessageWithId("testMessage", null, content.getBytes());
        return message;
    }

    @Test
    public void testMessage() {
        Message expectedMessage = createTestMessage("testMessage");
        memoryDataCenter.addMessage(expectedMessage);

        Message actualMessage = memoryDataCenter.getMessage(expectedMessage.getMessageId());
        Assertions.assertEquals(expectedMessage, actualMessage);

        memoryDataCenter.removeMessage(expectedMessage.getMessageId());

        actualMessage = memoryDataCenter.getMessage(expectedMessage.getMessageId());
        Assertions.assertNull(actualMessage);
    }

    @Test
    public void testSendMessage() {
        MSGQueue queue = createTestQueue("testQueue");
        List<Message> expectedMessage = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Message message = createTestMessage("testMessage" + i);
            memoryDataCenter.sendMessage(queue, message);
            expectedMessage.add(message);
        }

        List<Message> actualMessage = new ArrayList<>();
        while (true) {
            Message message = memoryDataCenter.pollMessage("testQueue");
            if (message == null) {
                break;
            }
            actualMessage.add(message);
        }

        Assertions.assertEquals(expectedMessage.size(), actualMessage.size());
        for (int i = 0; i < expectedMessage.size(); i++) {
            Assertions.assertEquals(expectedMessage.get(i), actualMessage.get(i));
        }
    }


    // 测试未确认消息
    @Test
    public void testMessageWaitAck() {
        Message expectedMessage = createTestMessage("expectedMessage");
        memoryDataCenter.addMessageWaitAck("testQueue", expectedMessage);

        Message actualMessage = memoryDataCenter.getMessageWaitAck("testQueue", expectedMessage.getMessageId());
        Assertions.assertEquals(expectedMessage, actualMessage);

        memoryDataCenter.removeMessageWaitAck("testQueue", expectedMessage.getMessageId());
        actualMessage = memoryDataCenter.getMessageWaitAck("testQueue", expectedMessage.getMessageId());
        Assertions.assertNull(actualMessage);
    }


    // 加载硬盘数据到内存
    @Test
    public void testRecovery() throws IOException, MqException, ClassNotFoundException {

        DemoApplication.context = SpringApplication.run(DemoApplication.class);
        // 1.在硬盘上构造好数据
        DiskDataCenter diskDataCenter = new DiskDataCenter();
        diskDataCenter.init();

        Exchange expectedExchange = createTestExchange("testExchange");
        diskDataCenter.insertExchange(expectedExchange);

        MSGQueue expectedQueue = createTestQueue("testQueue");
        diskDataCenter.insertQueue(expectedQueue);

        Binding expectedBinding = new Binding();
        expectedBinding.setExchangeName("testExchange");
        expectedBinding.setQueueName("testQueue");
        expectedBinding.setBindingKey("testBindingKey");
        diskDataCenter.insertBinding(expectedBinding);

        Message expectedMessage = createTestMessage("testMessage");
        diskDataCenter.sendMessage(expectedQueue, expectedMessage);

        // 2.执行恢复操作
        memoryDataCenter.recovery(diskDataCenter);

        // 3.对比结果
        Exchange actualExchange = memoryDataCenter.getExchange("testExchange");
        Assertions.assertEquals(expectedExchange.getName(), actualExchange.getName());
        Assertions.assertEquals(expectedExchange.getType(), actualExchange.getType());
        Assertions.assertEquals(expectedExchange.isAutoDelete(), actualExchange.isAutoDelete());
        Assertions.assertEquals(expectedExchange.isDurable(), actualExchange.isDurable());

        MSGQueue actualQueue = memoryDataCenter.getQueue("testQueue");
        Assertions.assertEquals(expectedQueue.getName(), actualQueue.getName());
        Assertions.assertEquals(expectedQueue.isAutoDelete(), actualQueue.isAutoDelete());
        Assertions.assertEquals(expectedQueue.isDurable(), actualQueue.isDurable());
        Assertions.assertEquals(expectedQueue.isExclusive(), actualQueue.isExclusive());

        Binding actualBinding = memoryDataCenter.getBinding("testExchange", "testQueue");
        Assertions.assertEquals(expectedBinding.getBindingKey(), actualBinding.getBindingKey());
        Assertions.assertEquals(expectedBinding.getQueueName(), actualBinding.getQueueName());
        Assertions.assertEquals(expectedBinding.getExchangeName(), actualBinding.getExchangeName());

        Message actualMessage = memoryDataCenter.pollMessage("testQueue");
        Assertions.assertEquals(expectedMessage.getMessageId(), actualMessage.getMessageId());
        Assertions.assertEquals(expectedMessage.getDeliverMode(), actualMessage.getDeliverMode());
        Assertions.assertEquals(expectedMessage.getRoutingKey(), actualMessage.getRoutingKey());
        Assertions.assertArrayEquals(expectedMessage.getBody(), actualMessage.getBody());

        // 清理硬盘数据( mate.db  和  队列目录)
        DemoApplication.context.close();
        File dataDir = new File("./data");
        FileUtils.deleteDirectory(dataDir);
    }
}
