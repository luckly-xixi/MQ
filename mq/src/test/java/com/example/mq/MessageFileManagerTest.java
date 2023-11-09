package com.example.mq;


import com.example.mq.common.MqException;
import com.example.mq.mqserver.core.MSGQueue;
import com.example.mq.mqserver.core.Message;
import com.example.mq.mqserver.datacenter.MessageFileManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

@SpringBootTest
public class MessageFileManagerTest {
    private MessageFileManager messageFileManager = new MessageFileManager();


    private static final String queueName1 = "testQueue1";
    private static final String queueName2 = "testQueue2";

    @BeforeEach
    public void setUp() throws IOException {
        messageFileManager.createQueueFiles(queueName1);
        messageFileManager.createQueueFiles(queueName2);
    }

    @AfterEach
    public void tearDown() throws IOException {
        messageFileManager.destroyQueueFiles(queueName1);
        messageFileManager.destroyQueueFiles(queueName2);
    }



    @Test
    public void testCreateFiles() {
        File queueDataFile1 = new File("./data/" + queueName1 + "/queue_data.txt");
        Assertions.assertEquals(true, queueDataFile1.isFile());
        File queueStatFile1 = new File("./data/" + queueName1 + "/queue_stat.txt");
        Assertions.assertEquals(true, queueStatFile1.isFile());

        File queueDataFile2 = new File("./data/" + queueName2 + "/queue_data.txt");
        Assertions.assertEquals(true, queueDataFile2.isFile());
        File queueStatFile2 = new File("./data/" + queueName2 + "/queue_stat.txt");
        Assertions.assertEquals(true, queueStatFile2.isFile());
    }

    @Test
    public void testReadWriteStat() {
        MessageFileManager.Stat stat = new MessageFileManager.Stat();
        stat.validCount = 50;
        stat.totalCount = 100;

        // 使用 Spring 提供的 反射 工具类
        ReflectionTestUtils.invokeMethod(messageFileManager, "writeStat", queueName1, stat);

        MessageFileManager.Stat newStat = ReflectionTestUtils.invokeMethod(messageFileManager, "readStat", queueName1);
        Assertions.assertEquals(100, newStat.totalCount);
        Assertions.assertEquals(50, newStat.validCount);
    }


    private MSGQueue createTestQueue(String queueName) {
        MSGQueue queue = new MSGQueue();
        queue.setName(queueName);
        queue.setDurable(true);
        queue.setAutoDelete(false);
        queue.setExclusive(false);
        return queue;
    }

    private Message createTestMessage(String content) {
        Message message = Message.createMessageWithId("testRoutingKey", null, content.getBytes());
        return message;
    }

    @Test
    public void testSendMessage() throws IOException, MqException, ClassNotFoundException {
        Message message = createTestMessage("testMessage");
        MSGQueue queue = createTestQueue(queueName1);

        messageFileManager.sendMessage(queue, message);

        // 检查 stat 文件
        MessageFileManager.Stat stat = ReflectionTestUtils.invokeMethod(messageFileManager, "readStat", queueName1);
        Assertions.assertEquals(1, stat.validCount);
        Assertions.assertEquals(1, stat.totalCount);

        // 检查 data 文件
        LinkedList<Message> messages = messageFileManager.loadAllMessageFromQueue(queueName1);
        Assertions.assertEquals(1, messages.size());
        Message curMessage = messages.get(0);
        Assertions.assertEquals(message.getMessageId(), curMessage.getMessageId());
        Assertions.assertEquals(message.getRoutingKey(), curMessage.getRoutingKey());
        Assertions.assertEquals(message.getDeliverMode(), curMessage.getDeliverMode());
        Assertions.assertArrayEquals(message.getBody(), curMessage.getBody());

        System.out.println("message: " + curMessage);
    }



    @Test
    public void testLoadAllMessageFromQueue() throws IOException, MqException, ClassNotFoundException {

        MSGQueue queue = createTestQueue(queueName1);
        List<Message> expectedMessages = new LinkedList<>();
        // 写入
        for (int i = 0; i < 100; i++) {
            Message message = createTestMessage("testMessage" + 1);
            messageFileManager.sendMessage(queue, message);
            expectedMessages.add(message);
        }

        // 读取
        LinkedList<Message> actualMessages = messageFileManager.loadAllMessageFromQueue(queueName1);
        Assertions.assertEquals(expectedMessages.size(), actualMessages.size());

        for (int i = 0; i < expectedMessages.size(); i++) {
            Message expectedMessage = expectedMessages.get(i);
            Message actualMessage = actualMessages.get(i);
            System.out.println("[" + i + "]" + actualMessage);

            Assertions.assertEquals(expectedMessage.getMessageId(), actualMessage.getMessageId());
            Assertions.assertEquals(expectedMessage.getRoutingKey(), actualMessage.getRoutingKey());
            Assertions.assertEquals(expectedMessage.getDeliverMode(), actualMessage.getDeliverMode());
            Assertions.assertArrayEquals(expectedMessage.getBody(), actualMessage.getBody());
            Assertions.assertEquals(0x1, actualMessage.getIsValid());
        }
    }


    @Test
    public void testDeleteMessage() throws IOException, MqException, ClassNotFoundException {
        MSGQueue queue = createTestQueue(queueName1);
        List<Message> expectedMessages = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            Message message = createTestMessage("testMessage" + i);
            messageFileManager.sendMessage(queue, message);
            expectedMessages.add(message);
        }

        // 删除几个
        messageFileManager.deleteMessage(queue, expectedMessages.get(7));
        messageFileManager.deleteMessage(queue, expectedMessages.get(8));
        messageFileManager.deleteMessage(queue, expectedMessages.get(9));

        LinkedList<Message> actualMessages = messageFileManager.loadAllMessageFromQueue(queueName1);
        Assertions.assertEquals(7, actualMessages.size());
        for (int i = 0; i < actualMessages.size(); i++) {
            Message expectedMessage = expectedMessages.get(i);
            Message actualMessage = actualMessages.get(i);
            System.out.println("[" + i + "]" + actualMessage);

            Assertions.assertEquals(expectedMessage.getMessageId(), actualMessage.getMessageId());
            Assertions.assertEquals(expectedMessage.getRoutingKey(), actualMessage.getRoutingKey());
            Assertions.assertEquals(expectedMessage.getDeliverMode(), actualMessage.getDeliverMode());
            Assertions.assertArrayEquals(expectedMessage.getBody(), actualMessage.getBody());
            Assertions.assertEquals(0x1, actualMessage.getIsValid());
        }
    }


    @Test
    public void testGC() throws IOException, MqException, ClassNotFoundException {
        MSGQueue queue = createTestQueue(queueName1);
        List<Message> expectedMessages = new LinkedList<>();
        for (int i = 0; i < 100; i++) {
            Message message = createTestMessage("testMessage" + i);
            messageFileManager.sendMessage(queue, message);
            expectedMessages.add(message);
        }

        // 获取gc前文件的大小
        File beforeGCFile = new File("./data/" + queueName1 + "/queue_data.txt");
        long beforeGCLength = beforeGCFile.length();

        // 删除文件
        for (int i = 0; i < 100; i += 2) {
            messageFileManager.deleteMessage(queue, expectedMessages.get(i));
        }

        // 手动GC
        messageFileManager.gc(queue);

        // 重新读取文件，检验
        LinkedList <Message> actualMessages = messageFileManager.loadAllMessageFromQueue(queueName1);
        Assertions.assertEquals(50, actualMessages.size());
        for (int i = 0; i < actualMessages.size(); i++) {
            Message expectedMessage = expectedMessages.get(2 * i + 1);
            Message actualMessage = actualMessages.get(i);

            Assertions.assertEquals(expectedMessage.getMessageId(), actualMessage.getMessageId());
            Assertions.assertEquals(expectedMessage.getRoutingKey(), actualMessage.getRoutingKey());
            Assertions.assertEquals(expectedMessage.getDeliverMode(), actualMessage.getDeliverMode());
            Assertions.assertArrayEquals(expectedMessage.getBody(), actualMessage.getBody());
            Assertions.assertEquals(0x1, actualMessage.getIsValid());
        }

        File afterGCFile = new File("./data/" + queueName1 + "/queue_data.txt");
        long afterGCLength = afterGCFile.length();
        System.out.println("before" + beforeGCLength);
        System.out.println("after" + actualMessages);
        Assertions.assertTrue(beforeGCLength > afterGCLength);
    }

}
