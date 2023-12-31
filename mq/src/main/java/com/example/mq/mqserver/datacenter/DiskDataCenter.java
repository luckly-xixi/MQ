package com.example.mq.mqserver.datacenter;


import com.example.mq.common.MqException;
import com.example.mq.mqserver.core.Binding;
import com.example.mq.mqserver.core.Exchange;
import com.example.mq.mqserver.core.MSGQueue;
import com.example.mq.mqserver.core.Message;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/*
*  管理所有硬盘上的数据 (数据库 和 文件)
*/
public class DiskDataCenter {
    // 管理数据库中消息
    private DataBaseManager dataBaseManager = new DataBaseManager();
    // 管理文件中数据
    private MessageFileManager messageFileManager = new MessageFileManager();


    public void init() {
        dataBaseManager.init();
        messageFileManager.init();
    }


    // 封装交换机操作
    public void insertExchange(Exchange exchange) {
        dataBaseManager.insertExchange(exchange);
    }

    public void deleteExchange(String exchangeName) {
        dataBaseManager.deleteExchange(exchangeName);
    }

    public List<Exchange> selectAllExchanges() {
        return dataBaseManager.selectAllExchanges();
    }


    // 封装队列操作
    public void insertQueue(MSGQueue queue) throws IOException {
        // 创建队列的同时，不仅要把队列的对象写道数据库中，还需要创建出对于的目录结构
        dataBaseManager.insertQueue(queue);
        messageFileManager.createQueueFiles(queue.getName());
    }

    public void deleteQueue(String queueName) throws IOException {
        // 删除队列的同时，不仅把队列从数据库删除，还需要删除对应的目录结构
        dataBaseManager.deleteQueue(queueName);
        messageFileManager.destroyQueueFiles(queueName);
    }

    public List<MSGQueue> selectAllQueue() {
        return dataBaseManager.selectAllQueues();
    }


    // 封装绑定操作
    public void insertBinding(Binding binding) {
        dataBaseManager.insertBinding(binding);
    }

    public void deleteBinding(Binding binding) {
        dataBaseManager.deleteBinding(binding);
    }

    public List<Binding> selectAllBindings() {
        return dataBaseManager.selectAllBindings();
    }


    // 封装消息操作
    public void sendMessage(MSGQueue queue, Message message) throws IOException, MqException {
        messageFileManager.sendMessage(queue, message);
    }

    public void deleteMessage(MSGQueue queue, Message message) throws IOException, ClassNotFoundException, MqException {
        messageFileManager.deleteMessage(queue, message);
        if(messageFileManager.checkGC(queue.getName())) {
            messageFileManager.gc(queue);
        }
    }

    public LinkedList<Message> loadAllMessageFromQueue(String queueName) throws IOException, MqException, ClassNotFoundException {
        return messageFileManager.loadAllMessageFromQueue(queueName);
    }
}
