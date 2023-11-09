package com.example.mq.mqserver.datacenter;

/*
*  统一管理内存中所有数据
*/

import com.example.mq.common.MqException;
import com.example.mq.mqserver.core.Binding;
import com.example.mq.mqserver.core.Exchange;
import com.example.mq.mqserver.core.MSGQueue;
import com.example.mq.mqserver.core.Message;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryDataCenter {

    // 后续可能涉及到多线程
    // 交换机  Key是 exchangeName ， value是 exchange 对象
    private ConcurrentHashMap<String, Exchange> exchangeMap = new ConcurrentHashMap<>();

    // 队列  Key是 queueName ， value是 MSGQueue 对象
    private ConcurrentHashMap<String, MSGQueue> queueMap = new ConcurrentHashMap<>();

    // 绑定  第一个Key是 exchangeName ， 第二个 Key 是 queueName
    private ConcurrentHashMap<String, ConcurrentHashMap<String, Binding>> bindingsMap = new ConcurrentHashMap<>();

    // 消息  Key是 messageId ， value是 message 对象
    private ConcurrentHashMap<String, Message> messageMap = new ConcurrentHashMap<>();

    // 队列和消息之间的关联  Key是 queueName ， value是 MSGQueue 链表
    private ConcurrentHashMap<String, LinkedList<Message>> queueMessageMap = new ConcurrentHashMap<>();

    // 未确认消息  第一个Key是 queueName ， 第二个 Key 是 messageId
    private ConcurrentHashMap<String, ConcurrentHashMap<String, Message>> queueMessageWaitAckMap = new ConcurrentHashMap<>();


    //交换机
    public void insertExchange(Exchange exchange) {
        exchangeMap.put(exchange.getName(), exchange);
        System.out.println("[MemoryDataCenter] 新交换机添加成功！ exchangeName=" + exchange.getName());
    }

    public Exchange getExchange(String exchangeName) {
        return exchangeMap.get(exchangeName);
    }

    public void deleteExchange(String exchangeName) {
        exchangeMap.remove(exchangeName);
        System.out.println("[MemoryDataCenter] 交换机删除成功！ exchangeName=" + exchangeName);
    }


    //队列
    public void insertQueue(MSGQueue queue) {
        queueMap.put(queue.getName(), queue);
        System.out.println("[MemoryDataCenter] 新队列添加成功！ queueName=" + queue.getName());
    }

    public MSGQueue getQueue(String queueName) {
        return queueMap.get(queueName);
    }

    public void deleteQueue(String queueName) {
        queueMap.remove(queueName);
        System.out.println("[MemoryDataCenter] 队列删除成功！ queueName=" + queueName);
    }


    //绑定
    public void insertBinding(Binding binding) throws MqException {
        // 对应的哈希表不存在，创建一个再插入
//        ConcurrentHashMap<String, Binding> bindingMap = bindingsMap.get(binding.getExchangeName());
//        if(bindingMap == null) {
//            bindingMap = new ConcurrentHashMap<>();
//            bindingsMap.put(binding.getExchangeName(), bindingMap);
//        }
//    }


        // 上方代码的简化
        ConcurrentHashMap<String, Binding> bindingMap = bindingsMap.computeIfAbsent(binding.getExchangeName(),
                k -> new ConcurrentHashMap<>());
        synchronized (bindingMap) {
            // 不存在才能插入
            if(bindingMap.get(binding.getQueueName()) != null) {
                throw new MqException("[MemoryDataCenter] 绑定已经存在！ exchangeName=" + binding.getExchangeName() +
                        "，queueName=" + binding.getQueueName());
            }
            bindingMap.put(binding.getQueueName(), binding);
        }
        System.out.println("[MemoryDataCenter] 新绑定添加成功！ exchangeName=" + binding.getExchangeName() +
                "，queueName" + binding.getQueueName());
    }

    public Binding getBinding(String exchangeName, String queueName) {
        ConcurrentHashMap<String, Binding> bindingMap = bindingsMap.get(exchangeName);
        if (bindingMap == null) {
            return null;
        }
        return bindingMap.get(queueName);
    }

    public ConcurrentHashMap<String, Binding> getBindings(String exchangeName) {
        return bindingsMap.get(exchangeName);
    }

    public void deleteBinding(Binding binding) throws MqException {
        ConcurrentHashMap<String, Binding> bindingMap = bindingsMap.get(binding.getExchangeName());
        if (bindingMap == null) {
            throw new MqException("[MemoryDataCenter] 绑定不存在！exchangeName=" + binding.getExchangeName() +
                    "，queueName=" + binding.getQueueName());
        }
        bindingsMap.remove(binding.getQueueName());
        System.out.println("[MemoryDataCenter] 绑定删除成功！ exchangeName=" + binding.getExchangeName() +
                "，queueName" + binding.getQueueName());
    }


    //消息
    public void addMessage(Message message) {
        messageMap.put(message.getMessageId(), message);
        System.out.println("[MemoryDataCenter] 新消息添加成功！ message=" + message.getMessageId());
    }

    public Message getMessage(String messageId) {
        return messageMap.get(messageId);
    }

    public void removeMessage(String messageId) {
        messageMap.remove(messageId);
        System.out.println("[MemoryDataCenter] 消息被移除！ message=" + messageId);
    }



    //队列和消息之间的关联


    //未确认消息
}



