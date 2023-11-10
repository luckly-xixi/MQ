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
import java.util.List;
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
        System.out.println("[MemoryDataCenter] 新消息添加成功！ messageId=" + message.getMessageId());
    }

    public Message getMessage(String messageId) {
        return messageMap.get(messageId);
    }

    public void removeMessage(String messageId) {
        messageMap.remove(messageId);
        System.out.println("[MemoryDataCenter] 消息被移除！ messageId=" + messageId);
    }


    //队列和消息之间的关联
    // 发送消息到指定队列
    public void sendMessage(MSGQueue queue, Message message) {
        // 注意：如果队列不存在要先创建队列
//        LinkedList<Message> messages = queueMessageMap.get(queue.getName());
//        if(messages == null) {
//            messages = new LinkedList<>();
//            queueMessageMap.put(queue.getName(), messages);
//        }
        // 上方代码可以简化为：
        LinkedList<Message> messages = queueMessageMap.computeIfAbsent(queue.getName(), k -> new LinkedList<>());
        synchronized (messages) {
            messages.add(message);
        }
        // 在消息中也加一份
        addMessage(message);
        System.out.println("[MemoryDataCenter] 消息被投递到队列！ messageId=" + message.getMessageId());
    }

    // 从队列中取消息
    public Message pollMessage(String queueName) {
        LinkedList<Message> messages = queueMessageMap.get(queueName);
        if(messages == null) {
            return null;
        }
        synchronized (messages) {
            if (messages.size() == 0) {
                return null;
            }
            Message currentMessage = messages.remove(0);
            System.out.println("[MemoryDataCenter] 消息从队列中取出！ messageId=" + currentMessage.getMessageId());
            return currentMessage;
        }
    }

    // 获取指定队列中的消息个数
    public int getMessageCount(String queueName) {
        LinkedList<Message> messages = queueMessageMap.get(queueName);
        if (messages == null) {
            return 0;
        }

        synchronized(messages) {
            return messages.size();
        }
    }


    //未确认消息
    // 添加未确认的消息
    public void addMessageWaitAck(String queueName, Message message) {
        ConcurrentHashMap<String, Message> messageHashMap = queueMessageWaitAckMap.computeIfAbsent(queueName,
                k -> new ConcurrentHashMap<>());
        messageHashMap.put(message.getMessageId(), message);
        System.out.println("[MemoryDataCenter] 消息进入待确认队列！ messageId=" + message.getMessageId());
    }

    // 删除未确认的消息（消息已经确认了）
    public void removeMessageWaitAck(String queueName, String messageId) {
        ConcurrentHashMap<String, Message> messageHashMap = queueMessageWaitAckMap.get(queueName);
        if (messageHashMap == null) {
            return;
        }
        messageHashMap.remove(messageId);
        System.out.println("[MemoryDataCenter] 消息从待确认队列删除！ messageId=" + messageId);
    }

    // 获取指定的未确认的消息
    public Message getMessageWaitAck(String queueName, String messageId) {
        ConcurrentHashMap<String, Message> messageHashMap = queueMessageWaitAckMap.get(queueName);
        if (messageHashMap == null) {
            return null;
        }
        return messageHashMap.get(messageId);
    }




    // 重启后加载硬盘持久化数据到内存
    public void recovery(DiskDataCenter diskDataCenter) {
        // 将之前的数据都给清空
        exchangeMap.clear();
        queueMap.clear();
        bindingsMap.clear();
        messageMap.clear();
        queueMessageMap.clear();

        // 1.恢复交换机数据
        List<Exchange> exchanges = diskDataCenter.selectAllExchanges();
        for(Exchange exchange : exchanges) {
            exchangeMap.put(exchange.getName(), exchange);
        }
        // 2.恢复队列数据
        diskDataCenter.selectAllQueue()
        // 3.恢复绑定数据
        // 4.恢复消息数据

    }
}



