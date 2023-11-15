package com.example.mq.mqserver.core;


import com.example.mq.common.ConsumerEnv;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * 存储消息的队列
 * MSG => Message
 */
public class MSGQueue {
    // 队列的身份标识.
    private String name;
    // 是否持久化
    private boolean durable = false;
    // 是否独有
    private boolean exclusive = false;
    //没人使用自动删除
    private boolean autoDelete = false;

    private Map<String, Object> arguments = new HashMap<>();

    // 当前队列有哪些消费者订阅
    private List<ConsumerEnv> consumerEnvList = new ArrayList<>();

    // 记录当前取到了第几个消费者, 方便实现轮询策略，考虑到多线程使用原子操作
    private AtomicInteger consumerSeq = new AtomicInteger(0);

    // 添加一个订阅者
    public void addConsumerEnv(ConsumerEnv consumerEnv) {
            consumerEnvList.add(consumerEnv);
    }

    // 挑选一个消费者，来处理消息（轮询）
    public ConsumerEnv chooseConsumer() {
        if(consumerEnvList.size() == 0) {
            return null;
        }
        int index = consumerSeq.get() % consumerEnvList.size();
        consumerSeq.getAndIncrement(); // 自增
        return consumerEnvList.get(index);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public void setExclusive(boolean exclusive) {
        this.exclusive = exclusive;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public void setAutoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
    }

    public String getArguments() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(arguments);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return "{}";
    }

    public void setArguments(String argumentsJson) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            this.arguments = objectMapper.readValue(argumentsJson, new TypeReference<HashMap<String, Object>>() {});
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }


    public Object getArguments(String key) {
        return arguments.get(key);
    }

    public void setArguments(String key, Object value) {
        arguments.put(key, value);
    }

    public void setArguments(Map<String, Object> arguments) {
        this.arguments = arguments;
    }
}
