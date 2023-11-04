package com.example.mq.mqserver.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

// MSG -> Message
/*
* 表示存储消息的队列
* */
public class MSGQueue {
    private String name;

    private boolean durable = false;

    // 是否是独有的
    private boolean exclusive = false;

    private boolean autoDelete = false;

    private Map<String,Object> arguments = new HashMap<>();

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



//    数据库交互使用
    public String getArguments() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
           return objectMapper.writeValueAsString(arguments);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return "{}";
    }

    public void setArguments(String arguments) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            this.arguments = objectMapper.readValue(arguments, new TypeReference<HashMap<String, Object>>() {});
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }


//    测试使用
//    public Object getArguments(String key) {
//        return arguments.get(key);
//    }
//
//    public void setArguments(String key, Object value) {
//        arguments.put(key, value);
//    }

    public Object getArguments1(String key) {
        return arguments.get(key);
    }

    public void setArguments1(String key, Object value) {
        arguments.put(key, value);
    }
}
