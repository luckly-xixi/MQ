package com.example.mq.mqserver.core;

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

    private Map<String,Object> argument = new HashMap<>();

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

    public Map<String, Object> getArgument() {
        return argument;
    }

    public void setArgument(Map<String, Object> argument) {
        this.argument = argument;
    }
}
