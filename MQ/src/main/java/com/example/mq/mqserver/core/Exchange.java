package com.example.mq.mqserver.core;


import java.util.HashMap;
import java.util.Map;

/*
*  表示一个交换机
* */
public class Exchange {

    //使用 name 作为交换机的唯一身份标识
    private String name;

    // 交换机类型 DIRECT、FANOUT、TOPIC
    private ExchangeType exchangeType = ExchangeType.DIRECT;

    // 该交换机是否持久化存储
    private boolean durable = false;

    // 如果交换机，没人使用，自动删除
    private boolean autoDelete = false;

    // argument 表示创建交换机时指定的额外的参数选项
    private Map<String,Object> argument = new HashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ExchangeType getExchangeType() {
        return exchangeType;
    }

    public void setExchangeType(ExchangeType exchangeType) {
        this.exchangeType = exchangeType;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
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
