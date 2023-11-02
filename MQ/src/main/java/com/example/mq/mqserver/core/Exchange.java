package com.example.mq.mqserver.core;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/*
*  表示一个交换机
* */
public class Exchange {

    //使用 name 作为交换机的唯一身份标识
    private String name;

    // 交换机类型 DIRECT、FANOUT、TOPIC
    private ExchangeType type = ExchangeType.DIRECT;

    // 该交换机是否持久化存储
    private boolean durable = false;

    // 如果交换机，没人使用，自动删除
    private boolean autoDelete = false;

    // argument 表示创建交换机时指定的额外的参数选项
    private Map<String,Object> arguments = new HashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ExchangeType getType() {
        return type;
    }

    public void setType(ExchangeType type) {
        this.type = type;
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

//    用于数据库交互时使用

    // 把当前 arguments 参数，从 Map 转成 String （JSON）
    public String getArguments() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(arguments);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return "{}";
    }


    // 从数据库读取数据后，构造 Exchange 对象
    public void setArguments(String argumentsJson) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            this.arguments = objectMapper.readValue(argumentsJson, new TypeReference<HashMap<String,Object>>(){});
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }


//    测试时候使用
    public Object getArguments(String key) {
        return arguments.get(key);
    }

    public void setArguments(String key, Object value) {
        arguments.put(key,value);
    }
}
