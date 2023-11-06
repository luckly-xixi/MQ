package com.example.mq.mqserver.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/*
 * 表示交换机
 */
public class Exchange {
    //  交换机唯一身份标识
    private String name;
    // 交换机类型, DIRECT, FANOUT, TOPIC
    private ExchangeType type = ExchangeType.DIRECT;
    // 是否持久化
    private boolean durable = false;
    // 没人使用自动被删除.
    private boolean autoDelete = false;
    // 额外的参数选项
    // 为了把这个 arguments 存到数据库中, 就需要把 Map 转成 json 格式的字符串.
    private Map<String, Object> arguments = new HashMap<>();



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

    // 这里的 get set 用于和数据库交互使用.
    public String getArguments() {
        // 是把当前的 arguments 参数, 从 Map 转成 String (JSON)
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(arguments);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        // 代码异常, 返回一个空的 json 字符串
        return "{}";
    }

    // 从数据库读数据之后, 构造 Exchange 对象, 会自动调用
    public void setArguments(String argumentsJson) {
        // 把参数中的 argumentsJson 按照 JSON 格式解析, 转成 Map 对象
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            this.arguments = objectMapper.readValue(argumentsJson, new TypeReference<HashMap<String, Object>>() {});
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    // 测试使用

    public Object getArguments(String key) {
        return arguments.get(key);
    }

    public void setArguments(String key, Object value) {
        arguments.put(key, value);
    }

//    public void setArguments(Map<String, Object> arguments) {
//        this.arguments = arguments;
//    }
}
