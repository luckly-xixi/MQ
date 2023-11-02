package com.example.mq.mqserver.core;

import java.io.Serializable;

public class BasicProperties implements Serializable {

    // 消息身份的唯一标识,使用UUID
    private String messageId;

    // 消息带的内容，和 binding key 匹配
    private String routingKey;

    // 消息是否持久化 ： 1不持久、2持久
    private int deliverMode = 1;




    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public int getDeliverMode() {
        return deliverMode;
    }

    public void setDeliverMode(int deliverMode) {
        this.deliverMode = deliverMode;
    }
}
