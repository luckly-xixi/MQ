package com.example.mq.mqserver.core;


import java.io.Serializable;

public class BasicProperties implements Serializable {

    //      消息的唯一身份标识
    private String messageId;
    private String routingKey;
    // 消息是否持久化
    private int deliverMode = 1; //1不持久化




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

    @Override
    public String toString() {
        return "BasicProperties{" +
                "messageId='" + messageId + '\'' +
                ", routingKey='" + routingKey + '\'' +
                ", deliverMode=" + deliverMode +
                '}';
    }
}
