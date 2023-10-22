package com.example.mq.mqserver.core;


/*
*  表示队列和交换机之间的关系
* */
public class Binding {

    private String exchangeName;

    private String queueName;

    private String bindingKey;


    public String getExchangeName() {

        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getBindingKey() {
        return bindingKey;
    }

    public void setBindingKey(String bindingKey) {
        this.bindingKey = bindingKey;
    }

}
