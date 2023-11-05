package com.example.mq.mqserver.core;


/*
*  表示一个绑定
*/
public class Binding {

    private String exchangeNme;
    private String queueName;
    private String bindingKey;


    public String getExchangeName() {
        return exchangeNme;
    }

    public void setExchangeName(String exchangeNme) {
        this.exchangeNme = exchangeNme;
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
