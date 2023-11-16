package com.example.mq.common;

import com.example.mq.mqserver.core.BasicProperties;

import java.io.IOException;

/*
*  函数式接口
*/


@FunctionalInterface
public interface Consumer {

    void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) throws MqException, IOException;
}
