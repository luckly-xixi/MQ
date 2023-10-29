package com.example.mq.mqserver.mapper;


import com.example.mq.mqserver.core.Binding;
import com.example.mq.mqserver.core.Exchange;
import com.example.mq.mqserver.core.MSGQueue;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface MetaMapper {
    //建库建表
    void createExchangeTable();
    void createQueueTable();
    void createBindingTable();

    //插入删除
    void insertExchange(Exchange exchange);
    List<Exchange> selectAllExchanges();
    void deleteExchange(String exchangeName);
    void insertQueue(MSGQueue queue);
    List<MSGQueue> selectAllQueues();
    void deleteQueue(String queueName);
    void insertBinding(Binding binding);
    List<Binding> selectAllBindings();
    void deleteBinding(Binding binding);
}
