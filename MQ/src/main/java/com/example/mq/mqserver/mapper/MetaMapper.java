package com.example.mq.mqserver.mapper;


import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface MetaMapper {

    void createExchangeTable();
    void createQueueTable();
    void createBindingTable();
}
