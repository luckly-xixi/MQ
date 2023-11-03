package com.example.mq.mqserver.datacenter;


import com.example.mq.MqApplication;
import com.example.mq.mqserver.core.Binding;
import com.example.mq.mqserver.core.Exchange;
import com.example.mq.mqserver.core.ExchangeType;
import com.example.mq.mqserver.core.MSGQueue;
import com.example.mq.mqserver.mapper.MetaMapper;
import org.apache.ibatis.annotations.Mapper;

import java.io.File;
import java.security.PrivateKey;
import java.util.List;

/*
*   整合数据库操作
* */
public class DataBaseManager {

    private MetaMapper metaMapper;


    //针对数据库进行初始化
    public void init() {

        metaMapper = MqApplication.context.getBean(MetaMapper.class);

        if(!checkDBExists()) {
            //创建数据库
            File dataDir = new File("./data");
            dataDir.mkdirs();

            createTable();
            createDefaultData();
            System.out.println("[DataBaseManager] 数据库初始化完成!");
        } else {
            System.out.println("[DataBaseManager] 数据库已经存在!");
        }
    }

    public void deleteDB() {
        File file = new File("./data/meta.db");
        boolean ret = file.delete();
        if(ret) {
            System.out.println("[DataBaseManager] 删除数据库文件成功!");
        } else {
            System.out.println("[DataBaseManager] 删除数据库文件失败!");
        }

        File dataDir = new File("./data");
        ret = dataDir.delete();
        if(ret) {
            System.out.println("[DataBaseManager] 删除数据库目录成功!");
        } else {
            System.out.println("[DataBaseManager] 删除数据库目录失败!");
        }
    }

    private boolean checkDBExists() {
        File file = new File("./data/meta.db");
        if(file.exists()) {
            return true;
        }
        return false;
    }

    private void createTable() {
        metaMapper.createExchangeTable();
        metaMapper.createQueueTable();
        metaMapper.createBindingTable();
        System.out.println("[DataBaseManager] 创建表完成!");
    }

    private void createDefaultData() {
        Exchange exchange = new Exchange();
        exchange.setName("");
        exchange.setType(ExchangeType.DIRECT);
        exchange.setDurable(true);
        exchange.setAutoDelete(false);
        metaMapper.insertExchange(exchange);
        System.out.println("[DataBaseManager] 创建初始数据完成!");
    }

    public void insertExchange(Exchange exchange) {
        metaMapper.insertExchange(exchange);
    }

    public List<Exchange> selectAllExchanges() {
        return metaMapper.selectAllExchanges();
    }

    public void deleteExchange(String exchangeName) {
        metaMapper.deleteExchange(exchangeName);
    }

    public void insertQueue(MSGQueue queue) {
        metaMapper.insertQueue(queue);
    }

    public List<MSGQueue> selectAllQueues() {
        return metaMapper.selectAllQueues();
    }

    public void deleteQueue(String queueName) {
        metaMapper.deleteQueue(queueName);
    }

    public void insertBinding(Binding binding) {
        metaMapper.insertBinding(binding);
    }

    public List<Binding> selectAllBindings() {
        return metaMapper.selectAllBindings();
    }

    public void deleteBinding(Binding binding) {
        metaMapper.deleteBinding(binding);
    }
}
