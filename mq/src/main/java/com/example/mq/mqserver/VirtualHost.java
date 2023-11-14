package com.example.mq.mqserver;


import com.example.mq.common.MqException;
import com.example.mq.mqserver.core.*;
import com.example.mq.mqserver.datacenter.DiskDataCenter;
import com.example.mq.mqserver.datacenter.MemoryDataCenter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*
*  虚拟主机，管理自己的  交换机、队列、绑定、消息  数据
* 同时提供  api  供上层调用
*
*/
public class VirtualHost {

    private String virtualHostName;
    private MemoryDataCenter memoryDataCenter = new MemoryDataCenter();
    private DiskDataCenter diskDataCenter = new DiskDataCenter();
    private Router router = new Router();

    // 创建锁对象
    private final Object exchangeLocker = new Object();  // 交换机锁
    private final Object queueLocker = new Object();  //  队列锁

    public String getVirtualHostName() {
        return virtualHostName;
    }

    public MemoryDataCenter getMemoryDataCenter() {
        return memoryDataCenter;
    }

    public DiskDataCenter getDiskDataCenter() {
        return diskDataCenter;
    }

    public VirtualHost (String name) {
        this.virtualHostName = name;
        // 初始化
        diskDataCenter.init();
        // 恢复数据
        try {
            memoryDataCenter.recovery(diskDataCenter);
        } catch (IOException | MqException | ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("[VirtualHost] 恢复内存数据失败！");
        }
    }


    // 创建交换机
    public boolean exchangeDeclare(String exchangeName, ExchangeType exchangeType, boolean durable, boolean autoDelete,
                                   Map<String, Object> arguments) {
        // 在交换机前加虚拟机名来区分不同的虚拟机
        exchangeName = virtualHostName + exchangeName;

        try {
            synchronized (exchangeLocker) {
                Exchange existsExchange = memoryDataCenter.getExchange(exchangeName);

                if (exchangeName != null) {
                    System.out.println("[VirtualHost] 交换机已存在！ exchangeName=" + exchangeName);
                    return true;
                }
                Exchange exchange = new Exchange();
                exchange.setName(exchangeName);
                exchange.setType(exchangeType);
                exchange.setDurable(durable);
                exchange.setAutoDelete(autoDelete);
                exchange.setArguments(arguments);

                // 先写硬盘后写内存，硬盘容易出错，报错后不会进行内存的写入（防止再对内存进行删除操作）
                if (durable) {
                    diskDataCenter.insertExchange(exchange);
                }

                memoryDataCenter.insertExchange(exchange);
                System.out.println("[VirtualHost] 交换机创建完成！ exchangeName=" + exchangeName);
            }
            return true;
        } catch (Exception e) {
            System.out.println("[VirtualHost] 交换机创建失败！ exchangeName=" + exchangeName);
            e.printStackTrace();
            return false;
        }
    }


    // 删除交换机
    public boolean exchangeDelete(String exchangeName) {
        exchangeName = virtualHostName + exchangeName;

        try {
            synchronized(exchangeLocker) {
                Exchange toDelete = memoryDataCenter.getExchange(exchangeName);
                if (toDelete == null) {
                    throw new MqException("[VirtualHost] 交换机不存在无法删除!");
                }

                if (toDelete.isDurable()) {
                    diskDataCenter.deleteExchange(exchangeName);
                }

                memoryDataCenter.deleteExchange(exchangeName);
                System.out.println("[VirtualHost] 交换机删除成功！ exchangeName=" + exchangeName);
            }
            return true;
        } catch(Exception e) {
            System.out.println("[VirtualHost] 交换机删除失败！ exchangeName=" + exchangeName);
            e.printStackTrace();
            return false;
        }
    }


    // 创建队列
    public boolean queueDeclare(String queueName, boolean durable, boolean exclusive, boolean autoDelete,
                                Map<String, Object> arguments) {
        queueName = virtualHostName + queueName;

        try {
            synchronized(queueLocker) {
                MSGQueue existsQueue = memoryDataCenter.getQueue(queueName);
                if (existsQueue != null) {
                    System.out.println("[VirtualHosts] 队列已经存在！ queueName=" + queueName);
                    return true;
                }

                MSGQueue queue = new MSGQueue();
                queue.setName(queueName);
                queue.setDurable(durable);
                queue.setExclusive(exclusive);
                queue.setAutoDelete(autoDelete);
                queue.setArguments(arguments);

                if (durable) {
                    diskDataCenter.insertQueue(queue);
                }

                memoryDataCenter.insertQueue(queue);
                System.out.println("[VirtualHost] 队列创建成功！ queueName=" + queueName);
            }
            return true;
        } catch(Exception e) {
            System.out.println("[VirtualHost] 队列创建失败！ queueName=" + queueName);
            e.printStackTrace();
            return false;
        }
    }


    // 删除队列
    public boolean queueDelete(String queueName) {
        queueName = virtualHostName + queueName;

        try {
            synchronized(queueLocker) {
                MSGQueue toDelete = memoryDataCenter.getQueue(queueName);
                if (toDelete == null) {
                    throw new MqException("[VirtualHost] 队列不存在无法删除！ queueName=" + queueName);
                }

                if (toDelete.isDurable()) {
                    diskDataCenter.deleteQueue(queueName);
                }
                memoryDataCenter.deleteQueue(queueName);
                System.out.println("[VirtualHost] 队列删除成功！ queueName=" + queueName);
            }
            return true;
        } catch (Exception e) {
            System.out.println("[VirtualHost] 队列删除失败！ queueName=" + queueName);
            e.printStackTrace();
            return false;
        }
    }



    // 创建绑定
    public boolean queueBind(String queueName, String exchangeName, String bindingKey) {
        queueName = virtualHostName + queueName;
        exchangeName = virtualHostName + exchangeName;
        try {
            synchronized (exchangeLocker){
                synchronized (queueLocker) {
                    Binding existsBinding = memoryDataCenter.getBinding(exchangeName, queueName);
                    if(existsBinding != null) {
                        throw new MqException("[VirtualHost] binding 已存在！ queueName=" + queueName +
                                "，exchangeName=" + exchangeName);
                    }
                    // 验证 bindingKey 是否合法
                    if(!router.checkBindingKey(bindingKey)) {
                        throw new MqException("[VirtualHost] bindingKey 非法！ bindingKey=" + bindingKey);
                    }
                    Binding binding = new Binding();
                    binding.setExchangeName(exchangeName);
                    binding.setQueueName(queueName);
                    binding.setBindingKey(bindingKey);

                    // 验证交换机和队列是否存在
                    MSGQueue queue = memoryDataCenter.getQueue(queueName);
                    if(queue == null) {
                        throw new MqException("[VirtualHost] 队列不存在！ queueName=" + queueName);
                    }
                    Exchange exchange = memoryDataCenter.getExchange(exchangeName);
                    if (exchange == null) {
                        throw new MqException("[VirtualHost] 交换机不存在！ exchangeName=" + exchangeName);
                    }

                    if(exchange.isDurable() && queue.isDurable()) {
                        diskDataCenter.insertBinding(binding);
                    }
                    memoryDataCenter.insertBinding(binding);
                    System.out.println("[VirtualHost] 绑定创建成功! exchangeName=" + exchangeName +
                            "，queueName=" + queueName);
                }
            }
            return true;
        } catch(Exception e) {
            System.out.println("[VirtualHost] 创建失败！");
            e.printStackTrace();
            return false;
        }
    }


    //  删除绑定
    public boolean queueUnBind(String queueName, String exchangeName) {
        queueName = virtualHostName + queueName;
        exchangeName = virtualHostName + exchangeName;

        try {
            synchronized (exchangeLocker) {
                synchronized(queueLocker) {
                    Binding toDelete = memoryDataCenter.getBinding(exchangeName, queueName);
                    if (toDelete == null) {
                        throw new MqException("[VirtualHost] 删除绑定失败绑定不存在！ exchangeName=" + exchangeName +
                                "，queueName=" + queueName);
                    }
                    //  考虑到如果用户先删除 交换机 和 队列 之后再删除 绑定 会删不掉 绑定
//            MSGQueue queue = memoryDataCenter.getQueue(queueName);
//            if (queue == null) {
//                throw new MqException("[Virtual] 对应的队列不存在！queueName=" + queueName);
//            }
//            Exchange exchange = memoryDataCenter.getExchange(exchangeName);
//            if (exchange == null) {
//                throw new MqException("[Virtual] 对应的交换机不存在！ exchangeName=" + exchangeName);
//            }
//
//            if(exchange.isDurable() && queue.isDurable()) {
//                diskDataCenter.deleteBinding(toDelete);
//            }
                    diskDataCenter.deleteBinding(toDelete);

                    memoryDataCenter.deleteBinding(toDelete);
                    System.out.println("[VirtualHost] 删除绑定成功！");
                }
            }
            return true;
        } catch (Exception e) {
            System.out.println("[VirtualHost] 删除绑定失败！");
            e.printStackTrace();
            return false;
        }
    }


    //  发送消息到指定的 交换机/队列 中
    public boolean basicPublish(String exchangeName, String routingKey, BasicProperties basicProperties, byte[] body) {
        exchangeName = virtualHostName + exchangeName;

        try {
            if (!router.checkRoutingKey(routingKey)) {
                throw new MqException("[VirtualHost] routingKey 非法！ routingKey=" + routingKey);
            }
            Exchange exchange = memoryDataCenter.getExchange(exchangeName);
            if (exchange == null) {
                throw new MqException("[VirtualHost] 交换机不存在! exchangeName=" + exchangeName);
            }
            // 判断交换机的类型
            if(exchange.getType() == ExchangeType.DIRECT) { // 直接交换机
                // 虚拟机名 + routingKey 就是队列名
                String queueName = virtualHostName + routingKey;
                // 构造消息
                Message message = Message.createMessageWithId(routingKey, basicProperties, body);
                // 查询队列是否存在
                MSGQueue queue = memoryDataCenter.getQueue(queueName);
                if (queue == null) {
                    throw new MqException("[VirtualHost] 队列不存在！ queueName=" + queueName);
                }
                sendMessage(queue, message);
            } else { // fanout 和 topic 交换机
                // 获取交换机的所有队列
                ConcurrentHashMap<String, Binding> bindingsMap = memoryDataCenter.getBindings(exchangeName);
                // 遍历
                for(Map.Entry<String, Binding> entry : bindingsMap.entrySet()) {
                    Binding binding = entry.getValue();
                    MSGQueue queue = memoryDataCenter.getQueue(binding.getQueueName());
                    if (queue == null) {
                        System.out.println("[VirtualHost] basicPublish 发送消息时，发现队列不存在！ queueName=" + binding);
                        continue;
                    }
                    Message message = Message.createMessageWithId(routingKey, basicProperties, body);
                    if(!router.route(exchange.getType(), binding, message)) {
                        continue;
                    }
                    sendMessage(queue, message);
                }
            }
            return true;
        } catch(Exception e) {
            System.out.println("[VirtualHost] 消息发送失败!");
            e.printStackTrace();
            return false;
        }
    }

    private void sendMessage(MSGQueue queue, Message message) throws IOException, MqException {
        int deliverMode = message.getDeliverMode();
        if(deliverMode == 2) {
            diskDataCenter.sendMessage(queue, message);
        }
        memoryDataCenter.sendMessage(queue, message);
    }
}
