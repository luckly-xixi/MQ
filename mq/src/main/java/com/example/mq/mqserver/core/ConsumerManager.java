package com.example.mq.mqserver.core;

/*
*   消费消息的核心逻辑
*/

import com.example.mq.common.Consumer;
import com.example.mq.common.ConsumerEnv;
import com.example.mq.common.MqException;
import com.example.mq.mqserver.VirtualHost;

import javax.lang.model.element.VariableElement;
import java.util.concurrent.*;

public class ConsumerManager {
        // 持有上层的 VirtualHost 对象的引用，来操作数据
        private VirtualHost parent;
        // 指定一个线程池，负责具体的回调任务
        private ExecutorService workerPool = Executors.newFixedThreadPool(4);
        // 存放令牌的队列
        private BlockingQueue<String> tokenQueue = new LinkedBlockingQueue<>();
        // 扫描线程
        private Thread scannerTread = null;

        public ConsumerManager(VirtualHost p) {
                parent = p;

                scannerTread = new Thread(() -> {
                        while(true) {
                                try {
                                        // 获取令牌
                                        String queueName = tokenQueue.take();
                                        // 根据令牌找队列
                                        MSGQueue queue = p.getMemoryDataCenter().getQueue(queueName);
                                        if (queue == null) {
                                                throw new MqException("[ConsumerManager] 取令牌后发现，该队列名不存在！ queueName=" + queueName);
                                        }
                                        // 从队列中消费消息
                                        synchronized (queue) {
                                                consumeMessage(queue);
                                        }
                                } catch (InterruptedException | MqException e) {
                                        e.printStackTrace();
                                }
                        }
                });
                // 把线程设为后台线程
                scannerTread.setDaemon(true);
                scannerTread.start();
        }

        // 发送消息的时候调用
        public void notifyConsume(String queueName) throws InterruptedException {
                tokenQueue.put(queueName);
        }

        public void addConsumer(String consumerTag, String queueName, boolean autoAck, Consumer consumer) throws MqException {
                MSGQueue queue = parent.getMemoryDataCenter().getQueue(queueName);
                if(queue == null) {
                        throw new MqException("[ConsumerManager] 队列不存在！ queueName=" + queueName);
                }
                ConsumerEnv consumerEnv = new ConsumerEnv(consumerTag, queueName, autoAck, consumer);
                synchronized (queue) {
                        queue.addConsumerEnv(consumerEnv);
                        // 如果队列中已经有消息，需要立即处理（消费）
                        int n = parent.getMemoryDataCenter().getMessageCount(queueName);
                        for (int i = 0; i < n; i++) {
                                // 调用一次消费一条消息
                                consumeMessage(queue);
                        }
                }
        }

        private void consumeMessage(MSGQueue queue) {
                // 轮询找消费者
                ConsumerEnv luckyDog = queue.chooseConsumer();
                if(luckyDog == null) {
                        return;
                }

                // 队列中取消息
                Message message = parent.getMemoryDataCenter().pollMessage(queue.getName());
                if (message == null) {
                        return;
                }

                // 消息放入消费者的回调函数，交给线程池完成
                workerPool.submit(() -> {
                        try {
                                // 消息先(执行回调之前)放入待确认集合，放丢失
                                parent.getMemoryDataCenter().addMessageWaitAck(queue.getName(), message);
                                // 回调
                                luckyDog.getConsumer().handleDelivery(luckyDog.getConsumerTag(), message.getBasicProperties(),
                                        message.getBody());
                                // 自动应答(true)就直接删除消息，手动应答(false)则交给 basicAck 处理
                                if(luckyDog.isAutoAck()) {
                                        // 硬盘
                                        if(message.getDeliverMode() == 2) {
                                                parent.getDiskDataCenter().deleteMessage(queue, message);
                                        }
                                        // 待确认集合
                                        parent.getMemoryDataCenter().removeMessageWaitAck(queue.getName(), message.getMessageId());
                                        // 内存中心
                                        parent.getMemoryDataCenter().removeMessage(message.getMessageId());
                                        System.out.println("[ConsumerManager] 消息被成功消费！ queueName=" + queue.getName()
                                                + "messageId=" + message.getMessageId());
                                }
                        } catch (Exception e) {
                                e.printStackTrace();
                        }
                });
        }
}
