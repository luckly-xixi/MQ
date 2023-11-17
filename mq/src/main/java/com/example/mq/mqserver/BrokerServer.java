package com.example.mq.mqserver;

/*
*  消息队列的本体服务器
*  本质上是 TCP 服务器
*/

import com.example.mq.common.*;
import com.example.mq.mqserver.core.BasicProperties;
import com.example.mq.mqserver.core.Exchange;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BrokerServer {

    private ServerSocket serverSocket = null;

    private VirtualHost virtualHost = new VirtualHost("default");

    // 哪些客户端在和服务器进行通信
    // key 是 channelId ， value 是对应的 Socket 对象
    private ConcurrentHashMap<String, Socket> sessions = new ConcurrentHashMap<String, Socket>();

    // 来处理多个客户端请求的线程池
    private ExecutorService executorService = null;

    // 控制服务器是否继续运行
    private volatile boolean runnable = true;


    public BrokerServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }


    // 启动服务器
    public void start() throws IOException {
        System.out.println("[BrokerServer] 启动！");
        executorService = Executors.newCachedThreadPool();
        while (runnable) {
            Socket clientSocket = serverSocket.accept();
            executorService.submit(() -> {
                processConnection(clientSocket);
            });
        }
    }

    // 停止服务器
    public void stop() throws IOException {
        runnable = false;
        executorService.shutdownNow();
        serverSocket.close();
    }


    // 处理一个客户端连接，可能涉及到多个请求和响应
    private void processConnection(Socket clientSocket) {
        try(InputStream inputStream = clientSocket.getInputStream();
            OutputStream outputStream = clientSocket.getOutputStream()){
            try(DataInputStream dataInputStream = new DataInputStream(inputStream);
                DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
                while(true) {
                    // 读取请求并解析
                    Request request = readRequest(dataInputStream);
                    // 根据请求计算响应
                    Response response = process(request, clientSocket);
                    // 写回响应给客户端
                    writeResponse(dataOutputStream, response);
                }
            }
        } catch (EOFException |SocketException e) {
            // 读取到文件末尾
            System.out.println("[BrokerServer] connection 关闭！ 客户端地址：" + clientSocket.getInetAddress().toString()
                    + ":" + clientSocket.getPort());
        } catch(IOException | ClassNotFoundException | MqException e) {
            System.out.println("[BrokerServer] connection 出现异常！");
            e.printStackTrace();
        }finally {
            try {
                // 连接处理完，关闭 socket
                clientSocket.close();
                // 清理连接中的会话channel
                clearCloseSession(clientSocket);
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
    }



    private Request readRequest(DataInputStream dataInputStream) throws IOException {
        Request request = new Request();
        request.setType(dataInputStream.readInt());
        request.setLength(dataInputStream.readInt());
        byte[] payload = new byte[request.getLength()];
        int n = dataInputStream.read(payload);
        if (n != request.getLength()) {
            throw new IOException("读取请求格式出错！");
        }
        request.setPayload(payload);
        return null;
    }

    private void writeResponse(DataOutputStream dataOutputStream, Response response) throws IOException {
        dataOutputStream.writeInt(response.getType());
        dataOutputStream.writeInt(response.getLength());
        dataOutputStream.write(response.getPayload());
        // 刷新缓存区
        dataOutputStream.flush();
    }

    private Response process(Request request, Socket clientSocket) throws IOException, ClassNotFoundException, MqException {
        // 1.初步解析 request 的 payload
        BasicArguments basicArguments = (BasicArguments) BinaryTool.fromBytes(request.getPayload());
        System.out.println("[Request] rid=" + basicArguments.getRid() + ",channelId=" + basicArguments.getChannelId()
         + ",type=" + request.getType() + ",length=" + request.getLength());
        // 2.根据 type 区分
        boolean ok = true;
        if(request.getType() == 0x1) { // 创建 channel
            sessions.put(basicArguments.getChannelId(), clientSocket);
            System.out.println("[BrokerServer] 创建 channel 完成！ channelId=" + basicArguments.getChannelId());
        } else if(request.getType() == 0x2) { // 销毁 channel
            sessions.remove(basicArguments.getChannelId());
            System.out.println("[BrokerServer] 销毁 channel 完成！ channelId=" + basicArguments.getChannelId());
        } else if(request.getType() == 0x3) { // 创建交换机
            ExchangeDeclareArguments arguments = (ExchangeDeclareArguments) basicArguments;
            ok = virtualHost.exchangeDeclare(arguments.getExchangeName(), arguments.getExchangeType(),
                    arguments.isDurable(), arguments.isAutoDelete(), arguments.getArguments());
        } else if(request.getType() == 0x4) { // 销毁交换机
            ExchangeDeleteArguments arguments = (ExchangeDeleteArguments) basicArguments;
            ok = virtualHost.exchangeDelete(arguments.getExchangeName());
        } else if(request.getType() == 0x5) { // 创建队列
            QueueDeclareArguments arguments = (QueueDeclareArguments) basicArguments;
            ok = virtualHost.queueDeclare(arguments.getQueueName(), arguments.isDurable(),
                     arguments.isExclusive(), arguments.isAutoDelete(), arguments.getArguments());
        } else if(request.getType() == 0x6) { // 销毁队列
            QueueDeleteArguments arguments = (QueueDeleteArguments) basicArguments;
            ok = virtualHost.queueDelete(arguments.getQueueName());
        } else if(request.getType() == 0x7) { // 创建绑定
            QueueBindArguments arguments = (QueueBindArguments) basicArguments;
            ok = virtualHost.queueBind(arguments.getQueueName(), arguments.getExchangeName(), arguments.getBindingKey());
        } else if(request.getType() == 0x8) { // 销毁绑定
            QueueUnbindArguments arguments = (QueueUnbindArguments) basicArguments;
            ok = virtualHost.queueUnBind(arguments.getQueueName(), arguments.getExchangeName());
        } else if(request.getType() == 0x9) { // 发送消息
            BasicPublishArguments arguments = (BasicPublishArguments) basicArguments;
            ok = virtualHost.basicPublish(arguments.getExchangeName(), arguments.getRoutingKey(),
                    arguments.getBasicProperties(), arguments.getBody());
        } else if(request.getType() == 0xa) { // 订阅消息
            BasicConsumeArguments arguments = (BasicConsumeArguments) basicArguments;
            ok = virtualHost.basicConsume(arguments.getConsumerTag(), arguments.getQueueName(),
                    arguments.isAutoAck(), new Consumer() {
                       // 把服务器收到的消息直接推送给对应的消费者客户端
                        @Override
                        public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) throws MqException, IOException {
                            // 根据 channelId 找到 socket 对象
                            Socket clientSocket = sessions.get(consumerTag);
                            if(clientSocket == null || clientSocket.isClosed()) {
                                throw new MqException("[BrokerServer] 订阅消息的客户端已经关闭！");
                            }
                            // 构造响应数据
                            SubScribeReturns subScribeReturns = new SubScribeReturns();
                            subScribeReturns.setChannelId(consumerTag);
                            subScribeReturns.setRid(""); // 只有相应，没有请求，不需要对应
                            subScribeReturns.setOk(true);
                            subScribeReturns.setConsumerTag(consumerTag);
                            subScribeReturns.setBasicProperties(basicProperties);
                            subScribeReturns.setBody(body);

                            byte[] payload = BinaryTool.toBytes(subScribeReturns);
                            Response response = new Response();
                            response.setType(0xc);
                            response.setLength(payload.length);
                            response.setPayload(payload);
                            // 写回客户端，切记不要关 dataOutputStream ，会导致clientSocket对象的也关闭
                            DataOutputStream dataOutputStream = new DataOutputStream(clientSocket.getOutputStream());
                            writeResponse(dataOutputStream, response);
                        }
                    });
        } else if(request.getType() == 0xb) { // 调用 BasicAck
            BasicAckArguments arguments = (BasicAckArguments) basicArguments;
            ok = virtualHost.basicAck(arguments.getQueueName(), arguments.getMessageId());
        } else { // type 非法
            throw new MqException("[BrokerServer] 未知的 type！ type=" + request.getType());
        }
        // 3.构造响应
        BasicReturns basicReturns = new BasicReturns();
        basicReturns.setChannelId(basicArguments.getChannelId());
        basicReturns.setRid(basicArguments.getRid());
        basicReturns.setOk(ok);

        byte[] payload = BinaryTool.toBytes(basicReturns);
        Response response = new Response();
        response.setType(request.getType());
        response.setLength(payload.length);
        response.setPayload(payload);
        System.out.println("[Response] rid=" + basicReturns.getRid() + "，channelId=" + basicReturns.getChannelId()
                + "，type=" + response.getType() + "，length=" + response.getLength());
        return response;
    }

    private void clearCloseSession(Socket clientSocket) {

        List<String> toDeleteChannelId = new ArrayList<>();
        for(Map.Entry<String, Socket> entry : sessions.entrySet()) {
            if(entry.getValue() == clientSocket) {
                // sessions.remove(entry.getKey()) 切记不能直接删除，会导致迭代器失效
                toDeleteChannelId.add(entry.getKey());
            }
        }
        for(String channelId : toDeleteChannelId) {
            sessions.remove(channelId);
        }
        System.out.println("[BrokerServer] 清理 sessions 完成！ 被清理的 channelId=" + toDeleteChannelId);
    }

}
