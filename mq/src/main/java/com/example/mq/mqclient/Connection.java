package com.example.mq.mqclient;


import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import com.example.mq.common.*;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Connection {

    private Socket socket = null;

    private ConcurrentHashMap<String, Channel> channelMap = new ConcurrentHashMap<>();

    private InputStream inputStream;
    private OutputStream outputStream;
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;

    private ExecutorService callbackPool = null;

    public Connection(String host, int port) throws IOException {
        socket = new Socket(host, port);

        inputStream = socket.getInputStream();
        outputStream = socket.getOutputStream();
        dataInputStream = new DataInputStream(inputStream);
        dataOutputStream = new DataOutputStream(outputStream);

        callbackPool = Executors.newFixedThreadPool(4);

        // 扫描线程，负责读取响应数据
        Thread t = new Thread(() -> {
            try {
                while(!socket.isClosed()) {
                    Response response = readResponse();
                    dispatchResponse(response);
                }
            } catch (SocketException e) {
                System.out.println("[Connection] 连接正常断开！");
            } catch (IOException | ClassNotFoundException | MqException e) {
                System.out.println("[Connection] 连接异常断开！");
                e.printStackTrace();
            }
        });
        t.start();;
    }


//    public Connection(String host, int port) throws IOException {
//        socket = new Socket(host, port);
//        inputStream = socket.getInputStream();
//        outputStream = socket.getOutputStream();
//        dataInputStream = new DataInputStream(inputStream);
//        dataOutputStream = new DataOutputStream(outputStream);
//
//        callbackPool = Executors.newFixedThreadPool(4);
//
//        // 创建一个扫描线程, 由这个线程负责不停的从 socket 中读取响应数据. 把这个响应数据再交给对应的 channel 负责处理.
//        Thread t = new Thread(() -> {
//            try {
//                while (!socket.isClosed()) {
//                    Response response = readResponse();
//                    dispatchResponse(response);
//                }
//            } catch (SocketException e) {
//                // 连接正常断开的. 此时这个异常直接忽略.
//                System.out.println("[Connection] 连接正常断开!");
//            } catch (IOException | ClassNotFoundException | MqException e) {
//                System.out.println("[Connection] 连接异常断开!");
//                e.printStackTrace();
//            }
//        });
//        t.start();
//    }

    public void close() {
        try{
            callbackPool.shutdownNow();
            channelMap.clear();
            inputStream.close();
            outputStream.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    // 处理不同响应（控制请求、推送消息）
    private void dispatchResponse(Response response) throws IOException, ClassNotFoundException, MqException {
        if (response.getType() == 0xc) { // 推送的消息数据
            SubScribeReturns subScribeReturns = (SubScribeReturns) BinaryTool.fromBytes(response.getPayload());

            Channel channel = channelMap.get(subScribeReturns.getChannelId());
            if (channel == null) {
                throw new MqException("[Connection] 该消息对应的 channel 在客户端不存在！ channelId=" + channel.getChannelId());
            }

            callbackPool.submit(() -> {
                try {
                    channel.getConsumer().handleDelivery(subScribeReturns.getConsumerTag(),
                            subScribeReturns.getBasicProperties(), subScribeReturns.getBody());
                } catch (MqException | IOException e) {
                    e.printStackTrace();
                }
            });
        } else { // 控制请求
            BasicReturns basicReturns = (BasicReturns) BinaryTool.fromBytes(response.getPayload());

            Channel channel = channelMap.get(basicReturns.getChannelId());
            if (channel == null) {
                throw new MqException("[Connection] 该消息对应的 channel 在客户端中不存在！ channelId=" + channel.getChannelId());
            }
            channel.putReturns(basicReturns);
        }
    }


    // 发送请求
    public void writeRequest(Request request) throws IOException {
        dataOutputStream.writeInt(request.getType());
        dataOutputStream.writeInt(request.getLength());
        dataOutputStream.write(request.getPayload());
        dataOutputStream.flush();
        System.out.println("[Connection] 发送请求成功！ type=" + request.getType() + "，length=" + request.getLength());
    }

    // 读取响应
    public Response readResponse() throws IOException {
        Response response = new Response();
        response.setType(dataInputStream.readInt());
        response.setLength(dataInputStream.readInt());
        byte[] payload = new byte[response.getLength()];
        int n = dataInputStream.read(payload);
        if(n != response.getLength()) {
            throw new IOException("读取的响应数据不完整！");
        }
        response.setPayload(payload);
        System.out.println("[Connection] 收到响应！type=" + response.getType() + "，length=" + response.getLength());
        return response;
    }
//    public Response readResponse() throws IOException {
//        Response response = new Response();
//        response.setType(dataInputStream.readInt());
//        response.setLength(dataInputStream.readInt());
//        byte[] payload = new byte[response.getLength()];
//        int n = dataInputStream.read(payload);
//        if (n != response.getLength()) {
//            throw new IOException("读取的响应数据不完整!");
//        }
//        response.setPayload(payload);
//        System.out.println("[Connection] 收到响应! type=" + response.getType() + ", length=" + response.getLength());
//        return response;
//    }



    // 创建 channel
    public Channel createChannel() throws IOException {
        String channelId = "C-" + UUID.randomUUID().toString();
        Channel channel = new Channel(channelId, this);
        channelMap.put(channelId, channel);
        // 通知服务器
        boolean ok = channel.createChannel();
        if(!ok) {
            channelMap.remove(channelId);
            return null;
        }
        return channel;
    }

}
