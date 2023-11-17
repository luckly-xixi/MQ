package com.example.mq.mqclient;



import com.example.mq.common.MqException;
import com.example.mq.common.Request;
import com.example.mq.common.Response;

import java.io.*;
import java.net.Socket;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class Connection {

    private Socket socket = null;

    private ConcurrentHashMap<String, Channel> channelMap = new ConcurrentHashMap<>();

    private InputStream inputStream;
    private OutputStream outputStream;
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;

    public Connection(String host, int port) throws IOException {
        socket = new Socket(host, port);

        inputStream = socket.getInputStream();
        outputStream = socket.getOutputStream();
        dataInputStream = new DataInputStream(inputStream);
        dataOutputStream = new DataOutputStream(outputStream);
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
