package com.example.mq.mqclient;

import java.io.IOException;

public class ConnectionFactory {
    // BrokerServer 的 IP 地址
    private String host;

    private int port;

    // BrokerServer 的哪个虚拟主机
//    private String virtualHostName;
//    private String username;
//    private String password;

    public Connection newConnection() throws IOException {
        Connection connection = new Connection(host, port);
        return connection;
    }


    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
