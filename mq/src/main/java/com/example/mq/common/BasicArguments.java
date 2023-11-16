package com.example.mq.common;

/*
*  公共参数/辅助字段
*
*/


import java.io.Serializable;

public class BasicArguments implements Serializable {

    protected String rid; // 一次请求/响应的身份标识
    protected String channelId;



    public String getRid() {
        return rid;
    }

    public void setRid(String rid) {
        this.rid = rid;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }
}
