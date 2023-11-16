package com.example.mq.common;

/*
*  表示各个远程调用的方法的返回值的公共信息
*
*/


import java.io.Serializable;

public class BasicReturns implements Serializable {

    protected String rid;
    protected String channelId;
    protected boolean ok; // 返回值



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

    public boolean isOk() {
        return ok;
    }

    public void setOk(boolean ok) {
        this.ok = ok;
    }
}
