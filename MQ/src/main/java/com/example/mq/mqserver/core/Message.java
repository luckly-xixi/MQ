package com.example.mq.mqserver.core;


import java.io.Serializable;
import java.util.UUID;

/*
*  表示一个传递的消息,消息能够在网路上传输，并且需要写入到文件（支持序列化反序列化）
* */
public class Message implements Serializable {

    // 消息属性
    private BasicProperties basicProperties = new BasicProperties();

    // 消息正文
    private byte[] body;


    // 辅助属性
//    [offsetBeg,offsetEnd)
    private transient long offsetBeg = 0; // 消息数据开头距离文件开头位置的偏移量(字节)
    private transient long offsetEnd = 0; // 消息数据结尾距离文件开头位置的偏移量(字节)

    private byte isValid = 0x1; // 是否有效 （逻辑删除 0x1：有效 0x0：无效）



    //创建一个工厂方法,用来封装传创建的Message对象,自动生成ID
    public static Message createMessageWithId(String routingKey,BasicProperties basicProperties,byte[] body) {
        Message message = new Message();
        if(basicProperties != null) {
            message.setBasicProperties(basicProperties);
        }
        message.setMessageId("M-" + UUID.randomUUID());
        message.setRoutingKey(routingKey);
        message.body = body;
        return message;
    }


    public String getMessageId() {
        return basicProperties.getMessageId();
    }

    public void setMessageId(String messageId) {
        basicProperties.setMessageId(messageId);
    }

    public String getRoutingKey() {
        return basicProperties.getRoutingKey();
    }

    public void setRoutingKey(String routingKey) {
        basicProperties.setRoutingKey(routingKey);
    }

    public int getDeliverMode() {
        return basicProperties.getDeliverMode();
    }

    public void setDeliverMode(int mode) {
        basicProperties.setDeliverMode(mode);
    }





    public BasicProperties getBasicProperties() {
        return basicProperties;
    }

    public void setBasicProperties(BasicProperties basicProperties) {
        this.basicProperties = basicProperties;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public long getOffsetBeg() {
        return offsetBeg;
    }

    public void setOffsetBeg(long offsetBeg) {
        this.offsetBeg = offsetBeg;
    }

    public long getOffsetEnd() {
        return offsetEnd;
    }

    public void setOffsetEnd(long offsetEnd) {
        this.offsetEnd = offsetEnd;
    }

    public byte getIsValid() {
        return isValid;
    }

    public void setIsValid(byte isValid) {
        this.isValid = isValid;
    }
}
