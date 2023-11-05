package com.example.mq.mqserver.core;


import java.io.Serializable;
import java.util.UUID;

/*
*  表示一个消息
* Message 需要能够在网络上传输，并写入到文件中
*  就要对message进行序列化和反序列化
*/
public class Message implements Serializable {
    // 属性
    private BasicProperties basicProperties = new BasicProperties();
    private byte[] body;  //正文

    // 辅助属性 [offsetBeg,offsetEnd)
    private transient long offsetBeg = 0; // 消息数据开头距离文件位置的偏移量
    private transient long offsetEnd = 0; //  结尾
    // transient 是让offsetBeg和offsetEnd不参与序列化
    private byte isValid = 0x1; // 是否有效（逻辑删除） 0x1有效  0x0无效


    // 工厂方法
    public static Message createMessageWithId(String routingKey, BasicProperties basicProperties, byte[] body) {
        Message message = new Message();
        if(message != null) {
            message.setBasicProperties(basicProperties);
        }
        message.setMessageId("M" + UUID.randomUUID());
        message.basicProperties.setRoutingKey(routingKey);
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





    //自动生成
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
