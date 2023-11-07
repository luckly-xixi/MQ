package com.example.mq.common;



/*
*  自定义异常
*
*/

public class MqException extends Exception{
    public MqException (String reason) {
        super(reason);
    }
}
