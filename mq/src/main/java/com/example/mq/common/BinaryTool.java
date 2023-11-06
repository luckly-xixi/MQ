package com.example.mq.common;

import java.io.*;

public class BinaryTool {

    // 把一个对象序列化为一个字节数组
    public static byte[] toBytes(Object object) throws IOException {
        // 变长字节数字
        try(ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();) {
            try(ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
                objectOutputStream.writeObject(object);
            }
            return byteArrayOutputStream.toByteArray();
        }
    }

    // 把一个字节数组反序列化为一个对象
    public static Object fromBytes(byte[] data) throws IOException, ClassNotFoundException {
        Object object = null;
        try(ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data)) {
            try (ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)){
                object = objectInputStream.readObject();
            }
        }
        return object;
    }
}
