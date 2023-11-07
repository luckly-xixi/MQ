package com.example.mq.mqserver.datacenter;


import com.example.mq.common.BinaryTool;
import com.example.mq.common.MqException;
import com.example.mq.mqserver.core.MSGQueue;
import com.example.mq.mqserver.core.Message;

import java.io.*;
import java.util.Scanner;

/*
*  管理存储消息的文件
*/
public class MessageFileManager {

    // 表示该队列的统计消息
    static public class Stat {
        public int totalCount;  // 总消息数量
        public int validCount;  // 有效消息数量
    }



    // 获取指定队列对应的消息文件所在路径
    private String getQueueDir(String queueName) {
        return "./data" + queueName;
    }

    // 获取该队列的消息数据文件路径
    private String getQueueDataPath(String queueName) {
      return getQueueDir(queueName) + "/queue_data.txt"; // 二进制文件一般用  .bin / .dat
    }

    // 获取该队列的消息统计文件的路径
    private String getQueueStatPath(String queueName) {
        return getQueueDir(queueName) + "./queue_stat.txt";
    }


    private Stat readStat(String queueName) {
        Stat stat = new Stat();
        try(InputStream inputStream = new FileInputStream(getQueueStatPath(queueName))) {
            Scanner scanner = new Scanner(inputStream);
            stat.totalCount = scanner.nextInt();
            stat.validCount = scanner.nextInt();
            return stat;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void writeStat(String queueName, Stat stat) {
        // OutputStream 打开文件，默认情况下，会直接把源文件清空，覆盖旧数据
        try(OutputStream outputStream = new FileOutputStream(getQueueStatPath(queueName))) {
          PrintWriter printWriter = new PrintWriter(outputStream);
          printWriter.write(stat.totalCount + "\t" + stat.validCount);
          printWriter.flush(); // 刷新缓冲区，上传数据
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // 创建队列对应的文件和目录
    public void createQueueFiles(String queueName) throws IOException {
        // 1.先创建队列对应的消息目录
        File baseDir = new File(getQueueDir(queueName));

        if(!baseDir.exists()) {
            boolean ok = baseDir.mkdirs();

            if(!ok) {
                throw new IOException("创建目录失败！ baseDir =" + baseDir.getAbsolutePath());
            }
        }

        // 2.创建队列数据文件
        File queueDataFile = new File(getQueueDataPath(queueName));

        if(!queueDataFile.exists()) {
            boolean ok = queueDataFile.createNewFile();

            if(!ok) {
                throw new IOException("创建文件失败！ queueDataFile =" + queueDataFile.getAbsolutePath());
            }
        }

        // 3.创建消息统计文件
        File queueStatFile = new File(getQueueStatPath(queueName));

        if(!queueStatFile.exists()) {

            boolean ok = queueStatFile.createNewFile();

            if(!ok) {
                throw new IOException("创建文件失败！ queueStatFile +" + queueStatFile.getAbsolutePath());
            }
        }

        // 4. 给消息统计文件设初始值：0\t0
        Stat stat = new Stat();
        stat.totalCount = 0;
        stat.validCount = 0;
        writeStat(queueName,stat);
    }


    // 删除队列目录和文件
    public void destroyQueueFiles(String queueName) throws IOException {
        File queueDataFile = new File(getQueueDataPath(queueName));
        boolean ok1 = queueDataFile.delete();

        File queueStatFile = new File(getQueueStatPath(queueName));
        boolean ok2 = queueStatFile.delete();

        File baseDir = new File(getQueueDir(queueName));
        boolean ok3 = baseDir.delete();

        if(!ok1 || !ok2 || !ok3) {
            throw new IOException("删除队列目录和文件失败！ baseDir =" + baseDir.getAbsolutePath());
        }
    }


    // 检查队列的目录和文件是否存在
    public boolean checkFileExists(String queueName) {
        File queueDataFile = new File(getQueueDataPath(queueName));
        if(!queueDataFile.exists()) {
            return false;
        }
        File queueStatFile = new File(getQueueStatPath(queueName));
        if(!queueStatFile.exists()) {
            return false;
        }
        return true;
    }


    // 把一个新消息，放到队列对应的文件中
    public void sendMessage(MSGQueue queue, Message message) throws MqException, IOException { // queue 表示要写入的队列，message表示要写入的消息
        // 1.检查要写入的队列对应的文件是否存在
        if(!checkFileExists(queue.getName())) {
            throw new MqException("[MessageFileManager] 队列队应的文件不存在！ queueName=" + queue.getName());
        }
        // 2.把 Message 对象序列化，转成二进制字节数组
        byte[] messageBinary = BinaryTool.toBytes(message);

        // 防止多线程导致数据错乱
        synchronized (queue) {
            // 3.获取当前队列数据文件的长度
            File queueDataFile = new File(getQueueDataPath(queue.getName()));

            message.setOffsetBeg(queueDataFile.length() + 4);
            message.setOffsetEnd(queueDataFile.length() + 4 + messageBinary.length);

            // 4.写入消息到数据文件，注意：追加
            try(OutputStream outputStream = new FileOutputStream(queueDataFile, true)) {
                // 注意 outputStream.write(); 方法虽然是int参数但是写入的是一个字节
                try(DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
                    // 先写消息长度，再写消息本体
                    dataOutputStream.writeInt(messageBinary.length);
                    dataOutputStream.write(messageBinary);
                }
            }

            // 5.更新消息统计
            Stat stat = readStat(queue.getName());
            stat.totalCount += 1;
            stat.validCount += 1;
            writeStat(queue.getName(), stat);
        }
    }


    // 删除消息(逻辑删除，将 isValid 设置为0)
    public void deleteMessage(MSGQueue queue, Message message) throws IOException, ClassNotFoundException {

        synchronized (queue) {
            try (RandomAccessFile randomAccessFile = new RandomAccessFile(getQueueDataPath(queue.getName()), "rw")) {
                // 1. 先从文件中获取对应的 Message 数据
                byte[] bufferSrc = new byte[(int) (message.getOffsetEnd() - message.getOffsetBeg())];
                randomAccessFile.seek(message.getOffsetBeg());
                randomAccessFile.read(bufferSrc);
                // 2. 把读取的数据转换为 Message 对象
                Message diskMessage = (Message) BinaryTool.fromBytes(bufferSrc);
                // 3. 设置 isValid 无效
                diskMessage.setIsValid((byte) 0x0);
                // 4. 写回文件
                byte[] bufferDest = BinaryTool.toBytes(diskMessage);

                randomAccessFile.seek(message.getOffsetBeg());
                randomAccessFile.write(bufferDest);
            }
            // 更新统计文件
            Stat stat = readStat(queue.getName());
            if (stat.validCount > 0) {
                stat.validCount -= 1;
            }
            writeStat(queue.getName(), stat);
        }
    }

}
