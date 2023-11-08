package com.example.mq.mqserver.datacenter;


import com.example.mq.common.BinaryTool;
import com.example.mq.common.MqException;
import com.example.mq.mqserver.core.MSGQueue;
import com.example.mq.mqserver.core.Message;

import java.io.*;
import java.util.LinkedList;
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

    public void init() {

    }

    // 获取指定队列对应的消息文件所在路径
    private String getQueueDir(String queueName) {
        return "./data/" + queueName;
    }

    // 获取该队列的消息数据文件路径
    private String getQueueDataPath(String queueName) {
      return getQueueDir(queueName) + "/queue_data.txt"; // 二进制文件一般用  .bin / .dat
    }

    // 获取该队列的消息统计文件的路径
    private String getQueueStatPath(String queueName) {
        return getQueueDir(queueName) + "/queue_stat.txt";
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
        writeStat(queueName, stat);
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


    // 加载磁盘文件到内存
    public LinkedList<Message> loadAllMessageFromQueue(String queueName) throws IOException, MqException, ClassNotFoundException {
        LinkedList<Message> messages = new LinkedList<>();
        try(InputStream inputStream = new FileInputStream(getQueueDataPath(queueName))) {
            try(DataInputStream dataInputStream = new DataInputStream(inputStream)) {
                // 文件中不止一条数据
                long currentOffset = 0;
                while (true) {
                    // 读取到的文件长度
                    // readInt 方法读到文件末尾会直接抛出 EOFException 异常
                    int messageSize = dataInputStream.readInt();
                    // 按照文件长度读取消息内容
                    byte[] buffer = new byte[messageSize];
                    int actualSize = dataInputStream.read(buffer);
                    // 文件可能会出错
                    if(messageSize != actualSize) {
                        throw new MqException("[MessageFileManger] 文件格式错误！ queueName=" + queueName);
                    }
                    // 读取的二进制数据，反序列化
                    Message message = (Message) BinaryTool.fromBytes(buffer);
                    if(message.getIsValid() == 0x0) {
                        // 无效数据直接跳过，并且更新currentOffset
                        currentOffset += (4 + messageSize);
                        continue;
                    }
                    // 处理有效数据
                    message.setOffsetBeg(currentOffset + 4);
                    message.setOffsetEnd(currentOffset + 4 + messageSize);
                    currentOffset += (4 + messageSize);
                    messages.add(message);
                }
            } catch (EOFException e ) {
                // 这个异常是 readInt 方法抛出，并非是异常而是正确的业务逻辑，也就是读取到了文件末尾
                System.out.println("[MessageFileManager] 恢复 Message 数据完成！");
            }
        }
        return messages;
    }



    // 检查文件是否要GC
    public boolean checkGC(String queueName) {
        Stat stat = readStat(queueName);
        if(stat.totalCount > 2000 && (double)stat.validCount / (double) stat.totalCount < 0.5) {
            return true;
        }
        return false;
    }

    private String getQueueDataNamePath(String queueName) {
        return getQueueDir(queueName) + "/queue_data_new.txt";
    }

    // 执行GC 使用复制算法
    public void gc(MSGQueue queue) throws MqException, IOException, ClassNotFoundException {
        // gc比较耗时，打印gc时间，产看是否是gc引起的卡顿
        synchronized (queue) { // gc 时防止多线程访问，记得加锁
            long gcBeg = System.currentTimeMillis();

            // 1. 创建一个新文件
            File queueDataNewFile = new File(getQueueDataNamePath(queue.getName()));
            if(queueDataNewFile.exists()) {
                // 如果新文件存在，说明上次gc为执行完毕
                throw new MqException("[MessageFileManager] gc 时发现消息队列的 queue_data_new 已经存在！ queue Name=" +
                        queue.getName());
            }
            boolean ok = queueDataNewFile.createNewFile();
            if(!ok) {
                throw new MqException("[MessageFileManger] 创建 gc 文件失败！ queueDataNewFile=" + queueDataNewFile.getAbsolutePath());
            }

            // 2. 从旧文件中，读取出所有有效消息
            LinkedList<Message> messages = loadAllMessageFromQueue(queue.getName());

            // 3. 把有效消息写入新文件
            try(OutputStream outputStream = new FileOutputStream(queueDataNewFile)) {
                try(DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {

                    for(Message message : messages) {
                        byte[] buffer = BinaryTool.toBytes(message);
                        dataOutputStream.writeInt(buffer.length);
                        dataOutputStream.write(buffer);
                    }
                }
            }

            // 4. 删除旧的数据文件，并且把新文件进行重命名
            File queueDataOldFile = new File(getQueueDataPath(queue.getName()));
            ok = queueDataOldFile.delete();
            if(!ok) {
                throw new MqException("[MessageFileManger] 删除旧的文件失败！ queueDataOldFile=" + queueDataOldFile.getAbsolutePath());
            }
            ok = queueDataNewFile.renameTo(queueDataOldFile);
            if(!ok) {
                throw new MqException("[MessageFileManager] 文件重命名失败！ queueDataNewFile=" + queueDataNewFile.getAbsolutePath() + "，queueDataOldFile=" + queueDataOldFile.getAbsolutePath());
            }

            // 5. 更新统计文件
            Stat stat = readStat(queue.getName());
            stat.totalCount = messages.size();
            stat.validCount = messages.size();
            writeStat(queue.getName(), stat);

            long gcEnd = System.currentTimeMillis();
            System.out.println("[MessageFileManager] gc 执行完毕！ queueName=" + queue.getName() + "time=" + (gcEnd - gcBeg) + "ms");
        }
    }

}
