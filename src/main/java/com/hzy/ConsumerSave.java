package com.hzy;

import com.hzy.util.DateUtil;
import com.hzy.util.HdfsDAO;
import com.hzy.util.PropertiesUtil;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Vector;

/**
 * Created by Hzy on 2016/7/26.
 */
public class ConsumerSave implements Runnable {

    //注意线程安全
    private static final Logger logger = LogManager.getLogger(ConsumerSave.class);
    private KafkaStream m_stream;
    private int m_threadNumber;
    private static final String consumerSaveHdfsPath = PropertiesUtil.getValue("ConsumerSaveHdfsPath");
    public ConsumerSave(KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        Vector<String> vector = new Vector<>();

        while (it.hasNext()) {
            MessageAndMetadata<byte[], byte[]> next = it.next();
            String itMsg = new String(next.message());
            //cd:格式开头的串才写入
            if(itMsg.startsWith("cd:"))
                vector.add(itMsg + "\n");
            //msg +=  itMsg + "\n";
            //logger.info("Thread " + m_threadNumber + ": " + itMsg);
        }
        logger.info("vector.size=" + vector.size());
        HdfsDAO hdfsDAO = new HdfsDAO();
        try {
            //logger.info("hdfs start date:" + DateUtil.getDate());
//            if(hdfsDAO.exists("/tmp/kafka/advert/001.txt")){
//
//            }
            //創建並寫入
            if (vector.size() > 0) {
                logger.info("有消息創建寫入");
                String msg = "";
                String dateNow;
                String dateNext = "";
                String year = "";
                String month = "";
                String day = "";
                String hour = "";
                //String minute = "";
                //獲取時間
                for (int i =0;i<vector.size();i++) {
                    try {
                        dateNow = vector.get(i).substring(5, 13);
                        //在最后一条之前，获取下一条的时间
                        if (i < vector.size() - 1)
                            dateNext = vector.get(i + 1).substring(5, 13);
                        msg += vector.get(i);

                        //logger.info("dateNow:" + dateNow);
                        //logger.info("dateNext:" + dateNext);


                        //比较这一条和下一条的的时间差异，不同就获取时间并保存
                        if (!dateNext.equals(dateNow)) {

                            //logger.info("進入不同");

                            year = dateNow.substring(0, 2);
                            month = dateNow.substring(2, 4);
                            day = dateNow.substring(4, 6);
                            hour = dateNow.substring(6, 8);

                            hdfsDAO.createFile(consumerSaveHdfsPath + year + "/" + month + "/" + day + "/" + hour + "/"
                                    + System.currentTimeMillis() + "-" + m_threadNumber + ".txt", msg);
                            //logger.info("不同寫入的msg："+msg);
                            msg = "";
                        }
                        //在最后一条的时候把剩余的写入
                        if (i == vector.size() - 1) {
                            //有数据才写入
                            if (msg.length() > 0) {
                                year = dateNow.substring(0, 2);
                                month = dateNow.substring(2, 4);
                                day = dateNow.substring(4, 6);
                                hour = dateNow.substring(6, 8);
                                hdfsDAO.createFile(consumerSaveHdfsPath + year + "/" + month + "/" + day + "/" + hour + "/"
                                        + System.currentTimeMillis() + "-" + m_threadNumber + ".txt", msg);
                                logger.info("最後寫入的msg：" + msg);
                                msg = "";
                            }
                        }
                    } catch (IOException e) {
                        logger.error("循环数据错误");
                    }
                }
            } else
                logger.info("沒有消息。不創建");
        } catch (Exception e) {
            logger.info("kafka消息寫入hdfs出錯了，錯誤原因：" + e.getMessage());
        }
        //logger.info("Shutting down Thread: " + m_threadNumber);
    }
}
