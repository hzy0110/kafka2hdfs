package com.hzy.test;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.hzy.util.HdfsDAO;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by Hzy on 2016/7/26.
 */
public class ConsumerTest extends Thread{
    private static final Logger logger = LogManager.getLogger(ConsumerTest.class);
    private final ConsumerConnector consumer;
    private final String topic;

    public static void main(String[] args) {
        ConsumerTest consumerThread = new ConsumerTest("test");
        consumerThread.start();



    }
    public ConsumerTest(String topic) {
        consumer =kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
        this.topic =topic;
    }

    private static ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        // 设置zookeeper的链接地址
        props.put("zookeeper.connect","hadoop01:2181,hadoop02:2181,hadoop03:2181");
        // group 代表一个消费组
        props.put("group.id", "advert");
        // kafka的group 消费记录是保存在zookeeper上的, 但这个信息在zookeeper上不是实时更新的, 需要有个间隔时间更新
        props.put("auto.commit.interval.ms", "1000");
        props.put("zookeeper.session.timeout.ms","10000");
        props.put("zookeeper.sync.time.ms", "2000");
        return new ConsumerConfig(props);
    }

    public void run(){
        //设置Topic=>Thread Num映射关系, 构建具体的流
        Map<String,Integer> topickMap = new HashMap<String, Integer>();
        topickMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[],byte[]>>>  streamMap=consumer.createMessageStreams(topickMap);
        KafkaStream<byte[],byte[]>stream = streamMap.get(topic).get(0);
        ConsumerIterator<byte[],byte[]> it =stream.iterator();
        String msg = "";
        logger.info("*********Results********");

        while(it.hasNext()){
            String itMsg = new String(it.next().message());
            msg +=  itMsg + "\n";
            logger.info("get data:" + itMsg);
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }



        HdfsDAO hdfsDAO = new HdfsDAO();
        logger.info("7");
        try {
//            if(hdfsDAO.exists("/tmp/kafka/advert/001.txt")){
//
//            }
            //創建並寫入
            logger.info("8");
            if(msg.length() > 0) {
                logger.info("有消息創建寫入");
                hdfsDAO.createFile("/tmp/kafka/advert/001-"+1+".txt", msg);
            }
            else
                logger.info("沒有消息。不創建");
            logger.info("9");
        }
        catch (IOException e){
            logger.info("kafka消息寫入hdfs出錯了，錯誤原因：" + e.getMessage());
        }


    }
}
