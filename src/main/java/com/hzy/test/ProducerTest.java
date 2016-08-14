package com.hzy.test;

import java.util.Date;
import java.util.Properties;
import java.text.SimpleDateFormat;

import com.hzy.util.PropertiesUtil;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Created by Hzy on 2016/7/26.
 * kafka發送
 */
public class ProducerTest {

    public static void main(String[] args) {
        ProducerTest producerTest = new ProducerTest();
        producerTest.send();
    }

    private void send(){
        Properties props = new Properties();
        props.put("zk.connect", PropertiesUtil.getValue("ZooKeeper"));
        // serializer.class为消息的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // 配置metadata.broker.list, 为了高可用, 最好配两个broker实例
        props.put("metadata.broker.list", "slave1:9092,slave2:9092");
        // 设置Partition类, 对队列进行合理的划分
        //props.put("partitioner.class", "idoall.testkafka.Partitionertest");
        // ACK机制, 消息发送需要kafka服务端确认
        props.put("request.required.acks", "1");

        props.put("num.partitions", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        for (int i = 0; i < 10; i++) {
            // KeyedMessage<K, V>
            // 　　K对应Partition Key的类型
            // 　　V对应消息本身的类型
//　　 topic: "test", key: "key", message: "message"
//            SimpleDateFormat formatter = new SimpleDateFormat   ("yyyy年MM月dd日 HH:mm:ss SSS");
//            Date curDate = new Date(System.currentTimeMillis());//获取当前时间
            String str = "";
            if (i < 5)
                str = "cd:20160805130003,aid:zhongxin,pid:201607121101,adt:0,iid:543,ap:513,dev:VLAN_20150609f655bebd-8d04-427a-bd03-ab697c96115a,proid:3305,m:185936162D75,ip:192.168.34.53,in:1609,t:PIC,ct:CE,os:ios9,bro:safari,ic:1";
            else
                str = "cd:201608051"+3+"0003,aid:zhongxin,pid:201607121101,adt:0,iid:543,ap:513,dev:VLAN_20150609f655bebd-8d04-427a-bd03-ab697c96115a,proid:3305,m:185936162D75,ip:192.168.34.53,in:1609,t:PIC,ct:CE,os:ios9,bro:safari,ic:1";

            //String msg = "idoall.org" + i+"="+str;
            String key = i + "";
            producer.send(new KeyedMessage<String, String>("testadv", key, str));
        }
    }
}
