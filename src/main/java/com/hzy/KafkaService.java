package com.hzy;

import com.hzy.util.PropertiesUtil;
import com.hzy.util.QuartzUtil;
import com.hzy.util.BaseJob;
import org.quartz.*;

/**
 * Created by Hzy on 2016/7/28.
 */
public class KafkaService extends BaseJob {

    public void execute(JobExecutionContext context) throws JobExecutionException {
        String zooKeeper = PropertiesUtil.getValue("ZooKeeper");
        String groupId = PropertiesUtil.getValue("GroupId");
        String topic = PropertiesUtil.getValue("Topic");

        ConsumerGroupExample consumerGroupExample = new ConsumerGroupExample(zooKeeper,groupId,topic);
        consumerGroupExample.run(2);
        try {
            Thread.sleep(20000);
            consumerGroupExample.shutdown();
        }
        catch (InterruptedException ie) {

        }
    }

    public static void main(String[] args) throws Exception {
        String [] clas ={"com.hzy.KafkaService"};
        //String [] clas ={"com.hzy.test.TestJob"};
        String [] crons ={"0/30 * * * * ?"};
        BaseJob job = null;
        for(int i=0;i<clas.length;i++){
            Class<?> newClass = Class.forName(clas[i]);
            job = (BaseJob) newClass.newInstance();
            job.setCron(crons[i]);
            job.setJobName("job");
            job.setGroupName("group");
            QuartzUtil.addJob(job);
        }

    }
}
