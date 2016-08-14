package com.hzy.test;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * Created by Hzy on 2016/7/28.
 */
public class TestJobOne  implements Job {

    /**
     * 执行任务的方法
     */
    public void execute(JobExecutionContext context) throws JobExecutionException {
        System.out.println("================执行任务一....");

        //do more...这里可以执行其他需要执行的任务
    }

}
