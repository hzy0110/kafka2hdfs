package com.hzy.util;

import java.util.UUID;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * Created by Hzy on 2016/8/3.
 */
public abstract  class BaseJob implements Job {
    private String jobName = UUID.randomUUID().toString().replaceAll("-", "");
    private String groupName = UUID.randomUUID().toString().replaceAll("-", "");
    private String cron = "0 30 10 ? * 6L 2015-2016";// 2015年至2016年的每月的最后一个星期五10:30触发一次事件
    /** 任务状态 0停用 1启用 2删除 */
    private String jobStatus;
    /** 任务描述 */
    private String desc;

    //public abstract void doJob(JobExecutionContext context);

//    @Override
//    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {

//    }

//    @Override
//    public void execute(JobExecutionContext context)
//            throws JobExecutionException {
//        //doJob(context);
//    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public String getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(String jobStatus) {
        this.jobStatus = jobStatus;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}