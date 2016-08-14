package com.hzy.util;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

/**
 * Created by Hzy on 2016/8/3.
 */
public class QuartzUtil {
    private static SchedulerFactory schedulerFactory;
    private static Scheduler scheduler =null;
    static {
        try {
            schedulerFactory = new StdSchedulerFactory();
            scheduler = schedulerFactory.getScheduler();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 添加job
     */
    public static void addJob(BaseJob job) throws SchedulerException {

        JobDetail jobDetail = JobBuilder.newJob(job.getClass())
                .withIdentity(job.getJobName(), job.getGroupName())
                .build();

        CronTrigger trigger = TriggerBuilder
                .newTrigger()
                .withIdentity(job.getJobName(), job.getGroupName())
                .withSchedule(
                        CronScheduleBuilder.cronSchedule(job.getCron()))
                .build();

        scheduler.scheduleJob(jobDetail, trigger);
        scheduler.start();
    }

    /**
     * 更新job执行时间
     */
    public static void modifyJobCron(BaseJob job) {
        try {
            String triggerName = job.getJobName();
            String triggerGroupName = job.getGroupName();
            TriggerKey triggerKey = new TriggerKey(triggerName,
                    triggerGroupName);
            CronTrigger trigger = (CronTrigger) scheduler
                    .getTrigger(triggerKey);
            if (trigger == null) {
                return;
            }
            String oldCron = trigger.getCronExpression();
            if (!oldCron.equalsIgnoreCase(job.getCron())) {
                // 修改时间
                trigger = trigger
                        .getTriggerBuilder()
                        .withIdentity(triggerKey)
                        .withSchedule(
                                CronScheduleBuilder.cronSchedule(job.getCron()))
                        .build();
                // 重启触发器
                scheduler.rescheduleJob(triggerKey, trigger);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 暂停任务
     */
    public static void pauseJob(BaseJob job) {
        pauseJob(job.getJobName(), job.getGroupName());
    }

    /**
     * 暂停任务
     */
    public static void pauseJob(String jobName, String jobGroup) {
        handleJobStatus(1, jobName, jobGroup);
    }

    /**
     * 移除job
     */
    public static void removeJob(BaseJob job) {
        removeJob(job.getJobName(), job.getGroupName());
    }

    /**
     * 移除job
     */
    public static void removeJob(String jobName, String groupName) {
        handleJobStatus(3, jobName, groupName);
    }

    /**
     * 恢复job
     */
    public static void recoverJob(BaseJob job) {
        recoverJob(job.getJobName(), job.getGroupName());
    }

    /**
     * 恢复job
     */
    public static void recoverJob(String jobName, String groupName) {
        handleJobStatus(2, jobName, groupName);
    }

    /**
     * 处理job状态
     */
    public static void handleJobStatus(int handleType, String jobName,
                                       String groupName) {
        JobKey jobKey = JobKey.jobKey(jobName, groupName);
        try {
            switch (handleType) {
                case 1:// 暂停任务
                    scheduler.pauseJob(jobKey);
                    break;
                case 2:// 恢复任务
                    scheduler.resumeJob(jobKey);
                    break;
                case 3:// 删除任务
                    scheduler.deleteJob(jobKey);
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void startJobs() {
        try {
            // 启动
            if (!scheduler.isShutdown()) {
                scheduler.start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void shutDownJobs() {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.shutdown();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        //模拟读取
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


        //Thread.sleep(60000);
        //QuartzUtil.removeJob(job);
    }
}