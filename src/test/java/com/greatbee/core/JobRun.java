package com.greatbee.core;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Author: CarlChen
 * Date: 2017/9/5
 */
public class JobRun {
    public static void main(String[] args) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        System.out.println("开始时间：" + df.format(new Date(System.currentTimeMillis())));

        try {


//       创建jobDetail实例，绑定Job实现类
            JobDetail job = JobBuilder.newJob(TestJob.class).withIdentity("job1", "jgroup").build();
//       定义调度触发规则
            Trigger trigger = TriggerBuilder.newTrigger().withIdentity("simpleTrigger", "triggerGroup")
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/6 * * * * ?"))
                    .startNow().build();

            JobDetail job2 = JobBuilder.newJob(TestJob.class).withIdentity("job2", "jgroup").build();
            Trigger trigger2 = TriggerBuilder.newTrigger().withIdentity("simpleTrigger2", "triggerGroup")
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/2 * * * * ?"))
                    .startNow().build();


            SchedulerFactory schedulerfactory = new StdSchedulerFactory();//通过schedulerFactory获取一个调度器
            Scheduler scheduler = schedulerfactory.getScheduler();//      通过schedulerFactory获取一个调度器
            scheduler.scheduleJob(job, trigger);//       把作业和触发器注册到任务调度中
            scheduler.scheduleJob(job2, trigger2);//       把作业和触发器注册到任务调度中
            scheduler.start();//       启动调度

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
