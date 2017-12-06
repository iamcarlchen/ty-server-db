package com.greatbee.core;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Author: CarlChen
 * Date: 2017/9/5
 */
public class TestJob implements Job {
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        String dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(System.currentTimeMillis()));
        System.out.println(dateStr);
    }
}
