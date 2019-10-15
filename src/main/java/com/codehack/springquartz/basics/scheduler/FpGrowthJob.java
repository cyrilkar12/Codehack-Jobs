package com.codehack.springquartz.basics.scheduler;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.codehack.springquartz.basics.service.CollabrativeFilterService;
import com.codehack.springquartz.basics.service.FpGrowthJobService;

@Component
public class FpGrowthJob implements Job {

    Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private FpGrowthJobService jobService;
    @Autowired
    private CollabrativeFilterService cfService;

    public void execute(JobExecutionContext context) throws JobExecutionException {

        logger.info("Job ** {} ** fired @ {}", context.getJobDetail().getKey().getName(), context.getFireTime());

        jobService.executeFpGrowthJob();
        cfService.executeCFService();

        logger.info("Next job scheduled @ {}", context.getNextFireTime());
    }
}
