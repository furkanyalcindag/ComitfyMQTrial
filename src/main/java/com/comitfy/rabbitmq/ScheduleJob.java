package com.comitfy.rabbitmq;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashSet;

@EnableScheduling
@Component
@Slf4j
public class ScheduleJob {

    @Scheduled(cron = "@hourly")
    public void cleanCache(){

        IOTDBService.cache=new HashSet<>();
        log.info("cache cleaned");
    }

}
