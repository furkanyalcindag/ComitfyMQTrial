package com.comitfy.rabbitmq;

import com.comitfy.rabbitmq.service.IOTDBService;
import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashSet;


@Component
@Slf4j
//@EnableScheduling
public class ScheduleJob {

    @Autowired
    IOTDBService iotdbService;

    @Scheduled(fixedDelay = 1000)
    public void cleanCache() throws IoTDBConnectionException, StatementExecutionException {

        iotdbService.deneme();

    }

}
