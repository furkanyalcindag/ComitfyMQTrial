package com.comitfy.rabbitmq;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableAsync;


@SpringBootApplication
@EnableRetry
@EnableAsync
public class RabbitmqApplication {


    public static void main(String[] args) throws IoTDBConnectionException {

        SpringApplication.run(RabbitmqApplication.class, args);
    }

}
