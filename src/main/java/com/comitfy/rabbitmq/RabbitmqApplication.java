package com.comitfy.rabbitmq;

import com.comitfy.rabbitmq.configuration.IOTDBConfig;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;


@SpringBootApplication
@EnableRetry
public class RabbitmqApplication {


    public static void main(String[] args) throws IoTDBConnectionException {

        SpringApplication.run(RabbitmqApplication.class, args);
    }

}
