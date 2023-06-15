package com.comitfy.rabbitmq.configuration;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
@Slf4j

public class IOTDBConfig {

    @Value("${iotdb.connection.host}")
    private String host;

    @Value("${iotdb.connection.port}")
    private String port;

    @Value("${iotdb.connection.user}")
    private String user;

    @Value("${iotdb.connection.pwd}")
    private String password;

    @Value("${iotdb.connection.storage.group}")
    private String storageGroup;


    private static IoTDBConnectionManager instance;

    @Bean
    public IoTDBConnectionManager ioTDBConnectionManager() {
        if (instance == null) {
            instance = new IoTDBConnectionManager(host, port, user, password);
        }
        return instance;
    }

    public String getStorageGroup() {
        return storageGroup;
    }
}
