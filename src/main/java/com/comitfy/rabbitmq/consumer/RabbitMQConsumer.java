package com.comitfy.rabbitmq.consumer;

import com.comitfy.rabbitmq.IOTDBService;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class RabbitMQConsumer {

    @Autowired
    IOTDBService iotdbService;
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQConsumer.class);

    @RabbitListener(queues = {"${rabbitmq.queue.name}"})
    public void consume(String message) throws IoTDBConnectionException, JsonProcessingException {
        LOGGER.info(String.format("Received message -> %s", message));
        iotdbService.insert(message);


    }
}
