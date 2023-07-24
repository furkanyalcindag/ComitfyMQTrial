package com.comitfy.rabbitmq.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RabbitMQProducer {

    private final RabbitTemplate rabbitTemplate;
    private final String queueName = "redisCollector";

    private final String datRequestQueueName = "datCollector";

    @Autowired
    public RabbitMQProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    @Async
    public void sendMessage(String message) {
        try {
            rabbitTemplate.convertAndSend(datRequestQueueName, message);
            //log.info("Message sent: " + message);
        } catch (Exception e) {

            log.error("Message not sent: " + e.getMessage());

        }

    }


    @Async
    public void sendMessageToDatRequest(String message) {
        try {
            rabbitTemplate.convertAndSend(queueName, message);
            //log.info("Message sent: " + message);
        } catch (Exception e) {

            log.error("Message not sent: " + e.getMessage());

        }

    }
}