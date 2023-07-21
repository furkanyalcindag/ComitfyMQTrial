package com.comitfy.rabbitmq.consumer;

import com.comitfy.rabbitmq.service.IOTDBService;
import com.comitfy.rabbitmq.service.RedisService;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class RabbitMQConsumer {

    @Autowired
    IOTDBService iotdbService;

    @Autowired
    RedisService redisService;
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQConsumer.class);

   /* @RabbitListener(queues = {"${rabbitmq.queue.name}"})
    public void consume(String message) throws IoTDBConnectionException, JsonProcessingException, StatementExecutionException {
        LOGGER.info(String.format("Received message -> %s", message));
        iotdbService.insert(message);
    }
    */


    @RabbitListener(queues = {"${rabbitmq.queue.lb1.name}"})

    public void consume(String message) throws IoTDBConnectionException, JsonProcessingException, StatementExecutionException {
        LOGGER.info(String.format("Received message -> %s", message));
        try{
            iotdbService.insert(message);
        }
        catch (Exception e){
            LOGGER.error(e.getMessage());
        }
    }

    @RabbitListener(queues = {"${rabbitmq.queue.lb2.name}"})
    public void consume2(String message) throws IoTDBConnectionException, JsonProcessingException, StatementExecutionException {
        LOGGER.info(String.format("Received message -> %s", message));
        try{
            iotdbService.insert(message);
        }
        catch (Exception e){
            LOGGER.error(e.getMessage());
        }
    }

    @RabbitListener(queues = {"${rabbitmq.queue.lb3.name}"})
    public void consume3(String message) throws IoTDBConnectionException, JsonProcessingException, StatementExecutionException {
        LOGGER.info(String.format("Received message -> %s", message));
        try{
            iotdbService.insert(message);
        }
        catch (Exception e){
            LOGGER.error(e.getMessage());
        }
    }

   @RabbitListener(queues = {"${rabbitmq.queue.lb4.name}"})
    public void consume4(String message) throws IoTDBConnectionException, JsonProcessingException, StatementExecutionException {
        LOGGER.info(String.format("Received message -> %s", message));
        try{
            iotdbService.insert(message);
        }
        catch (Exception e){
            LOGGER.error(e.getMessage());
        }
    }

   @RabbitListener(queues = {"${rabbitmq.queue.lb5.name}"})
    public void consume5(String message) throws IoTDBConnectionException, JsonProcessingException, StatementExecutionException {
        LOGGER.info(String.format("Received message -> %s", message));
        try{
            iotdbService.insert(message);
        }
        catch (Exception e){
            LOGGER.error(e.getMessage());
        }
    }

   @RabbitListener(queues = {"${rabbitmq.queue.lb6.name}"})
    public void consume6(String message) throws IoTDBConnectionException, JsonProcessingException, StatementExecutionException {
        //LOGGER.info(String.format("Received message -> %s", message));
        try{
            iotdbService.insert(message);
        }
        catch (Exception e){
            LOGGER.error(e.getMessage());
        }

    }



    @RabbitListener(queues = {"${rabbitmq.queue.redis.name}"})
    public void consume7(String message) throws IoTDBConnectionException, JsonProcessingException, StatementExecutionException {
        //LOGGER.info(String.format("Received message -> %s", message));
        try{
            redisService.setValue(message);
        }
        catch (Exception e){
            LOGGER.error(e.getMessage());
        }

    }
}
