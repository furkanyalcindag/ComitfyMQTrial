package com.comitfy.rabbitmq.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class RedisService {

    @Autowired
    private RedisTemplate<String, Long> template;

    // Methods to add and get objects from redis goes here



    public Long getCount(String key){
        return Long.valueOf(template.opsForHash().get(key, key).toString());
    }

    public void setValue(String key,String value){
         template.opsForHash().putIfAbsent(key, key,value);
    }

    public void setValue(String sessionId) {

        sessionId ="out_"+sessionId;
        try {
            boolean x = template.opsForHash().putIfAbsent(sessionId, sessionId, 1L);

            if (!x) {
                Long count = Long.valueOf(template.opsForHash().get(sessionId, sessionId).toString());
                count = count!=null?count:0;
                template.opsForHash().put(sessionId, sessionId,
                        count + 1L);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }


    }
}
