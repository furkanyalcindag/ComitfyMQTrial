package com.comitfy.rabbitmq.configuration;

import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;



@Configuration
public class RestTemplateConfig {

    private final int timeout = 5000; // Timeout in milliseconds

    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder restTemplateBuilder)
    {
        return restTemplateBuilder
                .setConnectTimeout(Duration.ofHours(1))
           .setReadTimeout(Duration.ofHours(1))
           .build();
    }

}
