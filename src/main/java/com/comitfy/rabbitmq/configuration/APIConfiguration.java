package com.comitfy.rabbitmq.configuration;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Data
public class APIConfiguration {


    @Value("${api.endpoint.map2heal}")
    private String map2healApiUrl;


    @Value("${api.endpoint.dart.frog}")
    private String dartFrogUrl;


}
