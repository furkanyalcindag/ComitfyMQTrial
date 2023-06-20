package com.comitfy.rabbitmq.dto;

import lombok.Data;

@Data
public class TokenDTO {

    private String id;
    private String session;
    private String apiToken;
    private Long valueFor;

}
