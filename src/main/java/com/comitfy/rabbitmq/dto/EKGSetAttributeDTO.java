package com.comitfy.rabbitmq.dto;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class EKGSetAttributeDTO {
    private Double minTs = Double.POSITIVE_INFINITY;
    private Double maxTs = Double.NEGATIVE_INFINITY;
    private Integer count = 0;
    private Double minVal = Double.POSITIVE_INFINITY;
    private Double maxVal = Double.NEGATIVE_INFINITY;
    private Double avg = 0.0;
    private Double std = 0.0;



}
