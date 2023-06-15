package com.comitfy.rabbitmq.dto;

import lombok.Data;

@Data
public class EKGMeasurementDTO {

    private String val;
    private String own;
    private String sn;
    private String sid;
    private Long ts;
    private Boolean isLead;
    private Boolean save;
    private Integer data_type;


}
