package com.comitfy.rabbitmq.service;

import com.comitfy.rabbitmq.ActionType;
import com.comitfy.rabbitmq.configuration.APIConfiguration;
import com.comitfy.rabbitmq.dto.BaseResponseDTO;
import com.comitfy.rabbitmq.dto.ConverterDTO;
import com.comitfy.rabbitmq.dto.EKGMeasurementDTO;
import com.comitfy.rabbitmq.dto.ResponseTokenDTO;
import jakarta.xml.bind.DatatypeConverter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

@Service
@Slf4j
public class RestApiClientService {

    @Autowired
    APIConfiguration apiConfiguration;


    @Retryable(
            maxAttempts = 3,
            backoff = @Backoff(delay = 1000, multiplier = 1.5)
    )
    public ResponseEntity<ResponseTokenDTO> getJWTToken(String patientId) {

        try {
            RestTemplate restTemplate = new RestTemplate();
            log.info("start authorization request");
            String url = apiConfiguration.getMap2healApiUrl() + "/remote-patient/external/get-token-by-remote-patient?id=" + patientId;
            ResponseEntity<ResponseTokenDTO> response
                    = restTemplate.getForEntity(url, ResponseTokenDTO.class);

            return response;
        } catch (Exception e) {
            log.error(e.getMessage());
            return null;
        }


    }

    HttpHeaders createHeaders(String token) {
        return new HttpHeaders() {{
            String authHeader = "Bearer " + new String(token);
            set("Authorization", authHeader);
        }};
    }

    @Retryable(
            maxAttempts = 3,
            backoff = @Backoff(delay = 1000, multiplier = 1.5)
    )
    public ResponseEntity<ConverterDTO> convertApiConsume(List<EKGMeasurementDTO> ekgMeasurementDTOList) {

        RestTemplate restTemplate = new RestTemplate();
        String convertUrl
                = apiConfiguration.getDartFrogUrl() + "/ecg/converter?from=json&action=toBin";


        HttpEntity<List<EKGMeasurementDTO>> entity = new HttpEntity<>(ekgMeasurementDTOList);


        ResponseEntity<ConverterDTO> responseConverterApi =
                restTemplate
                        .exchange(convertUrl,
                                HttpMethod.POST,
                                entity,
                                ConverterDTO.class);


        return responseConverterApi;

/*
        HttpHeaders headers = createHeaders(token);

        HttpEntity<List<EKGMeasurementDTO>> entity = new HttpEntity<>(ekgMeasurementDTOList, headers);


        ResponseEntity<ConverterDTO> productCreateResponse =
                restTemplate
                        .exchange(convertUrl,
                                HttpMethod.POST,
                                entity,
                                ConverterDTO.class);


        return productCreateResponse;
*/

    }


    /*@Retryable(
            maxAttempts = 3,
            backoff = @Backoff(delay = 1000, multiplier = 1.5)
    )*/
    @Async
    public ResponseEntity<BaseResponseDTO> collectorApiConsume(List<EKGMeasurementDTO> ekgMeasurementDTOList, String sessionId, ActionType actionType) throws NoSuchAlgorithmException {


        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(sessionId.getBytes());
        byte[] digest = md.digest();
        String myHash = DatatypeConverter
                .printHexBinary(digest).toUpperCase();

        String myHasy = sessionId;

        RestTemplate restTemplate = new RestTemplate();
        String convertUrl
                = apiConfiguration.getDartFrogUrl() + "/ecg/collector?action=" + actionType.name() + "&sid=" + myHash;


        HttpEntity<List<EKGMeasurementDTO>> entity = new HttpEntity<>(ekgMeasurementDTOList);

        if(actionType.equals(ActionType.publish)){
            log.info("publish api worked");
        }

        return restTemplate
                .exchange(convertUrl,
                        HttpMethod.POST,
                        entity,
                        BaseResponseDTO.class);


    }


}
