package com.comitfy.rabbitmq.service;

import com.comitfy.rabbitmq.ActionType;
import com.comitfy.rabbitmq.configuration.APIConfiguration;
import com.comitfy.rabbitmq.dto.BaseResponseDTO;
import com.comitfy.rabbitmq.dto.ConverterDTO;
import com.comitfy.rabbitmq.dto.EKGMeasurementDTO;
import com.comitfy.rabbitmq.dto.ResponseTokenDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.xml.bind.DatatypeConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.read.common.Field;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.*;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

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
/*
'x-locale': 'en-gb',
          'Content-Type': 'application/json',
          'Accept': 'application/json',
          'x-api': 'v8',
          'x-encrypted': 0,
 */
            HttpHeaders headers = new HttpHeaders() {{
                set("x-locale", "en-gb");
                set("ContentType", "application/json");
                set("Accept", "application/json");
                set("x-api", "v8");
                set("x-encrypted", "0");
            }};

            HttpEntity<Void> requestEntity = new HttpEntity<>(headers);


            RestTemplate restTemplate = new RestTemplate();
            log.info("start authorization request");
            String url = apiConfiguration.getMap2healApiUrl() + "/remote-patient/external/get-token-by-remote-patient?id=" + patientId;
            /*ResponseEntity<ResponseTokenDTO> response
                    = restTemplate.getForEntity(url, ResponseTokenDTO.class);*/

            ResponseEntity<ResponseTokenDTO> response = restTemplate.exchange(
                    url, HttpMethod.GET, requestEntity, ResponseTokenDTO.class);
            //header

            log.info("token {}", response);

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
            set("x-locale", "en-gb");
            set("ContentType", "application/json");
            set("Accept", "application/json");
            set("x-api", "v8");
            set("x-encrypted", "0");
        }};
    }

    @Autowired
    private ResourceLoader resourceLoader;

    public File createFile(JSONObject jsonObject, String sessionIdHash) {

        ClassLoader classLoader = getClass().getClassLoader();
        try {

            String fileSeparator = System.getProperty("file.separator");

            //String absoluteFilePath = fileSeparator + "var" + fileSeparator + sessionIdHash + ".json";
            String absoluteFilePath = fileSeparator + "C:" + fileSeparator + sessionIdHash + ".json";

            log.info(absoluteFilePath);


            // Create the file objectrp_${own}_$sid.dat
            File file = new File(absoluteFilePath);


            // Check if the file already exists
            if (file.exists()) {
                System.out.println("File already exists.");
                file.delete();
            }

            file = new File(absoluteFilePath);
            // Create a new file
            if (file.createNewFile()) {

                Files.writeString(file.toPath(), jsonObject.toString());

                System.out.println("File created successfully.");
                return file;

            } else {
                System.out.println("Failed to create the file.");
            }

        } catch (IOException e) {
            System.out.println("An error occurred: " + e.getMessage());
        }

        return null;

    }


    public JSONObject createMeasurementJsonForFile(Session session, String ownSessionHash) {

        /*
        {"ReadStreamHistory":{"type":"ecg","owner":"128","sn":"wC020000004F7","sid":"34525","end":"2023-03-30T14:48:31Z","start":"2023-03-30T14:48:29Z"}}
         */

        try {

            JSONObject jsonObject = new JSONObject();
            JSONObject jsonObjectRoot = new JSONObject();

            jsonObjectRoot.put("ReadStreamHistory", JSONObject.NULL);
            long startTS = 0;
            long endTS = 0;
            long count = 0;

            SessionDataSet sessionMinDataSet = session.executeQueryStatement("select min_time(val)  from root.ecg.*.*.sid" + ownSessionHash + ";");
            SessionDataSet sessionMaxDataSet = session.executeQueryStatement("select max_time(val)  from root.ecg.*.*.sid" + ownSessionHash + ";");

            SessionDataSet sessionCountDataSet = session.executeQueryStatement("select count(val)  from root.ecg.*.*.sid" + ownSessionHash + ";");


            log.info("column name {}", sessionMinDataSet.getColumnNames().get(0));
            String sn = sessionMinDataSet.getColumnNames().get(0).split("\\.")[2];
            String own = sessionMinDataSet.getColumnNames().get(0).split("\\.")[3].split("own")[1];

            if (sessionMinDataSet.hasNext()) {
                Field min = sessionMinDataSet.next().getFields().get(0);
                startTS = min.getLongV();
            }

            if (sessionMaxDataSet.hasNext()) {
                Field max = sessionMaxDataSet.next().getFields().get(0);
                endTS = max.getLongV();
            }


            if (sessionCountDataSet.hasNext()) {
                Field max = sessionCountDataSet.next().getFields().get(0);
                count = max.getLongV();
            }

            jsonObject.put("type", "ecg");
            jsonObject.put("owner", own);
            jsonObject.put("sn", sn);
            jsonObject.put("sid", ownSessionHash);
            jsonObject.put("start", String.valueOf(startTS));
            jsonObject.put("end", String.valueOf(endTS));
            jsonObject.put("count", count);

            jsonObjectRoot.put("ReadStreamHistory", jsonObject);
            return jsonObjectRoot;

        } catch (Exception e) {
            log.error(e.getMessage());
            return null;
        }

        /*
        {"ReadStreamHistory":{"type":"ecg","owner":"128","sn":"wC020000004F7","sid":"34525","end":"2023-03-30T14:48:31Z","start":"2023-03-30T14:48:29Z"}
         */


    }//Creating a JSONObject object


    public void convertApiConsume(Session session, String ownSession) throws NoSuchAlgorithmException, JsonProcessingException {

        String originalSession = ownSession;
        originalSession = getHash(ownSession);

        ObjectMapper objectMapper = new ObjectMapper();
        JSONObject jsonForFile = createMeasurementJsonForFile(session, originalSession);

        File file = createFile(jsonForFile, originalSession);


        JSONObject jsonObjectReadStreamHistory = (JSONObject) jsonForFile.get("ReadStreamHistory");

        String convertUrl
                = apiConfiguration.getMap2healApiUrl() + "/remote-patient/external/create-measurement";
        RestTemplate restTemplate = new RestTemplate();
    /*
        String convertUrl
                = apiConfiguration.getDartFrogUrl() + "/ecg/create-measurement";*/

        ResponseEntity<ResponseTokenDTO> response = getJWTToken((String) jsonObjectReadStreamHistory.get("owner"));//own_id
        String token = Objects.requireNonNull(response.getBody()).getData().getApiToken();


        //String token = "Objects.requireNonNull(response.getBody()).getData().getApiToken();";


        HttpHeaders headers = createHeaders(token);

        headers.setContentType(MediaType.MULTIPART_FORM_DATA);


        JSONObject jsonBody = new JSONObject();
        jsonBody.put("dataFile", file);
        jsonBody.put("RemotePatientMeasurement", JSONObject.NULL);

        //JSONObject jsonObjectRemoteData = new JSONObject();

        //Map<String, Object> jsonObjectRemoteData = new HashMap<>();


        MultiValueMap<String, Object> jsonObjectRemoteData = new LinkedMultiValueMap<>();


        jsonObjectRemoteData.add("RemotePatientMeasurement[remote_patient_id]", jsonObjectReadStreamHistory.get("owner"));
        jsonObjectRemoteData.add("RemotePatientMeasurement[param]", "json");
        jsonObjectRemoteData.add("RemotePatientMeasurement[ext]", "dat");
        jsonObjectRemoteData.add("RemotePatientMeasurement[remote_patient_loinc_num]", "71575-5");
        jsonObjectRemoteData.add("RemotePatientMeasurement[uuid]", originalSession);
        jsonObjectRemoteData.add("RemotePatientMeasurement[data_float]", jsonObjectReadStreamHistory.get("count"));
        jsonObjectRemoteData.add("RemotePatientMeasurement[force_update]", 1);
        if (originalSession.contains("sync")) {
            jsonObjectRemoteData.add("RemotePatientMeasurement[data]", "ECG-Sync");
        } else {
            jsonObjectRemoteData.add("RemotePatientMeasurement[data]", "ECG-Stream");
        }


        int i = 0;
        if (originalSession.contains("sync")) {

            jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][type]", "number");
            jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][name]", "ecgSync");
            jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][hidden]", 1);
            jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][value]", 1);
            i++;


        }


        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][type]", "number");
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][name]", "xScaleFactor");
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][hidden]", 1);
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][loinc]", "71575-5");
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][value]",
                jsonObjectReadStreamHistory.get("sn").toString().toUpperCase().startsWith("HC02") ? "0.25" : "1");
        i++;


        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][type]", "number");
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][name]", "yScaleFactor");
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][hidden]", 1);
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][loinc]", "71575-5");
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][value]", "1.0");
        i++;


        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][type]", "number");
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][name]", "centerPoint");
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][hidden]", 1);
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][loinc]", "71575-5");
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][value]", "0");


        i++;


        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][type]", "string");
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][name]", "deviceIdentity");
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][hidden]", 0);
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][value]", jsonObjectReadStreamHistory.get("sn").toString().toUpperCase());






        i++;


        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][type]", "datetime");
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][name]", "startDate");
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][hidden]", 0);
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][value]", jsonObjectReadStreamHistory.get("start"));


        i++;


        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][type]", "datetime");
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][name]", "endDate");
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][hidden]", 0);
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][value]", jsonObjectReadStreamHistory.get("end"));



        i++;


        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][type]", "number");
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][name]", "count");
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][hidden]", 0);
        jsonObjectRemoteData.add("RemotePatientMeasurement[addAttributes][" + i + "][value]", jsonObjectReadStreamHistory.get("count"));

        // jsonObjectRemoteData.put("addAttributes",attList);


        //jsonBody.put("RemotePatientMeasurement", jsonObjectRemoteData);

        String json = objectMapper.writeValueAsString(jsonObjectRemoteData);
        //MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
        jsonObjectRemoteData.add("dataFile", new FileSystemResource(file));
        //jsonObjectRemoteData.add("RemotePatientMeasurement", jsonObjectRemoteData);




        /*MultiValueMap<String, String> jsonPart = new LinkedMultiValueMap<>();
        jsonPart.add("RemotePatientMeasurement", jsonObjectRemoteData.toString());

        // Create the file part of the multipart request
        MultiValueMap<String, Object> filePart = new LinkedMultiValueMap<>();
        filePart.add("dataFile", new FileSystemResource(file));*/


        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(jsonObjectRemoteData, headers);


        //HttpEntity<String> requestEntity = new HttpEntity<>(jsonBody.toString(), headers);

        //HttpEntity<List<EKGMeasurementDTO>> entity = new HttpEntity<>(ekgMeasurementDTOList, headers);

        /*if (file != null) {
            boolean isDelete = file.delete();
            log.info("file delete result {}", isDelete);
        }*/


        ResponseEntity<ConverterDTO> measurementDTO =
                restTemplate
                        .exchange(convertUrl,
                                HttpMethod.POST,
                                requestEntity,
                                ConverterDTO.class);

        log.info("create meaurement response {} ", measurementDTO);

    }


    public String getHash(String sessionId) throws NoSuchAlgorithmException {


        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(sessionId.getBytes());
        byte[] digest = md.digest();
        String myHash = DatatypeConverter
                .printHexBinary(digest).toUpperCase();


        return myHash;

    }

    @Retryable(
            maxAttempts = 3,
            backoff = @Backoff(delay = 1000, multiplier = 1.5)
    )
    @Async
    public CompletableFuture<BaseResponseDTO> collectorApiConsume(List<EKGMeasurementDTO> ekgMeasurementDTOList, String sessionId, ActionType actionType) throws NoSuchAlgorithmException {


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

        if (actionType.equals(ActionType.publish)) {
            log.info("publish api worked");
        }

        var response = restTemplate
                .exchange(convertUrl,
                        HttpMethod.POST,
                        entity,
                        BaseResponseDTO.class);
        return CompletableFuture.completedFuture(response.getBody());


    }


}
