package com.comitfy.rabbitmq.service;

import com.comitfy.rabbitmq.ActionType;
import com.comitfy.rabbitmq.configuration.IOTDBConfig;
import com.comitfy.rabbitmq.dto.BaseResponseDTO;
import com.comitfy.rabbitmq.dto.EKGMeasurementDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
@Slf4j
public class IOTDBService {


    public static Set<String> cache = new HashSet<>();

    public static Map<String, Timer> timeSeriesCache = new ConcurrentHashMap<>();

    public static Map<String, EKGMeasurementDTO> measurementCacheByTimeSeriesPathMap = new HashMap<>();

    @Autowired
    IOTDBConfig iotdbConfig;

    @Autowired
    RestApiClientService restApiClientService;


    public boolean checkTimeSeriesExits(Session session, String path) throws IoTDBConnectionException, StatementExecutionException {


        if (timeSeriesCache.containsKey(path)) {

            Timer timer = timeSeriesCache.get(path);
            timer.cancel();
            timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    actionAfterTimeout(path);
                }
            }, 30000);
            timeSeriesCache.put(path, timer);
            return Boolean.TRUE;

        } else {
            if (!session.checkTimeseriesExists(path)) {
                //if (false) {
                TSEncoding encoding = TSEncoding.PLAIN;
                CompressionType compressionType = CompressionType.SNAPPY;

                try {
                    session.createTimeseries(path, TSDataType.DOUBLE, encoding, compressionType);
                } catch (Exception e) {
                    log.error(e.getMessage());
                }


                Timer timer = new Timer();
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        actionAfterTimeout(path);
                    }
                }, 30000);
                timeSeriesCache.put(path, timer);
                return Boolean.FALSE;
            } else {
                Timer timer = new Timer();
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        actionAfterTimeout(path);
                    }
                }, 30000);
                timeSeriesCache.put(path, timer);
                return Boolean.TRUE;
            }


        }

/*
        if (cache.stream().anyMatch(o -> o.equals(path))) {
            return Boolean.TRUE;
        } else {
            if (!session.checkTimeseriesExists(path)) {
                TSEncoding encoding = TSEncoding.PLAIN;
                CompressionType compressionType = CompressionType.SNAPPY;
                session.createTimeseries(path, TSDataType.INT32, encoding, compressionType);
                cache.add(path);
                return Boolean.FALSE;
            } else {
                cache.add(path);
                return Boolean.TRUE;
            }
        }*/
    }


    void actionAfterTimeout(String key) {
        try {
            int i = 0;
            while (i < 3) {
                log.info("start to publish");
                CompletableFuture<BaseResponseDTO> baseResponseDTOResponseEntity = restApiClientService.collectorApiConsume(null, key.split("_sid")[1], ActionType.publish);
                log.info("end to publish");
                if (Boolean.TRUE.equals(baseResponseDTOResponseEntity.get().getSuccess())) {
                    break;
                } else {
                    if (i == 2) {
                        log.info("start to dispose");
                        restApiClientService.collectorApiConsume(null, key.split("_sid")[1], ActionType.dispose);
                        log.info("end to dispose");
                        break;
                    }
                }
                i++;
            }


            timeSeriesCache.get(key).cancel();

            timeSeriesCache.remove(key);
        } catch (Exception e) {

            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    actionAfterTimeout("path");
                }
            }, 30000);

            timeSeriesCache.put("key", timer);

        }


    }


    public void insert(String message) throws JsonProcessingException, IoTDBConnectionException, StatementExecutionException {

        ObjectMapper mapper = new ObjectMapper();


        List<EKGMeasurementDTO> ekgMeasurementDTOList = new ArrayList<>();
        TypeReference<List<EKGMeasurementDTO>> typeReference = new TypeReference<List<EKGMeasurementDTO>>() {
        };

        ekgMeasurementDTOList = mapper.readValue(message, typeReference);
        ekgMeasurementDTOList = ekgMeasurementDTOList.stream().filter(e -> e.getSave() == Boolean.TRUE).collect(Collectors.toList());

        if (ekgMeasurementDTOList.isEmpty()) {

            return;

        }

        Session session = iotdbConfig.ioTDBConnectionManager().getSession();
        List<String> deviceIdList = new ArrayList<>();
        List<Long> timeSerieList = new ArrayList<>();
        List<List<String>> measurementList = new ArrayList<>();
        List<List<TSDataType>> tsDataTypeList = new ArrayList<>();
        List<List<Object>> valueList = new ArrayList<>();

        try {
            session.open();

            try {
                session.setStorageGroup(iotdbConfig.getStorageGroup());
            } catch (Exception e) {
                log.info(e.getMessage());
            }


            String sessionId = null;

            sessionId = ekgMeasurementDTOList.get(0).getSid().split("_")[0];
            if (sessionId == null)
                throw new Exception("data is corrupt");

            restApiClientService.collectorApiConsume(ekgMeasurementDTOList, sessionId, ActionType.collect);


            for (EKGMeasurementDTO ekg : ekgMeasurementDTOList) {

                ekg.setSn(ekg.getSn().replace("-", ""));
                String deviceId = iotdbConfig.getStorageGroup() + "." + ekg.getSn(); // Veri noktasının cihaz kimliği
                //String timeSeriesPath = deviceId + ".own" + ekg.getOwn() + ".sid." + ekg.getSid().split("_")[0];
                String timeSeriesPath = deviceId + ".own" + ekg.getOwn() + "_sid" + ekg.getSid().split("_")[0];
                //session.createTimeseries("root.binEcg.SDSDSDSD46.own3002_sid343434343", TSDataType.INT32, encoding, compressionType)

                   /* if (!session.checkTimeseriesExists(timeSeriesPath)) {
                        TSEncoding encoding = TSEncoding.PLAIN;
                        CompressionType compressionType = CompressionType.SNAPPY;
                        session.createTimeseries(timeSeriesPath, TSDataType.INT32, encoding, compressionType);
                    }*/


                checkTimeSeriesExits(session, timeSeriesPath);
                deviceIdList.add(deviceId);
                timeSerieList.add(ekg.getTs());
                measurementList.add(List.of("own" + ekg.getOwn()));
                tsDataTypeList.add(List.of(TSDataType.DOUBLE));
                valueList.add(List.of(Double.valueOf(ekg.getVal())));

                //setEKGSetAttributeFromRQ(timeSeriesPath, ekg);


                //session.insertRecord(deviceId, ekg.getTs(), List.of("own" + ekg.getOwn()), List.of(TSDataType.DOUBLE), List.of(Double.valueOf(ekg.getVal())));
                log.info("data was saved");
            }

            session.insertRecords(deviceIdList, timeSerieList, measurementList, tsDataTypeList, valueList);

        } catch (
                Exception e) {
            e.printStackTrace();
            session.close();
            deviceIdList.clear();
            timeSerieList.clear();
            measurementList.clear();
            tsDataTypeList.clear();
            valueList.clear();


        } finally {
            session.close();
            deviceIdList.clear();
            timeSerieList.clear();
            measurementList.clear();
            tsDataTypeList.clear();
            valueList.clear();
        }


    }


    public void deneme() throws IoTDBConnectionException, StatementExecutionException {


        Session session =
                new Session.Builder()
                        .host(iotdbConfig.getHost())
                        .port(Integer.valueOf(iotdbConfig.getPort())).
                        username(iotdbConfig.getUser()).password(iotdbConfig.getPassword()).build();


        // Session session = iotdbConfig.ioTDBConnectionManager().getSession();
        session.open();
        try {
            session.setStorageGroup(iotdbConfig.getStorageGroup());
        } catch (Exception e) {

        }
        TSEncoding encoding = TSEncoding.PLAIN;
        CompressionType compressionType = CompressionType.SNAPPY;
        String timeSeriesPath = "SDSDSDSD46" + ".own_" + "3002" + "_sid" + "343434343_3002".split("_")[0];

        session.createTimeseries(timeSeriesPath, TSDataType.INT32, encoding, compressionType);
    }


}
