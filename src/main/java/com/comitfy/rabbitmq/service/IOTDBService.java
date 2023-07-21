package com.comitfy.rabbitmq.service;

import com.comitfy.rabbitmq.ActionType;
import com.comitfy.rabbitmq.configuration.IOTDBConfig;
import com.comitfy.rabbitmq.consumer.RabbitMQProducer;
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

import java.security.NoSuchAlgorithmException;
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

    @Autowired
    RabbitMQProducer rabbitMQProducer;

    @Autowired
    RedisService redisService;


    public synchronized boolean checkTimeSeriesExits(Session session, String path,String ecgSession) throws IoTDBConnectionException, StatementExecutionException {



        if (timeSeriesCache.containsKey(path)) {

            Timer timer = timeSeriesCache.get(path);
            timer.cancel();
            timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    actionAfterTimeout(path,ecgSession);
                }
            }, 30000);
            timeSeriesCache.put(path, timer);
            return Boolean.TRUE;

        } else {
            boolean isExist = Boolean.FALSE;


            try {
                isExist = session.checkTimeseriesExists(path + ".val");
            } catch (Exception e) {
                log.error(e.getMessage());
            }
            if (!isExist) {
                //if (false) {
                TSEncoding encoding = TSEncoding.PLAIN;
                CompressionType compressionType = CompressionType.SNAPPY;
                List<TSDataType> tsDataTypeList = new ArrayList<>();
                tsDataTypeList.add(TSDataType.DOUBLE);
                tsDataTypeList.add(TSDataType.BOOLEAN);

                List<TSEncoding> tsEncodings = new ArrayList<>();
                tsEncodings.add(TSEncoding.PLAIN);
                tsEncodings.add(TSEncoding.PLAIN);

                List<CompressionType> compressionTypes = new ArrayList<>();
                compressionTypes.add(CompressionType.SNAPPY);
                compressionTypes.add(CompressionType.SNAPPY);


                List<String> measurements = new ArrayList<>();
                measurements.add("val");
                measurements.add("lead");
                try {
                    //session.createTimeseries(path, TSDataType.DOUBLE, encoding, compressionType);
                    //path, tsDataTypeList, tsEncodings, compressionTypes, measurements
                    session.createAlignedTimeseries(path, measurements, tsDataTypeList, tsEncodings, compressionTypes, new ArrayList<>(List.of("val", "lead")));
                    //session.createAlignedTimeseries(" root.ecg.mi_hythm_wC020000000CC.own4678.sid_hjdgsjhdgjs",measurements,tsDataTypeList,tsEncodings,compressionTypes,new ArrayList<>(List.of("val","lead")))
                } catch (Exception e) {
                    log.error(e.getMessage());
                }


                Timer timer = new Timer();
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        actionAfterTimeout(path, ecgSession);
                    }
                }, 30000);
                timeSeriesCache.put(path, timer);
                return Boolean.FALSE;
            } else {
                Timer timer = new Timer();
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        actionAfterTimeout(path,ecgSession);
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


    void actionAfterTimeout(String key,String ecgSession) {
        try {

            Long inCount =   redisService.getCount("in_"+ecgSession);
            Long outCount =   redisService.getCount("out_"+ecgSession);

            log.info("for "+ ecgSession +"redis inCount: "+inCount+" outcount: "+ outCount);

            if(inCount.equals(outCount)){
                Session session = iotdbConfig.ioTDBConnectionManager().getSession();
                restApiClientService.convertApiConsume(session,ecgSession);
                timeSeriesCache.get(key).cancel();
                timeSeriesCache.remove(key);
                redisService.delete(ecgSession);
            }
            else {
                timeSeriesCache.get(key).cancel();
                Timer timer = new Timer();
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        actionAfterTimeout(key, ecgSession);
                    }
                }, 30000);
                timeSeriesCache.put(key, timer);
            }


        } catch (Exception e) {

            log.error(e.getMessage());
            timeSeriesCache.get(key).cancel();
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    actionAfterTimeout(key,ecgSession);
                }
            }, 30000);

            timeSeriesCache.put(key, timer);

        }


    }


    public void insert(String message) throws JsonProcessingException, IoTDBConnectionException, StatementExecutionException, NoSuchAlgorithmException {

        ObjectMapper mapper = new ObjectMapper();


        List<EKGMeasurementDTO> ekgMeasurementDTOList = new ArrayList<>();
        TypeReference<List<EKGMeasurementDTO>> typeReference = new TypeReference<List<EKGMeasurementDTO>>() {
        };

        ekgMeasurementDTOList = mapper.readValue(message, typeReference);
        ekgMeasurementDTOList = ekgMeasurementDTOList.stream().filter(e -> e.getSave() == Boolean.TRUE).collect(Collectors.toList());

        if (ekgMeasurementDTOList.isEmpty()) {

            return;

        }


        String sessionId = null;

        String originalSessionId = ekgMeasurementDTOList.get(0).getSid();


        /*if (sessionId.contains("sync")) {
            sessionId = sessionId.split("_")[1];
        } else {
            sessionId = sessionId.split("_")[0];
        }*/


        sessionId = restApiClientService.getHash(originalSessionId);


        String sn = ekgMeasurementDTOList.get(0).getSn().replace("-", "_");

        String deviceId = iotdbConfig.getStorageGroup() + "." + sn;
        String own = ekgMeasurementDTOList.get(0).getOwn();

        String timeSeriesPath = deviceId + ".own" + own + ".sid" + sessionId;


       /* try {
            restApiClientService.collectorApiConsume(ekgMeasurementDTOList, sessionId, ActionType.collect);
        } catch (Exception e) {
            log.info(e.getMessage());
        }*/


        Session session = iotdbConfig.ioTDBConnectionManager().getSession();
        List<String> deviceIdList = new ArrayList<>();
        List<Long> timeSerieList = new ArrayList<>();
        List<List<String>> measurementList = new ArrayList<>();
        List<List<TSDataType>> tsDataTypeList = new ArrayList<>();
        List<List<Object>> valueList = new ArrayList<>();

        try {


           /* try {
                session.setStorageGroup(iotdbConfig.getStorageGroup());
            } catch (Exception e) {
                log.info(e.getMessage());
            }*/



            /*
            LİVE GELİRSE ==>> 9832983920382_3204
SYNC GELİRSE ==>> sync_9832983920382_3204
             */


            if (sessionId == null) throw new Exception("data is corrupt");


            for (EKGMeasurementDTO ekg : ekgMeasurementDTOList) {

                ekg.setSn(ekg.getSn().replace("-", "_"));

                if(ekg.getIsLead()!=null){
                    ekg.setIsLead(false);
                }
                // String deviceId = iotdbConfig.getStorageGroup() + "." + ekg.getSn(); // Veri noktasının cihaz kimliği
                //String timeSeriesPath = deviceId + ".own" + ekg.getOwn() + ".sid." + ekg.getSid().split("_")[0];
                //String timeSeriesPath = deviceId + ".own" + ekg.getOwn() + "_sid" + ekg.getSid().split("_")[0];
                //session.createTimeseries("root.binEcg.SDSDSDSD46.own3002_sid343434343", TSDataType.INT32, encoding, compressionType)

                   /* if (!session.checkTimeseriesExists(timeSeriesPath)) {
                        TSEncoding encoding = TSEncoding.PLAIN;
                        CompressionType compressionType = CompressionType.SNAPPY;
                        session.createTimeseries(timeSeriesPath, TSDataType.INT32, encoding, compressionType);
                    }*/


                // String timeSeriesPath = deviceId + ".own" + ekg.getOwn() + ".sid" + sessionId;

                checkTimeSeriesExits(session, timeSeriesPath,ekgMeasurementDTOList.get(0).getSid());
                deviceIdList.add(timeSeriesPath);
                timeSerieList.add(ekg.getTs());
                //measurementList.add(List.of("own" + ekg.getOwn()));
                List<String> measurements = new ArrayList<>();
                measurements.add("val");
                measurements.add("lead");
                measurementList.add(measurements);

                List<TSDataType> tsDataTypeList1 = new ArrayList<>();
                tsDataTypeList1.add(TSDataType.DOUBLE);
                tsDataTypeList1.add(TSDataType.BOOLEAN);

                // tsDataTypeList.add(List.of(TSDataType.DOUBLE));
                tsDataTypeList.add(tsDataTypeList1);

                List<Object> values = new ArrayList<>();
                values.add(Double.valueOf(ekg.getVal()));
                values.add(ekg.getIsLead());

                //valueList.add(List.of(Double.valueOf(ekg.getVal())));
                valueList.add(values);

                //setEKGSetAttributeFromRQ(timeSeriesPath, ekg);

                //session.insertRecord(deviceId, ekg.getTs(), List.of("own" + ekg.getOwn()), List.of(TSDataType.DOUBLE), List.of(Double.valueOf(ekg.getVal())));
            }


            //session.insertRecords(deviceIdList, timeSerieList, measurementList, tsDataTypeList, valueList);
            session.insertAlignedRecords(deviceIdList, timeSerieList, measurementList, tsDataTypeList, valueList);

            //redisService.setValue();

            rabbitMQProducer.sendMessage(ekgMeasurementDTOList.get(0).getSid());
            log.info("data was saved");

            //rabbitMQProducer.sendMessage(message);
        } catch (IoTDBConnectionException ioTDBConnectionException) {
            log.error(ioTDBConnectionException.getMessage());
            ioTDBConnectionException.printStackTrace();
            checkTimeSeriesExits(session, timeSeriesPath,originalSessionId);

            deviceIdList.clear();
            timeSerieList.clear();
            measurementList.clear();
            tsDataTypeList.clear();
            valueList.clear();
        } catch (Exception e) {
            e.printStackTrace();
            checkTimeSeriesExits(session, timeSeriesPath,originalSessionId);

            deviceIdList.clear();
            timeSerieList.clear();
            measurementList.clear();
            tsDataTypeList.clear();
            valueList.clear();

        } finally {

            deviceIdList.clear();
            timeSerieList.clear();
            measurementList.clear();
            tsDataTypeList.clear();
            valueList.clear();
        }


    }


    public void deneme() throws IoTDBConnectionException, StatementExecutionException {


        Session session = new Session.Builder().host(iotdbConfig.getHost()).port(Integer.valueOf(iotdbConfig.getPort())).username(iotdbConfig.getUser()).password(iotdbConfig.getPassword()).build();


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
