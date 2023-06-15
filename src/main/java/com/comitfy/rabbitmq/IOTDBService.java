package com.comitfy.rabbitmq;

import com.comitfy.rabbitmq.configuration.IOTDBConfig;
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
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
@Slf4j
public class IOTDBService {


    public static Set<String> cache = new HashSet<>();

    @Autowired
    IOTDBConfig iotdbConfig;




    public boolean checkTimeSeriesExits(Session session,String path) throws IoTDBConnectionException, StatementExecutionException {

        if(cache.stream().anyMatch(o->o.equals(path))){
            return Boolean.TRUE;
        }
        else{
            if (!session.checkTimeseriesExists(path)) {
                TSEncoding encoding = TSEncoding.PLAIN;
                CompressionType compressionType = CompressionType.SNAPPY;
                session.createTimeseries(path, TSDataType.INT32, encoding, compressionType);
                cache.add(path);
                return Boolean.FALSE;
            }
            else{
                cache.add(path);
                return Boolean.TRUE;
            }
        }
    }

    public void insert(String message) throws JsonProcessingException, IoTDBConnectionException {

        ObjectMapper mapper = new ObjectMapper();


        List<EKGMeasurementDTO> ekgMeasurementDTOList = new ArrayList<>();
        TypeReference<List<EKGMeasurementDTO>> typeReference = new TypeReference<List<EKGMeasurementDTO>>() {
        };

        ekgMeasurementDTOList = mapper.readValue(message, typeReference);

        Session session = iotdbConfig.ioTDBConnectionManager().getSession();


        try {
            session.open();

            try {
                session.setStorageGroup(iotdbConfig.getStorageGroup());
            } catch (Exception e) {
                log.info(e.getMessage());
            }

            for (EKGMeasurementDTO ekg : ekgMeasurementDTOList) {

                if (ekg.getSave()) {
                    ekg.setSn(ekg.getSn().replace("-", ""));
                    String deviceId = "root.binEcg." + ekg.getSn(); // Veri noktasının cihaz kimliği
                    String timeSeriesPath = deviceId + ".own" + ekg.getOwn();
                   /* if (!session.checkTimeseriesExists(timeSeriesPath)) {
                        TSEncoding encoding = TSEncoding.PLAIN;
                        CompressionType compressionType = CompressionType.SNAPPY;
                        session.createTimeseries(timeSeriesPath, TSDataType.INT32, encoding, compressionType);
                    }*/

                    checkTimeSeriesExits(session,timeSeriesPath);

                    session.insertRecord(deviceId, ekg.getTs(), List.of("own" + ekg.getOwn()), List.of(TSDataType.INT32), List.of(Long.valueOf(ekg.getVal())));
                    log.info("data was saved");

                } else {
                    log.info("isSaved : false for ekg sessionId {}", ekg.getSid());
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
            session.close();

        } finally {
            session.close();
        }


    }


}
