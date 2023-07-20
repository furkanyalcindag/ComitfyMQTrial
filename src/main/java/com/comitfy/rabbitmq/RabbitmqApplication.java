package com.comitfy.rabbitmq;

import com.comitfy.rabbitmq.configuration.IOTDBConfig;
import com.comitfy.rabbitmq.service.RestApiClientService;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.read.common.Field;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableAsync;


@SpringBootApplication
@EnableRetry
@EnableAsync
public class RabbitmqApplication implements ApplicationRunner {

    @Autowired
    IOTDBConfig iotdbConfig;

    @Autowired
    RestApiClientService restApiClientService;

    public static void main(String[] args) throws IoTDBConnectionException {

        SpringApplication.run(RabbitmqApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        /*Session session = iotdbConfig.ioTDBConnectionManager().getSession();

        long minTS = 0;
        long maxTS = 0;
        SessionDataSet sessionMinDataSet = session.executeQueryStatement("select min_time(val)  from root.ecg.*.*.sid1689496198171;");

        SessionDataSet sessionMaxDataSet = session.executeQueryStatement("select max_time(val)  from root.ecg.*.*.sid1689496198171;");

        if (sessionMinDataSet.hasNext()) {
            Field min = sessionMinDataSet.next().getFields().get(0);

            minTS = min.getLongV();
        }

        if (sessionMaxDataSet.hasNext()) {
            Field max = sessionMaxDataSet.next().getFields().get(0);
            maxTS = max.getLongV();
        }
        System.out.println("jkgjhjh");

*/
       //Session session = iotdbConfig.ioTDBConnectionManager().getSession();

        //restApiClientService.convertApiConsume(session,"sync_1689865144609_3002");
    }
}
