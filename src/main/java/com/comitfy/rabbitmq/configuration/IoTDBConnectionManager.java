package com.comitfy.rabbitmq.configuration;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;

public class IoTDBConnectionManager {

    private static IoTDBConnectionManager instance;
    private Session session;

    IoTDBConnectionManager(String host, String port, String username, String password) {
        session = new Session(host, Integer.parseInt(port), username, password);
    }

    public synchronized Session getSession() {
        return session;
    }

    public void closeSession() throws IoTDBConnectionException {
        session.close();
    }
}
