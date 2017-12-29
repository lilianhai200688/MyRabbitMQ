package com.rabbitmq.common;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeoutException;

public class RabbitMQCommon {

    private static Connection mqconn = null;

    public static Connection getMqconn() throws IOException, TimeoutException {

        ConnectionFactory mqcf = new ConnectionFactory();
        mqcf.setHost("localhost");
        mqcf.setUsername("admin");
        mqcf.setPassword("admin");
        mqcf.setVirtualHost("iotdev");
        mqcf.setPort(5672);
        mqconn = mqcf.newConnection();
        return mqconn;
    }

    public static int getRandom(){

        Random random = new Random();
        int place = random.nextInt(3);
        return place;
    }
}
