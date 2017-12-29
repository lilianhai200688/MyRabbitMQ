package com.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class MultipleThreadProducer extends  Thread{


    public static void main(String[] args) throws IOException, TimeoutException {


        Thread t1 = new MultipleThreadProducer();
//        Thread t2 = new MultipleThreadProducer();
//        Thread t3 = new MultipleThreadProducer();
//        Thread t4 = new MultipleThreadProducer();
//        Thread t5 = new MultipleThreadProducer();

        t1.start();
//        t2.start();
//        t3.start();
//        t4.start();
//        t5.start();
    }

    @Override
    public void run() {

        ConnectionFactory mqcf = new ConnectionFactory();
        mqcf.setHost("localhost");
        mqcf.setUsername("iotadmin");
        mqcf.setPassword("passw0rd");
        mqcf.setVirtualHost("iotdev");
        mqcf.setPort(5672);

        Connection mqconn = null;
        Channel mqchannel = null;

        //producer confirm
        //mqchannel.confirmSelect();
        String queuename = "MSG_INBOUND_QUEE";
        //String msg= "{\"appID\":\"APP7643\",\"appType\":1,\"token\":\"79872f34f091d17c\",\"topic\":\"MSG/EVENT/REG2\",\"payload\":\"what blabalbalbalbaablalabbaal\",\"time\":\"";
        String msg= "{\"appID\":\"APP7879\",\"appType\":1,\"token\":\"6dd1f23738189d79\",\"topic\":\"MSG/EVENT/CAR\",\"payload\":\"what blabalbalbalbaablalabbaal";

        try {
            mqconn = mqcf.newConnection();
            mqchannel = mqconn.createChannel();
            mqchannel.queueDeclare(queuename, true, false,false,null);

            BasicProperties basicProperties = MessageProperties.PERSISTENT_TEXT_PLAIN;
            AMQP.BasicProperties properties = new AMQP.BasicProperties("application/json",
                    null,
                    null,
                    2,
                    0, null, null, null,
                    null, null, null, null,
                    null, null);


            long num=0;
            for(int j =0;j<1;j++){

                String tt = msg+num+"\"}";
                mqchannel.basicPublish("", queuename, properties, tt.getBytes());
                num++;
               /* try {
                    Thread.sleep(20000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }finally {

            try {
                mqchannel.close();
                mqconn.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }
}
