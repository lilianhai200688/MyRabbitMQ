package java.com.rabbitmq.consumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeoutException;

public class RabbitMQConsumer {

    public static void main(String[] args) throws IOException, TimeoutException {

        String queuename = "MSG_OUTBOUND_APP7643";
        RabbitMQConsumer consumer = new RabbitMQConsumer();
        try {
            consumer.consumerFromMQ(queuename);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

    }

    public void consumerFromMQ(String queue) throws IOException, TimeoutException {

        ConnectionFactory mqcf = new ConnectionFactory();
        mqcf.setHost("192.168.0.239");
        mqcf.setUsername("iotadmin");
        mqcf.setPassword("passw0rd");
        mqcf.setVirtualHost("iotdev");
        mqcf.setPort(5672);
        Connection mqconn = mqcf.newConnection();
        final Channel mqchannel = mqconn.createChannel();
        //test
        //mqchannel.exchangeDeclare("test", "direct", true);
        mqchannel.queueDeclare(queue, true, false, false, null);
        //mqchannel.queueBind(queue, "", "");

        //consumer confirm
        //mqchannel.confirmSelect();
        boolean autoAck = true;
        Date date=new Date();
        DateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String time=format.format(date);

        mqchannel.basicConsume(queue, autoAck, new DefaultConsumer(mqchannel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");

                mqchannel.basicAck(envelope.getDeliveryTag(),true);
                System.out.println(" [x] Received '" + message + "'" + "------");

                //producer consumer
                //producer.produceToMQ(message,"MSG_OUTBOUND_APP4998",1);

                /*long deliverytag = envelope.getDeliveryTag();
                System.out.println(deliverytag);
                mqchannel.basicAck(deliverytag, false);*/

                /*try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/
            }
        });


    }
}
