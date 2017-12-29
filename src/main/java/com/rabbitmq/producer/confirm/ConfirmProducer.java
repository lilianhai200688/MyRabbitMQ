package com.rabbitmq.producer.confirm;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.common.Constant;
import com.rabbitmq.common.RabbitMQCommon;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ConfirmProducer {

    private Logger LOGGER = Logger.getLogger(ConfirmProducer.class.getName());
    private final String EXCHANGE_NAME = "test-confirm-publish";
    private final String QUEUE_NAME = "";

    public static void main(String[] args){

        //test
        String msg = "test confirm publish";
        ConfirmProducer confirmProducer = new ConfirmProducer();
        confirmProducer.confirmPublish(msg);

    }


    public void confirmPublish(String msg){

        LOGGER.info("test confirm publish");
        Connection conn = null;
        Channel channel = null;
        try {
            conn = RabbitMQCommon.getMqconn();
            channel = conn.createChannel();

            channel.exchangeDeclare(EXCHANGE_NAME, Constant.EXCHANGE_TYPE_DIRECT, false);
            //if the queue is not exist , and throw the exception as follows:
            //reply-code=404, reply-text=NOT_FOUND - no queue 'test-confirm-publish' in vhost 'iotdev', class-id=50, method-id=20
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "");

            channel.confirmSelect();
            channel.addConfirmListener(new ConfirmListener() {

                public void handleAck(long deliveryTag, boolean multiple) throws IOException {

                    //publish success
                    LOGGER.info("send success");
                    LOGGER.info("the deliveryTag is " + deliveryTag + " and the multiple is " + multiple);
                    if(multiple){

                    }else {

                    }
                }

                public void handleNack(long deliveryTag, boolean multiple) throws IOException {

                    //failed
                    LOGGER.info("send failed");
                    LOGGER.info("the deliveryTag is " + deliveryTag + " and the multiple is " + multiple);
                }
            });

            for(int i = 0; i < 100; i++){
                channel.basicPublish(EXCHANGE_NAME,"", MessageProperties.PERSISTENT_TEXT_PLAIN,msg.getBytes());
            }

            //channel.close();
            //conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
}
