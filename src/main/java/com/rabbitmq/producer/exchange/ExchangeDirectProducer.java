package com.rabbitmq.producer.exchange;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.common.RabbitMQCommon;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * direct type
 * binding key和routing key完全匹配，然后将消息路由到对应的队列上
 */
public class ExchangeDirectProducer {

    private static Logger LOGGER = Logger.getLogger(ExchangeDirectProducer.class.getName());
    private static final String EXCHANGE_TYPE = "direct";
    private static final String[] TYPE = {"info","warning","error"};
    private static final String EXCHANGE_NAME = "test_exchange_direct";



    public static void main(String[] args) {

        try {

            Connection conn = RabbitMQCommon.getMqconn();
            Channel channel = conn.createChannel();

            //声明转发器类型 durable
            channel.exchangeDeclare(EXCHANGE_NAME,EXCHANGE_TYPE,true);

            //声明消息队列,并将消息队列绑定到exchange
            for(int i = 0; i < TYPE.length; i++){

                channel.queueDeclare(TYPE[i], true, false, false, null);
                channel.queueBind(TYPE[i], EXCHANGE_NAME, TYPE[i]);
                LOGGER.info("queue : " + TYPE[i] +" bind to exchange : " + EXCHANGE_NAME);
            }


            //send message
            for(int i = 0; i<6; i++){

                int place = RabbitMQCommon.getRandom();
                String message = TYPE[place]+"_log:"+ UUID.randomUUID().toString();
                channel.basicPublish(EXCHANGE_NAME,TYPE[place], MessageProperties.PERSISTENT_TEXT_PLAIN,message.getBytes());

            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

}
