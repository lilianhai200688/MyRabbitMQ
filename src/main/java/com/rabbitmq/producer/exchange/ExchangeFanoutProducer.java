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
 * type fanout
 * 忽略binding key , 将消息send to 所有绑定到此exchange的队列
 */
public class ExchangeFanoutProducer {

    private static Logger LOGGER = Logger.getLogger(ExchangeFanoutProducer.class.getName());
    private static final String EXCHANGE_TYPE = "fanout";
    private static final String[] TYPE = {"fanout1","fanout2","fanout3"};
    private static final String EXCHANGE_NAME = "test_exchange_fanout";

    public static void main(String[] args) {

        Connection conn = null;
        Channel channel = null;

        try {
            conn = RabbitMQCommon.getMqconn();
            channel = conn.createChannel();
            //声明转发器类型 durable
            channel.exchangeDeclare(EXCHANGE_NAME,EXCHANGE_TYPE,true);

            //声明消息队列,并将消息队列绑定到exchange
            for(int i = 0; i < TYPE.length; i++){

                channel.queueDeclare(TYPE[i], true, false, false, null);
                channel.queueBind(TYPE[i], EXCHANGE_NAME, TYPE[i]);
                LOGGER.info("queue : " + TYPE[i] +" bind to exchange : " + EXCHANGE_NAME + " by binding key : "+TYPE[i]);
            }

            //send message
            for(int i = 0; i<3; i++){

                int place = RabbitMQCommon.getRandom();
                String message = TYPE[place]+"_log:"+ UUID.randomUUID().toString();
                channel.basicPublish(EXCHANGE_NAME,TYPE[place], MessageProperties.PERSISTENT_TEXT_PLAIN,message.getBytes());
            }

            //close conn
            channel.close();
            conn.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

    }
}
