package java.com.rabbitmq.producer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 *
 * publish message example
 * step by step
 *
 */
public class ExampleProducer {

    public static void main(String[] args){

        try {

            ConnectionFactory mqcf = new ConnectionFactory();
            mqcf.setHost("localhost");
            mqcf.setUsername("admin");
            mqcf.setPassword("admin");
            mqcf.setVirtualHost("dev");
            mqcf.setPort(5672);
            //底层是TCP连接
            Connection conn = mqcf.newConnection();

            //open channel信道,建立在TCP连接内的虚拟连接
            Channel channel = conn.createChannel();

            /**
             * AMQP消息路由
             * exchange have four type: direct,fanout,topic and header
             * header转发器与其他3个不同，它允许匹配AMQP消息的header而不是路由键
             */
            String exchangename = "command";
            channel.exchangeDeclare(exchangename, "direct", true);

            //队列
            String queuename = "MSG_COMMAND";
            channel.queueDeclare(queuename, true, false, false, null);

            /**
             * bind
             * 使用binding key，在exchange和queue之间建立好绑定关系
             * 与routing key 区分开来
             */
            String bindingkey = "test_routingkey";
            channel.queueBind(queuename, exchangename, bindingkey);

            //
            //channel.basicQos(1);

            //publish message
            String jsonmsg= "{\"appID\":\"APP4998\",\"appType\":1,\"token\":\"194ea432c9a04b73\",\"topic\":\"MSG/EVENT/PARKING\",\"payload\":\"what blabalbalbalbaablalabbaal\",\"time\":\"";

            String routingkey = "test_routingkey";

            /*AMQP.BasicProperties properties = new AMQP.BasicProperties("application/json",
                    null,
                    null,
                    2,
                    0, null, null, null,
                    null, null, null, null,
                    null, null);*/
            channel.basicPublish(exchangename,routingkey, new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(2)
                    .priority(1)
                    .build(),jsonmsg
            .getBytes());

            //close
            channel.close();
            conn.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
}
