package edu.umd.it.duplog;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Injector {
    private static final Logger logger = LoggerFactory.getLogger(Injector.class);
    private static final String RABBITMQ_HOST = "localhost";
    private static final int HEARTBEAT = 60;
    private static final String QUEUE_NAME = "syslog";

    public static int inject() {
        int ret = 0;
        Connection connection = null; 
        Channel channel = null;
        BufferedReader input = null;

        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(RABBITMQ_HOST);
            factory.setRequestedHeartbeat(HEARTBEAT);
            connection = factory.newConnection();
            channel = connection.createChannel();
    
            // Queue declaration is idempotent
            boolean durable = true;     // back queue by disk
            channel.queueDeclare(QUEUE_NAME, durable, false, false, null);

            // Simply read lines from stdin into RabbitMQ until told not to (EOF) 
            String line;
            input = new BufferedReader(new InputStreamReader(System.in));
            while ((line = input.readLine()) != null) { 
                channel.basicPublish("", QUEUE_NAME, durable ? MessageProperties.PERSISTENT_TEXT_PLAIN : null, line.getBytes());
            }
        } catch (IOException io) {
            /*
             * If a connection fails, this process will end
             * and rsyslog will restart it:
             * http://www.rsyslog.com/doc/omprog.html
             */
            logger.error("Failed to connect to RabbitMQ on " + RABBITMQ_HOST);
            logger.debug("", io);
            ret = 1;
        } finally {
            try {
                if (input != null) { 
                    input.close();
                }

                if (channel != null) {
                    channel.close();
                }

                if (connection != null) {
                    channel.close();
                }
            } catch (IOException io) {
                // do nothing
            }
        }

        return ret;
    }
}
