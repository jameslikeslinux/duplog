package edu.umd.duplog;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

public class Send {
    private static final String QUEUE_NAME = "syslog";

    public static void send() {
        Connection connection = null; 
        Channel channel = null;
        BufferedReader input = null;

        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            connection = factory.newConnection();
            channel = connection.createChannel();
    
            boolean durable = true;
            channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
   
            String line;
            input = new BufferedReader(new InputStreamReader(System.in));
            while ((line = input.readLine()) != null) { 
                channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, line.getBytes());
            }
        } catch (IOException io) {
            io.printStackTrace();
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
    }
}
