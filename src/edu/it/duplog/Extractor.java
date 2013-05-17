package edu.umd.it.duplog;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Extractor extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(Extractor.class);
    private static final String QUEUE_NAME = "syslog";
    private static PrintWriter output;
    private static Deduplicator deduplicator;

    private String host;
    private String[] otherHosts;
    private boolean running;

    public Extractor(String host, String[] otherHosts) {
        this.host = host;
        this.otherHosts = otherHosts;
    }

    public void run() {
        while (true) {
            extract();

            // This should only happen if the connection to RabbitMQ fails
            logger.info("Died listening to " + host);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ie) {
                logger.debug("", ie);
            }
            logger.info("Restarting extractor for " + host);
        }
    }

    private void extract() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            // Queue declaration is idempotent
            boolean durable = true;     // back queue by disk
            channel.queueDeclare(QUEUE_NAME, durable, false, false, null);

            // Process one message at a time; round-robin
            int prefetchCount = 1;
            channel.basicQos(prefetchCount);

            // ACK only after successfully processing messages
            boolean autoAck = false;
            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(QUEUE_NAME, autoAck, consumer);
            
            logger.info("Waiting for messages from " + host);
        
            while (true) {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();   // blocks

                String message = new String(delivery.getBody());
                if (!deduplicator.isDuplicate(message, host, otherHosts)) {
                    output.println(message);
                }

                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        } catch (IOException io) {
            logger.debug("", io);
        } catch (InterruptedException ie) {
            logger.debug("", ie);
        }
    }

    public static int extract(String[] hosts, String redisServer, String outputFile) {
        // Open log file for writing
        // XXX: How does this handle log rotation?
        try {
            boolean append = true;
            boolean autoFlush = true;
            output = new PrintWriter(new FileWriter(outputFile, append), autoFlush);
        } catch (IOException io) {
            logger.error("Cannot open log file", io);
            return 1;
        }

        deduplicator = new Deduplicator(redisServer);

        // Create extractor for every syslog host
        Set<String> hostsSet = new HashSet<String>(Arrays.asList(hosts));
        for (String host : hostsSet) {
            Set<String> otherHosts = new HashSet<String>(hostsSet);
            otherHosts.remove(host);
            new Extractor(host, otherHosts.toArray(new String[]{})).start();
        }

        // wait forever
        while (true) {
            synchronized (Extractor.class) {
                try {
                    Extractor.class.wait();
                } catch (InterruptedException ie) {
                    logger.debug("", ie);
                }
            }
        }

        // this shouldn't happen
        return 0;
    }
}
