package edu.umd.it.duplog;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Extractor extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(Extractor.class);
    private static final int HEARTBEAT = 60;
    private static final String QUEUE_NAME = "syslog";
    private static PrintStream output;
    private static Deduplicator deduplicator;

    private String host;
    private String[] otherHosts;
    private boolean running;


    private class Acknowledger extends Thread {
        private Channel channel;
        private long rate;
        private boolean end;
        private QueueingConsumer.Delivery delivery;

        public Acknowledger(Channel channel, long rate) {
            this.channel = channel;
            this.rate = rate;
        }

        public void run() {
            end = false;

            while (!end) {
                acknowledge();

                try {
                    sleep(rate);
                } catch (InterruptedException ie) {
                    logger.debug("", ie);
                }
            }
        }

        public void end() {
            end = true;
        }

        public synchronized void setDelivery(QueueingConsumer.Delivery delivery) {
            this.delivery = delivery;
        }

        private synchronized void acknowledge() {
            if (delivery != null) {
                try {
                    boolean multiple = true;
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), multiple);
                    delivery = null;
                } catch (IOException io) {
                    logger.debug("", io);
                }
            }
        }
    }

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
        Acknowledger acknowledger = null;

        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setRequestedHeartbeat(HEARTBEAT);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
           
            // Queue declaration is idempotent
            boolean durable = true;     // back queue by disk
            channel.queueDeclare(QUEUE_NAME, durable, false, false, null);

            // Receive unlimited messages when available
            int prefetchCount = 0;
            channel.basicQos(prefetchCount);

            // ACK only after successfully processing messages
            boolean autoAck = false;
            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(QUEUE_NAME, autoAck, consumer);
            
            // Acknowledge messages every second
            acknowledger = new Acknowledger(channel, 1000);
            acknowledger.start();

            logger.info("Waiting for messages from " + host);

            while (true) {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();   // blocks

                String message = new String(delivery.getBody());
                if (!deduplicator.isDuplicate(message, host, otherHosts)) {
                    output.println(message);
                }

                acknowledger.setDelivery(delivery);
            }
        } catch (Exception e) {
            logger.debug("", e);
        } finally {
            if (acknowledger != null) {
                acknowledger.end();
            }
        }
    }

    public static int extract(String[] hosts, String redisServer, String outputFile) {
        // Open log file for writing
        // XXX: How does this handle log rotation?
        try {
            if (outputFile.equals("-")) {
                output = System.out;
            } else {
                boolean append = true;
                boolean autoFlush = true;
                output = new PrintStream(new FileOutputStream(outputFile, append), autoFlush);
            }
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
    }
}
