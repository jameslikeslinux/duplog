package edu.umd.it.duplog;

import com.google.common.hash.Hashing;
import com.google.common.hash.HashFunction;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Response;

public class Recv extends Thread {
    private static final String QUEUE_NAME = "syslog";
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();

    private String host;
    private String[] otherHosts;
    private boolean running;

    private Jedis jedis;

    public Recv(String host, String[] otherHosts) {
        this.host = host;
        this.otherHosts = otherHosts;

        jedis = new Jedis("localhost");
    }

    public void run() {
        while (true) {
            receive();
            System.out.println(" [*] Restarting receiver for " + host);
        }
    }

    private void receive() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            boolean durable = true;
            channel.queueDeclare(QUEUE_NAME, durable, false, false, null);

            int prefetchCount = 1;
            channel.basicQos(prefetchCount);

            boolean autoAck = false;
            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(QUEUE_NAME, autoAck, consumer);
            
            System.out.println(" [*] Waiting for messages from " + host);
        
            while (true) {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();

                String message = new String(delivery.getBody());
                processMessage(message);

                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        } catch (IOException io) {
            io.printStackTrace();
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
    }

    private void processMessage(String message) {
        String hash = hashMessage(message);

        Transaction t = jedis.multi();
        for (String otherHost : otherHosts) {
            t.decr(otherHost + ":" + hash);
        }

        Response<Long> count = t.incr(host + ":" + hash);
	t.exec();

        if (count.get() > 0) {
            System.out.println(" [ ] Received '" + message + "' from " + host);
        } else {
            //System.out.println(" [D] Received '" + message + "' from " + host);
        }
    }

    private static String hashMessage(String message) {
        return HASH_FUNCTION.hashString(message).toString();
    }

    public static void recv(String[] hosts) {
        Set<String> hostsSet = new HashSet<String>(Arrays.asList(hosts));

        for (String host : hostsSet) {
            Set<String> otherHosts = new HashSet<String>(hostsSet);
            otherHosts.remove(host);
            new Recv(host, otherHosts.toArray(new String[]{})).start();
        }

        synchronized (Recv.class) {
            try {
                Recv.class.wait();
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
        }
    }
}
