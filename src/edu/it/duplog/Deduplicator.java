package edu.umd.it.duplog;

import com.google.common.hash.Hashing;
import com.google.common.hash.HashFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class Deduplicator {
    private static final Logger logger = LoggerFactory.getLogger(Deduplicator.class);
    private static final HashFunction hashFunction = Hashing.murmur3_128();

    private String redisServer;
    private JedisPool jedisPool;
    private boolean connectionOk;

    public Deduplicator(String redisServer) {
        this.redisServer = redisServer;

        // XXX: Do we need to set better defaults in JedisPoolConfig?
        jedisPool = new JedisPool(new JedisPoolConfig(), redisServer);

        connectionOk = true;
    }

    public boolean isDuplicate(String message, String fromHost, String[] otherHosts) {
        /*
         * If there are problems connecting to the Redis dedup cache,
         * degrade gracefully by allowing duplicate messages
         */
        if (!connectionOk) {
            return false;
        }

        boolean duplicate = false;
        Jedis jedis = null;

        try {
            // JedisPool guarantees thread safety
            jedis = jedisPool.getResource();
            
            String hash = hashMessage(message);

            /*
             * Execute dedup logic as an atomic transaction to prevent
             * race conditions from other threads and processes
             */
            Transaction t = jedis.multi();
            for (String otherHost : otherHosts) {
                t.decr(otherHost + ":" + hash);
            }

            Response<Long> count = t.incr(fromHost + ":" + hash);
            t.exec();

            if (count.get() <= 0) {
                duplicate = true;
            }
        } catch (JedisConnectionException jce) {
            logger.error("Connection to Redis server at " + redisServer + " failed");
            logger.debug("", jce);

            // Don't try talk to Redis for five seconds after a failure
            // TODO: Make this an exponential backoff?
            connectionOk = false;
            new Timer().schedule(new TimerTask() {
                public void run() {
                    connectionOk = true;
                }
            }, 5000);
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }

        return duplicate;
    }
    
    private static String hashMessage(String message) {
        return hashFunction.hashString(message).toString();
    }
}
