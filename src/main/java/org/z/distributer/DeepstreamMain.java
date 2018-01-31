package org.z.distributer;

import io.deepstream.DeepstreamClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.net.URISyntaxException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class DeepstreamMain {
    public static void main(String[] args) {
        String deepstreamHost = args[0];
        String redisHost = args[1];
        try {
            JedisPool jedisPool = createJedisPool(redisHost);
            DeepstreamClient deepstream = new DeepstreamClient(deepstreamHost + ":6020");
            deepstream.login();

            ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(20);
            ClientStateListener listener = new ClientStateListener(scheduledExecutor, deepstream, jedisPool);
            deepstream.record.listen("entities_around/.*", listener);

            addShutdownHook(deepstream, jedisPool);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    private static JedisPool createJedisPool(String redisHost) {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(20);
        jedisPoolConfig.setMaxIdle(20);
        return new JedisPool(jedisPoolConfig, redisHost);
    }

    private static void addShutdownHook(DeepstreamClient deepstream, JedisPool jedisPool) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            deepstream.close();
            jedisPool.close();
        }));
    }
}
