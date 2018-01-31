package org.z.seeder;

import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpServer;
import io.deepstream.DeepstreamClient;
import io.redisearch.client.Client;
import org.z.common.EntityWriter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) {
        String redisHost = args[0];
        String deepstreamHost = args[1];
        int port = Integer.parseInt(args[2]);

        try {
            JedisPool jedisPool = createJedisPool(redisHost);
            DeepstreamClient deepstream = new DeepstreamClient(deepstreamHost + ":6020");
            deepstream.login();
            JsonParser parser = new JsonParser();

            try {
                HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
                server.createContext("/", new RequestHandler(jedisPool, deepstream, parser));
                server.setExecutor(Executors.newCachedThreadPool());
                server.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            addShutdownHook(deepstream, jedisPool);
        } catch (URISyntaxException e) {
            System.out.println(e);
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
