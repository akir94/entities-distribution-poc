package org.z.seeder;

import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpServer;
import io.deepstream.DeepstreamClient;
import io.redisearch.client.Client;
import org.z.common.EntityWriter;
import redis.clients.jedis.Jedis;

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
            Jedis jedis = new Jedis(redisHost);
            DeepstreamClient deepstream = new DeepstreamClient(deepstreamHost + ":6020");
            deepstream.login();
            EntityWriter entityWriter = new EntityWriter(jedis, deepstream, new Random());
            JsonParser parser = new JsonParser();

            try {
                HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
                server.createContext("/", new RequestHandler(entityWriter, parser));
                server.setExecutor(Executors.newCachedThreadPool());
                server.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (URISyntaxException e) {
            System.out.println(e);
        }
    }
}
