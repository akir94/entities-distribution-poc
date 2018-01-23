package org.z.seeder;

import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpServer;
import io.redisearch.client.Client;
import org.z.common.EntityWriter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) {
        String redisHost = args[0];
        int port = Integer.parseInt(args[1]);
        Client redisearchClient = new Client("entitiesFeed", redisHost, 6379, 3000, 100);
        EntityWriter entityWriter = new EntityWriter(redisearchClient, new Random());
        JsonParser parser = new JsonParser();

        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            //Create the context for the server.
            server.createContext("/", new RequestHandler(entityWriter, parser));

            server.setExecutor(Executors.newCachedThreadPool());
            server.start();
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
}
