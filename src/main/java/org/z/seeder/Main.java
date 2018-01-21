package org.z.seeder;

import com.google.gson.JsonParser;
import io.javalin.Javalin;
import io.redisearch.client.Client;

import java.util.Random;

public class Main {
    public static void main(String[] args) {
        String redisHost = args[1];
        Client redisearchClient = new Client("entitiesFeed", redisHost, 6379, 3000, 100);
        Random random = new Random();
        JsonParser parser = new JsonParser();
        Javalin app = Javalin.start(7003);
        app.post("/", new RequestHandler(redisearchClient, random, parser));
    }
}
