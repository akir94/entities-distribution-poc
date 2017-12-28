package org.z.generator;

import com.google.gson.JsonObject;
import io.redisearch.Schema;
import io.redisearch.client.Client;
import redis.clients.jedis.exceptions.JedisDataException;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        String redisHost = args[0];
        int totalEntityAmount = Integer.parseInt(args[1]);
        int workersAmount = Integer.parseInt(args[2]);
        Client client = new Client("entitiesFeed", redisHost, 6379);
        try {
            client.dropIndex();
        } catch (JedisDataException e) {
            // Index doesn't exist
        }
        Schema sc = new Schema().addGeoField("location");
        client.createIndex(sc, Client.IndexOptions.Default());

        ExecutorService executor = Executors.newFixedThreadPool(workersAmount);
        for (int i = 0; i < workersAmount; i++) {
            int startIndex = (totalEntityAmount / workersAmount) * i;
            int endIndex = (totalEntityAmount / workersAmount) * (i + 1);
            executor.submit(() -> updateEntities(client, startIndex, endIndex));
        }

        executor.shutdown();
        try {
            while(true) {
                executor.awaitTermination(10, TimeUnit.HOURS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void updateEntities(Client client, int startIndex, int endIndex) {
        Random random = new Random();
        Map<String, Object> fields = new HashMap<>();
        try {
            while (true) {
                for (int i = startIndex; i < endIndex; i++) {
                    String entityId = "entity" + i;
                    double longitude = 30 + random.nextDouble() * 10;
                    double latitude = 30 + random.nextDouble() * 10;
                    Instant lastUpdateTime = Instant.now();
                    double someData = random.nextDouble();

                    fields.put("location", longitude + "," + latitude); // Yup, that's the syntax
                    byte[] payload = generatePayload(entityId, longitude, latitude, lastUpdateTime, someData);
                    client.addDocument(entityId, 1.0, fields, false, true, payload);
                    System.out.println(String.format("ID: %s, Long: %f, Lat: %f, updateTime: %d, someData: %s", entityId, longitude, latitude, lastUpdateTime.toEpochMilli(), someData));
                }
                Thread.sleep(50);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static byte[] generatePayload(String entityId, double longitude, double latitude, Instant lastUpdateTime, double someData) {
        JsonObject payload = new JsonObject();
        payload.addProperty("id", entityId);
        payload.addProperty("longitude", longitude);
        payload.addProperty("latitude", latitude);
        payload.addProperty("lastUpdateTime", lastUpdateTime.toString());
        payload.addProperty("someData", someData);
        payload.addProperty("action", "update");
        return payload.toString().getBytes(StandardCharsets.UTF_8);
    }

}
