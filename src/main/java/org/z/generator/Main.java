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

public class Main {
    public static void main(String[] args) {
        Client client = new Client("entitiesFeed", "192.168.0.53", 6379);
        try {
            client.dropIndex();
        } catch (JedisDataException e) {
            // Index doesn't exist
        }
        Schema sc = new Schema().addGeoField("location");
        client.createIndex(sc, Client.IndexOptions.Default());

        Random random = new Random();
        Map<String, Object> fields = new HashMap<>();
        while (true) {
            for (int i = 0; i < 300; i++) {
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
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private static byte[] generatePayload(String entityId, double longitude, double latitude, Instant lastUpdateTime, double someData) {
        JsonObject payload = new JsonObject();
        payload.addProperty("id", entityId);
        payload.addProperty("longitude", longitude);
        payload.addProperty("latitude", latitude);
        payload.addProperty("lastUpdateTime", lastUpdateTime.toEpochMilli());
        payload.addProperty("someData", someData);
        return payload.toString().getBytes(StandardCharsets.UTF_8);
    }

}
