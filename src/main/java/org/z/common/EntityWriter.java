package org.z.common;

import com.google.gson.JsonObject;
import io.redisearch.client.Client;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.net.SocketTimeoutException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class EntityWriter {
    private Client redisearchClient;
    private Random random;

    public EntityWriter(Client redisearchClient, Random random) {
        this.redisearchClient = redisearchClient;
        this.random = random;
    }

    public void writeRandomEntity(String entityId, PopulationArea populationArea, Instant triggerTime) {
        double longitude = randomInRange(populationArea.minLong, populationArea.maxLong);
        double latitude = randomInRange(populationArea.minLat, populationArea.maxLat);

        JsonObject data = randomEntityData(entityId, longitude, latitude);
        if (triggerTime != null) {
            data.addProperty("triggerTime", triggerTime.toString());
        }

        Map<String, Object> fields = new HashMap<>();
        fields.put("location", longitude + "," + latitude);  // Yup, that's the syntax
        fields.put("data", data.toString());
        writeWithRetries(entityId, fields);
    }

    private double randomInRange(double min, double max) {
        double range = max - min;
        return random.nextDouble() * range + min;
    }

    private JsonObject randomEntityData(String entityId, double longitude, double latitude) {
        JsonObject data = new JsonObject();
        data.addProperty("entityId", entityId);
        data.addProperty("longitude", longitude);
        data.addProperty("latitude", latitude);
        data.addProperty("redisTime", Instant.now().toString());
        return data;
    }

    private void writeWithRetries(String entityId, Map<String, Object> fields) {
        boolean success = false;
        while (!success) {
            try {
                redisearchClient.replaceDocument(entityId, 1.0, fields);
                success = true;
            } catch (JedisConnectionException e) {
                if (e.getCause() instanceof SocketTimeoutException) {
                    System.out.println("got timeout exception, retrying");
                } else {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static class PopulationArea {
        private double minLong;
        private double maxLong;
        private double minLat;
        private double maxLat;

        public PopulationArea(double minLong, double maxLong, double minLat, double maxLat) {
            this.minLong = minLong;
            this.maxLong = maxLong;
            this.minLat = minLat;
            this.maxLat = maxLat;
        }
    }
}
