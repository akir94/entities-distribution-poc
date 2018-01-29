package org.z.common;

import com.google.gson.JsonObject;
import io.deepstream.DeepstreamClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.net.SocketTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;

public class EntityWriter {
    private Jedis jedis;
    private DeepstreamClient deepstream;
    private Random random;

    public EntityWriter(Jedis jedis, DeepstreamClient deepstream, Random random) {
        this.jedis = jedis;
        this.deepstream = deepstream;
        this.random = random;
    }

    public void writeRandomEntity(String entityId, PopulationArea populationArea, Instant triggerTime) {
        double longitude = randomInRange(populationArea.minLong, populationArea.maxLong);
        double latitude = randomInRange(populationArea.minLat, populationArea.maxLat);

        JsonObject data = randomEntityData(entityId, longitude, latitude);
        if (triggerTime != null) {
            data.addProperty("triggerTime", triggerTime.toString());
        }

        Instant before = Instant.now();
        writeToRedis(entityId, longitude, latitude);
        Instant between = Instant.now();
        writeToDeepstream(entityId, data);
        Instant after = Instant.now();
        System.out.println("redis delta = " + Duration.between(before, between));
        System.out.println("deepstream delta = " + Duration.between(between, after));
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

    private void writeToRedis(String entityId, double longitude, double latitude) {
        boolean success = false;
        while (!success) {
            try {
                jedis.geoadd("EntitiesGeospace", longitude, latitude, entityId);
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

    private void writeToDeepstream(String entityId, JsonObject data) {
        deepstream.record.getRecord("entity/" + entityId).set(data);
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
