package org.z.common;

import com.google.gson.JsonObject;
import io.deepstream.DeepstreamClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.net.SocketTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class EntityWriter {
    private Jedis jedis;
    private DeepstreamClient deepstream;

    public EntityWriter(Jedis jedis, DeepstreamClient deepstream) {
        this.jedis = jedis;
        this.deepstream = deepstream;
    }

    public void writeRandomEntity(String entityId, PopulationArea populationArea, Instant triggerTime) {
        double longitude = randomInRange(populationArea.minLong, populationArea.maxLong);
        double latitude = randomInRange(populationArea.minLat, populationArea.maxLat);

        JsonObject data = randomEntityData(entityId, longitude, latitude);
        if (triggerTime != null) {
            data.addProperty("triggerTime", triggerTime.toString());
        }

        writeToRedis(entityId, longitude, latitude);
        writeToDeepstream(entityId, data);
    }

    private double randomInRange(double min, double max) {
        double range = max - min;
        return ThreadLocalRandom.current().nextDouble() * range + min;
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PopulationArea that = (PopulationArea) o;

            if (Double.compare(that.minLong, minLong) != 0) return false;
            if (Double.compare(that.maxLong, maxLong) != 0) return false;
            if (Double.compare(that.minLat, minLat) != 0) return false;
            return Double.compare(that.maxLat, maxLat) == 0;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            temp = Double.doubleToLongBits(minLong);
            result = (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(maxLong);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(minLat);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(maxLat);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            return result;
        }
    }
}
