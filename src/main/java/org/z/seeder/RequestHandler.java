package org.z.seeder;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.javalin.Context;
import io.javalin.Handler;
import io.redisearch.client.Client;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class RequestHandler implements Handler{
    private Client redisearchClient;
    private Random random;
    private JsonParser jsonParser;

    public RequestHandler(Client redisearchClient, Random random, JsonParser jsonParser) {
        this.redisearchClient = redisearchClient;
        this.random = random;
        this.jsonParser = jsonParser;
    }

    @Override
    public void handle(Context ctx) throws Exception {
        JsonObject seedData = jsonParser.parse(ctx.body()).getAsJsonObject();
        double minLong = seedData.get("minLongitude").getAsDouble();
        double maxLong = seedData.get("maxLongitude").getAsDouble();
        double minLat = seedData.get("minLatitude").getAsDouble();
        double maxLat = seedData.get("maxLatitude").getAsDouble();
        Instant triggerTime = Instant.parse(seedData.get("triggerTime").getAsString());

        for(int i = 0; i < 10; i++) {
            String entityId = UUID.randomUUID().toString();
            double longitude = generateInRange(random, minLong, maxLong);
            double latitude = generateInRange(random, minLat, maxLat);
            byte[] payload = generatePayload(entityId, longitude, latitude, triggerTime);

            Map<String, Object> fields = new HashMap<>();
            fields.put("location", longitude + "," + latitude);
            redisearchClient.addDocument(entityId, 1.0, fields, false, true, payload);
        }
    }

    private static byte[] generatePayload(String entityId, double longitude, double latitude, Instant triggerTime) {
        JsonObject payload = new JsonObject();
        payload.addProperty("id", entityId);
        payload.addProperty("longitude", longitude);
        payload.addProperty("latitude", latitude);
        payload.addProperty("triggerTime", triggerTime.toString());
        payload.addProperty("redisTime", Instant.now().toString());
        return payload.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static double generateInRange(Random random, double min, double max) {
        double range = max - min;
        return random.nextDouble() * range + min;
    }
}
