package org.z.seeder;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.deepstream.DeepstreamClient;
import org.z.common.EntityWriter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RequestHandler implements HttpHandler {
    private JedisPool jedisPool;
    private DeepstreamClient deepstream;
    private JsonParser jsonParser;

    private ConcurrentMap<EntityWriter.PopulationArea, List<String>> identifierByArea;

    public RequestHandler(JedisPool jedisPool, DeepstreamClient deepstream, JsonParser jsonParser) {
        this.jedisPool = jedisPool;
        this.deepstream = deepstream;
        this.jsonParser = jsonParser;

        this.identifierByArea = new ConcurrentHashMap<>();
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        JsonObject seedData = jsonParser.parse(new InputStreamReader(httpExchange.getRequestBody())).getAsJsonObject();
        System.out.println("got request with body: " + seedData);
        EntityWriter.PopulationArea populationArea = new EntityWriter.PopulationArea(
                seedData.get("minLongitude").getAsDouble(),
                seedData.get("maxLongitude").getAsDouble(),
                seedData.get("minLatitude").getAsDouble(),
                seedData.get("maxLatitude").getAsDouble());
        int entitiesAmount = seedData.get("entitiesAmount").getAsInt();
        Instant triggerTime = Instant.parse(seedData.get("triggerTime").getAsString());

        writeEntities(populationArea, entitiesAmount, triggerTime);
        writeResponse(httpExchange);
    }

    private void writeEntities(EntityWriter.PopulationArea populationArea, int entitiesAmount, Instant triggerTime){
        try(Jedis jedis = jedisPool.getResource()) {
            EntityWriter entityWriter = new EntityWriter(jedis, deepstream);
            List<String> identifiers = identifiersFor(populationArea, entitiesAmount);
            for (String entityId : identifiers) {
                entityWriter.writeRandomEntity(entityId, populationArea, triggerTime);
            }
        }
    }

    private List<String> identifiersFor(EntityWriter.PopulationArea populationArea, int amount) {
        return identifierByArea.computeIfAbsent(populationArea,
                (area) -> randomIdentifiers(amount));  // don't care about area
    }

    private List<String> randomIdentifiers(int amount) {
        return Stream.generate(UUID::randomUUID)
                .map(UUID::toString)
                .limit(amount)
                .collect(Collectors.toList());
    }

    private void writeResponse(HttpExchange httpExchange) {
        try {
            String response = "Ok";
            httpExchange.sendResponseHeaders(200, response.length());
            OutputStream os = httpExchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        } catch (IOException e) {
            System.out.println("failed to return response to the request");
            e.printStackTrace();
        }
    }
}
