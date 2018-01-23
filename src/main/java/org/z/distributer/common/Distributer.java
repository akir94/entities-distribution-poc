package org.z.distributer.common;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.redisearch.Document;
import io.redisearch.Query;
import io.redisearch.SearchResult;
import io.redisearch.client.Client;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

/**
 * Queries redis, and sends the entities that changed since last time.
 */
public class Distributer {
    private Client redisearchClient;
    private ConcurrentMap<String, ClientState> clients;
    private String clientName;
    private BiConsumer<String, JsonObject> updateConsumer;

    private JsonParser jsonParser;

    public Distributer(Client redisearchClient, ConcurrentMap<String, ClientState> clients,
                       String clientName, BiConsumer<String, JsonObject> updateConsumer) {
        this.redisearchClient = redisearchClient;
        this.clients = clients;
        this.clientName = clientName;
        this.updateConsumer = updateConsumer;

        this.jsonParser = new JsonParser();
    }

    public void distribute() {
        try {
            ClientState clientState = clients.get(clientName);
            List<Document> documents = queryDocuments(clientState);
            Map<String, Instant> newRedisTimes = new HashMap<>(documents.size());

            for (Document document : documents) {
                processDocument(document, clientState, newRedisTimes);
            }
            // ClientStateListener replaces the ClientState object, so no race condition
            clientState.setPreviousRedisTimes(newRedisTimes);
        } catch (RuntimeException e) {
            System.out.println("Failed to distribute to client " + clientName);
            System.out.println(e);
        }
    }

    private List<Document> queryDocuments(ClientState clientState) {
        String queryString = "@location:[" + clientState.getCenterLongitude()
                + " " + clientState.getCenterLatitude()
                + " " + clientState.getQueryRadius() + " km]";
        Query query = new Query(queryString).setWithPaload().limit(0, 100000);
        SearchResult res = redisearchClient.search(query);
        return res.docs;
    }

    private void processDocument(Document document, ClientState clientState, Map<String, Instant> newUpdateTimes) {
        JsonObject data = (JsonObject) jsonParser.parse(document.get("data").toString());
        if (isEntityInBounds(data, clientState)) {
            String entityId = data.get("entityId").getAsString();
            Instant redisTime = Instant.parse(data.get("redisTime").getAsString());
            newUpdateTimes.put(entityId, redisTime);

            Instant previousRedisTime = clientState.getPreviousRedisTimes().get(entityId);
            if (previousRedisTime == null || redisTime.isAfter(previousRedisTime)) {
                prepareAndDistribute(data, redisTime);
            }
        }
    }

    private boolean isEntityInBounds(JsonObject data, ClientState clientState) {
        return clientState.isInBounds(data.get("longitude").getAsDouble(), data.get("latitude").getAsDouble());
    }

    private void prepareAndDistribute(JsonObject data, Instant redisTime) {
        Duration redisDelta = Duration.between(redisTime, Instant.now());
        data.addProperty("redisDelta", redisDelta.toMillis());
        updateConsumer.accept(clientName, data);
    }
}
