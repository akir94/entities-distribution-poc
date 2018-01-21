package org.z.distributer.common;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.redisearch.Document;
import io.redisearch.Query;
import io.redisearch.SearchResult;
import io.redisearch.client.Client;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

            Map<String, ActionAndData> entitiesAndActions = chooseActionsForEntities(documents, clientState);
            Map<String, Instant> newUpdateTimes = new HashMap<>(entitiesAndActions.size());
            sendToClientAndStoreUpdateTimes(clientName, entitiesAndActions, newUpdateTimes);
            clientState.setPreviouslyUpdateTimes(newUpdateTimes);
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

    private Map<String, ActionAndData> chooseActionsForEntities(List<Document> documents, ClientState clientState) {
        Map<String, Instant> previousUpdateTimes = clientState.getPreviouslyUpdateTimes();
        Set<String> entitiesNotSeenYet = new HashSet<>(previousUpdateTimes.keySet());
        Map<String, ActionAndData> entitiesAndActions = new HashMap<>(previousUpdateTimes.size());
        for (Document document : documents) {
            JsonObject payload = (JsonObject) jsonParser.parse(new String(document.getPayload(), StandardCharsets.UTF_8));
            if (isEntityInBounds(payload, clientState)) {
                String entityId = document.getId();
                entitiesNotSeenYet.remove(entityId);
                Instant previousUpdateTime = previousUpdateTimes.get(entityId);
                ActionAndData actionAndData = decideUpdateOrNot(payload, previousUpdateTime);
                entitiesAndActions.put(entityId, actionAndData);
            }
        }
        for (String entityId : entitiesNotSeenYet) {
            entitiesAndActions.put(entityId, new ActionAndData(Action.REMOVE, null, null));
        }
        return entitiesAndActions;
    }

    private boolean isEntityInBounds(JsonObject payload, ClientState clientState) {
        return clientState.isInBounds(payload.get("longitude").getAsDouble(),
                                      payload.get("latitude").getAsDouble());
    }

    private ActionAndData decideUpdateOrNot(JsonObject currentPayload, Instant previousUpdateTime) {
        Instant redisTime = Instant.parse(currentPayload.get("redisTime").getAsString());
        if (previousUpdateTime == null || redisTime.isAfter(previousUpdateTime)) {
            return new ActionAndData(Action.UPDATE, redisTime, currentPayload);
        } else {
            return new ActionAndData(Action.NOT_UPDATE, previousUpdateTime, null);
        }
    }

    private void sendToClientAndStoreUpdateTimes(String clientName, Map<String, ActionAndData> entitiesAndActions,
                                                 Map<String, Instant> newUpdateTimes) {
        for (Map.Entry<String, ActionAndData> entitiesEntry : entitiesAndActions.entrySet()) {
            String entityId = entitiesEntry.getKey();
            ActionAndData stateAndAction = entitiesEntry.getValue();
            switch (stateAndAction.action) {
                case UPDATE:
                    Duration redisLatency = Duration.between(stateAndAction.redisTime, Instant.now());
                    stateAndAction.state.addProperty("redisLatency", redisLatency.toMillis());
                    stateAndAction.state.addProperty("action", "update");
                    updateConsumer.accept(clientName, stateAndAction.state);
                    newUpdateTimes.put(entityId, stateAndAction.redisTime);
                    break;
                case NOT_UPDATE:
                    newUpdateTimes.put(entityId, stateAndAction.redisTime);
                    break;
                case REMOVE:
                    JsonObject deleteMessage = new JsonObject();
                    deleteMessage.addProperty("id", entityId);
                    deleteMessage.addProperty("action", "delete");
                    updateConsumer.accept(clientName, deleteMessage);
                    break;
            }
        }
    }

    private static class ActionAndData {
        public Action action;
        public Instant redisTime;
        public JsonObject state;

        public ActionAndData(Action action, Instant redisTime, JsonObject state) {
            this.action = action;
            this.redisTime = redisTime;
            this.state = state;
        }
    }

    private enum Action {
        UPDATE,
        NOT_UPDATE,
        REMOVE;
    }
}
