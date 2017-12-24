package org.z.distributer.common;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.vertx.core.eventbus.Message;

import java.util.concurrent.ConcurrentMap;

public class ClientStateListener {

    private ConcurrentMap<String, ClientState> clients;

    public ClientStateListener(ConcurrentMap<String, ClientState> clients) {
        this.clients = clients;
    }

    public void handleDeepstreamEvent(String eventName, Object data) {
        setClientState((JsonObject)data);
    }

    public void handleVertxEvent(Message<io.vertx.core.json.JsonObject > message) {
        io.vertx.core.json.JsonObject newState = message.body();
        JsonObject newStateAsJson = new JsonParser().parse(newState.encode()).getAsJsonObject();
        setClientState(newStateAsJson);
    }

    private void setClientState(JsonObject newState) {
        String clientName = newState.get("name").getAsString();
        ClientState newClientState = new ClientState(
                newState.get("maxLongitude").getAsDouble(),
                newState.get("minLongitude").getAsDouble(),
                newState.get("maxLatitude").getAsDouble(),
                newState.get("minLatitude").getAsDouble());
        System.out.println("registering client called " + clientName + " with data " + newState);
        clients.put(clientName, newClientState);
    }
}
