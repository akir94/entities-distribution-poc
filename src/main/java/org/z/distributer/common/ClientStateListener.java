package org.z.distributer.common;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.vertx.core.eventbus.Message;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class ClientStateListener {

    private ConcurrentMap<String, ClientState> clients;
    private ScheduledExecutorService executorService;
    private Function<String, Distributer> distributerFactory;

    public ClientStateListener(ConcurrentMap<String, ClientState> clients,
                               ScheduledExecutorService executorService,
                               Function<String, Distributer> distributerFactory) {
        this.clients = clients;
        this.executorService = executorService;
        this.distributerFactory = distributerFactory;
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
        ClientState previousClientSatet = clients.put(clientName, newClientState);
        if (previousClientSatet == null) {
            initDistributerThread(clientName);
        }
    }

    private void initDistributerThread(String clientName) {
        Distributer distributer = distributerFactory.apply(clientName);
        executorService.scheduleAtFixedRate(() -> distributer.distribute(), 0, 50, TimeUnit.MILLISECONDS);
    }


}
