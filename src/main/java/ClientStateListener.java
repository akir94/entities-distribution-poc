import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.deepstream.EventListener;
import io.vertx.core.eventbus.Message;

import java.util.concurrent.ConcurrentMap;

public class ClientStateListener {

    private ConcurrentMap<String, ClientState> clients;

    public ClientStateListener(ConcurrentMap<String, ClientState> clients) {
        this.clients = clients;
    }

    public void handleDeepstreamEvent(String eventName, Object data) {
        setClientState(data.toString());
    }

    public void handleVertxEvent(Message<String> message) {
        setClientState(message.body());
    }

    private void setClientState(String newState) {
        JsonObject newStateAsJson = new JsonParser().parse(newState)
                .getAsJsonObject();
        String clientName = newStateAsJson.get("name").getAsString();
        ClientState newClientState = new ClientState(
                newStateAsJson.get("maxLongitude").getAsDouble(),
                newStateAsJson.get("minLongitude").getAsDouble(),
                newStateAsJson.get("maxLatitude").getAsDouble(),
                newStateAsJson.get("minLatitude").getAsDouble());
        System.out.println("registering client called " + clientName);
        clients.put(clientName, newClientState);
    }
}
