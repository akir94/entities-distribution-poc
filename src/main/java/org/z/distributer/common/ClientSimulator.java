package org.z.distributer.common;

import com.google.gson.JsonObject;

import java.util.Random;
import java.util.function.BiConsumer;

public class ClientSimulator {
    String clientName;
    private BiConsumer<String, String> eventConsumer;
    private Random random;

    public ClientSimulator(String clientName, BiConsumer<String, String> eventConsumer) {
        this.clientName = clientName;
        this.eventConsumer = eventConsumer;
        this.random = new Random();
    }

    public void simulateStateChange() {
        JsonObject eventData = new JsonObject();
        eventData.addProperty("name", clientName);
        eventData.addProperty("maxLongitude", 35 + random.nextDouble() * 5);
        eventData.addProperty("minLongitude", 30 + random.nextDouble() * 5);
        eventData.addProperty("maxLatitude", 35 + random.nextDouble() * 5);
        eventData.addProperty("minLatitude", 30 + random.nextDouble() * 5);
        System.out.println("sending state event with data " + eventData.toString());
        eventConsumer.accept("setClientState", eventData.toString());
    }
}
