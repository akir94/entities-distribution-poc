package org.z.distributer;

import io.deepstream.DeepstreamClient;
import io.redisearch.client.Client;
import org.z.distributer.common.ClientSimulator;
import org.z.distributer.common.ClientState;
import org.z.distributer.common.ClientStateListener;
import org.z.distributer.common.Distributer;

import java.net.URISyntaxException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DeepstreamMain {
    public static void main(String[] args) {
        String deepstreamHost = args[0];
        String redisHost = args[1];
        try {
            DeepstreamClient deepstreamClient = new DeepstreamClient(deepstreamHost + ":6020");
            deepstreamClient.login();

            ConcurrentMap<String, ClientState> clients = new ConcurrentHashMap<>();

            ClientStateListener clientStateListener = new ClientStateListener(clients);
            deepstreamClient.event.subscribe("setClientState", clientStateListener::handleDeepstreamEvent);

            //Thread clientThread = new Thread(() -> clientThread(deepstreamClient));
            //clientThread.start();

            Client redisearchClient = new Client("entitiesFeed", redisHost, 6379);
            Thread distributerThread = new Thread(() -> pollAndSendUpdates(deepstreamClient, redisearchClient, clients));
            distributerThread.start();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    private static void pollAndSendUpdates(DeepstreamClient deepstreamClient, Client redisearchClient,
                                    ConcurrentMap<String, ClientState> clients) {
        Distributer distributer = new Distributer(redisearchClient, clients,
                (eventName, data) -> deepstreamClient.event.emit(eventName, data));
        try {
            while (true) {
                Thread.sleep(50);
                distributer.distribute();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void clientThread(DeepstreamClient deepstreamClient) {
        ClientSimulator clientSimulator = new ClientSimulator(
                "clientThread",
                (eventName, data) -> deepstreamClient.event.emit(eventName, data));
        try {
            deepstreamClient.event.subscribe("clientThread",
                    (eventName, data) -> System.out.println("clientThread received event called " + eventName + " with data " + data));

            while (true) {
                Thread.sleep(1000);
                clientSimulator.simulateStateChange();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
