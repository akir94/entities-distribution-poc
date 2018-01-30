package org.z.distributer;

import io.deepstream.DeepstreamClient;
import io.deepstream.List;
import io.deepstream.ListenListener;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ClientStateListener implements ListenListener {
    private ScheduledExecutorService scheduledExecutor;
    private DeepstreamClient deepstream;
    private Jedis jedis;

    private Map<String, ScheduledFuture> threadTokens;


    public ClientStateListener(ScheduledExecutorService scheduledExecutor,
                               DeepstreamClient deepstream,
                               Jedis jedis) {
        this.scheduledExecutor = scheduledExecutor;
        this.deepstream = deepstream;
        this.jedis = jedis;

        this.threadTokens = new HashMap<>();
    }

    @Override
    public boolean onSubscriptionForPatternAdded(String listName) {
        System.out.println("subscription pattern added: " + listName);
        String[] parts = listName.split("/");
        double minLongitude = Double.parseDouble(parts[1]);
        double maxLongitude = Double.parseDouble(parts[2]);
        double minLatitude = Double.parseDouble(parts[3]);
        double maxLatitude = Double.parseDouble(parts[4]);

        ClientState clientState = new ClientState(minLongitude, maxLongitude, minLatitude, maxLatitude);
        initNotifierThread(listName, clientState);
        return true;
    }

    @Override
    public void onSubscriptionForPatternRemoved(String listName) {
        System.out.println("subscription pattern removed: " + listName);
        ScheduledFuture token = threadTokens.get(listName);
        if (token != null) {
            token.cancel(true);
        } else {
            System.out.println("No token found for unsubscription of " + listName);
        }
    }

    private void initNotifierThread(String listName, ClientState clientState) {
        System.out.println("scheduling a new distributer thread for " + listName);
        ClientNotifier notifier = new ClientNotifier(jedis, clientState, listName, deepstream);
        ScheduledFuture token = scheduledExecutor.scheduleAtFixedRate(
                notifier::queryKeysAndNotifyClient, 0, 5, TimeUnit.SECONDS);
        threadTokens.put(listName, token);
    }
}
