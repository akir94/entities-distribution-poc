package org.z.distributer;

import io.deepstream.DeepstreamClient;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.Jedis;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Queries redis, and sends the entities that changed since last time.
 */
public class ClientNotifier {
    private Jedis jedis;
    private ClientState clientState;
    private String listName;
    private DeepstreamClient deepstream;

    public ClientNotifier(Jedis jedis, ClientState clientState, String listName, DeepstreamClient deepstream) {
        this.jedis = jedis;
        this.clientState = clientState;
        this.listName = listName;
        this.deepstream = deepstream;
    }

    public void queryKeysAndNotifyClient() {
        System.out.println("notifier querying for " + listName);
        try {
            List<GeoRadiusResponse> responses = queryKeys();
            System.out.println("found " + responses.size() + " entities in area");
            String[] keysArray = new String[responses.size()];
            for (int i = 0; i < responses.size(); i++) {
                keysArray[i] = responses.get(i).getMemberByString();
            }

            deepstream.record.getList(listName)
                    .setEntries(keysArray);
            // No need to discard list here, this invocation is counted as part of the "active data provider"
        } catch (RuntimeException e) {
            System.out.println("Failed to distribute to list " + listName);
            e.printStackTrace();
        }
    }

    private List<GeoRadiusResponse> queryKeys() {
        return jedis.georadius("EntitiesGeospace",
                clientState.getCenterLongitude(),
                clientState.getCenterLatitude(),
                clientState.getQueryRadius(),
                GeoUnit.KM);
    }
}
