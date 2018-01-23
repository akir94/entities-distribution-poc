package org.z.generator;

import io.redisearch.Schema;
import io.redisearch.client.Client;
import org.z.common.EntityWriter;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        String redisHost = args[0];
        int totalEntityAmount = Integer.parseInt(args[1]);
        int workersAmount = Integer.parseInt(args[2]);
        Client client = new Client("entitiesFeed", redisHost, 6379, 1500, 100);
        EntityWriter.PopulationArea populationArea = new EntityWriter.PopulationArea(30, 40, 30, 40);

        try {
            client.dropIndex();
        } catch (JedisDataException e) {
            // Index doesn't exist
        }
        Schema sc = new Schema().addGeoField("location");
        client.createIndex(sc, Client.IndexOptions.Default());

        ExecutorService executor = Executors.newFixedThreadPool(workersAmount);
        for (int i = 0; i < workersAmount; i++) {
            int startIndex = (totalEntityAmount / workersAmount) * i;
            int endIndex = (totalEntityAmount / workersAmount) * (i + 1);
            executor.submit(() -> updateEntities(client, populationArea, startIndex, endIndex));
        }

        executor.shutdown();
        try {
            while (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                System.out.println("An hour has passed, woohoo!");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void updateEntities(Client client, EntityWriter.PopulationArea populationArea, int startIndex, int endIndex) {
        EntityWriter writer = new EntityWriter(client, new Random());
        try {
            while (true) {
                for (int i = startIndex; i < endIndex; i++) {
                    String entityId = "entity" + i;
                    writer.writeRandomEntity(entityId, populationArea, null);
                }
                System.out.println("finished a loop");
                Thread.sleep(50);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
