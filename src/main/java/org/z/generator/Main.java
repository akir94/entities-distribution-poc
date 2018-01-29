package org.z.generator;

import io.deepstream.DeepstreamClient;
import io.redisearch.Schema;
import io.redisearch.client.Client;
import org.z.common.EntityWriter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.net.URISyntaxException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        String redisHost = args[0];
        String deepstreamHost = args[1];
        int totalEntityAmount = Integer.parseInt(args[2]);
        int workersAmount = Integer.parseInt(args[3]);

        try {
            Jedis jedis = new Jedis(redisHost);
            DeepstreamClient deepstream = new DeepstreamClient(deepstreamHost + ":6020");
            deepstream.login();
            EntityWriter.PopulationArea populationArea = new EntityWriter.PopulationArea(30, 40, 30, 40);

            ExecutorService executor = Executors.newFixedThreadPool(workersAmount);
            for (int i = 0; i < workersAmount; i++) {
                int startIndex = (totalEntityAmount / workersAmount) * i;
                int endIndex = (totalEntityAmount / workersAmount) * (i + 1);
                EntityWriter entityWriter = new EntityWriter(jedis, deepstream, new Random());
                executor.submit(() -> updateEntities(entityWriter, populationArea, startIndex, endIndex));
            }

            executor.shutdown();
            try {
                while (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                    System.out.println("An hour has passed, woohoo!");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } catch (URISyntaxException e) {
            System.out.println(e);
        }
    }

    private static void updateEntities(EntityWriter entityWriter, EntityWriter.PopulationArea populationArea, int startIndex, int endIndex) {
        try {
            while (true) {
                for (int i = startIndex; i < endIndex; i++) {
                    System.out.println("entity number " + i);
                    String entityId = "entity" + i;
                    entityWriter.writeRandomEntity(entityId, populationArea, null);
                }
                System.out.println("finished a loop");
                Thread.sleep(50);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
