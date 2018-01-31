package org.z.generator;

import io.deepstream.DeepstreamClient;
import io.redisearch.Schema;
import io.redisearch.client.Client;
import org.z.common.EntityWriter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisDataException;

import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) {
        String redisHost = args[0];
        String deepstreamHost = args[1];
        int totalEntityAmount = Integer.parseInt(args[2]);
        int updateAmountPerSecond = Integer.parseInt(args[3]);

        int initializationThreads = 10;

        try {
            JedisPool jedisPool = createJedisPool(redisHost, initializationThreads);
            DeepstreamClient deepstream = new DeepstreamClient(deepstreamHost + ":6020");
            deepstream.login();
            addShutdownHook(deepstream, jedisPool);

            EntityWriter.PopulationArea populationArea = new EntityWriter.PopulationArea(30, 40, 30, 40);
            initializeEntities(jedisPool, deepstream, totalEntityAmount, initializationThreads, populationArea);
            System.out.println("entities initialized");
            updateRandomEntities(jedisPool, deepstream, populationArea, totalEntityAmount, updateAmountPerSecond);
        } catch (URISyntaxException e) {
            System.out.println(e);
        }
    }

    private static JedisPool createJedisPool(String redisHost, int connectionsAmount) {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(connectionsAmount);
        jedisPoolConfig.setMaxIdle(connectionsAmount);
        return new JedisPool(jedisPoolConfig, redisHost);
    }

    private static void initializeEntities(JedisPool jedisPool, DeepstreamClient deepstream, int totalEntityAmount,
                                           int workersAmount, EntityWriter.PopulationArea populationArea) {
        ExecutorService executor = Executors.newFixedThreadPool(workersAmount);
        for (int i = 0; i < workersAmount; i++) {
            int startIndex = (totalEntityAmount / workersAmount) * i;
            int amount = (totalEntityAmount / workersAmount);
            executor.submit(() -> writeSubsetOfEntities(jedisPool, deepstream, populationArea, startIndex, amount));
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
                throw new RuntimeException("Couldn't initialize entities in 5 minutes");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void updateRandomEntities(JedisPool jedisPool, DeepstreamClient deepstream,
                                       EntityWriter.PopulationArea populationArea, int totalEntityAmount, int updateAmountPerSecond) {
        int updateInterval = 50;
        int updatesPerSecond = 1000 / updateInterval;
        int updateAmountPerInterval = updateAmountPerSecond / updatesPerSecond;
        ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);
        Runnable writingTask = () -> writeRandomSubsetOfEntities(jedisPool, deepstream, populationArea, totalEntityAmount, updateAmountPerInterval);
        scheduledExecutor.scheduleAtFixedRate(writingTask, 0, updateInterval, TimeUnit.MILLISECONDS);

        try {
            Thread.sleep(1000 * 60 * 60 * 24 * 10); // ten days
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void writeRandomSubsetOfEntities(JedisPool jedisPool, DeepstreamClient deepstream,
                                                    EntityWriter.PopulationArea populationArea,
                                                    int totalEntityAmount, int subsetSize) {
        int startIndex = ThreadLocalRandom.current().nextInt(totalEntityAmount - subsetSize);
        System.out.println("writing random subset of size " + subsetSize + " starting at " + startIndex);
        writeSubsetOfEntities(jedisPool, deepstream, populationArea, startIndex, subsetSize);
    }

    private static void writeSubsetOfEntities(JedisPool jedisPool, DeepstreamClient deepstream,
                                              EntityWriter.PopulationArea populationArea, int startIndex, int amount) {
        try(Jedis jedis = jedisPool.getResource()) {
            EntityWriter entityWriter = new EntityWriter(jedis, deepstream);
            for (int i = startIndex; i < startIndex + amount; i++) {
                String entityId = "entity" + i;
                entityWriter.writeRandomEntity(entityId, populationArea, null);
            }
        }
    }

    private static void updateEntities(EntityWriter entityWriter, EntityWriter.PopulationArea populationArea, int startIndex, int endIndex) {
        try {
            boolean first = true;
            while (true) {
                Instant start = Instant.now();
                for (int i = startIndex; i < endIndex; i++) {
                    String entityId = "entity" + i;
                    entityWriter.writeRandomEntity(entityId, populationArea, null);
                }
                System.out.println("loop delta = " + Duration.between(start, Instant.now()).toMillis());
                System.out.println("finished a loop");
                if(first) {
                    System.out.println("First loop finished for range " + startIndex + " - " + endIndex);
                    first = false;
                }
                Thread.sleep(50);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void addShutdownHook(DeepstreamClient deepstream, JedisPool jedisPool) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            deepstream.close();
            jedisPool.close();
        }));
    }

}
