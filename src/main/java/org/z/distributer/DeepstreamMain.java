package org.z.distributer;

import io.deepstream.DeepstreamClient;
import redis.clients.jedis.Jedis;

import java.net.URISyntaxException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class DeepstreamMain {
    public static void main(String[] args) {
        String deepstreamHost = args[0];
        String redisHost = args[1];
        try {
            Jedis jedis = new Jedis(redisHost);
            DeepstreamClient deepstream = new DeepstreamClient(deepstreamHost + ":6020");
            deepstream.login();

            ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(20);
            ClientStateListener listener = new ClientStateListener(scheduledExecutor, deepstream, jedis);
            deepstream.record.listen("entities_around/.*", listener);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
