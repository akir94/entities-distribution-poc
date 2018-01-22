package org.z.trigger;

import com.google.gson.JsonObject;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    public static final MediaType JSON
            = MediaType.parse("application/json; charset=utf-8");

    public static void main(String[] args) {
        OkHttpClient client = new OkHttpClient();
        String url = args[1];
        JsonObject requestData = new JsonObject();
        requestData.addProperty("entitiesAmount", Integer.parseInt(args[2]));
        requestData.addProperty("minLongitude", Double.parseDouble(args[3]));
        requestData.addProperty("maxLongitude", Double.parseDouble(args[4]));
        requestData.addProperty("minLatitude", Double.parseDouble(args[5]));
        requestData.addProperty("maxLatitude", Double.parseDouble(args[6]));

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(() -> worker(requestData, client, url), 1, 1, TimeUnit.SECONDS);
        executorService.shutdown();
        try {
            while (!executorService.awaitTermination(1, TimeUnit.DAYS)){
                System.out.println("A day has passed, woohoo!");
            }
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void worker(JsonObject requestData, OkHttpClient client, String url) {
        requestData.addProperty("triggerTime", Instant.now().toString());
        try {
            post(client, url, requestData.toString());
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    private static void post(OkHttpClient client, String url, String json) throws IOException {
        RequestBody body = RequestBody.create(JSON, json);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
        Response response = client.newCall(request).execute();
        System.out.println("response: " + response);
    }
}
