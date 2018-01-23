package org.z.seeder;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.z.common.EntityWriter;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.time.Instant;
import java.util.UUID;

public class RequestHandler implements HttpHandler {
    private EntityWriter entityWriter;
    private JsonParser jsonParser;

    public RequestHandler(EntityWriter entityWriter, JsonParser jsonParser) {
        this.entityWriter = entityWriter;
        this.jsonParser = jsonParser;
    }

//    @Override
//    public void handle(Context ctx) throws Exception {
//        System.out.println("got request with body: " + ctx.body());
//        JsonObject seedData = jsonParser.parse(ctx.body()).getAsJsonObject();
//        EntityWriter.PopulationArea populationArea = new EntityWriter.PopulationArea(
//                seedData.get("minLongitude").getAsDouble(),
//                seedData.get("maxLongitude").getAsDouble(),
//                seedData.get("minLatitude").getAsDouble(),
//                seedData.get("maxLatitude").getAsDouble());
//        Instant triggerTime = Instant.parse(seedData.get("triggerTime").getAsString());
//
//        for (int i = 0; i < 10; i++) {
//            String entityId = UUID.randomUUID().toString();
//            entityWriter.writeRandomEntity(entityId, populationArea, triggerTime);
//        }
//        ctx.result("ok");
//    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        JsonObject seedData = jsonParser.parse(new InputStreamReader(httpExchange.getRequestBody())).getAsJsonObject();
        System.out.println("got request with body: " + seedData);
        EntityWriter.PopulationArea populationArea = new EntityWriter.PopulationArea(
                seedData.get("minLongitude").getAsDouble(),
                seedData.get("maxLongitude").getAsDouble(),
                seedData.get("minLatitude").getAsDouble(),
                seedData.get("maxLatitude").getAsDouble());
        Instant triggerTime = Instant.parse(seedData.get("triggerTime").getAsString());

        for (int i = 0; i < 10; i++) {
            String entityId = UUID.randomUUID().toString();
            entityWriter.writeRandomEntity(entityId, populationArea, triggerTime);
        }

        String response = "This is the response";
        httpExchange.sendResponseHeaders(200, response.length());
        OutputStream os = httpExchange.getResponseBody();
        os.write(response.getBytes());
        os.close();
    }
}
