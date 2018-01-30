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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RequestHandler implements HttpHandler {
    private EntityWriter entityWriter;
    private JsonParser jsonParser;

    private Map<EntityWriter.PopulationArea, List<String>> identifierByArea;

    public RequestHandler(EntityWriter entityWriter, JsonParser jsonParser) {
        this.entityWriter = entityWriter;
        this.jsonParser = jsonParser;

        this.identifierByArea = new HashMap<>();
    }

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

        List<String> identifiers = identifiersFor(populationArea, seedData.get("entitiesAmount").getAsInt());
        for (String entityId : identifiers) {
            entityWriter.writeRandomEntity(entityId, populationArea, triggerTime);
        }

        String response = "Ok";
        httpExchange.sendResponseHeaders(200, response.length());
        OutputStream os = httpExchange.getResponseBody();
        os.write(response.getBytes());
        os.close();
    }

    private List<String> identifiersFor(EntityWriter.PopulationArea populationArea, int amount) {
        return identifierByArea.computeIfAbsent(populationArea,
                (area) -> randomIdentifiers(amount));  // don't care about area
    }

    private List<String> randomIdentifiers(int amount) {
        return Stream.generate(UUID::randomUUID)
                .map(UUID::toString)
                .limit(amount)
                .collect(Collectors.toList());
    }
}
