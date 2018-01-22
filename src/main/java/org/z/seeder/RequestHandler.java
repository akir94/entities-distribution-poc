package org.z.seeder;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.javalin.Context;
import io.javalin.Handler;
import org.z.common.EntityWriter;

import java.time.Instant;
import java.util.UUID;

public class RequestHandler implements Handler{
    private EntityWriter entityWriter;
    private JsonParser jsonParser;

    public RequestHandler(EntityWriter entityWriter, JsonParser jsonParser) {
        this.entityWriter = entityWriter;
        this.jsonParser = jsonParser;
    }

    @Override
    public void handle(Context ctx) throws Exception {
        System.out.println("got request with body: " + ctx.body());
        JsonObject seedData = jsonParser.parse(ctx.body()).getAsJsonObject();
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
        ctx.result("ok");
    }
}
