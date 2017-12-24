package org.z.distributer.util;

import com.google.gson.JsonObject;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

public class GsonJsonMessageCodec implements MessageCodec<JsonObject, io.vertx.core.json.JsonObject> {
    @Override
    public void encodeToWire(Buffer buffer, JsonObject jsonObject) {
        // Encode object to string
        String jsonToStr = jsonObject.toString();

        // Length of JSON: is NOT characters count
        int length = jsonToStr.getBytes().length;

        // Write data into given buffer
        buffer.appendInt(length);
        buffer.appendString(jsonToStr);
    }

    @Override
    public io.vertx.core.json.JsonObject decodeFromWire(int pos, Buffer buffer) {
        // My custom message starting from this *position* of buffer
        int _pos = pos;

        // Length of JSON
        int length = buffer.getInt(_pos);

        // Get JSON string by it`s length
        // Jump 4 because getInt() == 4 bytes
        String jsonStr = buffer.getString(_pos+=4, _pos+=length);

        // We can finally create custom message object
        return new io.vertx.core.json.JsonObject(jsonStr);
    }

    @Override
    public io.vertx.core.json.JsonObject transform(JsonObject jsonObject) {
        return new io.vertx.core.json.JsonObject(jsonObject.toString());
    }

    @Override
    public String name() {
        return "class" + JsonObject.class.getCanonicalName();
    }

    @Override
    public byte systemCodecID() {
        return -1;
    }
}
