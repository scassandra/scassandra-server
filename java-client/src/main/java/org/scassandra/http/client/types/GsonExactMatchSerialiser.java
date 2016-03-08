package org.scassandra.http.client.types;

import com.google.gson.*;
import org.scassandra.http.client.MultiPrimeRequest;

import java.lang.reflect.Type;

public class GsonExactMatchSerialiser implements JsonSerializer<MultiPrimeRequest.ExactMatch> {
    @Override
    public JsonElement serialize(MultiPrimeRequest.ExactMatch src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("matcher", context.serialize(src.matcher()));
        jsonObject.addProperty("type", src.type().toString());
        return jsonObject;
    }
}
