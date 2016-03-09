package org.scassandra.http.client.types;

import com.google.gson.*;
import org.scassandra.http.client.MultiPrimeRequest;

import java.lang.reflect.Type;

public class GsonVariableMatchDeserialiser implements JsonDeserializer<MultiPrimeRequest.VariableMatch> {
    @Override
    public MultiPrimeRequest.VariableMatch deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        JsonObject jsonObject = (JsonObject) json;

        String type = jsonObject.get("type").getAsString();

        if (type.equalsIgnoreCase("any")) {
            return new MultiPrimeRequest.AnyMatch();
        } else if (type.equalsIgnoreCase("exact")) {
            String matcher = jsonObject.get("matcher").getAsString();
            return new MultiPrimeRequest.ExactMatch(matcher);
        } else {
            throw new JsonParseException("Unexpected variable matcher type received: " + type);
        }
    }
}
