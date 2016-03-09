package org.scassandra.http.client.types;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Test;
import org.scassandra.http.client.MultiPrimeRequest;

import static org.junit.Assert.assertEquals;

public class GsonExactMatchSerialiserTest {

    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(MultiPrimeRequest.VariableMatch.class, new GsonVariableMatchDeserialiser())
            .registerTypeAdapter(MultiPrimeRequest.ExactMatch.class, new GsonExactMatchSerialiser())
            .enableComplexMapKeySerialization()
            .create();

    @Test
    public void serialisesExactMatchers() throws Exception {
        //given
        MultiPrimeRequest.ExactMatch exactMatch = new MultiPrimeRequest.ExactMatch("hello");

        //when
        String json = gson.toJson(new Request(exactMatch));

        //then
        assertEquals("{\"variableMatch\":{\"matcher\":\"hello\",\"type\":\"exact\"}}", json);
    }

    private class Request {
        private final MultiPrimeRequest.VariableMatch variableMatch;

        private Request(MultiPrimeRequest.VariableMatch variableMatch) {
            this.variableMatch = variableMatch;
        }
    }
}