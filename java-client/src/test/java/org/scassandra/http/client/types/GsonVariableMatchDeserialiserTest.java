package org.scassandra.http.client.types;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import org.junit.Test;
import org.scassandra.http.client.MultiPrimeRequest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GsonVariableMatchDeserialiserTest {

    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(MultiPrimeRequest.VariableMatch.class, new GsonVariableMatchDeserialiser())
            .create();

    @Test
    public void deserialisesAnyMatchers() throws Exception {
        //given
        String json = "{\"variableMatch\": {\"type\": \"any\"}}";

        //when
        Response response = gson.fromJson(json, Response.class);

        //then
        MultiPrimeRequest.AnyMatch expectedVariableMatcher = new MultiPrimeRequest.AnyMatch();
        assertEquals(expectedVariableMatcher.getClass(), response.getVariableMatch().getClass());
    }

    @Test
    public void deserialisesStringExactMatchers() throws Exception {
        //given
        String json = "{\"variableMatch\": {\"type\": \"exact\", \"matcher\": \"Adam\"}}";

        //when
        Response response = gson.fromJson(json, Response.class);

        //then
        MultiPrimeRequest.ExactMatch expectedVariableMatcher = new MultiPrimeRequest.ExactMatch("Adam");
        assertEquals(expectedVariableMatcher, response.getVariableMatch());
    }

    @Test
    public void throwsJsonParseExceptionGivenUnknownType() throws Exception {
        //given
        String json = "{\"variableMatch\": {\"type\": \"unknown\"}}";

        //when
        try {
            gson.fromJson(json, Response.class);
            fail("Expected JsonParseException");
        } catch (JsonParseException jpe) {
            assertEquals("Unexpected variable matcher type received: unknown", jpe.getMessage());
        }
    }

    private static class Response {
        private final MultiPrimeRequest.VariableMatch variableMatch;

        private Response(MultiPrimeRequest.VariableMatch variableMatch) {
            this.variableMatch = variableMatch;
        }

        public MultiPrimeRequest.VariableMatch getVariableMatch() {
            return variableMatch;
        }
    }
}