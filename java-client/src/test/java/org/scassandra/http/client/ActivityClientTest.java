/*
 * Copyright (C) 2014 Christopher Batey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra.http.client;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.Assert.assertEquals;
import static org.scassandra.http.client.ActivityClient.*;

import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.scassandra.cql.PrimitiveType.*;
import java.util.Arrays;
import java.util.List;

public class ActivityClientTest {
    private static final int PORT = 1235;
    private final String preparedStatementExecutionUrl = "/prepared-statement-execution";
    private final String queryUrl = "/query";
    private final String connectionUrl = "/connection";

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(PORT);

    private ActivityClient underTest;

    @Before
    public void setup() {
        ActivityClientBuilder builder = ActivityClient.builder();
        underTest = builder.withHost("localhost").withPort(PORT).build();
    }

    @Test
    public void testRetrievalOfZeroQueries() {
        //given
        stubFor(get(urlEqualTo(queryUrl)).willReturn(aResponse().withBody("[]")));
        //when
        List<Query> queries = underTest.retrieveQueries();
        //then
        assertEquals(0, queries.size());
    }

    @Test
    public void testRetrievalOfASingleQuery() {
        //given
        stubFor(get(urlEqualTo(queryUrl)).willReturn(aResponse().withBody("[{\"query\":\"select * from people\",\"consistency\":\"TWO\"}]")));
        //when
        List<Query> queries = underTest.retrieveQueries();
        //then
        assertEquals(1, queries.size());
        assertEquals("select * from people", queries.get(0).getQuery());
        assertEquals("TWO", queries.get(0).getConsistency());
    }

    @Test(expected = ActivityRequestFailed.class)
    public void testErrorDuringQueryRetrieval() {
        //given
        stubFor(get(urlEqualTo(queryUrl))
                .willReturn(aResponse().withFault(Fault.RANDOM_DATA_THEN_CLOSE)));
        //when
        underTest.retrieveQueries();
        //then

    }

    @Test
    public void testRetrievalOfZeroConnections() {
        //given
        stubFor(get(urlEqualTo(connectionUrl)).willReturn(aResponse().withBody("[]")));
        //when
        List<Connection> connections = underTest.retrieveConnections();
        //then
        assertEquals(0, connections.size());
    }

    @Test
    public void testRetrievalOfOnePlusConnections() {
        //given
        stubFor(get(urlEqualTo(connectionUrl)).willReturn(aResponse().withBody("[{\"result\":\"success\"}]")));
        //when
        List<Connection> connections = underTest.retrieveConnections();
        //then
        assertEquals(1, connections.size());
        assertEquals("success", connections.get(0).getResult());
    }

    @Test(expected = ActivityRequestFailed.class)
    public void testErrorDuringConnectionRetrieval() {
        //given
        stubFor(get(urlEqualTo(connectionUrl))
                .willReturn(aResponse().withFault(Fault.RANDOM_DATA_THEN_CLOSE)));
        //when
        underTest.retrieveConnections();
        //then
    }

    @Test(expected = ActivityRequestFailed.class, timeout = 2500)
    public void testServerHanging() {
        //given
        stubFor(get(urlEqualTo(connectionUrl))
                .willReturn(aResponse().withFixedDelay(5000)));
        //when
        underTest.retrieveConnections();
        //then
    }

    @Test
    public void testDeletingOfConnectionHistory() {
        //given
        //when
        underTest.clearConnections();
        //then
        verify(deleteRequestedFor(urlEqualTo(connectionUrl)));
    }

    @Test(expected = ActivityRequestFailed.class)
    public void testDeletingOfConnectionHistoryFailing() {
        //given
        stubFor(delete(urlEqualTo(connectionUrl))
                .willReturn(aResponse().withFault(Fault.RANDOM_DATA_THEN_CLOSE)));
        //when
        underTest.clearConnections();
        //then

    }

    @Test
    public void testDeletingOfQueryHistory() {
        //given
        //when
        underTest.clearQueries();
        //then
        verify(deleteRequestedFor(urlEqualTo(queryUrl)));
    }

    @Test(expected = ActivityRequestFailed.class)
    public void testDeletingOfQueryHistoryFailing() {
        //given
        stubFor(delete(urlEqualTo(queryUrl))
                .willReturn(aResponse().withFault(Fault.RANDOM_DATA_THEN_CLOSE)));
        //when
        underTest.clearQueries();
        //then

    }

    @Test
    public void retrievingPreparedStatementExecutions() throws Exception {
        //given
        stubFor(get(urlEqualTo(preparedStatementExecutionUrl))
                .willReturn(aResponse().withStatus(200).withBody(
                "[{\n" +
                        "  \"preparedStatementText\": \"select * from people where name = ?\",\n" +
                        "  \"consistency\": \"ONE\",\n" +
                        "  \"variables\": [\"Chris\"],\n" +
                        "  \"variableTypes\": [\"text\"]\n" +
                        "}]"
                )));
        //when
        List<PreparedStatementExecution> executions = underTest.retrievePreparedStatementExecutions();
        //then
        assertEquals(1, executions.size());
        PreparedStatementExecution singleExecution = executions.get(0);
        assertEquals("ONE", singleExecution.getConsistency());
        assertEquals("select * from people where name = ?", singleExecution.getPreparedStatementText());
        assertEquals(Arrays.asList("Chris"), singleExecution.getVariables());
        assertEquals(Arrays.asList(TEXT), singleExecution.getVariableTypes());
    }

    @Test(expected = ActivityRequestFailed.class)
    public void retrievingPreparedStatementExecutionsFailure() throws Exception {
        //given
        stubFor(get(urlEqualTo(preparedStatementExecutionUrl))
                .willReturn(aResponse().withStatus(200).withFault(Fault.RANDOM_DATA_THEN_CLOSE)));
        //when
        underTest.retrievePreparedStatementExecutions();
        //then
    }

    @Test(expected = ActivityRequestFailed.class)
    public void retrievingPreparedStatementExecutionsNot200() throws Exception {
        //given
        stubFor(get(urlEqualTo(preparedStatementExecutionUrl))
                .willReturn(aResponse().withStatus(500)));
        //when
        underTest.retrievePreparedStatementExecutions();
        //then
    }

    @Test
    public void testDeletingOfPreparedExecutionHistory() {
        //given
        stubFor(delete(urlEqualTo(preparedStatementExecutionUrl))
                .willReturn(aResponse().withStatus(200)));
        //when
        underTest.clearPreparedStatementExecutions();
        //then
        verify(deleteRequestedFor(urlEqualTo(preparedStatementExecutionUrl)));
    }

    @Test(expected = ActivityRequestFailed.class)
    public void testDeletingOfPreparedExecutionHistoryFailure() {
        //given
        stubFor(delete(urlEqualTo(preparedStatementExecutionUrl))
                .willReturn(aResponse().withStatus(200).withFault(Fault.RANDOM_DATA_THEN_CLOSE)));
        //when
        underTest.clearPreparedStatementExecutions();
        //then
    }

    @Test
    public void testClearAllActivityHistory() {
        //given
        stubFor(delete(urlEqualTo(preparedStatementExecutionUrl))
                .willReturn(aResponse().withStatus(200)));
        stubFor(delete(urlEqualTo(queryUrl))
                .willReturn(aResponse().withStatus(200)));
        stubFor(delete(urlEqualTo(connectionUrl))
                .willReturn(aResponse().withStatus(200)));

        //when
        underTest.clearAllRecordedActivity();
        //then
        verify(deleteRequestedFor(urlEqualTo(preparedStatementExecutionUrl)));
        verify(deleteRequestedFor(urlEqualTo(queryUrl)));
        verify(deleteRequestedFor(urlEqualTo(connectionUrl)));
    }
}
