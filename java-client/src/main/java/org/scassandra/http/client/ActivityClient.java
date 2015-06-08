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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.scassandra.cql.CqlType;
import org.scassandra.http.client.types.GsonCqlTypeDeserialiser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Responsible for retrieving and clearing interactions with the Scassandra server. Including
 * - Queries
 * - Prepared statements
 * - Connections
 */
public class ActivityClient {

    public static final String REQUEST_FOR_QUERIES_FAILED = "Request for queries failed";
    public static final String REQUEST_FOR_CONNECTIONS_FAILED = "Request for connections failed";
    public static final String REQUEST_FAILED = "Request failed";

    public static class ActivityClientBuilder {

        private String host = "localhost";
        private int port = 8043;

        private ActivityClientBuilder() {
        }

        public ActivityClientBuilder withHost(String host) {
            this.host = host;
            return this;
        }

        public ActivityClientBuilder withPort(int port) {
            this.port = port;
            return this;
        }

        public ActivityClient build() {
            return new ActivityClient(this.host, this.port);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ActivityClient.class);

    public static ActivityClientBuilder builder() {
        return new ActivityClientBuilder();
    }

    private Gson gson = new GsonBuilder()
            .registerTypeAdapter(CqlType.class, new GsonCqlTypeDeserialiser())
            .create();

    private CloseableHttpClient httpClient = HttpClients.createDefault();
    private final String connectionUrl;
    private final String queryUrl;
    private final String preparedStatementExecutionUrl;

    private ActivityClient(String host, int port) {
        RequestConfig.Builder requestBuilder = RequestConfig.custom();
        requestBuilder = requestBuilder.setConnectTimeout(500);
        requestBuilder = requestBuilder.setConnectionRequestTimeout(500);
        requestBuilder = requestBuilder.setSocketTimeout(500);
        HttpClientBuilder builder = HttpClientBuilder.create();
        builder.setDefaultRequestConfig(requestBuilder.build());
        httpClient = builder.build();
        this.connectionUrl = "http://" + host + ":" + port + "/connection";
        this.queryUrl = "http://" + host + ":" + port + "/query";
        this.preparedStatementExecutionUrl = "http://" + host + ":" + port + "/prepared-statement-execution";
    }

    /**
     * Retrieves all the queries that have been sent to the configured Scassandra server.
     *
     * @return A List of Query objects
     */
    public List<Query> retrieveQueries() {
        HttpGet get = new HttpGet(queryUrl);
        try {
            CloseableHttpResponse response = httpClient.execute(get);
            String body = EntityUtils.toString(response.getEntity());
            LOGGER.debug("Received response {}", body);
            Query[] queries = (Query[]) gson.fromJson(body, (Class) Query[].class);
            LOGGER.debug("Parsed queries {}", Arrays.toString(queries));
            return Arrays.asList(queries);
        } catch (IOException e) {
            LOGGER.info(REQUEST_FOR_QUERIES_FAILED, e);
            throw new ActivityRequestFailed(REQUEST_FOR_QUERIES_FAILED, e);
        }
    }

    /**
     * Retrieves all the connections that have been sent to the configured Scassandra server.
     *
     * @return A List of Connection objects
     */
    public List<Connection> retrieveConnections() {
        HttpGet get = new HttpGet(connectionUrl);

        try {
            CloseableHttpResponse response = httpClient.execute(get);
            String body = EntityUtils.toString(response.getEntity());
            LOGGER.debug("Received response {}", body);
            Connection[] queries = (Connection[]) gson.fromJson(body, (Class) Connection[].class);
            LOGGER.debug("Parsed connections {}", Arrays.toString(queries));
            return Arrays.asList(queries);
        } catch (IOException e) {
            LOGGER.info(REQUEST_FOR_CONNECTIONS_FAILED, e);
            throw new ActivityRequestFailed(REQUEST_FOR_CONNECTIONS_FAILED, e);
        }
    }

    /**
     * Deletes all the recorded connections from the configured Scassandra server.
     */
    public void clearConnections() {
        httpDelete(connectionUrl, "Clearing of connections failed");
    }

    /**
     * Deletes all the recorded queries from the configured Scassandra server.
     */
    public void clearQueries() {
        httpDelete(queryUrl, "Clearing of queries failed");
    }

    /**
     * Deletes all the recorded prepared statement executions from the configured Scassandra server.
     */
    public void clearPreparedStatementExecutions() {
        httpDelete(preparedStatementExecutionUrl, "Clearing of prepared statement executions failed");
    }

    /**
     * Deletes the recorded prepared statement executions, recorded queries and recorded connections.
     */
    public void clearAllRecordedActivity() {
        clearConnections();
        clearQueries();
        clearPreparedStatementExecutions();
    }

    /**
     * Retrieves the recorded prepared statement executions. Note this the executions, not the prepare
     * calls your applications makes.
     *
     * If you haven't primed the prepared statement then the variable types will be empty.
     *
     * @return PreparedStatementExecution
     */
    public List<PreparedStatementExecution> retrievePreparedStatementExecutions() {
        HttpGet get = new HttpGet(preparedStatementExecutionUrl);
        try {
            CloseableHttpResponse response = httpClient.execute(get);
            String body = EntityUtils.toString(response.getEntity());
            LOGGER.debug("Received response {}", body);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                String errorMessage = String.format("Non 200 status code when retrieving prepared statement executions %s", statusCode);
                LOGGER.info(errorMessage);
                throw new ActivityRequestFailed(errorMessage);
            }
            PreparedStatementExecution[] executions = (PreparedStatementExecution[]) gson.fromJson(body, (Class) PreparedStatementExecution[].class);
            LOGGER.debug("Parsed prepared statement executions {}", Arrays.toString(executions));
            return Arrays.asList(executions);
        } catch (IOException e) {
            LOGGER.info(REQUEST_FAILED, e);
            throw new ActivityRequestFailed(REQUEST_FAILED, e);
        }
    }

    private void httpDelete(String url, String warningMessage) {
        HttpDelete delete = new HttpDelete(url);
        try {
            CloseableHttpResponse httpResponse = httpClient.execute(delete);
            EntityUtils.consumeQuietly(httpResponse.getEntity());
        } catch (IOException e) {
            LOGGER.warn(warningMessage, e);
            throw new ActivityRequestFailed(warningMessage, e);
        }
    }


}
