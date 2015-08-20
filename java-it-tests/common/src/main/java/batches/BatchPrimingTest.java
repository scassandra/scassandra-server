package batches;/*
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

import common.*;
import org.junit.Test;
import org.scassandra.http.client.*;

import java.util.Collections;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.*;
import static org.scassandra.cql.PrimitiveType.*;
import static org.scassandra.http.client.BatchQueryKind.prepared_statement;
import static org.scassandra.http.client.BatchQueryKind.query;
import static org.scassandra.http.client.BatchQueryPrime.batchQueryPrime;
import static org.scassandra.http.client.PrimingRequest.Result.read_request_timeout;
import static org.scassandra.http.client.PrimingRequest.Result.success;
import static org.scassandra.http.client.PrimingRequest.then;

abstract public class BatchPrimingTest extends AbstractScassandraTest {

    public BatchPrimingTest(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void executeLoggedBatch() {
        CassandraResult result = cassandra().executeBatch(newArrayList(
                        new CassandraQuery("select * from blah"),
                        new CassandraQuery("select * from blah2")
                ), BatchType.UNLOGGED
        );

        assertEquals(PrimingRequest.Result.success, result.status().getResult());
        assertEquals(Collections.emptyList(), result.rows());
    }

    @Test
    public void primeBatchWithPreparedStatement() {
        // prime the prepared statements
        primingClient.prime(PrimingRequest.preparedStatementBuilder()
                        .withQuery("insert ? ?")
                        .withThen(then().withVariableTypes(ASCII, INT))
        );
        primingClient.primeBatch(
                BatchPrimingRequest.batchPrimingRequest()
                        .withQueries(
                                batchQueryPrime("insert something else", query),
                                batchQueryPrime("insert ? ?", prepared_statement))
                        .withThen(then().withResult(read_request_timeout))

        );

        CassandraResult result = cassandra().executeBatch(newArrayList(
                        new CassandraQuery("insert something else"),
                        new CassandraQuery("insert ? ?",
                                CassandraQuery.QueryType.PREPARED_STATEMENT, "one", 2)
                ), BatchType.LOGGED
        );

        assertEquals(read_request_timeout, result.status().getResult());
    }

    @Test
    public void capturesPreparedStatementVariables() {
        // prime the prepared statements
        primingClient.prime(PrimingRequest.preparedStatementBuilder()
                        .withQuery("insert ? ?")
                        .withThen(then().withVariableTypes(ASCII, INT))
        );
        primingClient.primeBatch(
                BatchPrimingRequest.batchPrimingRequest()
                        .withQueries(
                                batchQueryPrime("insert ? ?", prepared_statement))
                        .withThen(then().withResult(success))

        );

        cassandra().executeBatch(newArrayList(
                        new CassandraQuery("insert ? ?",
                                CassandraQuery.QueryType.PREPARED_STATEMENT, "one", 2)
                ), BatchType.LOGGED
        );

        List<BatchExecution> recordedBatchExecutions = activityClient.retrieveBatches();
        assertEquals(1, recordedBatchExecutions.size());
        List<BatchQuery> queries = recordedBatchExecutions.get(0).getBatchQueries();
        assertEquals(newArrayList("one", 2.0), queries.get(0).getVariables());
    }

    @Test
    public void primeBatchWithReadTimeout() {

        primingClient.primeBatch(BatchPrimingRequest.batchPrimingRequest()
                .withThen(BatchPrimingRequest
                        .then()
                        .withResult(read_request_timeout)
                        .build())
                .withQueries(
                        batchQueryPrime("select * from blah", BatchQueryKind.query))
                .build());

        CassandraResult result = cassandra().executeBatch(newArrayList(
                        new CassandraQuery("select * from blah")
                ), BatchType.UNLOGGED
        );

        assertEquals(read_request_timeout, result.status().getResult());
    }

    @Test
    public void primeBatchWithWriteTimeout() {
        primingClient.primeBatch(BatchPrimingRequest.batchPrimingRequest()
                .withThen(BatchPrimingRequest
                        .then()
                        .withResult(PrimingRequest.Result.write_request_timeout)
                        .build())
                .withQueries(
                        batchQueryPrime("select * from blah", BatchQueryKind.query))
                .build());

        CassandraResult result = cassandra().executeBatch(newArrayList(
                        new CassandraQuery("select * from blah")
                ), BatchType.UNLOGGED
        );

        assertEquals(PrimingRequest.Result.write_request_timeout, result.status().getResult());
    }

    @Test
    public void primeBatchWithUnavailable() {
        primingClient.primeBatch(BatchPrimingRequest.batchPrimingRequest()
                .withThen(BatchPrimingRequest.then()
                        .withResult(PrimingRequest.Result.unavailable)
                        .build())
                .withQueries(
                        batchQueryPrime("select * from blah", BatchQueryKind.query))
                .build());

        CassandraResult result = cassandra().executeBatch(newArrayList(
                        new CassandraQuery("select * from blah")
                ), BatchType.UNLOGGED
        );

        assertEquals(PrimingRequest.Result.unavailable, result.status().getResult());
    }
}
