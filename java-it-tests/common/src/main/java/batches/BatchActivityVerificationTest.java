package batches;

import com.google.common.collect.Lists;
import common.AbstractScassandraTest;
import common.CassandraExecutor;
import common.CassandraQuery;
import org.junit.Test;
import org.scassandra.http.client.BatchExecution;
import org.scassandra.http.client.BatchQuery;
import org.scassandra.http.client.BatchType;

import java.util.List;

import static org.junit.Assert.assertEquals;

abstract public class BatchActivityVerificationTest extends AbstractScassandraTest {
    public BatchActivityVerificationTest(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void executeUnLoggedBatch() {
        cassandra().executeBatch(Lists.newArrayList(
                        new CassandraQuery("select * from blah"),
                        new CassandraQuery("select * from blah2")
                ), BatchType.UNLOGGED
        );

        List<BatchExecution> batches = activityClient.retrieveBatches();

        assertEquals(1, batches.size());
        assertEquals(BatchExecution.builder().withBatchQueries(
                BatchQuery.builder().withQuery("select * from blah").build(),
                BatchQuery.builder().withQuery("select * from blah2").build())
                .withConsistency( "ONE")
                .withBatchType(BatchType.UNLOGGED).build(), batches.get(0));
    }

    @Test
    public void executeLoggedBatch() {
        cassandra().executeBatch(Lists.newArrayList(
                        new CassandraQuery("select * from blah"),
                        new CassandraQuery("select * from blah2")
                ), BatchType.LOGGED
        );

        List<BatchExecution> batches = activityClient.retrieveBatches();

        assertEquals(1, batches.size());
        assertEquals(BatchExecution.builder().withBatchQueries(
                BatchQuery.builder().withQuery("select * from blah").build(),
                BatchQuery.builder().withQuery("select * from blah2").build())
                .withConsistency("ONE").withBatchType(BatchType.LOGGED).build(), batches.get(0));
    }

    @Test
    public void batchWithQueryParametersUnPrimed() {
        cassandra().executeBatch(Lists.newArrayList(
                        new CassandraQuery("select * from blah where blah = ? and wah = ?", "Hello", 1)
                ), BatchType.LOGGED
        );

        List<BatchExecution> batches = activityClient.retrieveBatches();

        assertEquals(1, batches.size());
        assertEquals(BatchExecution.builder().withBatchQueries(
                BatchQuery.builder().withQuery("select * from blah where blah = ? and wah = ?").build())
                .withConsistency("ONE").withBatchType(BatchType.LOGGED).build(), batches.get(0));
    }

    @Test
    public void clearAllRecordedActivity() {
        cassandra().executeBatch(Lists.newArrayList(
                        new CassandraQuery("select * from blah"),
                        new CassandraQuery("select * from blah2")
                ), BatchType.UNLOGGED
        );

        activityClient.clearAllRecordedActivity();
        List<BatchExecution> batches = activityClient.retrieveBatches();

        assertEquals(0, batches.size());
    }

    @Test
    public void clearJustBatchExecution() {
        cassandra().executeBatch(Lists.newArrayList(
                        new CassandraQuery("select * from blah"),
                        new CassandraQuery("select * from blah2")
                ), BatchType.UNLOGGED
        );

        activityClient.clearBatchExecutions();
        List<BatchExecution> batches = activityClient.retrieveBatches();

        assertEquals(0, batches.size());
    }

    @Test
    public void preparedStatementsInBatches() {
        cassandra().executeBatch(Lists.newArrayList(
                        new CassandraQuery("query", "Hello"),
                        new CassandraQuery("prepared statement ? ?",
                                CassandraQuery.QueryType.PREPARED_STATEMENT, "one", "twp")
                ), BatchType.LOGGED
        );

        List<BatchExecution> batches = activityClient.retrieveBatches();

        assertEquals(1, batches.size());
        assertEquals(BatchExecution.builder().withBatchQueries(
                BatchQuery.builder().withQuery("query").build(),
                BatchQuery.builder().withQuery("prepared statement ? ?").withType(BatchQuery.BatchQueryKind.prepared_statement).build())
                .withConsistency("ONE").withBatchType(BatchType.LOGGED).build(), batches.get(0));
    }
}
