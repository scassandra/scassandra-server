package batches;

import com.google.common.collect.Lists;
import common.AbstractScassandraTest;
import common.CassandraExecutor;
import common.CassandraQuery;
import org.junit.Test;
import org.scassandra.http.client.BatchExecution;
import org.scassandra.http.client.BatchStatement;

import java.util.List;

import static org.junit.Assert.assertEquals;

abstract public class BatchActivityVerificationTest extends AbstractScassandraTest {
    public BatchActivityVerificationTest(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void executeLoggedBatch() {
        cassandra().executeBatch(Lists.newArrayList(
                        new CassandraQuery("select * from blah"),
                        new CassandraQuery("select * from blah2")
                )
        );

        List<BatchExecution> batches = activityClient.retrieveBatches();

        assertEquals(1, batches.size());
        assertEquals(new BatchExecution(Lists.newArrayList(
                        new BatchStatement("select * from blah"),
                        new BatchStatement("select * from blah2")), "ONE"), batches.get(0));
    }
}
