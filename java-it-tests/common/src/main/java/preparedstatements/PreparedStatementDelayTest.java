package preparedstatements;

import com.google.common.collect.ImmutableMap;
import common.AbstractScassandraTest;
import common.CassandraExecutor;
import common.CassandraResult;
import common.CassandraRow;
import org.junit.Test;
import org.scassandra.http.client.PrimingRequest;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.scassandra.http.client.PrimingRequest.Result.success;
import static org.scassandra.http.client.PrimingRequest.preparedStatementBuilder;

abstract public class PreparedStatementDelayTest extends AbstractScassandraTest {

    public PreparedStatementDelayTest(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void preparedStatementDelay() {
        //given
        String query = "select * from people where name = ?";
        long primedDelay = 500;
        primingClient.prime(preparedStatementBuilder()
                .withFixedDelay(primedDelay)
                .withQuery(query)
                .build());

        //when
        long before = System.currentTimeMillis();
        CassandraResult results = cassandra().prepareAndExecute(query, "Chris");
        long duration = System.currentTimeMillis() - before;

        //then
        assertEquals(success, results.status().getResult());
        assertTrue("Expected delay of " + primedDelay + " got " + duration, duration > primedDelay && duration < (primedDelay + 200));
    }

    @Test
    public void preparedStatementPatternDelay() {
        //given
        long primedDelay = 500;
        primingClient.primePreparedStatement(preparedStatementBuilder()
                .withFixedDelay(primedDelay)
                .withQueryPattern(".*")
                .build());

        //when
        long before = System.currentTimeMillis();
        CassandraResult results = cassandra().prepareAndExecute("select * from people where name = ?", "Chris");
        long duration = System.currentTimeMillis() - before;

        //then
        assertEquals(success, results.status().getResult());
        assertTrue("Expected delay of " + primedDelay + " got " + duration, duration > primedDelay && duration < (primedDelay + 200));
    }
}
