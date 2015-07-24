package queries;

import common.AbstractScassandraTest;
import common.CassandraExecutor;
import common.CassandraResult;
import org.junit.Test;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.ReadTimeoutConfig;
import org.scassandra.http.client.UnavailableConfig;
import org.scassandra.http.client.WriteTimeoutConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.scassandra.http.client.PrimingRequest.Result.*;
import static org.scassandra.http.client.WriteTypePrime.BATCH_LOG;

abstract public class QueryErrorPrimingTest extends AbstractScassandraTest {

    public QueryErrorPrimingTest(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void testPrimingReadTimeout() {
        String query = "select * from people";
        ReadTimeoutConfig readTimeoutConfig = new ReadTimeoutConfig(2,3,false);
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withResult(read_request_timeout)
                .withConfig(readTimeoutConfig)
                .build();
        primingClient.prime(prime);
        String consistency = "LOCAL_QUORUM";

        CassandraResult cassandraResult = cassandra().executeSimpleStatement(query, consistency);

        CassandraResult.ResponseStatus status = cassandraResult.status();
        assertEquals(read_request_timeout, status.getResult());
        assertEquals(consistency, ((CassandraResult.ReadTimeoutStatus) status).getConsistency());
        assertEquals(2, ((CassandraResult.ReadTimeoutStatus) status).getReceivedAcknowledgements());
        assertEquals(3, ((CassandraResult.ReadTimeoutStatus) status).getRequiredAcknowledgements());
        assertFalse(((CassandraResult.ReadTimeoutStatus) status).WasDataRetrieved());
    }

    @Test
    public void testPrimingWriteTimeout() {
        String query = "select * from people";
        String consistency = "ALL";
        WriteTimeoutConfig writeTimeoutConfig = new WriteTimeoutConfig(BATCH_LOG, 2, 3);
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withResult(write_request_timeout)
                .withConfig(writeTimeoutConfig)
                .build();
        primingClient.prime(prime);

        CassandraResult cassandraResult = cassandra().executeSimpleStatement(query, consistency);

        CassandraResult.ResponseStatus status = cassandraResult.status();
        assertEquals(write_request_timeout, status.getResult());
        assertEquals(consistency, ((CassandraResult.WriteTimeoutStatus) status).getConsistency());
        assertEquals(2, ((CassandraResult.WriteTimeoutStatus) status).getReceivedAcknowledgements());
        assertEquals(3, ((CassandraResult.WriteTimeoutStatus) status).getRequiredAcknowledgements());
        assertEquals(BATCH_LOG, ((CassandraResult.WriteTimeoutStatus) status).getWriteTypePrime());
    }

    @Test
    public void testPrimingUnavailable() {
        String query = "select * from people";
        String consistency = "LOCAL_ONE";
        UnavailableConfig unavailableConfig = new UnavailableConfig(4,3);
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withResult(unavailable)
                .withConfig(unavailableConfig)
                .build();
        primingClient.prime(prime);

        CassandraResult cassandraResult = cassandra().executeSimpleStatement(query, consistency);

        CassandraResult.ResponseStatus status = cassandraResult.status();
        assertEquals(unavailable, status.getResult());
        assertEquals(consistency, ((CassandraResult.UnavailableStatus) status).getConsistency());
        assertEquals(4, ((CassandraResult.UnavailableStatus) status).getRequiredAcknowledgements());
        assertEquals(3, ((CassandraResult.UnavailableStatus) status).getAlive());
    }
}
