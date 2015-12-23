package queries;

import common.AbstractScassandraTest;
import common.CassandraExecutor;
import common.CassandraResult;
import org.junit.Ignore;
import org.junit.Test;
import org.scassandra.http.client.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.scassandra.http.client.Result.*;
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
        assertEquals(BATCH_LOG, ((CassandraResult.WriteTimeoutStatus)status).getWriteTypePrime());
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

    @Test
    public void testPrimingServerError() {
        String errorMessage = "Arbitrary Server Error";
        ErrorMessageConfig config = new ErrorMessageConfig(errorMessage);
        assertErrorMessageStatus(server_error, config, "Host replied with server error: " + errorMessage);
    }

    @Test
    public void testPrimingProtocolError() {
        String errorMessage = "Arbitrary Protocol Error";
        ErrorMessageConfig config = new ErrorMessageConfig(errorMessage);
        assertErrorMessageStatus(protocol_error, config, "An unexpected protocol error occurred on host localhost/127.0.0.1:8042. This is a bug in this library, please report: " + errorMessage);
    }

    @Test
    public void testBadCredentials() {
        String errorMessage = "Bad Credentials";
        ErrorMessageConfig config = new ErrorMessageConfig(errorMessage);
        assertErrorMessageStatus(bad_credentials, config, "Authentication error on host localhost/127.0.0.1:8042: " + errorMessage);
    }

    @Test
    public void testOverloadedError() {
        ErrorMessageConfig config = new ErrorMessageConfig("");
        assertErrorMessageStatus(overloaded, config, "Host overloaded");
    }

    @Test
    public void testIsBootstrapping() {
        String errorMessage = "Lay off, i'm bootstrapping.";
        ErrorMessageConfig config = new ErrorMessageConfig(errorMessage);
        assertErrorMessageStatus(is_bootstrapping, config, "Host is bootstrapping");
    }

    @Test
    public void testTruncateError() {
        String errorMessage = "Truncate Failure";
        ErrorMessageConfig config = new ErrorMessageConfig(errorMessage);
        assertErrorMessageStatus(truncate_error, config, errorMessage);
    }

    @Test
    public void testSyntaxError() {
        String errorMessage = "Bad Syntax";
        ErrorMessageConfig config = new ErrorMessageConfig(errorMessage);
        assertErrorMessageStatus(syntax_error, config, errorMessage);
    }

    @Test
    public void testUnauthorized() {
        String errorMessage = "Not allowed to do that";
        ErrorMessageConfig config = new ErrorMessageConfig(errorMessage);
        assertErrorMessageStatus(unauthorized, config, errorMessage);
    }

    @Test
    public void testInvalid() {
        String errorMessage = "Invalid query";
        ErrorMessageConfig config = new ErrorMessageConfig(errorMessage);
        assertErrorMessageStatus(invalid, config, errorMessage);
    }

    @Test
    @Ignore
    // Ignore as this is a legitimate bug in the driver (returns InvalidQueryException instead of
    // InvalidConfigurationInQueryException.
    public void testConfigError() {
        String errorMessage = "Configuration Error 12345";
        ErrorMessageConfig config = new ErrorMessageConfig(errorMessage);
        assertErrorMessageStatus(config_error, config, errorMessage);
    }

    @Test
    public void testAlreadyExists() {
        String keyspace = "hello";
        String table = "world";
        AlreadyExistsConfig config = new AlreadyExistsConfig(keyspace, table);
        assertErrorMessageStatus(already_exists, config, "Table hello.world already exists");

        // keyspace only
        assertErrorMessageStatus(already_exists, new AlreadyExistsConfig(keyspace), "Keyspace hello already exists");
    }

    @Test
    public void testUnprepared() {
        // This should never happen as result of a simple statement,
        // but interesting to see how driver behaves nonetheless.
        String prepareId = "0x86753090";
        UnpreparedConfig config = new UnpreparedConfig(prepareId);
        assertErrorMessageStatus(unprepared, config, "Tried to execute unknown prepared query " + prepareId);
    }

    @Test
    public void testClosedOnRequest() {
        ClosedConnectionConfig config = new ClosedConnectionConfig(ClosedConnectionConfig.CloseType.RESET);
        assertErrorMessageStatus(closed_connection, config,
            "All host(s) tried for query failed (tried: localhost/127.0.0.1:8042 (com.datastax.driver.core.TransportException: [localhost/127.0.0.1:8042] Connection has been closed))",
            server_error);
    }

    private CassandraResult assertErrorMessageStatus(Result result, Config config, String expectedMsg) {
        return assertErrorMessageStatus(result, config, expectedMsg, result);
    }

    private CassandraResult assertErrorMessageStatus(Result result, Config config, String expectedMsg, Result expectedResult) {
        String query = "select * from people";
        String consistency = "LOCAL_ONE";
        PrimingRequest prime = PrimingRequest.queryBuilder()
            .withQuery(query)
            .withResult(result)
            .withConfig(config)
            .build();
        primingClient.prime(prime);

        CassandraResult cassandraResult = cassandra().executeSimpleStatement(query, consistency);

        CassandraResult.ResponseStatus status = cassandraResult.status();
        assertEquals(expectedResult, status.getResult());
        assertEquals(expectedMsg, ((CassandraResult.ErrorMessageStatus) status).getMessage());
        return cassandraResult;
    }
}
