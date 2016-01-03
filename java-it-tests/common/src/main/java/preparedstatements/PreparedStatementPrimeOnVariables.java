package preparedstatements;

import common.AbstractScassandraTest;
import common.CassandraExecutor;
import common.CassandraResult;
import org.junit.Test;
import org.scassandra.http.client.Consistency;
import org.scassandra.http.client.MultiPrimeRequest;
import org.scassandra.http.client.Result;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.scassandra.cql.PrimitiveType.*;
import static org.scassandra.http.client.MultiPrimeRequest.*;

abstract public class PreparedStatementPrimeOnVariables extends AbstractScassandraTest {

    public PreparedStatementPrimeOnVariables(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void testPrimeBasedOnMatchOnConsistency() {
        String query = "select * from person where name = ?";
        MultiPrimeRequest prime = MultiPrimeRequest.request()
                .withWhen(when()
                        .withQuery(query).withConsistency(Consistency.TWO))
                .withThen(then()
                        .withVariableTypes(TEXT)
                        .withOutcomes(
                                outcome(match().withVariableMatchers(variableMatch().withMatcher("Andrew").build()), action().withResult(Result.read_request_timeout))
                        )
                )
                .build();

        primingClient.multiPrime(prime);

        CassandraResult result = cassandra().prepareAndExecute(query, "Andrew");

        // shouldn't match due to consistency
        assertEquals(Result.success, result.status().getResult());
    }

    @Test
    public void testPrimeBasedOnMatchText() {
        String query = "select * from person where name = ?";
        MultiPrimeRequest prime = MultiPrimeRequest.request()
                .withWhen(when()
                        .withQuery(query))
                .withThen(then()
                        .withVariableTypes(TEXT)
                        .withOutcomes(
                                outcome(match().withVariableMatchers(variableMatch().withMatcher("Chris").build()), action().withResult(Result.success)),
                                outcome(match().withVariableMatchers(variableMatch().withMatcher("Andrew").build()), action().withResult(Result.read_request_timeout))
                        )
                )
                .build();

        primingClient.multiPrime(prime);

        CassandraResult chrisResult = cassandra().prepareAndExecute(query, "Chris");
        CassandraResult andrewResult = cassandra().prepareAndExecute(query, "Andrew");

        assertEquals(Result.success, chrisResult.status().getResult());
        assertEquals(Result.read_request_timeout, andrewResult.status().getResult());
    }

    @Test
    public void testPrimeBasedOnMatcherNumericTypes() {
        String query = "select * from person where i = ? and bi = ? and f = ? and doub = ? and dec = ?";
        MultiPrimeRequest prime = MultiPrimeRequest.request()
                .withWhen(when()
                        .withQuery(query))
                .withThen(then()
                        .withVariableTypes(INT, BIG_INT, FLOAT, DOUBLE, DECIMAL)
                        .withOutcomes(
                                outcome(match().withVariableMatchers(
                                        variableMatch(1),
                                        variableMatch(2L),
                                        variableMatch(3.0F),
                                        variableMatch(4.0),
                                        variableMatch(new BigDecimal("5.0"))), action().withResult(Result.unavailable)),
                                outcome(match().withVariableMatchers(
                                        variableMatch(11),
                                        variableMatch(12L),
                                        variableMatch(13.0F),
                                        variableMatch(14.0),
                                        variableMatch(new BigDecimal("15.0"))), action().withResult(Result.write_request_timeout))

                        )
                )
                .build();

        primingClient.multiPrime(prime);

        CassandraResult unavailable = cassandra().prepareAndExecute(query, 1, 2L, 3.0F, 4.0, new BigDecimal("5.0"));
        CassandraResult writeTimeout = cassandra().prepareAndExecute(query, 11, 12L, 13F, 14.0, new BigDecimal("15.0"));

        assertEquals(Result.unavailable, unavailable.status().getResult());
        assertEquals(Result.write_request_timeout, writeTimeout.status().getResult());
    }

    @Test
    public void testPrimeBasedOnMatcherUUIDs() {
        String query = "select * from person where u = ? and t = ?";
        java.util.UUID uuidOne = java.util.UUID.randomUUID();
        java.util.UUID tUuidOne = java.util.UUID.randomUUID();
        java.util.UUID uuidTwo = java.util.UUID.randomUUID();
        java.util.UUID tUuidTwo = java.util.UUID.randomUUID();

        MultiPrimeRequest prime = MultiPrimeRequest.request()
                .withWhen(when()
                        .withQuery(query))
                .withThen(then()
                        .withVariableTypes(UUID, TIMEUUID)
                        .withOutcomes(
                                outcome(match().withVariableMatchers(
                                        variableMatch(uuidOne),
                                        variableMatch(tUuidOne)),
                                        action().withResult(Result.overloaded)),
                                outcome(match().withVariableMatchers(
                                        variableMatch(uuidTwo),
                                        variableMatch(tUuidTwo)),
                                        action().withResult(Result.write_request_timeout))

                        )
                )
                .build();

        primingClient.multiPrime(prime);

        CassandraResult overloaded = cassandra().prepareAndExecute(query, uuidOne, tUuidOne);
        CassandraResult writeTimeout = cassandra().prepareAndExecute(query, uuidTwo, tUuidTwo);

        assertEquals(Result.overloaded, overloaded.status().getResult());
        assertEquals(Result.write_request_timeout, writeTimeout.status().getResult());
    }

    @Test
    public void testPrimeBasedOnMatchBoolean() {
        String query = "select * from person where clever = ?";
        MultiPrimeRequest prime = MultiPrimeRequest.request()
                .withWhen(when()
                        .withQuery(query))
                .withThen(then()
                        .withVariableTypes(BOOLEAN)
                        .withOutcomes(
                                outcome(match().withVariableMatchers(variableMatch().withMatcher(false).build()), action().withResult(Result.read_request_timeout)),
                                outcome(match().withVariableMatchers(variableMatch().withMatcher(true).build()), action().withResult(Result.write_request_timeout))
                        )
                )
                .build();

        primingClient.multiPrime(prime);

        CassandraResult falseResult = cassandra().prepareAndExecute(query, false);
        CassandraResult trueResult = cassandra().prepareAndExecute(query, true);

        assertEquals(Result.read_request_timeout, falseResult.status().getResult());
        assertEquals(Result.write_request_timeout, trueResult.status().getResult());
    }

    //todo inet
    //todo blob
    //todo timestamp
    //todo varint

    // todo returns rows
    // todo delays
    // todo error test: mismatch of types
    // todo error test: mismatch of number of variable types with outcome match
    // todo support matching on collections
}
