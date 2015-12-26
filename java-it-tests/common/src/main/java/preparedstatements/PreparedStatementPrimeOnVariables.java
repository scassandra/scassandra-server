package preparedstatements;

import common.AbstractScassandraTest;
import common.CassandraExecutor;
import common.CassandraResult;
import org.junit.Test;
import org.scassandra.http.client.MultiPrimeRequest;
import org.scassandra.http.client.Result;

import static org.junit.Assert.assertEquals;
import static org.scassandra.cql.PrimitiveType.TEXT;
import static org.scassandra.http.client.MultiPrimeRequest.*;

abstract public class PreparedStatementPrimeOnVariables extends AbstractScassandraTest {

    public PreparedStatementPrimeOnVariables(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void testPrimeBasedOnMatchedString() {
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
}
