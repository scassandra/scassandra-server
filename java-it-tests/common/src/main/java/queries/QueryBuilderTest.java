package queries;

import com.google.common.base.Optional;
import common.AbstractScassandraTest;
import common.CassandraExecutor;
import common.CassandraRow;
import common.WhereEquals;
import org.junit.Ignore;
import org.junit.Test;
import org.scassandra.http.client.Query;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryBuilderTest extends AbstractScassandraTest {
    public QueryBuilderTest(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Ignore
    @Test
    public void testBasicQueryVerification() {
        Query expectedSelect = Query.builder().withQuery("SELECT * FROM table;").withConsistency("ONE").build();

        List<CassandraRow> cassandraResult = cassandra().executeSelectWithBuilder("table").rows();

        assertEquals(0, cassandraResult.size());
        List<Query> recordedQueries = activityClient.retrieveQueries();
        assertTrue("Actual queries: " + recordedQueries, recordedQueries.contains(expectedSelect));
    }

    @Ignore("TODO")
    @Test
    public void testQueryWithWhereClause() {
        Query expectedSelect = Query.builder().withQuery("SELECT * FROM table WHERE name='chris';").withConsistency("ONE").build();

        WhereEquals clause = new WhereEquals("name", "chris");
        List<CassandraRow> cassandraResult = cassandra().executeSelectWithBuilder("table", Optional.of(clause)).rows();

        assertEquals(0, cassandraResult.size());
        List<Query> recordedQueries = activityClient.retrieveQueries();
        assertTrue("Actual queries: " + recordedQueries, recordedQueries.contains(expectedSelect));
    }
}
