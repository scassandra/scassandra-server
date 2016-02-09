package queries;

import cassandra.CassandraExecutor30;
import org.junit.Ignore;

@Ignore
public class QueryErrorPrimingTest30 extends QueryErrorPrimingTest {
    public QueryErrorPrimingTest30() {
        super(new CassandraExecutor30());
    }
}
