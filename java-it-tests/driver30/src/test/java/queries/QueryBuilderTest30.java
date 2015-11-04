package queries;

import cassandra.CassandraExecutor30;

public class QueryBuilderTest30 extends QueryBuilderTest {
    public QueryBuilderTest30() {
        super(new CassandraExecutor30());
    }
}
