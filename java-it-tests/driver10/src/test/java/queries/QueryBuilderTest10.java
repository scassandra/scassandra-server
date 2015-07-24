package queries;

import cassandra.CassandraExecutor10;

public class QueryBuilderTest10 extends QueryBuilderTest {
    public QueryBuilderTest10() {
        super(new CassandraExecutor10());
    }
}
