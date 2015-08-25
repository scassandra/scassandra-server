package queries;

import cassandra.CassandraExecutor21;

public class QueryBuilderTest21 extends QueryBuilderTest {
    public QueryBuilderTest21() {
        super(new CassandraExecutor21());
    }
}
