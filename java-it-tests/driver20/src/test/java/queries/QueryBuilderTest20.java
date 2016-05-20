package queries;

import cassandra.CassandraExecutor20;

public class QueryBuilderTest20 extends QueryBuilderTest {
    public QueryBuilderTest20() {
        super(new CassandraExecutor20(scassandra.getBinaryPort()));
    }
}
