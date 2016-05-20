package queries;

import cassandra.CassandraExecutor20;

public class QueryErrorPrimingTest20 extends QueryErrorPrimingTest {

    public QueryErrorPrimingTest20() {
        super(new CassandraExecutor20(scassandra.getBinaryPort()));
    }
}
