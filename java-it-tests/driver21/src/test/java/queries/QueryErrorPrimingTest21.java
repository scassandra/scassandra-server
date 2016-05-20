package queries;

import cassandra.CassandraExecutor21;

public class QueryErrorPrimingTest21 extends QueryErrorPrimingTest {

    public QueryErrorPrimingTest21() {
        super(new CassandraExecutor21(scassandra.getBinaryPort()));
    }
}
