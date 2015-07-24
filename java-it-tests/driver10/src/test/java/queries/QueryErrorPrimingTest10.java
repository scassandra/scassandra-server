package queries;

import cassandra.CassandraExecutor10;

public class QueryErrorPrimingTest10 extends QueryErrorPrimingTest {

    public QueryErrorPrimingTest10() {
        super(new CassandraExecutor10());
    }
}
