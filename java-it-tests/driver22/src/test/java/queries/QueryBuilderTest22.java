package queries;

import cassandra.CassandraExecutor22;

public class QueryBuilderTest22 extends QueryBatchStatementBatchExecutionBuilderTest {
    public QueryBuilderTest22() {
        super(new CassandraExecutor22());
    }
}
