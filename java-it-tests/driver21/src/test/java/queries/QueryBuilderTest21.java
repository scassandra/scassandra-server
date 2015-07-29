package queries;

import cassandra.CassandraExecutor21;

public class QueryBuilderTest21 extends QueryBatchStatementBatchExecutionBuilderTest {
    public QueryBuilderTest21() {
        super(new CassandraExecutor21());
    }
}
