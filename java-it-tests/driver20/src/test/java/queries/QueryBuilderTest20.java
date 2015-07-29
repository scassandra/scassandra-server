package queries;

import cassandra.CassandraExecutor20;

public class QueryBuilderTest20 extends QueryBatchStatementBatchExecutionBuilderTest {
    public QueryBuilderTest20() {
        super(new CassandraExecutor20());
    }
}
