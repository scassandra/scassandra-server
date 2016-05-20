package preparedstatements;

import cassandra.CassandraExecutor30;

public class PreparedStatementCollectionVariables30 extends PreparedStatementCollectionVariables {

    public PreparedStatementCollectionVariables30() {
        super(new CassandraExecutor30(scassandra.getBinaryPort()));
    }
}
