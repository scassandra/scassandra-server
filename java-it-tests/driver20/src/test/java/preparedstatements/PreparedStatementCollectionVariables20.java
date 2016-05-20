package preparedstatements;

import cassandra.CassandraExecutor20;

public class PreparedStatementCollectionVariables20 extends PreparedStatementCollectionVariables {

    public PreparedStatementCollectionVariables20() {
        super(new CassandraExecutor20(scassandra.getBinaryPort()));
    }
}
