package preparedstatements;

import cassandra.CassandraExecutor21;

public class PreparedStatementCollectionVariables21 extends PreparedStatementCollectionVariables {

    public PreparedStatementCollectionVariables21() {
        super(new CassandraExecutor21(scassandra.getBinaryPort()));
    }
}
