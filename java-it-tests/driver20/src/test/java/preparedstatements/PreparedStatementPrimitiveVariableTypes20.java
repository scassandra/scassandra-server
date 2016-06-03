package preparedstatements;

import cassandra.CassandraExecutor20;

public class PreparedStatementPrimitiveVariableTypes20 extends PreparedStatementPrimitiveVariableTypes {

    public PreparedStatementPrimitiveVariableTypes20() {
        super(new CassandraExecutor20(scassandra.getBinaryPort()));
    }
}
