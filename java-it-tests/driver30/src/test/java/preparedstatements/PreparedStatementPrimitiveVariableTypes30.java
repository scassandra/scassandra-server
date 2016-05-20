package preparedstatements;

import cassandra.CassandraExecutor30;

public class PreparedStatementPrimitiveVariableTypes30 extends PreparedStatementPrimitiveVariableTypes {

    public PreparedStatementPrimitiveVariableTypes30() {
        super(new CassandraExecutor30(scassandra.getBinaryPort()));
    }
}
