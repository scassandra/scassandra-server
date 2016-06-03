package preparedstatements;

import cassandra.CassandraExecutor21;

public class PreparedStatementPrimitiveVariableTypes21 extends PreparedStatementPrimitiveVariableTypes {

    public PreparedStatementPrimitiveVariableTypes21() {
        super(new CassandraExecutor21(scassandra.getBinaryPort()));
    }
}
