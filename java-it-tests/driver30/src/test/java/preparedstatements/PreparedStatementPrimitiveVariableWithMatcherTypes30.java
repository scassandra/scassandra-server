package preparedstatements;

import cassandra.CassandraExecutor30;

public class PreparedStatementPrimitiveVariableWithMatcherTypes30 extends PreparedStatementPrimitiveVariableWithMatcherTypes {
    public PreparedStatementPrimitiveVariableWithMatcherTypes30() {
        super(new CassandraExecutor30(scassandra.getBinaryPort()));
    }
}
