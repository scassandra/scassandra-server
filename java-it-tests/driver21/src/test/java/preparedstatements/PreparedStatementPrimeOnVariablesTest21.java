package preparedstatements;

import cassandra.CassandraExecutor21;

public class PreparedStatementPrimeOnVariablesTest21 extends PreparedStatementPrimeOnVariables {
    public PreparedStatementPrimeOnVariablesTest21() {
        super(new CassandraExecutor21(scassandra.getBinaryPort()));
    }
}
