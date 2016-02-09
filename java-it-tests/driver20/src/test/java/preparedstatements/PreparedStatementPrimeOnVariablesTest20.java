package preparedstatements;

import cassandra.CassandraExecutor20;

public class PreparedStatementPrimeOnVariablesTest20 extends PreparedStatementPrimeOnVariables {
    public PreparedStatementPrimeOnVariablesTest20() {
        super(new CassandraExecutor20());
    }
}
