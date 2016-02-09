package preparedstatements;

import cassandra.CassandraExecutor30;

public class PreparedStatementPrimeOnVariablesTest30 extends PreparedStatementPrimeOnVariables {
    public PreparedStatementPrimeOnVariablesTest30() {
        super(new CassandraExecutor30());
    }
}
