package preparedstatements;

import cassandra.CassandraExecutor21;

public class PreparedStatementsSetVariablesWithMatcher21 extends PreparedStatementsSetVariablesWithMatcher {
    public PreparedStatementsSetVariablesWithMatcher21() {
        super(new CassandraExecutor21(scassandra.getBinaryPort()));
    }
}
