package preparedstatements;

import cassandra.CassandraExecutor30;

public class PreparedStatementsSetVariablesWithMatcher30 extends PreparedStatementsSetVariablesWithMatcher {
    public PreparedStatementsSetVariablesWithMatcher30() {
        super(new CassandraExecutor30(scassandra.getBinaryPort()));
    }
}
