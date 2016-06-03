package preparedstatements;

import cassandra.CassandraExecutor20;

public class PreparedStatementsSetVariablesWithMatcher20 extends PreparedStatementsSetVariablesWithMatcher {
    public PreparedStatementsSetVariablesWithMatcher20() {
        super(new CassandraExecutor20(scassandra.getBinaryPort()));
    }
}
