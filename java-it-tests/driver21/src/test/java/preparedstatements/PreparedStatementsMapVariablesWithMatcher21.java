package preparedstatements;

import cassandra.CassandraExecutor21;

public class PreparedStatementsMapVariablesWithMatcher21 extends PreparedStatementsMapVariablesWithMatcher {
    public PreparedStatementsMapVariablesWithMatcher21() {
        super(new CassandraExecutor21());
    }
}
