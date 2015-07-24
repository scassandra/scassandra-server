package preparedstatements;

import cassandra.CassandraExecutor10;

public class PreparedStatementsSetVariablesWithMatcher10 extends PreparedStatementsSetVariablesWithMatcher {
    public PreparedStatementsSetVariablesWithMatcher10() {
        super(new CassandraExecutor10());
    }
}
