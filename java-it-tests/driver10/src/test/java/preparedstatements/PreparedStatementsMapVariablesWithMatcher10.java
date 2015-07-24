package preparedstatements;

import cassandra.CassandraExecutor10;

public class PreparedStatementsMapVariablesWithMatcher10 extends PreparedStatementsMapVariablesWithMatcher {
    public PreparedStatementsMapVariablesWithMatcher10() {
        super(new CassandraExecutor10());
    }
}
