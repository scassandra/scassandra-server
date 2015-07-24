package preparedstatements;

import cassandra.CassandraExecutor10;

public class PreparedStatementsListVariablesWithMatcher10 extends PreparedStatementsListVariablesWithMatcher {
    public PreparedStatementsListVariablesWithMatcher10() {
        super(new CassandraExecutor10());
    }
}
